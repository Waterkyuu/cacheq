# 统一查询筛选

[English](query-filters.en-US.md)

`QueryFilter` 定义可复用的查询筛选，供 `Invalidate`、`InvalidateAndWait`、`Cancel` 和 `Remove` 使用。它通过精确键或业务条件选择一组查询，不创建额外缓存，也不管理订阅生命周期。

## 替代逐键编排

取消操作仅接受单键时，业务需要维护候选键，并在每种操作外重复匹配和遍历。失效原本已经支持条件函数，因此重复主要来自枚举和操作编排，而不是条件函数本身必须定义多次：

```go
matches := func(key any) bool {
    resource, ok := key.(ResourceKey)
    return ok && resource.TenantID == tenantID
}
if err := client.Invalidate(matches); err != nil {
    return err
}
for _, key := range knownKeys {
    if matches(key) {
        if err := client.Cancel(key); err != nil {
            return err
        }
    }
}
```

该片段应放在返回 `error` 的函数中；`ResourceKey` 见下方完整程序，`knownKeys` 是业务维护的键列表。统一筛选后，同一份筛选条件可以直接传入操作：

```go
filter := cacheq.QueryFilter{Predicate: matches}
if err := client.Cancel(filter); err != nil {
    return err
}
```

由 Client 枚举它管理的查询键，业务不再为此操作维护 `knownKeys`。四个操作也都支持直接传入条件函数。需要命名、复用筛选或同时按键和条件缩小范围时，使用 `QueryFilter`。

## 完整租户示例

将程序保存为已导入 cacheq 的 Go 模块中的 `main.go`，运行 `go run .`。它先预加载类型化查询并订阅，修改后端数据后等待租户 42 的详情和列表完成刷新。取消保留缓存数据，删除只清理选中的结果。

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// ResourceKey identifies a result within a tenant.
type ResourceKey struct {
	// TenantID separates otherwise identical resource queries.
	TenantID int
	// Resource identifies the kind of result.
	Resource string
}

// main reuses a tenant filter to refresh, cancel, and remove related queries.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()
	ctx := context.Background()
	var backend atomic.Value
	backend.Store("Alice")
	detailDefinition := cacheq.QueryDefinition[string]{
		Key: ResourceKey{TenantID: 42, Resource: "user"},
		Fetcher: func(context.Context) (string, error) {
			return backend.Load().(string), nil
		},
	}
	listDefinition := cacheq.QueryDefinition[[]string]{
		Key: ResourceKey{TenantID: 42, Resource: "users"},
		Fetcher: func(context.Context) ([]string, error) {
			return []string{backend.Load().(string)}, nil
		},
	}
	if err := detailDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	if err := listDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	detail := detailDefinition.Query(client)
	list := listDefinition.Query(client)
	defer detail.Close()
	defer list.Close()
	otherKey := ResourceKey{TenantID: 99, Resource: "user"}
	if err := cacheq.Set(client, otherKey, "Carol"); err != nil {
		panic(err)
	}
	filter := cacheq.QueryFilter{Predicate: func(key any) bool {
		resource, ok := key.(ResourceKey)
		return ok && resource.TenantID == 42
	}}
	backend.Store("Bob")
	results, err := client.InvalidateAndWait(ctx, filter)
	if err != nil {
		panic(err)
	}
	fmt.Println("refreshed:", len(results))
	fmt.Println("detail:", detail.Snapshot().Data)
	fmt.Println("list:", list.Snapshot().Data)
	if err := client.Cancel(filter); err != nil {
		panic(err)
	}
	fmt.Println("after cancel:", detail.Snapshot().Data)
	if err := client.Remove(filter); err != nil {
		panic(err)
	}
	fmt.Println("removed:", !detail.Snapshot().HasData && !list.Snapshot().HasData)
	fmt.Println("other tenant:", cacheq.Get[string](client, otherKey).Data)
}
```

预期输出：

```text
refreshed: 2
detail: Bob
list: [Bob]
after cancel: Bob
removed: true
other tenant: Carol
```

租户隔离来自查询键中的 `ResourceKey.TenantID`，不由筛选器保证。影响结果的每个查询参数和身份范围仍必须包含在键中。

## 筛选契约

| 字段 | 含义 |
| --- | --- |
| `Keys []any` | 将候选范围限制为精确的可比较键；nil 表示不限制键 |
| `Predicate func(any) bool` | 进一步限制候选范围；nil 表示接受全部候选键 |

- `QueryFilter{}` 选择全部已有缓存、加载中或已订阅的键。
- 非 nil 的空 `Keys` 切片选择零个查询；缺失的键被跳过，不会创建。
- 键和条件函数取交集。在完整校验所有传入键后，每个已有候选键只调用一次条件函数。
- 指定键时按首次出现的顺序处理；未指定 `Keys` 时顺序没有保证。
- 键按 Go 精确相等规则及动态类型匹配，不隐式解释字符串前缀、数组前缀或租户范围。
- 条件函数在 Client 锁外执行，可以调用 Client API。它基于初始键快照；新增键不参与，应用操作前已删除的键被跳过。筛选和应用操作不构成与并发业务写入之间的事务。
- 筛选中的切片和条件函数状态由调用者管理，并发修改需要同步。条件函数的副作用属于调用者，不会回滚。

在一组明确的键中进一步筛选租户：

```go
filter := cacheq.QueryFilter{
    Keys: []any{detailDefinition.Key, listDefinition.Key},
    Predicate: func(key any) bool {
        resource, ok := key.(ResourceKey)
        return ok && resource.TenantID == 42
    },
}
if err := client.Remove(filter); err != nil {
    return err
}
```

该片段应放在返回 `error` 的函数中，使用完整示例中的定义。

## 支持的操作

| 操作 | 对选中查询的影响 |
| --- | --- |
| `client.Invalidate(filter, options...)` | 保留数据并标记过期，按配置安排已启用订阅的刷新 |
| `client.InvalidateAndWait(ctx, filter, options...)` | 使用相同筛选，等待失效后的刷新结果 |
| `client.Cancel(filter)` | 取消共享加载，保留已完成的数据和订阅 |
| `client.Remove(filter)` | 取消共享加载并丢弃已完成状态，订阅仍可继续使用 |

四个操作都接受单键、`[]any`、`func(any) bool` 或 `QueryFilter` 值。只有 `[]any` 表示直接批量键，其他切片类型非法。直接按键失效可以为缺失键创建过期元数据；筛选和条件函数只操作已有键。直接传 nil 目标或 nil 条件函数非法，而筛选中的 nil 条件字段表示不加限制。

非法键在操作自身产生任何变更、调用条件函数之前返回 `ErrInvalidKey`。关闭的 Client 返回 `ErrClosed`，不执行条件函数。条件函数执行期间关闭 Client，会在应用筛选前返回 `ErrClosed`。`InvalidateAndWait` 在筛选前检查已取消的 context；取消等待不会取消 Client 管理的刷新。显式调用 `Cancel(filter)` 或 `Remove(filter)` 才会取消选中的共享加载，并影响这些加载的全部等待者。

筛选只选择目标，不修改 `QueryOptions.Disable`，不启动未订阅的加载函数，也不增加轮询。取消或删除查询不会关闭查询对象，也不阻止后续显式读取和刷新。本次 API 尚未包含标签。

## 相关文档

- [查询定义与统一配置](query-options.zh-CN.md)：组合键、加载函数和消费者配置。
- [失效与刷新](invalidation.zh-CN.md)：刷新策略、等待结果和有界补刷。
- [缓存与生命周期](cache.zh-CN.md)：取消、删除及 Client 所有权。

公开行为由[单元测试](../query_filter_test.go)和 [HTTP 端到端测试](../e2e/query_filter_test.go)验证。
