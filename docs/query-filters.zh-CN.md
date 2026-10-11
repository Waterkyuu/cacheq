# 统一查询筛选

[English](query-filters.en-US.md) · [失效与刷新](invalidation.zh-CN.md) · [缓存与生命周期](cache.zh-CN.md)

`QueryFilter` 用于选择同一个 Client 内的一组查询。`Invalidate`、`InvalidateAndWait`、`Cancel` 和 `Remove` 都接受同一份筛选配置，分别用于使数据失效、等待刷新、取消加载和删除缓存。

适用场景包括修改资源后使详情与列表失效、切换租户时取消旧租户的加载，以及退出登录时删除相关缓存。筛选配置只描述目标；具体操作的加载、取消和删除行为由调用的方法决定。

## 快速开始

以下完整程序选择租户 42 的详情与列表，先标记为过期，再删除缓存。租户 99 的数据不受影响。`RefetchNone` 表示本次失效不启动刷新。

在已添加 cacheq 依赖的 Go 模块中，将程序保存为 `main.go`，运行 `go run .`。示例不需要 HTTP 服务或数据库。

```go
package main

import (
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// ResourceKey identifies a resource within a tenant.
type ResourceKey struct {
	// TenantID separates data belonging to different tenants.
	TenantID int
	// Resource identifies the kind of query result.
	Resource string
}

// main invalidates and removes tenant 42's cached results without affecting tenant 99.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()

	detailKey := ResourceKey{TenantID: 42, Resource: "profile"}
	listKey := ResourceKey{TenantID: 42, Resource: "users"}
	otherKey := ResourceKey{TenantID: 99, Resource: "profile"}
	if err := cacheq.Set(client, detailKey, "Alice"); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, listKey, []string{"Alice"}); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, otherKey, "Carol"); err != nil {
		panic(err)
	}

	filter := cacheq.QueryFilter{
		Predicate: func(key any) bool {
			resource, ok := key.(ResourceKey)
			return ok && resource.TenantID == 42
		},
	}
	if err := client.Invalidate(filter, cacheq.InvalidateOptions{
		Refetch: cacheq.RefetchNone,
	}); err != nil {
		panic(err)
	}
	fmt.Println("detail stale:", cacheq.Get[string](client, detailKey).Stale)
	fmt.Println("list stale:", cacheq.Get[[]string](client, listKey).Stale)
	fmt.Println("other tenant stale:", cacheq.Get[string](client, otherKey).Stale)

	if err := client.Remove(filter); err != nil {
		panic(err)
	}
	fmt.Println("detail retained:", cacheq.Get[string](client, detailKey).HasData)
	fmt.Println("list retained:", cacheq.Get[[]string](client, listKey).HasData)
	fmt.Println("other tenant data:", cacheq.Get[string](client, otherKey).Data)
}
```

预期输出：

```text
detail stale: true
list stale: true
other tenant stale: false
detail retained: false
list retained: false
other tenant data: Carol
```

`Predicate` 接收查询键，返回 true 表示选中该键。这里先断言为业务定义的 `ResourceKey`，再按 `TenantID` 匹配；其他类型的键不会被选中。不同租户的数据通过查询键中的 `TenantID` 区分，筛选条件不替代键中的租户范围。

## `target`：指定操作目标

四个方法的 `target any` 参数支持以下形式：

| 参数形式 | 示例 | 选择规则 |
| --- | --- | --- |
| 单个查询键 | `"users"` | 精确指定该键 |
| `[]any` | `[]any{"users", "user:42"}` | 指定多个键，按首次出现的顺序处理并去重 |
| `func(any) bool` | `func(key any) bool { return key == "users" }` | 从 Client 已有查询中选出返回 true 的键 |
| `QueryFilter` 值 | `cacheq.QueryFilter{Keys: []any{"users"}}` | 组合键列表和条件函数，供多个操作复用 |

查询键必须满足 cacheq 的可比较键要求。键按 Go 精确相等规则匹配，动态类型必须一致；字符串和数组不进行前缀匹配。只有 `[]any` 表示批量参数，`[]string` 等其他切片类型不能直接作为批量参数。

“已有查询”是指 Client 仍持有缓存状态、活动加载或订阅的键，包括暂时没有数据的已订阅查询。仅构造 `QueryDefinition` 不会向 Client 注册查询。

筛选值和条件函数只选中已有查询，缺失的键被跳过。直接传单键或 `[]any` 时按指定键执行；其中 `Invalidate` 可以为缺失的键建立过期状态。空或 nil 的 `[]any` 批量参数均不执行任何操作。

## `QueryFilter`：组合筛选条件

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `Keys` | `[]any` | nil | 限制候选键；nil 不限制，非 nil 的空切片不选择任何键 |
| `Tags` | `[]string` | nil | 要求命中任一已登记标签；nil 不限制，非 nil 的空切片不选择任何键 |
| `Predicate` | `func(key any) bool` | nil | 对候选键进一步筛选；nil 表示接受所有候选键 |

已设置的字段取交集：查询键必须满足 `Keys`、命中 `Tags` 中任一标签，并且 `Predicate` 返回 true，才会被选中。`Keys` 中的重复键只处理一次；完整校验键列表后，满足键和标签条件的候选键最多调用一次条件函数。通过 `QueryDefinition.Tags` 声明标签，登记与生命周期规则见[查询标签](query-tags.zh-CN.md)。

以下局部片段使用快速开始中的 `ResourceKey` 类型，可放在业务函数内：

```go
filter := cacheq.QueryFilter{
	Keys: []any{
		ResourceKey{TenantID: 42, Resource: "profile"},
		ResourceKey{TenantID: 99, Resource: "profile"},
	},
	Predicate: func(key any) bool {
		resource, ok := key.(ResourceKey)
		return ok && resource.TenantID == 42
	},
}
```

该筛选只选中租户 42 的 `profile` 查询，前提是该查询已经由 Client 持有。

| 筛选配置 | 选中范围 |
| --- | --- |
| `cacheq.QueryFilter{}` | 全部已有查询 |
| `cacheq.QueryFilter{Keys: nil}` | 全部已有查询 |
| `cacheq.QueryFilter{Keys: []any{}}` | 零个查询 |
| `cacheq.QueryFilter{Keys: []any{"users"}}` | 已有的 `"users"` 查询 |
| `cacheq.QueryFilter{Predicate: func(key any) bool { return key == "users" }}` | 已有的 `"users"` 查询 |

未指定键列表时，处理顺序没有保证。筛选值保存的是条件，不是上次匹配的结果；每次操作都会重新选择查询。

## 操作行为与返回值

| 方法 | 返回值 | 对选中查询的影响 |
| --- | --- | --- |
| `client.Invalidate(target, options...)` | `error` | 保留数据并标记为过期，按配置安排刷新；不等待刷新结束 |
| `client.InvalidateAndWait(ctx, target, options...)` | `([]RefreshResult, error)` | 标记为过期，并等待本次失效所关联的刷新结果 |
| `client.Cancel(target)` | `error` | 取消正在进行的共享加载，保留已完成的数据和订阅 |
| `client.Remove(target)` | `error` | 取消正在进行的共享加载，清除缓存数据、错误和对应清理定时器，保留订阅 |

`Invalidate` 和 `InvalidateAndWait` 的刷新条件与直接按键调用相同：只有存在已启用订阅的查询才会自动刷新。`Invalidate` 默认不为正在加载的查询追加刷新；可通过 `RefetchAfterLoad` 开启有界补刷。`InvalidateAndWait` 自动开启该配置。`RefetchNone` 禁止启动刷新，也不会取消已有请求。完整配置见[失效与刷新](invalidation.zh-CN.md)。

`Cancel` 和 `Remove` 会影响选中加载的所有等待者，不只影响调用它们的业务函数。二者发出取消并更新查询状态，但不等待加载函数退出；加载函数需要响应 context。

取消或删除后不会自动重新加载，后续仍可调用 `Fetch`、`Refetch`，或将查询从禁用状态切换为启用状态来加载数据。关闭订阅需要调用查询对象的 `Close()`。`Remove` 后仍存在订阅时，该键的结果类型约束继续保留，详见[缓存与生命周期](cache.zh-CN.md)。

## 等待刷新

以下完整程序先预加载数据，再创建已启用的订阅。修改模拟后端数据后，`InvalidateAndWait` 等待查询刷新完成，随后读取到 Bob。

将程序保存为 `main.go`，运行 `go run .`。加载函数可能在后台执行，因此示例通过 `atomic.Value` 同步后端值。

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main waits for an enabled query to reload a changed backend value.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()
	ctx := context.Background()

	var backend atomic.Value
	backend.Store("Alice")
	load := func(context.Context) (string, error) {
		return backend.Load().(string), nil
	}
	if err := cacheq.Prefetch(ctx, client, "profile", load); err != nil {
		panic(err)
	}
	query := cacheq.Query(client, "profile", load)
	defer query.Close()

	backend.Store("Bob")
	filter := cacheq.QueryFilter{Keys: []any{"profile"}}
	results, err := client.InvalidateAndWait(ctx, filter)
	if err != nil {
		panic(err)
	}
	fmt.Println("refreshed:", results[0].Refreshed)
	fmt.Println("data:", query.Snapshot().Data)
}
```

预期输出：

```text
refreshed: true
data: Bob
```

每个选中键对应一个 `RefreshResult`：

| 字段 | 含义 |
| --- | --- |
| `Key` | 选中的查询键，保留其动态类型 |
| `Refreshed` | 本次失效后的加载成功完成，且结果未再次失效或被本地写入替换 |
| `Err` | 该键的刷新失败、被替换或等待取消错误 |

没有已启用订阅的查询，或使用了 `RefetchNone`，返回 `Refreshed == false`、`Err == nil`，表示跳过刷新。缺失的筛选键不会出现在结果中。空筛选结果返回长度为零的结果切片和 nil 错误。

多个查询的刷新可以同时执行。返回结果按显式键列表的首次出现顺序排列；刷新错误会包装查询键信息并合并到返回的 `error` 中，可使用 `errors.Is`、`errors.As` 检查。某个键刷新失败不撤销其他键已经完成的刷新；成功返回也不表示多个键在同一时刻形成一致快照。

## 错误与 context

`InvalidateAndWait` 的 `ctx` 必须是非 nil 的 `context.Context`。

| 情况 | 返回行为 |
| --- | --- |
| nil 目标、不可比较键、键列表包含非法键 | `ErrInvalidKey`，操作不产生部分变更，也不调用条件函数 |
| 直接传入 nil 的 `func(any) bool` | `ErrInvalidKey` |
| `QueryFilter.Predicate` 为 nil | 合法，不增加条件限制 |
| 调用时 Client 已关闭 | `ErrClosed`，不调用条件函数 |
| 条件函数执行期间关闭 Client | 在应用操作前返回 `ErrClosed` |
| `InvalidateAndWait` 的 context 在调用前已取消 | 返回 context 错误，不筛选、不失效 |
| 等待刷新期间 context 取消 | 结束当前调用尚未完成的等待；Client 管理的刷新继续执行 |

需要停止实际刷新时，显式调用 `Cancel` 或 `Remove`。`InvalidateAndWait` 的逐键刷新错误及执行预算错误见[失效与刷新](invalidation.zh-CN.md)。

## 并发与生命周期

条件函数在 Client 锁外执行，可以调用 Client API。它基于操作开始时的候选键快照；之后新增的键不参与本次筛选，应用操作前已不再由 Client 持有的键会被跳过。

筛选与操作不是后端事务，也不与其他业务写入组成事务。条件函数的业务副作用不会被回滚，应优先使用只判断键的条件函数。

筛选中的切片、条件函数捕获的可变数据，以及查询键表达的业务范围由调用者管理。并发修改这些数据时需要业务自行同步。筛选不会修改 `QueryOptions.Disable`，也不会增加轮询或加载未订阅的查询。

## 相关文档

- [统一查询配置与查询定义](query-options.zh-CN.md)：复用键、加载函数和配置。
- [失效与刷新](invalidation.zh-CN.md)：刷新策略、逐键结果和有界补刷。
- [缓存与生命周期](cache.zh-CN.md)：缓存读写、取消、删除和资源释放。

筛选契约由[单元测试](../query_filter_test.go)和 [HTTP 端到端测试](../e2e/query_filter_test.go)验证。
