# 查询标签

[English](query-tags.en-US.md) · [统一查询筛选](query-filters.zh-CN.md) · [查询定义](query-options.zh-CN.md)

查询标签用于声明查询之间的关联关系。详情、列表和统计等查询可以使用不同的查询键及结果类型，同时声明相同标签，以便通过 `QueryFilter` 一起失效、等待刷新、取消加载或删除缓存。

`Key` 标识具体查询，`Tags` 用于分组。相同标签不会合并不同查询键的缓存，也不改变类型约束或加载策略。

本指南规定本地标签登记和 QueryFilter 匹配。通过 `ShareTags` 可选登记远端关系，以及通过 `WatchShared` 接收跨实例失效，参见[共享失效通知与标签](shared-invalidation.zh-CN.md)。

## 快速开始

以下完整程序为租户 42 的用户详情和列表声明同一个标签。预加载后创建已启用的订阅，修改模拟后端数据，再按标签等待两项查询刷新。租户 99 的数据不受影响。

在已添加 cacheq 依赖的 Go 模块中，将程序保存为 `main.go`，运行 `go run .`。示例通过 `atomic.Value` 同步后台加载读取的数据，无需外部服务。

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// User is the user data returned by detail and list queries.
type User struct {
	// Name contains the user's display name.
	Name string
}

// main refreshes tagged detail and list queries without touching another tenant's data.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()
	ctx := context.Background()
	var backend atomic.Value
	backend.Store(User{Name: "Alice"})
	tags := []string{"tenant:42:users"}

	detailDefinition := cacheq.QueryDefinition("tenant:42:user:7", func(context.Context) (User, error) {
		return backend.Load().(User), nil
	}, cacheq.QueryOptions{})
	detailDefinition.Tags = tags
	listDefinition := cacheq.QueryDefinition("tenant:42:users", func(context.Context) ([]User, error) {
		return []User{backend.Load().(User)}, nil
	}, cacheq.QueryOptions{})
	listDefinition.Tags = tags
	otherDefinition := cacheq.QueryDefinition("tenant:99:user:7", func(context.Context) (User, error) {
		return User{Name: "Carol"}, nil
	}, cacheq.QueryOptions{})
	otherDefinition.Tags = []string{"tenant:99:users"}
	if err := detailDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	if err := listDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	if err := otherDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	detail := detailDefinition.Query(client)
	list := listDefinition.Query(client)
	defer detail.Close()
	defer list.Close()

	backend.Store(User{Name: "Bob"})
	filter := cacheq.QueryFilter{Tags: []string{"tenant:42:users"}}
	results, err := client.InvalidateAndWait(ctx, filter, cacheq.InvalidateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Println("refreshed:", len(results))
	fmt.Println("detail:", detail.Snapshot().Data.Name)
	fmt.Println("list:", list.Snapshot().Data[0].Name)
	fmt.Println("other tenant:", cacheq.Get[User](client, otherDefinition.Key).Data.Name)

	if err := client.Remove(filter); err != nil {
		panic(err)
	}
	fmt.Println("selected removed:", !detail.Snapshot().HasData && !list.Snapshot().HasData)
}
```

预期输出：

```text
refreshed: 2
detail: Bob
list: Bob
other tenant: Carol
selected removed: true
```

`Prefetch` 登记标签并初始化缓存，`Query` 建立启用的订阅。`InvalidateAndWait` 只刷新存在已启用订阅的查询，因此本次刷新返回两个结果。`Remove` 清除选中查询的数据，现有订阅仍由调用者负责关闭。

## 声明与登记

| 字段 | 类型 | 默认值 | 职责 |
| --- | --- | --- | --- |
| `Definition.Tags` | `[]string` | nil | 声明该查询键的标签；nil 和空切片均表示不声明标签 |
| `QueryFilter.Tags` | `[]string` | nil | 选择至少命中一个指定标签的已有查询；nil 不限制标签，非 nil 的空切片选择零个查询 |

调用 `QueryDefinition` 不登记标签。调用返回定义的 `Fetch`、`Prefetch` 或 `Query` 时，在成功通过校验并复用缓存、开始或加入加载、或者创建订阅时登记标签。命中新鲜缓存和创建禁用的订阅也会登记，不要求实际发起请求。

标签在操作开始时复制。后续修改定义的 `Tags` 切片不会改变已经登记的标签；使用修改后的定义发起新操作时，新增标签会合并到同一查询键。标签保存于对应 Client，使用同一份定义的另一个 Client 需要独立登记。

标签由查询定义声明，不属于 `QueryOptions` 的加载策略。直接调用 `Fetch`、`Query` 或 `Prefetch` 不增加标签，也不会清除该键已登记的标签。`Set` 和 `Update` 保留已有标签。

## 筛选规则

- 多个标签采用“任一匹配”：`Tags: []string{"users", "orders"}` 选择包含 `"users"` 或 `"orders"` 的查询。
- `Keys`、`Tags` 和 `Predicate` 之间取交集。键须同时满足每个已设置的限制。
- 标签按字符串精确匹配，区分大小写，不进行前缀匹配或空白处理。空字符串也是一个普通标签值。
- 同一查询声明多个匹配标签或筛选列表重复标签时，该查询仍只处理一次。
- 显式 `Keys` 的首次出现顺序决定结果顺序；未指定键列表时顺序没有保证。

以下局部片段可放在业务函数内，选择两个指定键中带有任一指定标签的查询，再排除详情键：

```go
filter := cacheq.QueryFilter{
    Keys: []any{"tenant:42:user:7", "tenant:42:users"},
    Tags: []string{"tenant:42:users", "tenant:42:orders"},
    Predicate: func(key any) bool {
        return key != "tenant:42:user:7"
    },
}
```

只有已登记且仍由 Client 持有的查询参与筛选。未使用的定义、缺失键和未登记目标标签的查询不会被选中。条件函数只接收已满足键和标签条件的候选键，其他[统一筛选规则](query-filters.zh-CN.md)保持一致。

## 按标签执行操作

| 操作 | 行为 |
| --- | --- |
| `client.Invalidate(filter, options)` | 保留数据并标记为过期，按配置安排刷新，不等待完成 |
| `client.InvalidateAndWait(ctx, filter, options)` | 等待选中查询的失效后刷新结果 |
| `client.Cancel(filter)` | 取消选中的共享加载，保留已完成的数据和订阅 |
| `client.Remove(filter)` | 取消选中的共享加载并删除缓存数据，保留订阅 |

标签只改变选中范围，不启动未订阅的加载函数，也不修改 `QueryOptions.Disable`。自动刷新继续使用已有的订阅选择规则。某个查询的加载失败不会使其他已完成的结果回滚；逐键结果及错误处理见[失效与刷新](invalidation.zh-CN.md)。

## 同键合并与生命周期

同一查询键被多个定义或消费者使用时，登记的标签取并集，不按调用顺序覆盖。关闭某个查询对象不会撤销它已经登记的标签。即使通过不声明标签的入口读取同一个键，已有标签也继续保留。

| 状态变化 | 标签行为 |
| --- | --- |
| 加载完成或失败、失效、取消、`Set`、`Update` | 保留已有标签 |
| 删除或淘汰数据后仍保留订阅或查询状态 | 标签随保留的查询状态继续存在 |
| 无订阅的键被 `Remove`、`Clear`、容量淘汰或 GC 完整移除 | 一并释放标签 |
| Client 关闭 | 释放该 Client 的全部标签 |

`MaxAge` 到期使数据不可用，不代表查询状态已经完整移除；标签是否保留取决于现有查询状态的清理规则。

重新声明标签会增加关联，不会删除旧标签。需要替换某个键的标签集合时，先关闭该键的全部订阅，再调用 `Remove(key)` 完整释放旧状态，随后使用新的定义。生命周期策略见[缓存与生命周期](cache.zh-CN.md)。

## 校验、并发与业务范围

非法键、结果类型冲突、未满足加载要求的 nil Fetcher，以及调用前已经取消的 context，不登记标签。查询已登记后发生后端失败或等待取消，不回滚登记。

筛选在 Client 锁内读取已登记标签，业务条件函数在锁外执行。标签匹配基于本次筛选开始时的查询状态；条件函数执行期间新增的标签不会扩大本次选中范围。筛选与后端写入不构成事务。

调用者需要同步对定义切片及条件函数捕获数据的并发修改。查询键仍需包含影响结果的参数和身份范围；多租户共享一个 Client 时，使用包含租户范围的标签，例如 `"tenant:42:users"`，或结合键条件限制目标。

## 相关文档

- [统一查询筛选](query-filters.zh-CN.md)：目标参数、交集规则和操作返回值。
- [查询定义与配置](query-options.zh-CN.md)：定义复用、加载配置和 Client 所有权。
- [失效与刷新](invalidation.zh-CN.md)：等待刷新、逐键错误和有界补刷。

标签行为由[单元测试](../query_tags_test.go)和 [HTTP 端到端测试](../e2e/query_tags_test.go)验证。
