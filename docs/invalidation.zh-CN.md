# 缓存失效与可等待刷新

[English](invalidation.en-US.md) · [查询与条件请求](queries.zh-CN.md) · [缓存与生命周期](cache.zh-CN.md)

缓存失效保留已有数据，并将其标记为过期。修改用户资料后，可以同时使详情和列表失效，即使它们分别返回 `User` 和 `[]User`。

`Invalidate` 接受精确单键、`[]any` 列表、`func(any) bool` 条件函数或可复用的 `QueryFilter`。[统一查询筛选](query-filters.zh-CN.md)可以组合精确键和条件函数，并复用于等待刷新、取消和删除。配置可以省略，最多传入一份 `InvalidateOptions`；多份配置会在失效前返回 `ErrTooManyOptions`。只有 `[]any` 被识别为批量列表，其他切片类型需要显式转换。nil 参数和非法键返回 `ErrInvalidKey`；空列表或 nil `[]any` 列表不做任何操作。

## 快速开始

在导入 cacheq 的 Go 模块中，将以下完整程序保存为 `main.go`，运行 `go run .`。示例修改本地后端值，使已订阅的键失效，并等待新鲜数据。

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main refreshes an observed value after a successful backend mutation.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()
	var backend atomic.Value
	backend.Store("initial")
	load := func(context.Context) (string, error) {
		return backend.Load().(string), nil
	}
	query := cacheq.Query(client, "message", load)
	defer query.Close()
	ctx := context.Background()
	before, err := cacheq.Fetch(
		ctx,
		client,
		"message",
		load,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("before:", before)

	backend.Store("updated")
	if _, err := client.InvalidateAndWait(ctx, "message"); err != nil {
		panic(err)
	}
	after := query.Snapshot().Data
	fmt.Println("after:", after)
}
```

预期输出：

```text
before: initial
after: updated
```

## 目标类型与片段前提

| 目标 | 范围 |
| --- | --- |
| 单个可比较的键 | 精确匹配该键 |
| `[]any` | 显式指定多个键，统一校验并去重 |
| `func(any) bool` | 通过条件函数选择已有键 |

以下局部片段使用[查询文档](queries.zh-CN.md)中的共享 `client`、`getUser`、`getUsers`，放在返回 `error` 的函数中。`ctx` 为非 nil 的调用者 context，需导入各片段使用的包。

## 单键失效

```go
if err := client.Invalidate("user:42"); err != nil {
	return err
}

```

数据保留、`Stale` 变成 true，并通知查询对象。有启用的查询对象时，用其加载函数启动后台刷新；没有启用的对象时只标记过期。

它不等待刷新完成。可以通过 `Updates()` 接收结果，通过 `detail.Refetch(ctx)` 等待单个查询的加载，或通过 `InvalidateAndWait` 等待失效后的关联刷新。

## 批量失效

先建立返回不同类型的查询：

```go
detail := cacheq.Query(client, "user:42", getUser) // QueryHandle[User]
users := cacheq.Query(client, "users", getUsers)   // QueryHandle[[]User]
defer detail.Close()
defer users.Close()

```

业务先成功提交用户修改，再让相关缓存失效：

```go
// Call the application's user-update API successfully before invalidating.
if err := client.Invalidate([]any{"user:42", "users"}); err != nil {
	return err
}

```

一次调用覆盖同一客户端中的不同结果类型。默认刷新有启用查询对象的键，其余键只变旧。重复键只处理一次，避免重复启动请求，或再次标记刚启动的刷新为过期。

`[]any` 保存的是键，不是返回数据。它允许在一个列表里混合字符串键和可比较结构体键。数据通过每个查询对象保持自己的类型。

整个批次先检查键的有效性；任何键无法比较时返回 `ErrInvalidKey`，不执行部分失效。空批次没有影响，关闭的客户端返回 `ErrClosed`。

## `InvalidateOptions` 与 `RefetchMode`

| 模式 | 行为 |
| --- | --- |
| `RefetchObserved` | 零值和默认模式；刷新有启用查询对象的键 |
| `RefetchNone` | 只标记过期，本次不启动请求 |

单键也支持选择模式：

```go
if err := client.Invalidate("user:42", cacheq.InvalidateOptions{
	Refetch: cacheq.RefetchNone,
}); err != nil {
	return err
}
```

多个关联缓存延后请求：

```go
if err := client.Invalidate(
	[]any{"user:42", "users"},
	cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone},
); err != nil {
	return err
}
```

这时即使有启用的查询对象，也保留数据并暂时不请求。之后的 `Fetch`、新建 `Query`、从禁用变为启用或手动 `Refetch` 可以加载过期数据。已有启用查询对象不会仅因为时间经过就重新发起查询。

## 条件失效

比如所有用户详情和用户列表需要更新。需要导入 `strings`。

```go
if err := client.Invalidate(func(key any) bool {
	name, ok := key.(string)
	return ok && (name == "users" || strings.HasPrefix(name, "user:"))
}, cacheq.InvalidateOptions{
	Refetch: cacheq.RefetchNone,
}); err != nil {
	return err
}
```

只有返回 true 的键失效。条件函数选择操作目标；`QueryOptions.Disable` 和查询对象的 `SetEnabled` 独立控制自动加载。

条件函数收到键，运行在客户端锁外，可以安全调用 `Get`、`Set` 或其他客户端操作。它针对开始匹配时已有的键快照执行，每个键最多一次；新加入的键不参与，失效前已经删除的键跳过。顺序没有保证。nil 条件函数返回 `ErrInvalidKey`；关闭的客户端返回 `ErrClosed`，不执行匹配。

如果在条件函数里读取数据，应使用该键正确的结果类型，例如 `cacheq.Get[User](client, key)`。所有键不一定都是 `User`，不能无条件按同一种类型读取。

## 结构体键：按租户与资源匹配

```go
// ResourceKey identifies a query within a tenant.
type ResourceKey struct {
	// Resource selects the query family.
	Resource string
	// Tenant identifies the data owner.
	Tenant int
}

if err := client.Invalidate(func(key any) bool {
	resource, ok := key.(ResourceKey)
	return ok && resource.Resource == "users" && resource.Tenant == 42
}); err != nil {
	return err
}

```

字符串前缀没有内置的特殊含义。使用字符串时自己匹配前缀；使用结构体时匹配字段。键仍然必须可比较，包含切片或映射的结构体不能使用。

## 请求正在运行时失效

默认情况下，正在进行的同键请求不会被取消，也不会复制。它的结果会保留为过期状态，避免把失效之前开始获取的数据当成最新数据。完成后不会自动再启动第二次请求。在此请求结束前调用 `Fetch` 或 `Refetch` 会共用它的结果；结束后调用才能启动新的加载。

`RefetchNone` 也不会取消已经运行的请求，只是不创建新请求。需要取消使用 `client.Cancel(key)`。需要删除数据使用 `client.Remove(key)`，不是失效。

## `RefetchAfterLoad`：有界后台补刷

只安排补刷，不等待完成：

```go
if err := client.Invalidate("users", cacheq.InvalidateOptions{
	RefetchAfterLoad: true,
}); err != nil {
	return err
}
```

该行为需要启用的查询对象。没有活动加载时，失效启动普通后台刷新；已有加载时，多次启用补刷的失效合并为一次后续加载，在旧加载结束后执行，包括旧加载失败或其调用者取消的情况。补刷由 Client 拥有，使用调度时最早创建的启用查询对象的加载函数和配置，遵循原有重试、超时和执行预算规则。

每条加载链最多自动补刷一次。补刷期间再次失效会使其结果保持过期，不会自动启动第三次加载；后续显式操作可以开启新的加载链。此上限防止持续写入引发无限刷新。补刷期间，旧请求的结果可能作为过期数据展示。

调度前所有查询对象关闭或禁用时，跳过补刷。`Set`、`Update`、`Cancel`、`Remove`、`Clear` 和 Client 关闭会丢弃已安排的补刷。`RefetchNone` 优先控制本次调用，但不会取消之前已经安排的补刷。

## `InvalidateAndWait`：等待关联刷新

```go
results, err := client.InvalidateAndWait(ctx, []any{"user:42", "users"})
for _, result := range results {
	fmt.Printf("key=%v refreshed=%t err=%v\n", result.Key, result.Refreshed, result.Err)
}
if err != nil {
	return err
}

```

该片段需要导入 `fmt`。此入口自动启用 `RefetchAfterLoad`，接受与 `Invalidate` 相同的单键、`[]any`、条件函数和可选配置。只有存在启用查询对象及加载函数的键才刷新。没有订阅、仅有禁用对象或使用 `RefetchNone` 的键只标记过期，结果显示跳过；不会为仅使用 Fetch 的键保存加载函数。

先安排所有目标的刷新，再等待结果；单个键失败不会阻止其他键。显式批次按首次出现的顺序返回，重复键合并。条件筛选顺序没有保证，匹配期间删除的键不返回结果。非法目标不会产生任何修改，也不返回结果；空目标返回空结果切片。

| `RefreshResult` | 含义 |
| --- | --- |
| `Key` | 保留原始动态类型的查询键 |
| `Refreshed: true`、`Err: nil` | 失效后启动的加载成功完成，期间未再次失效或被替换 |
| `Refreshed: false`、`Err: nil` | 已无启用查询对象，或使用 `RefetchNone`，跳过刷新 |
| `Refreshed: false`、非 nil `Err` | 加载失败、再次失效、本地替换或等待取消 |

返回的总错误包含逐键上下文并合并所有错误，保留 `errors.Is` 和 `errors.As` 判断原始原因的能力。等待的加载再次失效时，本次完成通知报告 `ErrRefreshInvalidated`。后续启用补刷的操作可以独立安排补刷，但不会改变较早的完成通知；共用同一次补刷的等待者收到相同结果。

`Refreshed` 表示加载完成，不承诺返回时数据仍然新鲜。例如 `StaleTime` 为零时，成功刷新的数据立即过期。结果不是跨键的原子快照，也不提供新的服务端一致性保证。

调用者 context 限制整个等待，包括旧请求和补刷。调用前已经取消时不执行失效；安排刷新后取消，只结束本次等待，Client 拥有的请求及其他等待者继续执行。已完成或跳过的结果保留，其余键报告 context 错误。显式 `Cancel`、`Remove` 或 `Clear` 使相关等待返回 `context.Canceled`；`Set` 或 `Update` 返回 `ErrRefreshSuperseded`；Client 关闭返回 `ErrClosed`，均不需要等待被分离的回调结束。

通过已经持有目标查询 Executor 的回调 context 等待刷新时，该键返回 `ErrExecutionReentry`，不会安排额外加载。应避免在加载函数中等待自身查询，或建立后端循环依赖。检测依赖传入的 context 和选中的 Client 层预算，不能推断隐藏的 Batcher 预算或应用依赖环。

## 错误

| 错误 | 条件 |
| --- | --- |
| `ErrInvalidKey` | nil 目标、nil 条件函数，或批次中存在 nil 或不可比较的键 |
| `ErrClosed` | 客户端已关闭，即使目标不会匹配任何键 |
| `ErrRefreshInvalidated` | 等待的刷新在运行期间再次失效 |
| `ErrRefreshSuperseded` | 本地 `Set` 或 `Update` 替换了等待的刷新 |
| `ErrExecutionReentry` | 等待者已经持有选中刷新的执行预算 |

后三种错误属于 `InvalidateAndWait`；加载和 context 错误也通过其逐键结果返回。`Invalidate` 保持不等待，仅返回目标选择错误。

在未关闭的客户端上，nil 或空 `[]any` 批次成功返回。其他切片类型不作为批次处理，返回 `ErrInvalidKey`。批次中有非法键时，在标记任何键失效前拒绝操作。后台刷新错误通过查询状态报告，不由 `Invalidate` 返回。

## 验证

```sh
go test -race ./e2e -run 'Test(SharedClientMutation|DeferredPredicateInvalidation)' -count=1
```

[HTTP e2e](../e2e/query_lifecycle_test.go) 先执行真实 PUT 修改，再验证详情与列表同时刷新、其他类型不受影响，以及延后失效确实不会发出 HTTP 请求。
