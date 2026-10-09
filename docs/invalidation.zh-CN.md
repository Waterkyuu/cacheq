# 单键、批量和条件缓存失效

[English](invalidation.en-US.md) · [查询与条件请求](queries.zh-CN.md) · [缓存与生命周期](cache.zh-CN.md)

缓存失效保留已有数据，并将其标记为过期。修改用户资料后，可以同时使详情和列表失效，即使它们分别返回 `User` 和 `[]User`。

`Invalidate` 是统一入口：传单个键精确指定，传 `[]any` 列表批量指定，传 `func(any) bool` 条件函数筛选已有键。配置可省略；传多个配置时最后一个生效。只有 `[]any` 被识别为批量列表，其他切片类型需要显式转换。nil 参数和非法键返回 `ErrInvalidKey`；空列表或 nil `[]any` 列表不做任何操作。

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
	if err := client.Invalidate("message"); err != nil {
		panic(err)
	}
	after, err := cacheq.Fetch(
		ctx,
		client,
		"message",
		load,
	)
	if err != nil {
		panic(err)
	}
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

它不等待刷新完成。可以通过查询对象的 `Updates()` 接收结果。如果需要主动刷新并等待结果，使用 `detail.Refetch(ctx)`。

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

正在进行的同键请求不会被取消，也不会复制。它的结果会保留为过期状态，避免把失效之前开始获取的数据当成最新数据。完成后不会自动再启动第二次请求；需要新鲜数据时用下一次 `Fetch`、新 `Query` 或 `Refetch`。

`RefetchNone` 也不会取消已经运行的请求，只是不创建新请求。需要取消使用 `client.Cancel(key)`。需要删除数据使用 `client.Remove(key)`，不是失效。

## 错误

| 错误 | 条件 |
| --- | --- |
| `ErrInvalidKey` | nil 目标、nil 条件函数，或批次中存在 nil 或不可比较的键 |
| `ErrClosed` | 客户端已关闭，即使目标不会匹配任何键 |

在未关闭的客户端上，nil 或空 `[]any` 批次成功返回。其他切片类型不作为批次处理，返回 `ErrInvalidKey`。批次中有非法键时，在标记任何键失效前拒绝操作。后台刷新错误通过查询状态报告，不由 `Invalidate` 返回。

## 验证

```sh
go test -race ./e2e -run 'Test(SharedClientMutation|DeferredPredicateInvalidation)' -count=1
```

[HTTP e2e](../e2e/query_lifecycle_test.go) 先执行真实 PUT 修改，再验证详情与列表同时刷新、其他类型不受影响，以及延后失效确实不会发出 HTTP 请求。
