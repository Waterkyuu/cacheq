# 单键、批量和条件缓存失效

[English](invalidation.en-US.md) · [查询与条件请求](queries.zh-CN.md) · [缓存与生命周期](cache.zh-CN.md)

“失效”表示保留旧数据，但告诉客户端它已经不能算新鲜数据。修改用户资料后，详情和列表可以一起失效，即使它们分别返回 `User` 和 `[]User`。

以下使用[查询文档](queries.zh-CN.md)中的同一个 `client`，以及 `getUser`、`getUsers` 两个业务加载函数。片段放在返回 `error` 的业务函数里。

## `Invalidate`：一个缓存过期

```go
if err := client.Invalidate("user:42"); err != nil {
	return err
}
```

数据保留、`Stale` 变成 true，并通知查询对象。有启用的查询对象时，用其加载函数启动后台刷新；没有启用的对象时只标记过期。

它不等待刷新完成。可以通过查询对象的 `Updates()` 接收结果。如果需要主动刷新并等待结果，使用 `detail.Refetch(ctx)`。

## `InvalidateMany`：关联数据一起过期

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
if err := client.InvalidateMany(
	[]any{"user:42", "users"},
	cacheq.InvalidateOptions{},
); err != nil {
	return err
}
```

一次调用覆盖同一客户端中的不同结果类型。默认刷新有启用查询对象的键，其余键只变旧。重复键只处理一次，不会因为重复出现而多次启动请求或再次弄旧刚启动的刷新。

`[]any` 保存的是键，不是返回数据。它允许在一个列表里混合字符串键和可比较结构体键。数据通过每个查询对象保持自己的类型。

整个批次先检查键的有效性；任何键无法比较时返回 `ErrInvalidKey`，不执行部分失效。空批次没有影响，关闭的客户端返回 `ErrClosed`。

## `InvalidateOptions` 与 `RefetchMode`：刷新还是延后

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
if err := client.InvalidateMany(
	[]any{"user:42", "users"},
	cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone},
); err != nil {
	return err
}
```

这时即使有启用的查询对象，也保留数据并暂时不请求。之后的 `Fetch`、新建 `Query`、从禁用变为启用或手动 `Refetch` 可以加载过期数据。已有启用查询对象不会仅因为时间经过就重新发起查询。

## `InvalidateWhere`：按条件挑出缓存

比如所有用户详情和用户列表需要更新。需要导入 `strings`。

```go
client.InvalidateWhere(func(key any) bool {
	name, ok := key.(string)
	return ok && (name == "users" || strings.HasPrefix(name, "user:"))
}, cacheq.InvalidateOptions{
	Refetch: cacheq.RefetchNone,
})
```

只有返回 true 的键失效。这里的条件是在选择缓存；条件查询中的 `Enabled` 是在决定能否发请求，两者用途不同。

条件函数收到键，运行在客户端锁外，可以安全调用 `Get`、`Set` 或其他客户端操作。它针对开始匹配时已有的键快照执行，每个键最多一次；新加入的键不参与，失效前已经删除的键跳过。顺序没有保证。nil 条件函数和关闭的客户端不执行匹配。

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

client.InvalidateWhere(func(key any) bool {
	resource, ok := key.(ResourceKey)
	return ok && resource.Resource == "users" && resource.Tenant == 42
}, cacheq.InvalidateOptions{})
```

字符串前缀没有内置的特殊含义。使用字符串时自己匹配前缀；使用结构体时匹配字段。键仍然必须可比较，包含切片或映射的结构体不能使用。

## 请求正在运行时失效

正在进行的同键请求不会被取消，也不会复制。它的结果会保留为过期状态，避免把失效之前开始获取的数据当成最新数据。完成后不会自动再启动第二次请求；需要新鲜数据时用下一次 `Fetch`、新 `Query` 或 `Refetch`。

`RefetchNone` 也不会取消已经运行的请求，只是不创建新请求。需要取消使用 `client.Cancel(key)`。需要删除数据使用 `client.Remove(key)`，不是失效。

## 运行用户修改流程

```sh
go test -race ./e2e -run 'Test(SharedClientMutation|DeferredPredicateInvalidation)' -count=1
```

[HTTP e2e](../e2e/query_lifecycle_test.go) 先执行真实 PUT 修改，再验证详情与列表同时刷新、其他类型不受影响，以及延后失效确实不会发出 HTTP 请求。
