# 缓存读写、过期与生命周期

[English](cache.en-US.md) · [查询与条件请求](queries.zh-CN.md) · [缓存失效](invalidation.zh-CN.md)

所有功能都使用同一个 `*cacheq.Client`。下面的片段放在业务函数中，`ctx` 是调用者的 `context.Context`，`client` 是应用入口创建并注入的客户端。需要导入 `cacheq "github.com/Waterkyuu/cacheq"`，以及各片段用到的 `context`、`time`、`errors` 或 `fmt`。

## `NewClient` 与 `Options`：配置共享缓存

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime: time.Minute,
	GCTime:    5 * time.Minute,
	Retry:     2,
	RetryDelay: func(attempt int) time.Duration {
		return time.Duration(attempt) * 100 * time.Millisecond
	},
	Timeout: 10 * time.Second,
})
defer client.Close()
```

客户端通常在应用入口创建，传给各个业务模块。它可以同时保存字符串、用户详情、用户列表、配置等不同类型；每个键自己的结果类型必须一致。

| 配置 | 用途 | 默认值 |
| --- | --- | --- |
| `StaleTime` | 普通查询结果能直接复用多长时间 | 0，结果立即变旧 |
| `GCTime` | 不再被使用的缓存保留多长时间 | 0，不自动删除；负数也禁用 |
| `Retry` | 初次失败后最多追加多少次尝试 | 0，不重试 |
| `RetryDelay` | 第几次重试前等待多久，第一次编号为 1 | 从 1 秒开始指数增长，上限 30 秒 |
| `Timeout` | 整次加载的时间上限，包括重试与等待 | 0，不额外设置超时，仍响应调用者 context |
| `Clock` | 新鲜期和保留期比较使用的时钟 | `time.Now` |

配置作用于这个客户端。取消和截止时间错误不重试。超时会取消 context，不能强制终止完全忽略 context 的加载函数。

## `Get[V]`：只读缓存，不请求

```go
state := cacheq.Get[string](client, "greeting")
if state.HasData {
	fmt.Println(state.Data)
}
```

`Get` 不需要加载函数，不启动请求，也不建立订阅。显式写 `[string]` 是因为没有加载函数可供推断结果类型。未知键返回 `Idle`，不会把这个键绑定成 `string`。

读取已有缓存会重新计算闲置保留期。持续更新通知使用查询对象的 `Updates()`，见[查询文档](queries.zh-CN.md)。

## `Set`：直接安装本地数据

```go
if err := cacheq.Set(client, "greeting", "hello"); err != nil {
	return err
}
if err := cacheq.Set(client, "feature-flags", map[string]bool{"search": true}); err != nil {
	return err
}
```

类型由传入值推断。写入后数据的新鲜期按 `StaleTime` 计算，并通知这个键的查询对象。旧请求会被取消并脱离缓存，不能用迟到的结果覆盖本地数据。类型冲突先检查，拒绝写入不会取消原本合法的请求。

缓存值是共享的只读数据。修改切片、映射或指针指向的数据前，先复制，再 `Set`；直接修改共享数据不会发送通知，也可能引起数据竞争。

## `Prefetch`：提前准备后面要用的数据

```go
getGreeting := func(context.Context) (string, error) {
	return "hello", nil
}
if err := cacheq.Prefetch(
	ctx,
	client,
	"greeting",
	getGreeting,
); err != nil {
	return err
}
```

等待缓存准备完成，只返回错误，不返回数据。后续 `Fetch` 或 `Query` 能复用它。已有新鲜缓存不会重新加载；同键正在加载时共用请求。实际加载函数可以是查询文档中的 HTTP 请求。

## `FetchWithExpiry` 与 `Loader`：数据自带截止时间

比如磁盘保存的数据原本一分钟后过期，恢复时应保留原截止时间。

```go
// In production, obtain both fields from the same persisted cache record.
savedValue := "restored"
savedExpiresAt := time.Now().Add(time.Minute)
load := func(context.Context) (string, time.Time, error) {
	return savedValue, savedExpiresAt, nil
}
value, err := cacheq.FetchWithExpiry(
	ctx,
	client,
	"restored-value",
	load,
)
if err != nil {
	return err
}
_ = value
```

`Loader[V]` 表示 `func(context.Context) (V, time.Time, error)`。第二项是绝对截止时间，而非从这次读取开始重新计时。零截止时间或已过期时间使结果立即变旧。

通常错误不会安装新数据。如果加载函数同时返回未来的截止时间和错误，则明确缓存兜底值及该错误，截止前直接返回它们；即使兜底值是零值也有 `HasData: true`。刷新失败不会删除之前成功的数据。

## `GCTime`：自动删除闲置缓存

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime: time.Minute,
	GCTime:    5 * time.Minute,
})
```

新鲜期和保留期是两回事：一分钟后数据变旧，仍可在后台刷新时显示；只有没有查询对象、没有请求、持续五分钟没有读写时才会删除。

读写重新计时。查询对象存在时暂停删除，包括禁用的查询对象；请求运行时也暂停。最后一个查询对象关闭或请求结束后恢复计时。`Remove`、`Clear`、`Close` 会释放对应定时器。没有 LRU 或容量上限。

`Clock` 可以替换比较用的时钟，但不替换真实定时器调度器。库内的 GC 边界测试同时替换时钟和内部调度器，避免业务测试依赖真实等待。

## `Cancel`：结束这个键正在进行的请求

```go
if err := client.Cancel("greeting"); err != nil {
	return err
}
```

保留已有数据，取消正在运行的请求，通知消费者请求已停止。加载函数需要响应 context。没有请求时也可以调用，不会自动重启请求。

## `Remove`：删除一个键

```go
if err := client.Remove("greeting"); err != nil {
	return err
}
```

取消请求、删除数据和清理定时器，并通知现有查询对象没有数据。不会自动重新加载，之后可以通过现有查询对象 `Refetch` 或新查询再次获取。

现有查询对象还在时，该键的类型约束继续保留，防止它收到其他类型的数据。要改用另一种类型，应先关闭所有旧查询对象，再删除该键，然后创建新类型的查询。

## `Clear`：清空数据，继续使用客户端

```go
client.Clear()
```

清空所有结果、取消正在进行的请求、停止清理定时器。现有查询对象及其类型约束仍在，收到无数据状态，之后可以主动刷新。不会因清空而自动发起请求。

## 两种 `Close`：释放消费者与关闭整个应用客户端

```go
users.Close()  // Release one query consumer.
client.Close() // Stop all client-owned work and close every query channel.
```

查询对象 `Close` 不会取消共享请求，也不会删除已有数据。客户端 `Close` 取消所有请求、关闭所有更新通道、清空缓存和定时器，并永久拒绝新操作。重复关闭安全。

查询对象关闭后 `Snapshot()` 仍可读取兼容的共享缓存，`SetEnabled` 和 `Refetch` 返回 `ErrQueryClosed`。客户端关闭后读写操作返回 `ErrClosed`。

## 错误如何判断

```go
state := cacheq.Get[int](client, "greeting")
if errors.Is(state.Err, cacheq.ErrTypeMismatch) {
	// Use the result type associated with this key, or choose a different key.
}
```

| 错误 | 触发条件 |
| --- | --- |
| `ErrClosed` | 操作已经关闭的客户端 |
| `ErrQueryClosed` | 用已释放的查询对象启用请求或刷新 |
| `ErrTypeMismatch` | 同一个键使用不同静态结果类型 |
| `ErrInvalidKey` | 键为 nil，或包含不能比较的值 |
| `ErrNoFetcher` | 创建查询没有加载函数，或需要请求却没有加载函数 |

`Query` 始终需要非 nil 加载函数。`Fetch[V]` 和 `FetchWithExpiry[V]` 可以传 nil 来读取**已有新鲜缓存**；缺失或过期时返回 `ErrNoFetcher`。只想读状态时使用 `Get[V]`。

静态类型与动态值不同：`Query[any]` 的键绑定的是 `any`，对它写数据应使用 `Set[any]`；直接传入具体字符串的 `Set` 会推断为 `string`，因此产生类型冲突。正常业务应使用具体类型。

## 验证这些功能

```sh
go test -race ./e2e -run 'Test(HTTPCancellation|HTTPRetriesAndRetainedData|PublicCacheOperations)' -count=1
go test -race . -run 'TestGC' -count=1
```

[e2e](../e2e/query_lifecycle_test.go) 从库外部验证 HTTP 取消、重试、缓存操作和生命周期；[缓存测试](../cache_test.go) 验证自动删除和精确时间边界。
