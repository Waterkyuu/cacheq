# 缓存读写、过期与生命周期

[English](cache.en-US.md) · [查询与条件请求](queries.zh-CN.md) · [缓存失效](invalidation.zh-CN.md)

一个 `Client` 保存类型化结果，管理新鲜期、保留期和共享加载。本文介绍本地读写、预加载、绝对过期时间、淘汰、取消及资源释放。

## API 片段前提

除标为完整程序的示例外，Go 代码块均为业务函数中的局部片段。包含 `return err` 的片段需要放在返回 `error` 的函数中。`ctx` 是非 nil 的调用者 context；`client` 由应用入口创建并注入；`users` 是[查询文档](queries.zh-CN.md)中的查询对象。需导入 `cacheq "github.com/Waterkyuu/cacheq"` 及各片段使用的标准库包。

缓存命中、请求合并和清理次数见[可观测性文档](observability.zh-CN.md)。

## `NewClient` 与 `Options`：配置共享缓存

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime:  time.Minute,
	GCTime:     5 * time.Minute,
	MaxEntries: 1000,
	MaxAge:     10 * time.Minute,
	Retry:      2,
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
| `StaleTime` | 普通结果的新鲜时间 | 0，结果立即过期 |
| `GCTime` | 闲置缓存的保留时间 | 0，不自动删除；负数也禁用 |
| `MaxEntries` | 缓存结果和错误的条目上限，按 LRU 淘汰 | 0，不限制；负数也禁用 |
| `MaxAge` | 数据从最后一次写入起的最长可用时间 | 0，不限制；负数也禁用 |
| `Retry` | 初次失败后的追加尝试次数上限 | 0，不重试 |
| `RetryIf` | 筛选允许追加尝试的错误 | nil，次数允许时重试所有非取消类错误 |
| `RetryDelay` | 追加尝试前的等待时间，第一次编号为 1 | 从 1 秒开始指数增长，上限 30 秒 |
| `Timeout` | 整次加载的时间上限，包括重试与等待 | 0，不额外设置超时，仍响应调用者 context |
| `Clock` | 新鲜期和保留期比较使用的时钟 | `time.Now` |

所有时长字段使用 `time.Duration`。`StaleTime` 为零或负数时，普通结果立即过期；`Retry` 非正时不追加尝试，`Timeout` 非正时不额外设置超时。

这些配置提供客户端默认值。[消费者配置](query-options.zh-CN.md)可以覆盖新鲜期、重试和超时；容量、GC、最长可用时间和时钟仍由客户端管理。取消和截止时间错误不重试。超时会取消 context，不能强制终止完全忽略 context 的加载函数。

## `Get[V]`：读取缓存状态

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

类型由传入值推断。写入后数据的新鲜期按 `StaleTime` 计算，启用 `MaxAge` 时不会超过可用时间上限，并通知这个键的查询对象。旧请求会被取消并脱离缓存，不能用迟到的结果覆盖本地数据。类型冲突先检查，拒绝写入不会取消原本合法的请求。

缓存值是共享的只读数据。修改切片、映射或指针指向的数据前，先复制，再 `Set`；直接修改共享数据不会发送通知，也可能引起数据竞争。

## `Prefetch`：预加载

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

## `FetchWithExpiry` 与 `Loader`：绝对新鲜期

恢复持久化数据时，可以保留该记录原有的新鲜期截止时间。

```go
// In production, obtain both fields from the same persisted record.
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

`Loader[V]` 表示 `func(context.Context) (V, time.Time, error)`。第二项保留原记录的绝对新鲜期截止时间，不会从本次读取重新计算新鲜期；`MaxAge` 仍从数据安装到当前客户端时开始计时。零截止时间或已过期时间使结果立即变旧。

通常错误不会安装新数据。如果加载函数同时返回未来的截止时间和错误，则明确缓存兜底值及该错误，截止前直接返回它们；即使兜底值是零值也有 `HasData: true`。启用 `MaxAge` 时，该截止时间可能被缩短。刷新失败保留之前成功的数据，但不会保留超过 `MaxAge` 的值。

## `GCTime`：自动删除闲置缓存

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime: time.Minute,
	GCTime:    5 * time.Minute,
})
defer client.Close()
```

新鲜期与保留期独立：一分钟后数据过期，仍可在后台刷新时读取；只有没有查询对象、没有请求、持续五分钟没有读写时才会删除。

读写重新计时。查询对象存在时暂停删除，包括禁用的查询对象；请求运行时也暂停。最后一个查询对象关闭或请求结束后恢复计时。`Remove`、`Clear`、`Close` 会释放对应定时器。容量淘汰和数据最长可用时间是独立策略，见下文。

`Clock` 替换时间比较使用的时钟，不替换真实定时器调度。仅改变注入时钟不会触发 GC 定时器。

## `MaxEntries`：按 LRU 限制缓存条目数

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime:  time.Minute,
	MaxEntries: 1000,
})
defer client.Close()
```

最多保留 1000 个已完成结果或缓存错误。写入新结果超过上限时，淘汰最久没有被使用的条目。读取、写入、创建查询和失效操作都会更新使用顺序；关闭查询对象不会。零值或负数不限制容量。

淘汰会清除数据和错误、停止该键的 GC 定时器，并向订阅对象通知无数据状态。它不会取消正在运行的请求，也不会自动重新请求。已有查询对象仍能 `Refetch`；只要订阅或请求仍拥有这个键，就保留结果类型约束。没有这两种所有者时，淘汰也会释放类型绑定。最后一个所有者退出后，会释放空的类型元数据，即使没有启用定时 GC。

上限统计缓存结果，不统计字节数、订阅数、并发请求数及其类型元数据。单个很大的结果仍可能占用较多内存。

## `MaxAge`：限制数据最长可用时间

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime: time.Minute,
	GCTime:    5 * time.Minute,
	MaxAge:    10 * time.Minute,
})
defer client.Close()
```

从成功加载或 `Set` 写入数据时开始计时。上面的配置表示新鲜 1 分钟，变旧后最多再可用 9 分钟。到第 10 分钟不再返回该值：`HasData` 为 false，`Data` 为该类型的零值，即使仍在刷新或查询对象处于禁用状态。最近一次错误和仍有所有者的类型约束保留；达到年龄上限不会取消正在运行的请求。闲置元数据仍受 GC 和容量清理策略控制。

读取、失效和普通刷新失败都不会重新计时。成功加载新结果、写入本地值，或通过 `FetchWithExpiry` 显式安装兜底值，才会开始新的可用时间窗口。加载函数给出的截止时间更早时保持原值；新鲜期超过年龄上限时缩短到年龄上限。

年龄在缓存操作和发布状态时检查。仅时间经过不会发送通知或启动请求。读取发现超龄数据时，会通知已有查询对象；新建启用的 `Query` 或调用 `Fetch` 可以加载替代值。之前拿到的快照是普通值，不会被追溯修改。零值或负数 `MaxAge` 禁用此策略。

## 完整示例：容量与最长可用时间

把程序复制到已引入 cacheq 的应用中。示例注入时钟，不需要等待或发起请求就能验证年龄边界。

```go
package main

import (
	"fmt"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// main demonstrates LRU eviction and maximum data age with a deterministic clock.
func main() {
	now := time.Unix(0, 0)
	client := cacheq.NewClient(cacheq.Options{
		StaleTime:  time.Minute,
		MaxEntries: 2,
		MaxAge:     10 * time.Minute,
		Clock:      func() time.Time { return now },
	})
	defer client.Close()
	if err := cacheq.Set(client, "a", "first"); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, "b", "second"); err != nil {
		panic(err)
	}
	_ = cacheq.Get[string](client, "a")
	if err := cacheq.Set(client, "c", "third"); err != nil {
		panic(err)
	}
	fmt.Println(
		"retained:",
		cacheq.Get[string](client, "a").HasData,
		cacheq.Get[string](client, "b").HasData,
		cacheq.Get[string](client, "c").HasData,
	)
	now = now.Add(10 * time.Minute)
	fmt.Println("over age:", cacheq.Get[string](client, "a").HasData,
		cacheq.Get[string](client, "c").HasData)
}
```

预期输出：

```text
retained: true false true
over age: false false
```

## `Cancel`：取消键的活动加载

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

## `Clear`：清空缓存

```go
client.Clear()
```

清空所有结果、取消正在进行的请求、停止清理定时器。现有查询对象及其类型约束仍在，收到无数据状态，之后可以主动刷新。不会因清空而自动发起请求。

## 关闭查询对象与客户端

```go
users.Close()  // Release one consumer without canceling shared work.
client.Close() // Cancel all work and close all query channels permanently.
```

查询对象 `Close` 不会取消共享请求，也不会删除已有数据。客户端 `Close` 取消所有请求、关闭查询更新通道及[事件订阅](events.zh-CN.md)、清空缓存和定时器，并永久拒绝新操作。重复关闭安全。

查询对象关闭后 `Snapshot()` 仍可读取兼容的共享缓存，`SetEnabled` 和 `Refetch` 返回 `ErrQueryClosed`。客户端关闭后读写操作返回 `ErrClosed`。

## 错误处理

```go
state := cacheq.Get[int](client, "greeting")
if errors.Is(state.Err, cacheq.ErrTypeMismatch) {
	// Use this key's declared result type or select a different key.
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

## 验证

```sh
go test -race ./e2e -run 'Test(HTTPCapacityEviction|HTTPMaxAge|HTTPCancellation|HTTPRetriesAndRetainedData|PublicCacheOperations)' -count=1
go test -race . -run 'Test(Capacity|MaxAge|GC)' -count=1
```

[e2e](../e2e/query_lifecycle_test.go) 从库外部验证 HTTP 取消、重试、缓存操作和生命周期；[容量 e2e](../e2e/cache_capacity_test.go) 验证 LRU 淘汰和重新请求，[年龄 e2e](../e2e/cache_age_test.go) 验证刷新失败、订阅及年龄边界上的共享 HTTP 请求；[缓存测试](../cache_test.go) 验证自动删除和精确时间边界。
