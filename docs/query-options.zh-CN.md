# 每个消费者独立的查询配置

[English](query-options.en-US.md)

`QueryOptions` 配置一个 `Query` 查询对象，`FetchOptions` 配置一次 `Fetch` 调用。两者都用于覆盖 Client 默认的新鲜期、重试和超时设置；`QueryOptions` 额外提供 `Disable`，控制是否自动加载。

## 快速开始

在调用末尾传入配置。以下完整示例为 `Query` 和 `Fetch` 设置一小时的新鲜期，Client 默认的一分钟新鲜期保持不变。

将程序保存为已导入 cacheq 的 Go 模块中的 `main.go`，运行 `go run .`。

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main applies the same freshness override to a query handle and a Fetch call.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	load := func(context.Context) (string, error) {
		return "hello", nil
	}
	hour := time.Hour

	query := cacheq.Query(
		client,
		"greeting",
		load,
		cacheq.QueryOptions{StaleTime: &hour},
	)
	defer query.Close()

	value, err := cacheq.Fetch(
		context.Background(),
		client,
		"greeting",
		load,
		cacheq.FetchOptions{StaleTime: &hour},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
}
```

预期输出：

```text
hello
```

`Query` 使用 `QueryOptions`，`Fetch` 使用 `FetchOptions`。示例中的两次调用共享 `"greeting"` 的缓存和正在进行的请求；`StaleTime` 覆盖只改变各自判断缓存新鲜度的规则。未设置的重试和超时字段继续使用 Client 默认值。

## QueryOptions

传入方式为 `Query(client, key, load, options)`。配置在创建查询对象时确定，后续自动加载、`SetEnabled` 和手动 `Refetch` 继续使用该配置。

除下文的共有字段外，`QueryOptions` 还支持：

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `Disable` | `bool` | false | true 时不自动加载；仍可读取缓存和接收状态更新 |

例如，创建一个暂不自动加载的查询时，传入以下配置：

```go
options := cacheq.QueryOptions{
	Disable: true,
}
```

禁用期间，创建查询和缓存失效都不会为该对象触发自动加载。调用 `query.SetEnabled(true)` 后，缺失或过期的数据可以开始加载。修改原来的 `options.Disable` 不会改变已有对象；禁用也不会取消正在运行的共享请求。

## FetchOptions

传入方式为 `Fetch(ctx, client, key, load, options)`。配置只对本次调用有效，不修改 Client 默认值，也不会保存为该键后续调用的配置。

例如，本次不重试，并设置三秒的加载超时：

```go
retries := 0
timeout := 3 * time.Second
options := cacheq.FetchOptions{
	Retry:   &retries,
	Timeout: &timeout,
}
```

将 `options` 作为 `Fetch` 的最后一个参数。`FetchOptions` 没有 `Disable` 字段；调用 `Fetch` 本身就表示需要获取数据，是否发起加载由缓存状态决定。

## 共有字段

以下字段同时适用于 `QueryOptions` 和 `FetchOptions`。

| 字段 | 类型 | nil 时 | 设置后的行为 |
| --- | --- | --- | --- |
| `StaleTime` | `*time.Duration` | 继承 Client 新鲜期 | 从数据的 `UpdatedAt` 计算新鲜期；零值表示立即过期 |
| `Retry` | `*int` | 继承 Client 重试次数 | 首次失败后的追加尝试次数；零值或负数不重试 |
| `RetryIf` | `func(error) bool` | 继承 Client 错误筛选函数 | 返回 true 才允许追加尝试，仍受次数和终止错误约束 |
| `RetryDelay` | `func(int) time.Duration` | 继承 Client 重试等待策略 | 返回追加尝试前的等待时间；参数从 1 开始 |
| `Timeout` | `*time.Duration` | 继承 Client 加载超时 | 限制整次加载，包括排队、执行和重试等待；零值或负数不额外设置超时 |

指针字段用于区分“继承默认值”和“显式设置零值”。例如 `Retry: nil` 继承默认重试次数，指向 0 则关闭重试。即使 `Timeout` 为零，调用者 context 的截止时间和取消仍然有效。

取消、截止时间错误、`ErrQueueFull` 和 `ErrBatcherClosed` 不重试，包括包装后的错误，也不会调用 `RetryIf`。`RetryIf` 仅在还有重试次数且错误可重试时执行。传入始终返回 true 的函数可以覆盖 Client 的筛选条件，但不能绕过终止错误。

`RetryIf` 和 `RetryDelay` 在 Client 锁外执行，可以调用 Client API。应用需自行同步回调捕获的共享可变数据。

## 默认值与覆盖规则

- 不传配置或传空配置时，使用 Client 默认值；`QueryOptions.Disable` 默认为 false。
- 传入多份配置时，仅最后一份生效，不合并各份字段。最后一份中为 nil 的字段继承 Client 默认值。
- 创建 `Query` 或调用 `Fetch` 时会复制指针指向的值。之后修改原变量不会改变已有配置。
- 同一个键仍共享数据和请求。不同配置不创建独立缓存，也不替代键中的查询参数或身份范围。

## 共享请求的配置

新加载使用发起者的加载函数、重试配置和超时。后加入的同键调用不会覆盖这些设置；加入者的 context 可以结束自己的等待，但其 `Timeout` 配置只用于自己发起的新加载。

失效触发自动刷新时，使用最早创建且仍启用的查询对象配置。该对象关闭或禁用后，由下一个启用对象负责后续自动刷新。手动 `Refetch` 发起新加载时使用当前查询对象配置。

## 缓存策略限制

`StaleTime` 只调整普通缓存的新鲜度判断，不触发轮询。时间经过不会自动发送通知或启动请求；`Snapshot()` 在读取时重新判断状态。`Get`、`Prefetch` 和未传配置的 `Fetch` 使用 Client 默认新鲜期。

显式失效和未写入新数据的刷新失败，会使普通缓存对所有查询变旧。`FetchWithExpiry` 返回的绝对截止时间不受 `StaleTime` 覆盖影响，包括显式缓存的兜底数据和错误。

缓存容量、`MaxAge`、GC 和时钟由 Client 管理。较长的 `StaleTime` 不能延长 `MaxAge` 限制的数据可用时间。

## 兼容性

`FetchWithOptions` 保留为已弃用的兼容入口，新代码直接使用 `Fetch(..., options)`。原来的四参数 `Fetch` 直接调用无需修改。v0.2.0 为 `Fetch` 增加了可变参数；如果将 `Fetch[V]` 作为固定四参数函数值传递，需要调整函数类型，或使用保持原签名的闭包转调。

旧的 `QueryOptions.Enabled *bool` 已替换为 `Disable bool`。原值为 nil 或指向 true 时省略 `Disable`，指向 false 时使用 `Disable: true`；已有 `SetEnabled(bool)` 调用不变。

## 相关文档

- [查询与条件请求](queries.zh-CN.md)：`Query`、`Fetch`、状态订阅和启用控制。
- [缓存与生命周期](cache.zh-CN.md)：Client 默认配置、过期和资源管理。
- [缓存失效](invalidation.zh-CN.md)：关联查询的失效与刷新。
