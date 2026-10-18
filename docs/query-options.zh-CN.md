# 统一查询配置与可复用查询定义

[English](query-options.en-US.md)

`QueryOptions` 是 `Query`、`Fetch` 和 `Prefetch` 共用的配置，用于覆盖 Client 默认的新鲜期、重试、超时和执行预算。`Disable` 只控制 Query 的自动加载；显式 `Fetch`、`Prefetch` 和查询对象的 `Refetch` 仍会执行。

`QueryDefinition(key, fetcher, options...)` 返回 `(Definition[V], error)`，从加载函数推断结果类型 `V`。使用可复用定义前必须处理构造错误。创建定义不会加载数据，也不会创建缓存或订阅。

## 快速开始：复用同一份配置

以下完整示例为三个入口使用同一份一小时新鲜期配置。禁用自动加载的订阅接收 `Prefetch` 显式加载的数据，`Fetch` 复用这份新鲜缓存。Client 默认值保持不变。将程序保存为已导入 soroi 的 Go 模块中的 `main.go`，运行 `go run .`。

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/soroi"
)

// main reuses one loading policy across subscription, prefetch, and explicit reads.
func main() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	defer client.Close()
	ctx := context.Background()
	load := func(context.Context) (string, error) { return "hello", nil }
	hour := time.Hour
	options := soroi.QueryOptions{Disable: true, StaleTime: &hour}
	query := soroi.Query(client, "greeting", load, options)
	defer query.Close()
	if err := soroi.Prefetch(ctx, client, "greeting", load, options); err != nil {
		panic(err)
	}
	value, err := soroi.Fetch(ctx, client, "greeting", load, options)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	fmt.Println("observed:", query.Snapshot().Data)
}
```

预期输出：

```text
hello
observed: hello
```

## 自动加载与显式读取

`Query(client, key, load, options)` 在创建时复制配置，后续自动加载、`SetEnabled` 和手动 `Refetch` 继续使用该配置。

| 入口 | `Disable: true` 的作用 |
| --- | --- |
| `Query` | 订阅共享状态更新，不启动自动加载 |
| `Fetch` | 执行显式读取，可以直接使用新鲜缓存 |
| `Prefetch` | 使用同一套加载策略显式预热缓存 |
| 查询对象的 `Refetch` | 禁用自动加载时仍允许显式刷新 |

省略配置、传入 `QueryOptions{}` 或 `Disable` 为 false 时，允许自动加载。禁用对象不会取消共享请求，也不会禁用其他对象。修改原始配置不会改变已有对象；后续启停使用 `SetEnabled(bool)`。

`Fetch(ctx, client, key, load, options)` 和 `Prefetch(ctx, client, key, load, options)` 的配置只对本次调用有效，二者都忽略 `Disable`。它们不修改 Client 默认值，也不注册该键后续调用的策略。`Prefetch` 返回加载错误，不返回数据，也不创建持续订阅。

## `QueryDefinition`：集中定义类型化查询

`QueryDefinition` 接收查询键、类型化加载函数和零份或一份 `QueryOptions`，返回 `(Definition[V], error)`，不会执行加载函数或校验查询键。查询键和结果类型的校验在操作使用 Client 时执行。省略配置或传入 `QueryOptions{}` 时使用 Client 默认值；为 nil 的字段继承对应默认值。多份配置会在构造时立即返回零值 `Definition[V]` 和 `ErrTooManyOptions`，定义内部不保留错误状态。在使用返回的定义前，可以配置其 `Shared` 和 `Tags` 字段。

返回的查询定义包含五个导出字段：

| 字段 | 职责 |
| --- | --- |
| `Key` | 用非 nil、可比较的键标识查询，包含全部查询参数和身份范围 |
| `Fetcher` | 通过业务的 HTTP、数据库或其他依赖加载 `V` |
| `Shared` | 通过可选[共享结果缓存](shared-cache.zh-CN.md)复用序列化结果；nil 保持本地缓存 |
| `Tags` | 声明用于关联查询分组的标签；登记与生命周期见[查询标签](query-tags.zh-CN.md) |
| `Options` | 将同一份 `QueryOptions` 用于读取、订阅和预加载 |

每次操作显式传入 Client，由 Client 管理缓存、共享请求和关闭生命周期。同一份定义可以用于多个 Client，各 Client 的缓存仍相互独立。返回的查询对象由调用者负责关闭。

以下完整示例展示定义构造、预加载、缓存复用和显式失效：

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/soroi"
)

// main reuses a typed definition without creating a second cache or automatic loading.
func main() {
	client := soroi.NewClient(soroi.Options{})
	defer client.Close()
	ctx := context.Background()
	hour := time.Hour
	calls := 0
	definition, err := soroi.QueryDefinition("greeting", func(context.Context) (string, error) {
		calls++
		return "hello", nil
	}, soroi.QueryOptions{Disable: true, StaleTime: &hour})
	if err != nil {
		panic(err)
	}
	query := definition.Query(client)
	defer query.Close()
	fmt.Println("initial calls:", calls)
	if err := definition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	value, err := definition.Fetch(ctx, client)
	if err != nil {
		panic(err)
	}
	fmt.Println(value, "calls:", calls)
	if err := client.Invalidate(definition.Key); err != nil {
		panic(err)
	}
	if _, err := definition.Fetch(ctx, client); err != nil {
		panic(err)
	}
	fmt.Println("refreshed calls:", calls)
}
```

预期输出：

```text
initial calls: 0
hello calls: 1
refreshed calls: 2
```

| 方法 | 返回结果 |
| --- | --- |
| `definition.Fetch(ctx, client)` | `(V, error)`，契约与 `Fetch` 相同 |
| `definition.Query(client)` | `*QueryHandle[V]`，契约与 `Query` 相同 |
| `definition.Prefetch(ctx, client)` | 加载错误，契约与 `Prefetch` 相同 |

原 `QueryDefinition` 结构体已替换为函数。创建定义时改用函数调用并处理返回的错误；显式声明变量、参数或返回值类型时使用 `Definition[V]`。类型化加载函数可以推断 `V`，包括有类型的 nil 加载函数。直接传入 nil 时没有可供推断的结果类型，必要时需要显式指定类型参数，例如 `soroi.QueryDefinition[string]("greeting", nil)`。nil 加载函数遵循已有操作契约，无法加载缺失或过期的数据。

方法直接使用定义中的键、加载函数和配置。如果需要定制一个消费者，可以先复制定义，再修改副本的配置并调用方法。修改 Key、Fetcher 或 Options 影响后续操作，不重新配置已有查询对象。后续操作声明的标签会合并到共享查询状态中。配置指针指向的值在每次操作开始时读取。业务需同步共享定义、指针值以及加载函数捕获的可变状态。

定义不会从加载函数推断查询键，不增加授权检查，也不串行业务操作。可以用普通业务函数构造带参数的定义，使键和加载函数捕获一致的参数。结果类型绑定、取消、错误及正在执行的请求共享遵循已有[查询契约](queries.zh-CN.md)。显式失效使用 `client.Invalidate(definition.Key)` 或 `client.InvalidateAndWait(ctx, definition.Key)`。

## 共有字段

以下加载策略字段同时适用于 `Query`、`Fetch` 和 `Prefetch`。

| 字段 | 类型 | 默认行为 | 设置后的行为 |
| --- | --- | --- | --- |
| `StaleTime` | `*time.Duration` | 继承 Client 新鲜期 | 从数据的 `UpdatedAt` 计算新鲜期；零值表示立即过期 |
| `Retry` | `*int` | 继承 Client 重试次数 | 首次失败后的追加尝试次数；零值或负数不重试 |
| `RetryIf` | `func(error) bool` | 继承 Client 错误筛选函数 | 返回 true 才允许追加尝试，仍受次数和终止错误约束 |
| `RetryDelay` | `func(int) time.Duration` | 继承 Client 重试等待策略 | 返回追加尝试前的等待时间；参数从 1 开始 |
| `Timeout` | `*time.Duration` | 继承 Client 加载超时 | 限制整次加载，包括排队、执行和重试等待；零值或负数不额外设置超时 |
| `Executor` | `*Executor` | nil，继承 Client 预算 | 新的加载尝试使用这个共享预算 |
| `DisableExecutor` | `bool` | false | 跳过外层 Client 预算，优先于 `Executor` |

指针字段用于区分“继承默认值”和“显式设置零值”。例如 `Retry: nil` 继承默认重试次数，指向 0 则关闭重试。即使 `Timeout` 为零，调用者 context 的截止时间和取消仍然有效。

取消、截止时间错误、`ErrQueueFull`、`ErrBatcherClosed` 和 `ErrExecutionReentry` 不重试，包括包装后的错误，也不会调用 `RetryIf`。`RetryIf` 仅在还有重试次数且错误可重试时执行。传入始终返回 true 的函数可以覆盖 Client 的筛选条件，但不能绕过终止错误。

`RetryIf` 和 `RetryDelay` 在 Client 锁外执行，可以调用 Client API。应用需自行同步回调捕获的共享可变数据。

## 执行预算覆盖

使用 `Executor` 为单个查询选择其他后端预算；如果 Batcher 已经限制实际批量回调，则使用 `DisableExecutor: true`。Executor 为 nil 时继承 Client 预算，除非显式禁用。禁用外层准入不会禁用 Batcher 自身的预算。

```go
options := soroi.QueryOptions{
	DisableExecutor: true,
}
```

使用 `Batcher.Fetcher` 时，可以将同一份配置传给 `Query`、`Fetch` 或 `Prefetch`。在两层使用相同预算且传递回调 context 时，会返回 `ErrExecutionReentry`。完整配置示例与检测限制见[执行并发预算](execution.zh-CN.md)。

## 默认值与覆盖规则

查询、读取、预加载、查询定义构造、Mutation 构造和失效入口允许省略配置，最多传入一份。省略配置和传入空结构体使用相同默认值。所有覆盖项应放在同一个结构体中；传入多份配置会在运行时报告 `ErrTooManyOptions`，且不产生操作副作用。弃用入口 `FetchWithOptions` 继续保持移除，统一使用 `Fetch`。

- 省略配置或传入 `QueryOptions{}` 时，使用 Client 默认值；`QueryOptions.Disable` 默认为 false。
- 每次操作最多接受一份配置；多份配置在运行时被拒绝，不会合并或丢弃。为 nil 的策略字段继承 Client 默认值。
- 创建 `Query` 或调用 `Fetch`、`Prefetch` 时会复制时间与重试次数指针指向的值。查询定义保留配置指针，直到这些操作实际应用配置。之后修改原变量不会改变已有配置。Executor 引用保持共享，不复制预算容量。
- 同一个键仍共享数据和请求。不同配置不创建独立缓存，也不替代键中的查询参数或身份范围。

## 配置错误

传入多份配置会报告 `ErrTooManyOptions`，支持 `errors.Is` 判断。被拒绝的操作不会加载数据、注册订阅或标签、使本地状态失效，也不会访问共享存储。

| 入口 | 错误报告方式 |
| --- | --- |
| `Fetch`、`Prefetch` 和失效方法 | 返回错误 |
| `Query` | 失败句柄的 `Snapshot().Err`、`Updates()` 初始状态和 `Refetch` |
| `Mutation` | 失败句柄的 `Snapshot().Err`、`Updates()` 初始状态和 `Mutate` |
| `QueryDefinition` | 在构造时立即返回零值 `Definition[V]` 和 `ErrTooManyOptions` |

无效的查询和 Mutation 句柄发布一次错误状态后关闭更新通道。`QueryDefinition` 在创建时直接报告配置错误。设置字段或调用方法前必须检查返回的错误；定义不保留构造错误。

## 共享请求的配置

新加载使用发起者的加载函数、重试配置、超时和执行预算。后加入的同键调用不会覆盖这些设置；加入者的 context 可以结束自己的等待，但其 `Timeout` 配置只用于自己发起的新加载。

失效触发自动刷新时，使用最早创建且仍启用的查询对象配置。该对象关闭或禁用后，由下一个启用对象负责后续自动刷新。手动 `Refetch` 发起新加载时使用当前查询对象配置。

## 缓存策略限制

`StaleTime` 只调整普通缓存的新鲜度判断，不触发轮询。时间经过不会自动发送通知或启动请求；`Snapshot()` 在读取时重新判断状态。`Get` 和省略配置或传入空配置的 `Fetch`、`Prefetch` 使用 Client 默认新鲜期。

显式失效和未写入新数据的刷新失败，会使普通缓存对所有查询变旧。`FetchWithExpiry` 返回的绝对截止时间不受 `StaleTime` 覆盖影响，包括显式缓存的兜底数据和错误。

缓存容量、`MaxAge`、GC 和时钟由 Client 管理。较长的 `StaleTime` 不能延长 `MaxAge` 限制的数据可用时间。

## 相关文档

- [查询与条件请求](queries.zh-CN.md)：`Query`、`Fetch`、状态订阅和启用控制。
- [缓存与生命周期](cache.zh-CN.md)：Client 默认配置、过期和资源管理。
- [缓存失效](invalidation.zh-CN.md)：关联查询的失效与刷新。
- [执行并发预算](execution.zh-CN.md)：共享后端容量并配置批量查询覆盖。
