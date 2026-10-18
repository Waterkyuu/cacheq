# 执行并发预算

[English](execution.en-US.md) · [批量加载](batching.zh-CN.md) · [写入与关联失效](mutations.zh-CN.md)

`Executor` 限制同时执行的加载或写入数量，并为等待执行的任务设置队列上限。它适用于保护数据库、HTTP 服务等并发容量有限的数据源。访问同一数据源的多个 Client、Mutation 或 Batcher 可以共享同一个 Executor。

## 快速开始

以下示例只允许一个加载执行，不保留排队位置。商品 42 的加载占用名额期间，商品 43 的加载被拒绝；第一个加载完成后，名额被释放。

将完整程序保存为已导入 soroi 的 Go 模块中的 `main.go`，运行 `go run .`。示例通过 channel 控制第一个加载的完成时间，无需外部服务或依赖短暂的计时等待。

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Waterkyuu/soroi"
)

// main rejects a second product load while the only execution slot is occupied.
func main() {
	budget, err := soroi.NewExecutor(soroi.ExecutionOptions{
		MaxConcurrent: 1,
		MaxQueued:     0,
	})
	if err != nil {
		panic(err)
	}
	client := soroi.NewClient(soroi.Options{
		Executor:  budget,
		StaleTime: time.Minute,
	})
	defer client.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	loadFirst := func(ctx context.Context) (string, error) {
		close(started)
		select {
		case <-release:
			return "product 42", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	first := soroi.Query(client, "product:42", loadFirst)
	defer first.Close()
	<-started

	_, err = soroi.Fetch(
		context.Background(),
		client,
		"product:43",
		func(context.Context) (string, error) { return "product 43", nil },
	)
	fmt.Println("second rejected:", errors.Is(err, soroi.ErrQueueFull))
	fmt.Println("active:", budget.Stats().Active)

	close(release)
	value, err := soroi.Fetch(context.Background(), client, "product:42", loadFirst)
	if err != nil {
		panic(err)
	}
	fmt.Println("first:", value)
	stats := budget.Stats()
	fmt.Printf("finished: active=%d queued=%d rejected=%d\n", stats.Active, stats.Queued, stats.Rejected)
}
```

预期输出：

```text
second rejected: true
active: 1
first: product 42
finished: active=0 queued=0 rejected=1
```

`Options.Executor` 将预算应用到 Client 的加载函数。第一个 `Query` 在后台加载，`started` 确保它已占用唯一名额。此时另一个键需要加载，但 `MaxQueued: 0` 不允许等待，所以 `Fetch` 返回 `ErrQueueFull`，不会调用商品 43 的加载函数。

关闭 `release` 后，第一个加载可以完成。最后的 `Fetch` 共用商品 42 的请求或缓存，不额外执行加载函数。`Rejected` 保留累计拒绝次数，因此结束后仍为 1。

## API 片段前提

以下片段放在返回 `error` 的业务函数中，并导入所用的标准库包。`budget` 是通过 `NewExecutor` 创建的预算；`ctx` 是非 nil 的调用者 context。批量示例还需要业务提供的 `loadProducts`，其签名在对应章节说明。各配置片段独立使用。

## `NewExecutor`：设置并发与排队上限

允许少量请求等待时，设置正数 `MaxQueued`：

```go
budget, err := soroi.NewExecutor(soroi.ExecutionOptions{
	MaxConcurrent: 4,
	MaxQueued:     16,
})
if err != nil {
	return err
}
```

这个配置最多允许 4 个回调同时执行，另外 16 个等待。运行名额用完后，新任务进入队列；队列也满时立即返回 `ErrQueueFull`。等待者按进入队列的顺序获得释放的名额。

| `ExecutionOptions` 字段 | 约束 | 说明 |
| --- | --- | --- |
| `MaxConcurrent` | 必须大于 0 | 同时执行回调的数量上限 |
| `MaxQueued` | 不能为负数 | 等待位置上限；0 表示没有名额时立即拒绝 |

通过 `NewExecutor` 创建预算，不能使用未配置的零值。容量创建后不可修改。Executor 不拥有后台 goroutine 或定时器，无需调用 `Close`。

## 多个 Client 共享预算

将同一个 `budget` 传入多个 Client：

```go
first := soroi.NewClient(soroi.Options{Executor: budget})
second := soroi.NewClient(soroi.Options{Executor: budget})
defer first.Close()
defer second.Close()
```

两个 Client 仍有各自的缓存，但执行名额来自同一个预算。快速开始的预算只有一个名额，因此两个 Client 合计也只能执行一个回调。分别创建两个 Executor 则会得到两组独立上限。

应在应用入口按数据源创建并注入预算。关闭一个 Client 会取消它拥有的工作，不会关闭共享 Executor 或其他 Client。

## 配置位置

| 配置字段 | 限制范围 | 默认行为 |
| --- | --- | --- |
| `Options.Executor` | Client 每一次加载尝试 | nil，不限制执行 |
| `QueryOptions.Executor` | 本次调用或查询对象发起的加载 | nil，继承 Client 的 Executor |
| `QueryOptions.DisableExecutor` | 跳过外层 Client 加载预算 | false，保留继承或覆盖配置 |
| `MutationOptions.Executor` | Mutation 的业务写入回调 | nil，继承 Client 的 Executor |
| `BatchOptions.Executor` | 收集完成后的实际批量回调 | nil，不限制执行 |

缓存命中不执行加载函数，不占用名额。同键的多个调用共用一次加载，只占用一个名额。不同键的加载分别获取名额。

`DisableExecutor: true` 优先于 `Executor`，只影响外层加载，不会禁用 Batcher 自身的预算。共享加载保留发起者的预算，后加入者禁用自己的外层预算不会改变正在执行的加载。查询对象的自动加载、启用、失效和手动刷新均保留这一配置。

Mutation 在业务写入回调结束后释放名额，再执行本地缓存同步和关联失效。显式设置 `MutationOptions.Executor` 可以让写入使用独立预算；nil 表示继承，不能用来禁用 Client 已配置的预算。

## 与批量加载一起使用

Client 通过 `Batcher.Fetcher` 加载时，应优先把预算配置在 `BatchOptions.Executor`，让不同键先收集成批次，再限制实际的后端调用。

以下片段中的 `loadProducts` 由业务实现，签名为 `func(context.Context, []int) (map[int]soroi.BatchResult[string], error)`。完整批量函数示例见[自动批量加载](batching.zh-CN.md)。

```go
products, err := soroi.NewBatcher(ctx, loadProducts, soroi.BatchOptions{
	Wait:         2 * time.Millisecond,
	MaxBatchSize: 100,
	Executor:     budget,
})
if err != nil {
	return err
}
defer products.Close()

client := soroi.NewClient(soroi.Options{StaleTime: time.Minute, Executor: budget})
defer client.Close()
value, err := soroi.Fetch(ctx, client, "product:42", products.Fetcher(42), soroi.QueryOptions{
	DisableExecutor: true,
})
if err != nil {
	return err
}
fmt.Println(value)
```

`MaxBatchSize` 限制一批包含多少个唯一键，Executor 限制多少个批量回调同时执行。一个批次被拒绝时，其中所有键收到 `ErrQueueFull`，不会执行批量函数。

此 Client 的普通查询与实际批量回调可以共用同一个预算。批量 Fetch 跳过外层准入，不占用逐键名额等待收集；未覆盖配置的普通查询仍使用 Client 预算。

通过查询对象订阅批量数据时，在对应对象上配置：

```go
query := soroi.Query(client, "product:43", products.Fetcher(43), soroi.QueryOptions{
	DisableExecutor: true,
})
defer query.Close()
```

如果外层回调已经持有 Batcher 的 Executor，`Load` 在收集或加入键之前返回 `ErrExecutionReentry`。Client 不重试这一错误，包括包装后的错误，也不会调用 `RetryIf`。通过回调 context 再次获取同一预算的嵌套查询和 Mutation 同样被拒绝。这种拒绝不会增加 `Stats().Rejected`，该计数只记录过载。

当回调 context 持有已有加载使用的 Executor 时，`Fetch`、`Prefetch` 和 `QueryHandle.Refetch` 也会拒绝同步加入。检查使用发起者的预算，因此覆盖或禁用加入者的预算不能绕过检查。新鲜缓存命中无需获取名额，仍可正常返回；已有加载使用其他 Executor 或禁用预算时，仍允许加入。

检测依赖传递回调收到的 context。替换为 `context.Background()` 或启动独立拥有生命周期的后台工作会丢失预算所有权信息。应避免嵌套依赖已持有的预算，在实际后端边界配置容量。不同预算仍各自生效；在多个相互等待的层配置预算仍可能形成依赖环。

## 过载与重试

通过 `errors.Is(err, soroi.ErrQueueFull)` 判断预算已满。该错误意味着此层回调尚未执行，应用可以向调用者报告繁忙，或由上层安排稍后再次调用。

Client 不自动重试 `ErrQueueFull`，也不会为它调用 `RetryIf`，包括批量函数或业务加载函数返回的包装错误。

其他允许重试的查询错误仍遵循原有重试配置。每次尝试重新获取名额；一次回调结束后，在判断是否重试和等待重试之前释放名额，因此退避期间不会占用执行容量。

## 取消与超时

排队时间计入查询的整体超时。拥有加载的调用者取消、`Client.Cancel` 或 Client 关闭会取消相应排队工作。仅加入已有共享加载的调用者取消时，只结束自己的等待，不取消共享加载。

回调开始运行后，只有真正返回才释放名额。context 取消不能强制终止忽略取消信号的函数，因此业务加载和写入函数仍需响应 context。

Mutation 使用调用者 context 的截止时间，不继承 Client 的查询超时。批量加载保持自己的取消规则：关闭 Batcher，或某批次所有等待者都取消，会取消该批次的排队等待。单个等待者取消不会影响其他等待者。

排队中的查询仍显示 `Fetching`，Mutation 显示 `Pending`，快照没有单独的排队状态。

## `Stats`：查看执行与排队情况

`budget.Stats()` 返回整个共享预算的统计，而不是某一个 Client 的统计。

| 字段 | 含义 |
| --- | --- |
| `Active` | 已分配的执行名额，包括取消后尚未返回的回调 |
| `Queued` | 当前等待数量；取消清理完成前可能短暂包含已取消任务 |
| `Rejected` | 因容量已满而返回 `ErrQueueFull` 的累计次数 |

名额移交给等待者后即计入 `Active`。快速开始中，第一个加载未结束时 `Active` 为 1；结束后为 0。

Client 的 `Stats().Loads` 统计逐键逻辑加载，包括排队或被拒绝的加载。`Retries` 和诊断事件的 `Attempts` 统计实际进入的 Client 加载函数；首次调用前就被拒绝时，尝试数为 0。逐键 Fetcher 次数不等于批量后端调用次数，Mutation 也不计入 Client 查询统计。

## 错误与限制

| 条件 | 错误 |
| --- | --- |
| `MaxConcurrent` 非正数或 `MaxQueued` 为负数 | `ErrInvalidExecutionOptions` |
| 执行名额与排队容量均已满 | `ErrQueueFull` |
| 回调 context 已持有所请求的预算 | `ErrExecutionReentry` |
| 排队期间 context 取消或超时 | `context.Canceled` 或 `context.DeadlineExceeded` |

Executor 限制回调的同时执行数量和等待数量，不限制每秒请求数。它不提供持久化任务队列，也不限制缓存字节数、订阅数量、批量收集窗口中的键数或瞬时 goroutine 数。缓存容量和保留时间仍由 Client 配置管理。

## 相关文档

- [缓存与生命周期](cache.zh-CN.md)：配置加载超时、重试、缓存容量和取消。
- [自动批量加载](batching.zh-CN.md)：收集不同键并执行批量后端请求。
- [写入与关联失效](mutations.zh-CN.md)：为写入设置预算，并在成功后同步关联查询。
