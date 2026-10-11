# 执行并发预算

[English](execution.en-US.md) · [批量加载](batching.zh-CN.md) · [写入与关联失效](mutations.zh-CN.md)

`Executor` 限制同时执行的回调数量和排队数量。访问同一后端的多个 Client、Mutation 句柄或 Batcher 可以共享一个实例。它不提供每秒请求速率限制、任务持久化或后台工作池。

## 可运行示例

在已导入 cacheq 的 Go 模块中保存以下程序，运行 `go run .`。

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main shares a bounded executor with cached query loads.
func main() {
	budget, err := cacheq.NewExecutor(cacheq.ExecutionOptions{
		MaxConcurrent: 4,
		MaxQueued:     16,
	})
	if err != nil {
		panic(err)
	}
	client := cacheq.NewClient(cacheq.Options{
		Executor:  budget,
		StaleTime: time.Minute,
	})
	defer client.Close()
	value, err := cacheq.Fetch(
		context.Background(),
		client,
		"answer",
		func(context.Context) (int, error) { return 42, nil },
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	fmt.Println(budget.Stats().Active, budget.Stats().Queued)
}
```

预期输出：

```text
42
0 0
```

## 配置与归属

| 配置 | 契约 |
| --- | --- |
| `ExecutionOptions.MaxConcurrent` | 同时执行回调的正数上限 |
| `ExecutionOptions.MaxQueued` | 非负排队容量；零表示名额耗尽时立即拒绝 |
| `Options.Executor` | 可选的共享查询预算；nil 保留不限制执行的行为 |
| `BatchOptions.Executor` | 收集完成后，对每次实际批量回调设置可选预算 |
| `MutationOptions.Executor` | 可选的写入预算；nil 继承 Client 预算 |

非法容量使 `NewExecutor` 返回 `ErrInvalidExecutionOptions`。必须使用该构造函数；零值不是已配置的预算。配置会被复制，容量不能动态修改。Executor 不持有定时器或 goroutine，无需 `Close`。生命周期继续由 Client 和调用方 context 管理。

## 准入、取消与重试

- 新鲜缓存命中和同键请求的加入者不额外占用名额。
- 等待者按 FIFO 顺序获得名额。执行与排队容量均耗尽时，返回 `ErrQueueFull`，不调用回调。应用可使用 `errors.Is` 判断。
- Client 不重试 `ErrQueueFull`，包括批量加载器或业务加载函数包装后的错误；不会为该错误调用 `RetryIf`。
- 每次查询加载尝试单独获取名额，在执行重试判断和退避前释放。下一次尝试重新参与排队。
- 排队时间计入查询现有整体超时或调用方截止时间。排队调用取消、`Client.Cancel` 和 Client 关闭会移除等待的查询工作。
- 正在运行的回调必须真正返回后才释放名额。否则忽略取消的回调会使实际并发超过预算。
- Mutation 仅在写入回调期间占用名额，在本地同步和失效前释放。写入使用调用方 context 截止时间，不继承 Client 的查询超时和重试默认值。
- 等待执行时，查询仍显示 `Fetching`，Mutation 显示 `Pending`。现有快照不新增排队状态。

## 批量配置位置与嵌套调用

Client 加载函数使用 `Batcher.Fetcher` 时，优先只配置 `BatchOptions.Executor`。这样不同键可以进入收集窗口，同时实际后端批量调用受到限制。`MaxBatchSize` 继续限制单批唯一键数量，不限制回调并发。

多个 Batcher 可以共享一个 Executor。批次被拒绝时，其中所有键收到 `ErrQueueFull`。关闭 Batcher，或取消某批次全部等待者，会取消该批次的排队等待。

不要在外层 Client 和它使用的 Batcher 同时配置同一个 Executor：外层加载可能占满名额，并等待需要同一名额的内部批量回调。加载函数或写入函数递归调用使用相同预算的查询，也有相同限制。应在实际后端边界配置预算，或使用具有明确依赖顺序的独立预算。限制外层逐键加载也可能缩小批次大小。

## 统计与资源边界

`Executor.Stats()` 返回 `Active`、`Queued` 和累计 `Rejected`。Active 包含已经移交给等待者的名额，以及取消后仍未返回的回调。Queued 在清理执行前可能短暂包含已取消调用。统计属于共享 Executor，不属于单个 Client。

Client 的 `Stats().Loads` 仍统计逻辑共享加载，包括等待或被拒绝的加载。`Retries` 和诊断 `Attempts` 按 Client 边界实际进入的加载回调计数；首次尝试前被拒绝的查询，其尝试数为零。批量 Fetcher 的调用不等于实际后端批次。Mutation 不计入 Client 查询统计。

预算限制回调并发和其准入队列，不限制缓存字节数、订阅数量、批量收集窗口、瞬时 goroutine 数或单次回调成本。仍需配置缓存保留策略并响应 context 取消。

## 验证

```sh
go test -race ./... -run 'Test(Executor|Execution|BatcherExecution|MutationExecution)' -count=1
```

[Executor 测试](../executor_test.go)、[查询集成](../execution_test.go)、[批量集成](../batcher_execution_test.go)和[Mutation 集成](../mutation_execution_test.go)验证过载、FIFO、取消竞争、共享预算、批量配置位置，以及在同步或重试退避前释放名额。
