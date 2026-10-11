# 写入与关联失效

[English](mutations.en-US.md) · [原子更新](updates.zh-CN.md) · [缓存失效](invalidation.zh-CN.md)

`Mutation` 为业务写入创建类型安全句柄，管理执行状态和成功后的缓存同步。写入不会自动重试或去重。后端事务、授权和幂等性仍由应用负责。

## 可运行示例

在已导入 cacheq 的 Go 模块中保存以下程序，运行 `go run .`。示例使用本地值完成写入，无需外部服务。

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main separates a committed write from subsequent cache synchronization.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	if err := cacheq.Set(client, "users", []string{"Alice"}); err != nil {
		panic(err)
	}
	change := cacheq.Mutation(
		client,
		func(_ context.Context, name string) (string, error) { return name, nil },
		cacheq.MutationOptions[string, string]{
			OnSuccess: func(_ string, name string) error {
				return cacheq.Set(client, "user:42", name)
			},
			Invalidate:        func(string, string) any { return []any{"users"} },
			InvalidateOptions: cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone},
		},
	)
	defer change.Close()
	result, err := change.Mutate(context.Background(), "Bob")
	if err != nil {
		panic(err)
	}
	if result.SyncErr != nil {
		panic(result.SyncErr)
	}
	fmt.Println(result.Data)
	fmt.Println(cacheq.Get[[]string](client, "users").Stale)
}
```

预期输出：

```text
Bob
true
```

## 执行与状态

`Mutator[I, V]` 为 `func(context.Context, I) (V, error)`。`Mutation` 不启动工作；`Mutate(ctx, input)` 执行一次，并等待写入和同步回调完成。多份 `MutationOptions` 以最后一份为准。配置会被复制，回调捕获的应用状态仍需自行同步。

`Snapshot()` 和 `Updates()` 提供 `MutationSnapshot[V]`，包含 `Status`、`Data`、`HasData`、`Err` 和 `SyncErr`。初始状态为 `Idle`；执行时发布 `Pending` 并清空上次结果。成功的零值同样是有效数据。慢消费者可能跳过中间状态。

同一个句柄只允许一个调用执行，包括缓存同步阶段。重叠调用返回 `ErrMutationBusy`，不排队、不修改当前状态。完成后重复相同输入会再次执行写入。不同句柄可并发执行，不按资源键串行化。

## 同步契约

业务写入返回成功后：

1. 存在 `OnSuccess(input, result)` 时执行本地同步。权威数据使用 `Set`，转换已有数据使用 [Update](updates.zh-CN.md)。
2. 求值 `Invalidate(input, result)`，将非 nil 目标传给 `Client.Invalidate`。目标支持单键、`[]any` 或 `func(any) bool`；nil 跳过失效。
3. 将同步错误合并到 `MutationResult.SyncErr` 和 `MutationSnapshot.SyncErr`，保留 `errors.Is` 和 `errors.As` 判断能力。

`OnSuccess` 失败仍会继续失效操作。两个回调均在内部锁外运行，可以调用 Client API。业务写入失败时不执行这两个回调。

`Mutate` 返回的 error 表示准入失败或业务执行失败。**error 为 nil 表示写入成功，即使 `SyncErr` 非 nil。** 此时应修复或使缓存失效，不应仅因为同步失败就重复已提交的业务写入。同步不提供跨键或后端事务。

数据库事务必须在提交成功后才返回写入成功。缓存键和失效条件必须包含租户或用户范围。库不会自动推断标签或数据关联关系。

失效沿用现有契约：默认后台刷新存在启用订阅者的查询；`RefetchNone` 只标记过期。Mutation 完成不等待这些刷新。加载期间发生失效时，已有加载完成后保持过期，不自动补发下一次加载。详见[缓存失效契约](invalidation.zh-CN.md)。

## 取消与执行预算

调用方取消、`Close` 和 Client 关闭都会取消写入 context。回调需要响应取消；关闭不会等待忽略取消的回调。Client 关闭后异步关闭 Mutation 更新通道；显式 `Close` 立即关闭通道，且可重复调用。

取消不能证明远端写入已回滚。若回调在并发取消后仍返回成功，缓存同步仍会执行，返回的业务 error 为 nil，快照记录成功。已关闭句柄不再发送通知。若 Client 已关闭，缓存同步可能单独返回 `SyncErr`。

`MutationOptions.Executor` 可覆盖 Client 的[执行预算](execution.zh-CN.md)，nil 表示继承。名额只覆盖写入回调，在缓存同步前释放。队列满返回 `ErrQueueFull`，不执行写入。Client 的查询重试和超时默认值不作用于 Mutation；使用调用方 context 设置截止时间。

## 错误与验证

nil 写入函数返回 `ErrNoMutator`；已释放句柄返回 `ErrMutationClosed`；已关闭客户端返回 `ErrClosed`。构造错误可通过状态和执行读取。开始前已经取消的调用不会修改状态或运行回调。

```sh
go test -race ./... -run TestMutation -count=1
```

[测试](../mutation_test.go)覆盖成功同步、关联查询、重复输入、写入失败、同步失败、重叠调用、取消、关闭和迟到的成功写入。
