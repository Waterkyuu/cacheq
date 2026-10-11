# 写入与关联失效

[English](mutations.en-US.md) · [原子更新](updates.zh-CN.md) · [缓存失效](invalidation.zh-CN.md)

`Mutation` 执行业务写入，提供执行状态，并在写入成功后更新关联缓存。例如修改用户姓名后，可以直接更新用户详情，同时让用户列表重新加载。

业务负责实现 HTTP 请求或数据库写入；cacheq 负责组织写入后的缓存同步。创建 Mutation 不会发起请求，调用 `Mutate` 才开始执行。

## 快速开始

以下示例修改用户姓名，将返回的用户写入详情缓存，并刷新已订阅的用户列表。将完整程序保存为已导入 cacheq 的 Go 模块中的 `main.go`，运行 `go run .`。

示例使用 `atomic.Value` 保存本地后端数据，无需外部服务；读写可以与列表的后台刷新并发执行。

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// User contains the profile returned by the backend.
type User struct {
	// Name contains the user's display name.
	Name string
}

// main synchronizes a detail cache and an observed list after changing a user.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	ctx := context.Background()

	var backend atomic.Value
	backend.Store(User{Name: "Alice"})
	getUsers := func(context.Context) ([]User, error) {
		return []User{backend.Load().(User)}, nil
	}
	users := cacheq.Query(client, "users", getUsers)
	defer users.Close()
	before, err := cacheq.Fetch(ctx, client, "users", getUsers)
	if err != nil {
		panic(err)
	}
	fmt.Println("before:", before[0].Name)

	rename := cacheq.Mutation(
		client,
		func(ctx context.Context, name string) (User, error) {
			if err := ctx.Err(); err != nil {
				return User{}, err
			}
			user := User{Name: name}
			backend.Store(user)
			return user, nil
		},
		cacheq.MutationOptions[string, User]{
			OnSuccess: func(ctx context.Context, user User, _ string) error {
				updateErr := cacheq.Set(client, "user:42", user)
				_, refreshErr := client.InvalidateAndWait(ctx, "users")
				return errors.Join(updateErr, refreshErr)
			},
		},
	)
	defer rename.Close()
	result, err := rename.Mutate(ctx, "Bob")
	if err != nil {
		panic(err)
	}
	if result.SyncErr != nil {
		panic(result.SyncErr)
	}

	after := users.Snapshot().Data
	fmt.Println("detail:", cacheq.Get[User](client, "user:42").Data.Name)
	fmt.Println("list:", after[0].Name)
}
```

预期输出：

```text
before: Alice
detail: Bob
list: Bob
```

`rename.Mutate(ctx, "Bob")` 先修改后端数据。写入成功后，`OnSuccess` 将返回的 `User` 存入 `"user:42"`，再调用 `client.InvalidateAndWait(ctx, "users")`。列表有启用的 `Query`，因此会在回调返回前完成刷新。

`Mutate` 等待写入和 `OnSuccess` 完成。回调通过 `Client.Invalidate` 选择后台刷新，或通过 `Client.InvalidateAndWait` 选择等待刷新。本例采用等待方式，因此 `Mutate` 返回且没有同步错误时，列表快照已经包含 Bob。

## API 片段前提

以下局部片段使用快速开始中的 `client`、`rename`、`User` 和非 nil 的 `ctx`。包含 `return` 的片段放在返回 `error` 的业务函数中，并导入所用的标准库包。配置片段用于替换创建 Mutation 时传入的 `MutationOptions`。

## `Mutation` 与 `Mutate`：创建和执行写入

`Mutation(client, write, options)` 返回 `MutationHandle[I, V]`。`I` 是输入类型，`V` 是写入结果类型；快速开始中分别为 `string` 和 `User`。业务函数 `Mutator[I, V]` 的签名为 `func(context.Context, I) (V, error)`。

`Mutate(ctx, input)` 执行一次业务函数，等待成功后的缓存同步，并返回 `MutationResult[V]` 和错误。数据库操作应在事务提交成功后才返回成功。cacheq 不自动重试或合并写入；完成后再次传入相同参数，会再次执行业务函数。

同一个句柄同时只接受一次执行，包括缓存同步阶段。重叠调用返回 `ErrMutationBusy`，不会排队或改变正在执行的状态。不同句柄可以并发执行，不会按用户 ID 或其他业务键自动串行化。

## `OnSuccess`：使用写入结果更新缓存

快速开始中的写入返回完整用户详情，因此 `OnSuccess` 直接调用 `Set`，无需再请求详情接口。如果只需改变已有列表中的一项，可以在回调中使用 [Update](updates.zh-CN.md)。

回调签名为 `func(ctx context.Context, result V, input I) error`。`result` 是业务写入成功返回的数据，`input` 是传给 `Mutate` 的原始参数。context 保留调用者的值和截止时间，句柄或 Client 关闭时也会被取消。Go 要求回调满足声明的函数签名；未使用的参数可写为 `_`。

回调在业务写入成功后执行一次，写入失败时不会调用。它在 Client 锁外运行，可以调用缓存 API。回调返回前，句柄保持 `Pending`。返回错误会报告同步失败，但不会撤销已提交的业务写入。

此设计参考 [TanStack Query 的 mutation 失效方式](https://tanstack.com/query/latest/docs/framework/react/guides/invalidations-from-mutations)：在成功回调中调用查询客户端的缓存 API，并在回调内等待同步完成。Go 使用阻塞方法调用和错误返回替代 JavaScript Promise；cacheq 保留写入提交后通过独立 `SyncErr` 报告同步失败的契约。

## 在 `OnSuccess` 中刷新关联查询

直接调用 `Client.Invalidate`，传入查询键、`[]any` 或筛选条件。应用声明详情与列表的关系，Mutation 不会自动选择失效目标。固定列表键无需额外的目标计算回调：

```go
options := cacheq.MutationOptions[string, User]{
	OnSuccess: func(context.Context, User, string) error {
		return client.Invalidate("users", cacheq.InvalidateOptions{RefetchAfterLoad: true})
	},
}
```

此配置将列表标记过期并安排后台刷新；如果已有请求正在执行，还会安排一次有界补刷。`Mutate` 等待回调完成，但回调在 HTTP 刷新完成前返回。后台错误通过对应查询的状态报告。

需要延后加载时，在 Client 调用处传入 `RefetchNone`：

```go
options := cacheq.MutationOptions[string, User]{
	OnSuccess: func(context.Context, User, string) error {
		return client.Invalidate(
			[]any{"user:42", "users"},
			cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone},
		)
	},
}
```

此配置保留旧的详情和列表数据，后续 `Fetch`、新建启用的 `Query` 或手动 `Refetch` 可以重新加载。此回调不会安装写入结果。

回调负责同步顺序和错误处理。提前返回会停止剩余步骤。如果缓存更新失败后仍需尝试失效，应像快速开始一样先执行两个步骤，再用 `errors.Join` 合并错误。查询键应包含租户及用户范围。

## 提交后等待刷新

在 `OnSuccess` 中调用 `Client.InvalidateAndWait`，即可等待关联刷新。仅刷新固定列表的回调如下：

```go
options := cacheq.MutationOptions[string, User]{
	OnSuccess: func(ctx context.Context, _ User, _ string) error {
		_, err := client.InvalidateAndWait(ctx, "users")
		return err
	},
}
```

`Mutate` 等待列表的失效后刷新，包括较早请求尚未结束时的有界补刷。检查 `err` 和 `result.SyncErr` 后，通过 `users.Snapshot().Data` 读取列表。需要区分已刷新和跳过的目标时，可以检查返回的逐键结果。

等待期间，句柄保持 `Pending` 并拒绝重叠执行。仅刷新有启用查询对象的键，没有订阅或仅有禁用对象的键只标记过期；`RefetchNone` 禁止刷新。等待契约与 `Client.InvalidateAndWait` 相同，包括再次失效和本地替换错误。等待不保证不同 Mutation 句柄的业务执行顺序或服务端一致性。

使用回调提供的 context 时，`Mutate` 的 context 和句柄生命周期限制等待。写入提交后取消，如果回调返回取消错误，则通过 `SyncErr` 报告；业务返回错误仍为 nil，状态仍为 `Success`。Client 关闭可能报告 `ErrClosed` 或 context 取消，取决于先观察到哪个信号。调用者结束等待后，Client 拥有的刷新可以继续。执行预算在回调开始前释放。

较晚返回成功的写入仍会调用 `OnSuccess`，即使 context 已取消。`Client.InvalidateAndWait` 对开始前已取消的 context 直接返回，不执行失效。如果已确认的写入必须始终将缓存标记过期，应在任何可取消的等待步骤之前，独立于取消调用 `Client.Invalidate`。Mutation 不会在回调返回后隐式执行失效。

## 迁移旧的 Mutation 配置

此次 API 调整移除 `MutationOptions.Invalidate`、`InvalidateOptions` 和 `WaitForRefresh`，并将 `OnSuccess` 签名从 `func(input I, result V) error` 改为 `func(ctx context.Context, result V, input I) error`。

将目标选择和刷新配置迁移到 `OnSuccess` 内的显式 Client 调用。后台失效使用 `client.Invalidate(target, options)`，等待失效后刷新使用 `client.InvalidateAndWait(ctx, target, options)`。缓存更新和失效由同一个回调组织，执行顺序和错误合并均由回调明确控制。

## 区分写入失败与同步失败

`Mutate` 返回的 `error` 表示本次写入没有成功完成，包括调用被拒绝和业务函数返回错误。`result.SyncErr` 表示业务写入已经成功，但后续缓存更新、失效或启用的刷新等待失败。

```go
result, err := rename.Mutate(ctx, "Bob")
if err != nil {
	return err
}
if result.SyncErr != nil {
	fmt.Printf("user saved; cache synchronization failed: %v\n", result.SyncErr)
}
fmt.Println(result.Data.Name)
return nil
```

**`err == nil` 时，不能仅因为 `SyncErr` 非 nil 就重复提交业务写入。** 例如扣款已经完成、缓存更新失败时，应修复缓存或重新查询业务状态。

`OnSuccess` 返回的错误成为 `SyncErr`，支持 `errors.Is` 和 `errors.As`。回调可以使用 `errors.Join` 保留多个同步步骤的错误。发生同步错误时，结果数据仍可用，Mutation 状态仍为 `Success`。应用可记录该错误，并安排缓存修复或重新查询。

## `Snapshot` 与 `Updates`：读取执行状态

`rename.Snapshot()` 返回当前 `MutationSnapshot[User]`；`rename.Updates()` 提供初始状态和后续最新状态。

| 状态 | 含义 |
| --- | --- |
| `Idle` | 已创建，还未执行 |
| `Pending` | 正在等待执行、写入或同步缓存；上一次结果已清空 |
| `Success` | 业务写入成功，`HasData` 为 true；仍需检查 `SyncErr` |
| `Error` | 本次执行失败，原因保存在 `Err` |

成功的零值同样是有效数据。快照是一次读取的值，不会自行更新。慢消费者可能跳过中间状态，`Updates()` 不提供逐条保留的事件日志。被拒绝的重叠调用和开始前已取消的调用不会修改当前状态。

## 配置

| `MutationOptions` 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `OnSuccess` | nil | 写入提交后更新缓存及显式失效，等待回调返回 |
| `Executor` | nil，继承 Client 配置 | 限制写入回调的并发和排队数量 |

多份配置以最后一份为准，不合并字段。配置会被复制；回调捕获的应用数据仍需自行保护。缓存键和失效条件应包含影响结果的租户及用户范围。

[执行预算](execution.zh-CN.md)只在业务写入回调期间占用名额，在缓存同步前释放。Client 的查询重试和超时配置不作用于 Mutation；写入截止时间通过 `Mutate` 的 context 设置。

## 取消与关闭

调用者取消、`rename.Close()` 或 Client 关闭都会取消写入 context。业务函数需要响应取消；`Close()` 不等待忽略取消的函数返回。

显式关闭句柄会立即关闭 `Updates()`，重复关闭没有影响。Client 关闭也会异步关闭所属 Mutation 的更新通道。业务函数未结束前，`Mutate` 仍可能继续等待。

取消不能证明远端写入已经回滚。如果业务函数在取消后仍返回成功，cacheq 会继续执行缓存同步，并保留成功结果。已关闭句柄不再发送通知，但 `Snapshot()` 仍会记录最终结果；Client 已关闭时，缓存同步可能返回单独的 `SyncErr`。

## 错误处理

| 条件 | 错误或结果 |
| --- | --- |
| 写入函数为 nil | `ErrNoMutator` |
| 同一句柄正在执行 | `ErrMutationBusy` |
| 句柄已关闭 | `ErrMutationClosed` |
| Client 已关闭 | `ErrClosed` |
| 执行并发和排队容量均已满 | `ErrQueueFull`，不执行业务函数 |
| 开始前 context 已取消 | context 错误，不执行业务函数 |
| 业务函数返回错误 | 通过返回的 `error` 和快照的 `Err` 报告 |
| 写入成功后缓存同步失败 | 通过结果和快照的 `SyncErr` 报告 |

构造错误通过 `Snapshot().Err`、`Updates()` 的初始状态和后续 `Mutate` 返回。该功能不提供跨键事务、后端回滚或自动幂等处理。

## 相关文档

- [原子缓存更新](updates.zh-CN.md)：基于已有缓存修改列表或合并局部数据。
- [缓存失效](invalidation.zh-CN.md)：单键、批量和条件失效，以及后台刷新规则。
- [执行并发预算](execution.zh-CN.md)：限制查询、批量加载和写入的后端并发。
