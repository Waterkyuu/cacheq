# 原子缓存更新

[English](updates.en-US.md) · [缓存与生命周期](cache.zh-CN.md) · [写入与关联失效](mutations.zh-CN.md)

`Update` 根据已有缓存计算新值，适用于修改列表中的一项、合并局部数据等操作。计算期间如果同一个键被其他操作修改，`Update` 返回冲突错误，保留其他操作的结果。

它只修改当前 Client 的缓存，不发送后端请求。需要先提交业务修改、再同步缓存时，使用 [Mutation](mutations.zh-CN.md)。

## 快速开始

以下示例将缓存列表中用户 42 的姓名改为 Bob，并保留之前读取的列表。将代码保存为已导入 soroi 的 Go 模块中的 `main.go`，运行 `go run .`。

```go
package main

import (
	"fmt"
	"slices"
	"time"

	"github.com/Waterkyuu/soroi"
)

// User contains the fields displayed in a user list.
type User struct {
	// ID identifies the user within the list.
	ID int
	// Name contains the user's display name.
	Name string
}

// main updates one cached list item while preserving previously read data.
func main() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	defer client.Close()
	if err := soroi.Set(client, "users", []User{{ID: 42, Name: "Alice"}}); err != nil {
		panic(err)
	}
	before := soroi.Get[[]User](client, "users")

	err := soroi.Update(client, "users", func(users []User) ([]User, error) {
		next := slices.Clone(users)
		for i := range next {
			if next[i].ID == 42 {
				next[i].Name = "Bob"
			}
		}
		return next, nil
	})
	if err != nil {
		panic(err)
	}
	after := soroi.Get[[]User](client, "users")
	fmt.Println("before:", before.Data[0].Name)
	fmt.Println("after:", after.Data[0].Name)
}
```

预期输出：

```text
before: Alice
after: Bob
```

`Update` 把当前的 `[]User` 传给回调，并尝试写入回调返回的新列表。`slices.Clone` 复制切片的底层数组，因此修改新列表不会改变 `before.Data`。写入成功后，这个键的查询对象会收到更新通知。

## 选择 `Set`、`Update` 或 `Invalidate`

| 已有信息 | 使用方式 |
| --- | --- |
| 已拿到该键完整的新数据，或需要初始化缓存 | `Set(client, key, value)` |
| 需要基于该键当前数据计算新值 | `Update(client, key, update)` |
| 只知道数据已经变化，需要重新查询 | `client.Invalidate(key)` |

分别调用 `Get` 和 `Set` 时，两次调用之间可能发生其他写入，最后的 `Set` 会覆盖它。`Update` 会检查计算期间的数据是否变化，避免基于旧值覆盖新结果。

## `Update`：读取、计算与写入

`Update[V]` 接收 Client、查询键和 `func(V) (V, error)` 回调。结果类型由回调推断，并且必须与该键已有的类型一致。

调用时先读取已有数据；没有数据或数据已超过 `MaxAge` 时，返回 `ErrNoData`，不调用回调。已经变旧但仍未超过 `MaxAge` 的数据可以参与更新。

回调在 Client 锁外执行，可以调用其他 Client API。回调返回错误时，直接返回该错误，不写入计算结果。回调成功后，再检查同一个键的版本；版本没有变化才写入。一次 `Update` 最多调用回调一次，不会在冲突时自动重新计算。

成功写入采用 `Set` 的行为：按 Client 默认配置重新计算新鲜期，更新 `UpdatedAt` 和 `MaxAge` 的计时起点，清除旧错误，取消旧加载并通知查询对象。`StaleTime` 为零时，新值立即变旧。原子性仅限同一个 Client 中的单个键，不包含后端事务或多个键的共同提交。

## 切片、映射和指针

缓存值由多个读取者共享，回调收到的值没有经过深拷贝。直接修改共享切片、映射或指针指向的数据，会在版本检查之前改变缓存内容，也可能产生数据竞争。

快速开始中的 `User` 只有整数和字符串字段，复制切片即可。如果元素还包含映射、切片或指针，需要继续复制将要修改的嵌套数据。`slices.Clone` 和 `maps.Clone` 都只执行浅拷贝。

回调应只计算新值。版本冲突只能阻止结果写入缓存，不能撤销回调已经发出的 HTTP 请求、数据库写入或其他外部副作用。

## 处理缺失与并发冲突

以下片段使用快速开始中的 `client` 和 `User`，放在返回 `error` 的函数中，并导入 `errors` 和 `slices`。它将列表同步作为可选操作：缓存不存在时跳过，发生冲突时使列表失效，让后续查询读取后端数据。

```go
err := soroi.Update(client, "users", func(users []User) ([]User, error) {
	next := slices.Clone(users)
	for i := range next {
		if next[i].ID == 42 {
			next[i].Name = "Bob"
		}
	}
	return next, nil
})
switch {
case errors.Is(err, soroi.ErrNoData):
	return nil
case errors.Is(err, soroi.ErrUpdateConflict):
	return client.Invalidate("users")
default:
	return err
}

```

`ErrUpdateConflict` 表示本次计算结果没有写入。示例中的 `Invalidate` 保留当前数据，并按默认规则刷新有启用查询对象的列表；没有订阅时只标记过期。详见[缓存失效](invalidation.zh-CN.md)。

如果业务操作必须在当前缓存值上重新计算，可以在捕获冲突后重新调用整个 `Update`，并限制尝试次数。Client 的 `Retry` 配置不作用于本地更新。

同键的 `Set`、其他 `Update`、加载完成（包括失败）、显式失效、强制刷新、删除、容量淘汰或 `MaxAge` 到期，都可能使正在计算的更新产生冲突。删除后重建同一个键也会被识别，即使重建后的值相等。其他键的变化不产生冲突。

## 错误处理

| 条件 | 错误 |
| --- | --- |
| 没有可用数据，或数据已超过 `MaxAge` | `ErrNoData` |
| 回调计算期间同键状态发生变化 | `ErrUpdateConflict` |
| 回调为 nil | `ErrNoUpdater` |
| 查询键非法 | `ErrInvalidKey` |
| 回调数据类型与键已有类型不一致 | `ErrTypeMismatch` |
| Client 已关闭，包括回调执行期间关闭 | `ErrClosed` |
| 回调返回业务错误 | 原样返回该错误 |

通过 `errors.Is` 判断库错误。缺失、校验失败和客户端已关闭等情况不会运行回调；回调已经开始后关闭 Client，会阻止其结果写入。

## 与查询和诊断的关系

更新成功后，`Get` 读取新数据，已有 `Query` 通过 `Updates()` 接收新状态。已经返回给调用者的快照不会被替换，因此仍需遵守共享数据只读的约定。

成功更新产生 `EventLocalWrite`，原因为 `ReasonSet`。冲突不会产生成功写入事件。事件字段见[诊断事件](events.zh-CN.md)。

## 相关文档

- [缓存与生命周期](cache.zh-CN.md)：直接写入缓存、数据新鲜期、最长可用时间和取消。
- [写入与关联失效](mutations.zh-CN.md)：业务写入成功后同步详情和列表。
- [缓存失效](invalidation.zh-CN.md)：更新冲突后让关联查询重新加载。
