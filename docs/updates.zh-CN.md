# 带冲突检查的缓存更新

[English](updates.en-US.md) · [缓存生命周期](cache.zh-CN.md) · [写入与关联失效](mutations.zh-CN.md)

`Update` 转换已有缓存值，并防止静默覆盖并发修改。应用回调在 Client 锁外执行一次，只有捕获的缓存版本仍然有效时才提交结果。该操作不加载缺失数据，也不写入后端。

## 可运行示例

在已导入 cacheq 的 Go 模块中保存以下程序，运行 `go run .`。

```go
package main

import (
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main updates a cached counter without an unprotected read-modify-write sequence.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	if err := cacheq.Set(client, "counter", 1); err != nil {
		panic(err)
	}
	if err := cacheq.Update(client, "counter", func(value int) (int, error) {
		return value + 1, nil
	}); err != nil {
		panic(err)
	}
	fmt.Println(cacheq.Get[int](client, "counter").Data)
}
```

预期输出：

```text
2
```

## 提交契约

- 回调接收已有的可用数据，包括过期但仍可用的数据。数据缺失或超过最大年龄时返回 `ErrNoData`，不调用回调。
- 回调在内部锁外恰好执行一次，可以调用 Client API。回调错误原样返回，不安装结果。
- 写入、刷新完成（包括失败）、显式失效、强制刷新、删除、淘汰和年龄过期都可能使捕获的版本无效。冲突返回 `ErrUpdateConflict`，不会自动重复回调。
- 删除后重建同一个键无法隐藏冲突，即使新值与旧值相等。不相关键的变化不产生冲突。
- 成功采用 `Set` 语义：使用 Client 默认配置安装普通新鲜数据，更新 `UpdatedAt` 和 `MaxAge` 起点，清除旧错误，取消旧加载并通知订阅者。诊断记录为 `EventLocalWrite`，原因为 `ReasonSet`。
- 原子性仅限单键，不提供多键或后端事务。版本检查不会回滚应用的外部副作用。

## 共享值与重试

缓存值为共享只读数据。修改切片、映射或指针指向的对象前必须先复制；直接修改回调参数会绕过冲突保护，并可能与读取者产生数据竞争。库不会深拷贝应用数据。

当 `errors.Is(err, cacheq.ErrUpdateConflict)` 为 true 时，应用可以按有界策略重新调用整个 `Update`。如果应用可能重试，转换回调应避免外部副作用。`Options.Retry` 不作用于本地更新。

安装权威完整数据或初始化缺失键时使用 `Set`。远端写入后同步缓存时使用 [Mutation](mutations.zh-CN.md)。

## 错误与验证

nil 回调返回 `ErrNoUpdater`。非法键、结果类型不兼容和客户端关闭分别返回 `ErrInvalidKey`、`ErrTypeMismatch` 和 `ErrClosed`。回调执行期间关闭 Client 会阻止结果提交。

```sh
go test -race ./... -run TestUpdate -count=1
```

[测试](../update_test.go)覆盖并发转换、显式重试、回调重入、失效、删除后重建、年龄与容量限制、回调失败和迟到的加载结果。
