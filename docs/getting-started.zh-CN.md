# 开始使用 cacheq

[English](getting-started.en-US.md)

cacheq 是 Go 的类型安全查询客户端，提供进程内缓存、同键请求共享和查询状态订阅，适用于服务端处理器、后台任务和交互式应用。

## 安装

需要 Go 1.22 或更新版本。cacheq 核心库没有外部依赖。

```sh
go get github.com/Waterkyuu/cacheq
```

在应用入口创建客户端，将它传给需要数据的业务模块，并在应用退出时关闭。缓存保存在本进程内存中，不会自动跨服务同步。

## 快速开始

在导入 cacheq 的 Go 模块中，将下面的完整程序保存为 `main.go`，运行 `go run .`。示例使用本地加载函数，无需网络服务。

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// User contains the profile fields required by this application.
type User struct {
	// Name contains the user's display name.
	Name string
}

// main shares one cached result between a subscription and an imperative read.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()

	var calls atomic.Int32
	loadUser := func(context.Context) (User, error) {
		calls.Add(1)
		return User{Name: "Alice"}, nil
	}
	profile := cacheq.Query(client, "user:42", loadUser)
	defer profile.Close()

	user, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		loadUser,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(user.Name)
	fmt.Printf("loader calls: %d\n", calls.Load())
}
```

预期输出：

```text
Alice
loader calls: 1
```

`Query` 立即返回查询对象并在后台加载。`Fetch` 等待新鲜结果：同键已有加载时加入该请求，加载已完成且数据新鲜时复用缓存。一分钟的新鲜期保证示例中的两次调用只执行一次加载函数。

## 查询生命周期

| 缓存状态 | 使用 `Query` 时的行为 |
| --- | --- |
| 还没有数据 | 后台加载，完成后通知查询对象 |
| 数据仍然新鲜 | 立即使用缓存 |
| 数据已经过期 | 保留旧数据，同时后台刷新 |
| 刷新失败 | 保留已有数据，并提供错误状态 |

用 `Snapshot()` 读取当前状态，用 `Updates()` 接收后续变化。快照是一次读取的值，不会自行更新。

## 相关文档

- [查询与条件请求](queries.zh-CN.md)：多个类型共用客户端、满足条件才请求、获取更新、手动刷新。
- [缓存与生命周期](cache.zh-CN.md)：读写缓存、预取、过期时间、自动清理、取消和关闭。
- [缓存失效](invalidation.zh-CN.md)：更新数据后让相关缓存一起作废，按条件筛选，选择立即刷新或下次再查。

同一个键只对应一个静态数据类型。查询键应包含影响结果的参数、租户和用户身份，避免不同请求错误地共享数据。
