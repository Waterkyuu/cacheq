# 查询、共享数据与条件请求

[English](queries.en-US.md) · [缓存与生命周期](cache.zh-CN.md) · [缓存失效](invalidation.zh-CN.md)

一个 `Client` 可以管理不同类型的数据。`Query` 订阅缓存状态并在后台加载；`Fetch` 等待新鲜数据，不保留订阅。两者均从加载函数推断结果类型，并共享同键的数据及正在进行的请求。

## 快速开始：多种结果类型

在导入 cacheq 的 Go 模块中，将以下完整程序保存为 `main.go`，运行 `go run .`。加载函数用固定数据展示类型关系；实际 HTTP 请求写法在后面。

```go
package main

import (
	"context"
	"fmt"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// User is a detail result and an element of a list result.
type User struct {
	// Name contains the user's display name.
	Name string
}

// Settings contains application configuration independent of user data.
type Settings struct {
	// Theme selects the application's appearance.
	Theme string
}

// main shares one application-owned client across differently typed queries.
func main() {
	client := cacheq.NewClient(cacheq.Options{
		StaleTime: time.Minute,
		GCTime:    5 * time.Minute,
	})
	defer client.Close()

	getUser := func(context.Context) (User, error) {
		return User{Name: "Alice"}, nil
	}
	getUsers := func(context.Context) ([]User, error) {
		return []User{{Name: "Alice"}}, nil
	}
	getSettings := func(context.Context) (Settings, error) {
		return Settings{Theme: "dark"}, nil
	}
	detail := cacheq.Query(client, "user:42", getUser)
	users := cacheq.Query(client, "users", getUsers)
	settings := cacheq.Query(client, "settings", getSettings)
	defer detail.Close()
	defer users.Close()
	defer settings.Close()

	// Fetch reuses fresh data or joins the active load started by Query.
	user, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		getUser,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(user.Name)
	listState := users.Snapshot()        // Snapshot[[]User]
	settingsState := settings.Snapshot() // Snapshot[Settings]
	_ = listState
	_ = settingsState
}
```

预期输出：

```text
Alice
```

一个键唯一对应一个静态数据类型：`"user:42"` 存 `User`，`"users"` 存 `[]User`。查询键还应包含影响结果的参数、租户和用户身份。字符串、整数和可比较的结构体都能作为键；切片、映射和 nil 不能作为键。客户端只共享同一进程里的内存，不会自动跨服务同步。

## API 片段前提

以下局部片段使用快速开始中的 `client`、`getUser`、`getUsers`，以及非 nil 的 `ctx`。包含 `return err` 的片段放在返回 `error` 的函数中。各片段独立使用，需补充片段所用的导入。

## `Query`：读取状态与后台加载

```go
users := cacheq.Query(client, "users", getUsers)
defer users.Close()
state := users.Snapshot()
```

没有缓存时启动首次请求；缓存新鲜时直接复用；缓存过期时保留旧数据并在后台刷新。`Query` 不等待网络请求完成，`Snapshot()` 也是一次快照，之前拿到的变量不会自己变化。

不传 `QueryOptions`、传空配置或省略 `Disable` 时都默认启用；设置 `Disable: true` 才禁用自动加载。如果传多个配置，最后一个生效。

无效键、类型冲突或缺少加载函数等构造错误通过 `Snapshot().Err` 和 `Updates()` 的初始状态返回；失败对象的更新通道随后关闭。

## `Updates`：持续接收最新状态

```go
for state := range users.Updates() {
	if state.Err != nil {
		// Report the error while retaining any available old data.
	}
	if state.HasData {
		// Forward state.Data to the application's UI or event loop.
	}
}
```

通道提供初始状态和后续最新状态。后台请求开始、结束、缓存更新和失效都可能产生通知。读取慢时允许跳过中间状态，因此这不是必须逐条处理的事件日志。`Close()` 会关闭通道；实际应用由界面或事件循环在独立的退出路径中释放查询，不能等待 `range` 自己结束再执行 `defer`。

## `SetEnabled`：自动加载控制

```go
users := cacheq.Query(
	client,
	"users",
	getUsers,
	cacheq.QueryOptions{
		Disable: true,
	},
)
defer users.Close()

// Called by the application's login-state change handler.
if err := users.SetEnabled(true); err != nil {
	return err
}
```

禁用时不启动首次请求，失效后也不自动刷新，但仍能读取已有缓存，并收到其他查询对象带来的共享更新。重新启用时加载缺失或过期的数据。

Go 不会监听布尔变量，登录状态变化时需要显式调用 `SetEnabled`。每个查询对象有自己的开关；禁用一个不会禁用其他消费者，也不会取消已经开始的请求。

## `Refetch`：使用保留的加载函数刷新

```go
updatedUsers, err := users.Refetch(ctx)
if err != nil {
	return err
}
_ = updatedUsers
```

忽略缓存新鲜期，等待新结果；同键已有请求时共用该请求。即使查询禁用，也能手动刷新。已经释放的查询对象不能再刷新，返回 `ErrQueryClosed`。

## `Fetch`：等待新鲜数据

```go
users, err := cacheq.Fetch(
	ctx,
	client,
	"users",
	getUsers,
)
if err != nil {
	return err
}
_ = users // []User
```

缓存新鲜时立即返回；缺失或过期时等待请求。它不建立长期订阅，适合 HTTP 处理器、后台任务或依赖查询结果的业务步骤。与同键 `Query` 共用数据和正在进行的请求。

`Fetcher[V]` 表示 `func(context.Context) (V, error)`，用于提供业务加载逻辑。客户端应在应用入口创建并注入，供多次请求共享缓存。

## 快照字段

| 字段 | 含义 |
| --- | --- |
| `Data` | 当前数据；刷新时可能是之前的结果 |
| `HasData` | 是否有可用数据，能区分有效零值和没有数据 |
| `Status` | `Idle`、`Pending`、`Success`、`Error` |
| `Err` | 最近一次请求错误，失败后可以同时保留旧数据 |
| `Stale` | 数据缺失、过期或被标记失效 |
| `Fetching` | 是否正在请求，包括保留旧数据时的后台刷新 |
| `UpdatedAt` | 最近一次安装可用数据的时间 |
| `ExpiresAt` | 绝对新鲜期截止时间；失效时为零时间 |

`Pending` 表示没有旧数据的首次请求正在进行；禁用且没数据时为 `Idle`。`Fetching` 与结果状态分开，所以后台刷新时可以同时是 `Success` 和 `Fetching: true`。数据变旧不会自行启动请求；也不是轮询，时间经过不会自动产生更新通知。

## HTTP 加载与取消

下面需要导入 `encoding/json`、`fmt`、`net/http` 和 `context`。`endpoint` 是业务提供的用户列表接口地址。

```go
getUsers := func(ctx context.Context) ([]User, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		endpoint,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create users request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch users: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch users: HTTP %d", response.StatusCode)
	}
	users := []User{}
	if err := json.NewDecoder(response.Body).Decode(&users); err != nil {
		return nil, fmt.Errorf("decode users: %w", err)
	}
	return users, nil
}
```

`Query` 的自动请求由客户端拥有；`Fetch` 和手动 `Refetch` 的请求由启动请求的调用者 context 拥有。等待别人的请求时，自己的 context 取消只结束自己的等待。拥有请求的调用者取消时，仍需要数据的其他调用者可以重新请求。

`Close()` 查询对象只释放订阅，不取消共享请求。`client.Cancel` 或 `client.Close` 会取消请求，加载函数需要响应 context。`Options.Timeout` 约束整次加载，包括重试和等待重试的时间。

## Bubble Tea 集成

[Bubble Tea 文档](bubbletea.zh-CN.md)说明可运行的任务看板、键盘操作、消息循环接入及资源释放。示例采用 Bubble Tea v2，独立模块需要 Go 1.26；核心库仍支持 Go 1.22。

[模型代码](../examples/bubbletea/model.go)在 `tea.Cmd` 中等待一条 `Updates()` 通知，将快照交给 `Update`，再安排下一次等待。仅消息循环修改显示状态。

## 验证

```sh
go test -race ./e2e -run 'Test(SharedClientMutation|ConditionalConsumers|HTTPSharedBackgroundLoad)' -count=1
```

测试使用本地 HTTP 服务，验证多类型共享缓存、真实修改后的刷新、条件开关和请求合并，源码见 [查询生命周期 e2e](../e2e/query_lifecycle_test.go)。
