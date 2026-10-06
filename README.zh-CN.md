<div align="center">
  <img src="./assets/cacheq.png" alt="cacheq" width="144" />

  <h1>cacheq</h1>

  <p><a href="./README.md">English</a> | <strong>简体中文</strong></p>

  <p><strong>缓存、重试、共享查询数据</strong></p>
  <p>面向 Go 1.22 及以上版本、没有外部依赖的查询客户端</p>
  <p>多个组件共享数据、合并重复请求，并订阅查询状态。</p>

  <p>
    <img src="https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.22+" />
  </p>

  <p><a href="#功能">功能</a> · <a href="#安装">安装</a></p>
</div>

## 功能

- 一个共享客户端缓存用户详情、列表、配置等不同类型的数据。
- 每个查询有明确的数据类型，同键类型冲突返回错误。
- 一个 `Query` 入口管理状态、更新通知、条件开关和手动刷新。
- 缓存新鲜期、后台刷新、同键请求合并、重试和请求超时。
- 单键、跨类型批量和按条件失效，可选择延后请求。
- 本地缓存更新、预加载、请求取消和闲置缓存自动删除。

## 安装

```sh
go get github.com/Waterkyuu/cacheq
```

导入路径为 `github.com/Waterkyuu/cacheq`，包名为 `cacheq`。需要 Go 1.22 或更新版本，没有外部依赖。

## 一个客户端，多种数据类型

```go
// User contains a user's display data.
type User struct {
	// Name contains the user's display name.
	Name string
}

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

detail := cacheq.Query(client, "user:42", getUser) // QueryHandle[User]
users := cacheq.Query(client, "users", getUsers)   // QueryHandle[[]User]
defer detail.Close()
defer users.Close()

state := users.Snapshot()
// state.Data 是 []User，state.Fetching 表示正在请求。
```

客户端通常在应用入口创建，传给各个业务模块。同键查询共享数据和正在进行的请求；不同键可以有不同类型。共享只发生在本进程内。

`Query` 立即返回查询对象。缺少数据时后台请求，新鲜缓存直接复用，旧数据保留并后台刷新。用 `Updates()` 接收后续状态；用 `cacheq.Fetch(ctx, client, key, fetcher)` 等待新鲜结果而不订阅。

## 满足条件才请求

```go
users := cacheq.Query(
	client,
	"users",
	getUsers,
	cacheq.QueryOptions{
		Enabled: loggedIn,
	},
)
defer users.Close()

// 登录状态变化时，由应用更新条件。
if err := users.SetEnabled(true); err != nil {
	return err
}

// 手动刷新使用已绑定的加载函数，禁用时也可以调用。
updatedUsers, err := users.Refetch(ctx)
```

不传配置默认启用；显式空 `QueryOptions{}` 默认禁用。每个查询对象独立控制自动请求，仍会收到其他消费者产生的数据更新。

## 修改后让相关缓存一起失效

```go
if err := client.Invalidate([]any{"user:42", "users"}); err != nil {
	return err
}
```

`Invalidate` 接收单个键、`[]any` 键列表或 `func(any) bool` 条件函数。详情和列表分别是 `User` 与 `[]User`，仍能在同一个客户端里一起失效。默认刷新有启用查询对象的键；传 `Refetch: cacheq.RefetchNone` 只标记过期，下次使用再查。

## 功能文档

在线阅读[中英文文档站](https://waterkyuu.github.io/cacheq/)。

| 文档 | 说明 |
| --- | --- |
| [查询与条件请求](docs/queries.zh-CN.md) | `Query`、状态、更新通知、条件开关、刷新、`Fetch` 和 HTTP 加载函数 |
| [缓存与生命周期](docs/cache.zh-CN.md) | 所有缓存读写、配置、自动删除、取消、删除、关闭及错误处理 |
| [缓存失效](docs/invalidation.zh-CN.md) | 单键、批量、条件失效及刷新模式 |

完整可运行程序、各 API 的用途和结果都在功能文档中。缓存值作为共享只读数据使用；修改切片或映射前先复制。不同类型不能复用同一个键。

## 验证

```sh
task lint
task test
task build
```

`task test` 包含竞态检测和 [HTTP e2e 测试](e2e/query_lifecycle_test.go)。e2e 使用本地测试服务，覆盖真实修改、多类型缓存、条件请求、共享加载、重试和取消；不会访问外部服务。
