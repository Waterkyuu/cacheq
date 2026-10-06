<div align="center">
  <img src="./assets/go-query.png" alt="go-query" width="144" />

  <h1>go-query</h1>

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

- 数据缓存，可配置 `StaleTime`，可判断数据是否过期。
- 同一个查询键的并发请求共用一次加载，不同键独立加载。
- 有限次数重试、指数退避、自定义重试间隔和请求超时。
- 多个组件共享查询数据与状态变化。
- 支持条件查询，每个订阅独立控制首次加载和失效后的自动刷新。
- `Query` 立即返回已有数据，在后台刷新过期数据。
- 支持单键、批量和条件失效、可选的后台刷新、强制刷新、预加载、本地更新和取消。
- 可选的闲置缓存自动删除，保留时间与数据新鲜期分别配置。

## 安装

```sh
go get github.com/Waterkyuu/go-query
```

## 查询和共享结果

```go
client := query.NewClient[string, string](query.Options{
    StaleTime: time.Minute,
    Retry:     2,
    Timeout:   10 * time.Second,
})
defer client.Close()

fetch := func(ctx context.Context) (string, error) {
    return "hello", nil
}
value, err := client.Fetch(ctx, "greeting", fetch)
```

导入路径为 `github.com/Waterkyuu/go-query`，包名为 `query`。多个组件应复用同一个客户端。缓存有效时直接读取；过期或缺失时，同一个键的并发读取只发起一次请求。查询键应包含影响结果的参数、服务商或用户身份。缓存值是共享的只读数据，修改切片或映射前应复制。

## 过期状态与后台刷新

```go
state := client.Snapshot("greeting") // 只读状态，不发请求
// state.HasData：是否已有数据
// state.Stale：是否过期或被手动标记失效
// state.Fetching：是否正在加载或刷新
// state.Err：最近一次错误

state = client.Query("greeting", fetch) // 立即返回当前数据，必要时后台刷新
```

查询状态包括 `Idle`、`Pending`、`Success` 和 `Error`。`HasData` 区分缺失数据和有效的零值。`Fetching` 独立于结果状态，刷新时已有数据仍可显示。`UpdatedAt` 与 `ExpiresAt` 分别记录数据更新时间和有效期。刷新失败会保留以前的数据，同时暴露错误。

## 多个组件订阅状态

```go
updates, unsubscribe := client.Subscribe("greeting")
defer unsubscribe()

for state := range updates {
    // 将 state 交给自己的 UI 或应用事件循环。
    _ = state
}
```

每个订阅都会收到初始状态和最新变化。慢速读取者可能跳过中间状态。组件关闭时应取消订阅；订阅或客户端关闭后通道结束。订阅本身不发请求，使用 `Query` 或 `Fetch` 启动查询。

## 满足条件才请求

通过 `Observe` 为一个订阅绑定加载函数和启用条件：

```go
users := client.Observe("users", getUsers, query.ObserveOptions{
    Enabled: loggedIn,
})
defer users.Close()
```

启用时，缺失或过期的数据会在后台加载，新鲜的缓存直接复用。禁用时仍可以读取缓存、接收共享数据更新，但不会自己启动请求，缓存失效后也不会自动刷新。`ObserveOptions{}` 默认禁用；需要创建时就请求，可以传入 `Enabled: true`。`users.Snapshot()` 读取当前状态，`users.Updates()` 接收初始状态和最新变化。

登录状态等条件变化时，由应用显式更新同一个订阅：

```go
users.SetEnabled(loggedIn)
```

从禁用改为启用时，会加载缺失或过期的数据，并与正在进行的请求合并。Go 不会自动监听原来的布尔变量变化。禁用一个订阅不会阻止同一个键的其他启用订阅加载数据；禁用订阅仍能收到这些共享结果。原有 `Subscribe` 订阅继续参与失效后的自动刷新，创建订阅本身不启动加载。

禁用或关闭订阅不会取消已经启动的请求。显式调用客户端的 `Fetch`、`Query` 或 `Refetch` 仍可以请求数据；开关只控制该订阅的自动加载。不再使用时调用 `Close` 释放订阅；禁用订阅也会阻止 `GCTime` 自动删除仍在观察的数据。

## 控制查询

| 方法 | 行为 |
| --- | --- |
| `Invalidate` | 标记数据失效；有启用订阅和加载函数的查询会自动刷新。 |
| `InvalidateMany` | 一次标记多个相关键失效，可选择是否立即后台刷新。 |
| `InvalidateWhere` | 按条件筛选已有缓存或正在加载的键，再应用相同的失效和刷新策略。 |
| `Refetch` | 缓存有效时也重新查询，与正在进行的请求合并。 |
| `Prefetch` | 提前填充同一份缓存。 |
| `Set` | 设置本地或乐观更新数据，阻止旧请求覆盖新数据。 |
| `Cancel` | 取消正在进行的请求，保留以前的数据。 |
| `Remove` | 删除缓存，并阻止旧请求恢复已删除数据。 |
| `Clear` | 清空数据并取消当前请求。 |
| `Close` | 停止请求、关闭订阅，并拒绝新查询。 |

## 一起作废相关缓存

业务更新成功后，可以一次作废同一个客户端里的多个相关键：

```go
keys := []string{"user:42", "users"}
client.InvalidateMany(keys, query.InvalidateOptions{})
```

默认使用 `RefetchObserved`：有启用订阅和可用加载函数的查询会在后台刷新，没有订阅者或所有订阅都禁用的查询只标记失效。如果希望先作废，暂不启动新的后台加载，改用 `RefetchNone`：

```go
client.InvalidateMany(keys, query.InvalidateOptions{
    Refetch: query.RefetchNone,
})
```

失效会保留旧数据并通知订阅者，不会取消或重复启动正在进行的加载；加载中被失效的请求完成后，其结果仍视为过期。重复键只处理一次，空列表不做任何操作。只处理一个键时，传入只包含该键的切片即可。原有 `Invalidate(key)` 的行为保持不变。

如果希望按条件筛选，而不是手动列出所有键，可以使用 `InvalidateWhere`。导入 `strings` 后，下面的例子会作废用户列表及所有用户详情，暂不启动新的请求：

```go
client.InvalidateWhere(func(key string) bool {
    return key == "users" || strings.HasPrefix(key, "user:")
}, query.InvalidateOptions{Refetch: query.RefetchNone})
```

刷新选项与 `InvalidateMany` 相同：空选项会刷新有启用订阅和加载函数的匹配查询。每个已有缓存或正在加载的键只判断一次，也包含首次加载尚未完成的键；结构体键可以直接按字段匹配。判断函数在客户端锁外执行，可以读取客户端状态。筛选基于调用时已有键的快照，期间新增的键不参与，应用失效前已删除的键会跳过。传入 `nil` 判断函数不做任何操作。

## 有效期、重试与取消

`StaleTime` 默认是零，即查询完成后立即视为过期；设置正值可避免重复请求。`Retry` 表示首次请求之外的重试次数，默认不重试。启用后，默认重试等待时间从一秒开始翻倍，最多三十秒，也可以通过 `RetryDelay` 自定义。取消和超时错误不重试。`Timeout` 限制包括重试在内的整个加载过程。

第一次调用者的 Context 控制加载任务。取消等待者不会影响其他人的任务；加载发起者取消后，其余调用者可以使用自己的 Context 重试。后台加载属于客户端，`Close` 会停止它们。查询函数必须遵守 Context 取消。

`FetchWithExpiry` 支持返回数据、绝对过期时间和错误，便于保留磁盘快照原有的有效期。零值过期时间表示不复用为有效缓存；失败时返回备用数据、未来的过期时间和错误，可在这段时间内避免反复请求。HTTP 与磁盘持久化由使用方负责。

## 自动删除闲置缓存

```go
client := query.NewClient[string, string](query.Options{
    StaleTime: time.Minute,
    GCTime:    5 * time.Minute,
})
defer client.Close()
```

`StaleTime` 控制多久之后需要刷新，`GCTime` 控制多久没人使用后删除缓存。通过 `Fetch`、`Query` 或 `Snapshot` 读取缓存，或更新该键，会重新计算保留时间。有订阅者或正在加载的键不会被自动删除；最后一个订阅释放且没有加载任务时，或没有订阅者的加载任务结束时，开始计算完整的保留时间。清理会删除数据、错误和保留的加载函数，不会发起请求。

`GCTime` 默认是零，表示关闭自动删除；负值也表示关闭。`Remove`、`Clear` 和 `Close` 会释放对应的清理定时器。目前没有容量限制或 LRU 淘汰。

测试可注入 `Options.Clock`，控制新鲜期和保留时间的判断。客户端不绑定浏览器窗口事件；使用方应在生命周期结束时关闭客户端。
