## 安装

```sh
go get github.com/Waterkyuu/go-query@v0.1.0
```

# go-query

面向 Go 1.22 及以上版本、没有外部依赖的查询客户端。多个组件复用同一个客户端，就能共享数据、合并重复请求，并订阅查询状态。

- 数据缓存，可配置 `StaleTime`，可判断数据是否过期。
- 同一个查询键的并发请求共用一次加载，不同键独立加载。
- 有限次数重试、指数退避、自定义重试间隔和请求超时。
- 多个组件共享查询数据与状态变化。
- `Query` 立即返回已有数据，在后台刷新过期数据。
- 支持手动失效、强制刷新、预加载、本地更新和取消。

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

## 控制查询

| 方法 | 行为 |
| --- | --- |
| `Invalidate` | 标记数据失效；有订阅者的查询会自动刷新。 |
| `Refetch` | 缓存有效时也重新查询，与正在进行的请求合并。 |
| `Prefetch` | 提前填充同一份缓存。 |
| `Set` | 设置本地或乐观更新数据，阻止旧请求覆盖新数据。 |
| `Cancel` | 取消正在进行的请求，保留以前的数据。 |
| `Remove` | 删除缓存，并阻止旧请求恢复已删除数据。 |
| `Clear` | 清空数据并取消当前请求。 |
| `Close` | 停止请求、关闭订阅，并拒绝新查询。 |

## 有效期、重试与取消

`StaleTime` 默认是零，即查询完成后立即视为过期；设置正值可避免重复请求。`Retry` 表示首次请求之外的重试次数，默认不重试。启用后，默认重试等待时间从一秒开始翻倍，最多三十秒，也可以通过 `RetryDelay` 自定义。取消和超时错误不重试。`Timeout` 限制包括重试在内的整个加载过程。

第一次调用者的 Context 控制加载任务。取消等待者不会影响其他人的任务；加载发起者取消后，其余调用者可以使用自己的 Context 重试。后台加载属于客户端，`Close` 会停止它们。查询函数必须遵守 Context 取消。

`FetchWithExpiry` 支持返回数据、绝对过期时间和错误，便于保留磁盘快照原有的有效期。零值过期时间表示不复用为有效缓存；失败时返回备用数据、未来的过期时间和错误，可在这段时间内避免反复请求。HTTP 与磁盘持久化由使用方负责。

测试可注入 `Options.Clock`。客户端不绑定浏览器窗口事件，也不自动清理未使用的键；使用方应删除不需要的数据，并在生命周期结束时关闭客户端。
