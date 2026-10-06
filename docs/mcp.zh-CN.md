# MCP 资源缓存

[English](mcp.en-US.md) · [缓存与生命周期](cache.zh-CN.md) · [可观测性](observability.zh-CN.md)

完整可运行实现放在 `examples/mcp`，使用官方 MCP Go SDK v1.8.0，需要 Go 1.25+。
示例使用独立模块，核心 cacheq 库继续支持 Go 1.22，且不增加依赖。

## 运行示例

从仓库根目录运行：

```sh
cd examples/mcp
go run .
```

程序启动一个本地 HTTP 服务和三个 MCP 客户端。两个 Alice 连接读取相同配置；
Bob 用相同 URI 读取自己的私有配置。后端只读取两次。随后 Alice 通过工具修改配置，
收到资源变化通知，再次读取新值。

最终统计为 `backend reads=3 cache hits=1 cache misses=3 loads=3`。
不需要外部服务或真实凭证。

## cacheq 放在哪里

官方 SDK 已支持客户端 TTL 缓存。示例把 cacheq 放在服务端资源处理函数中，
负责跨连接复用后端结果、合并并发读取。SDK 本地命中不会到达服务端，
因此不会增加服务端 cacheq 的统计。

| 操作 | 如何接入 |
| --- | --- |
| `resources/read` | 用 `FetchWithExpiry` 读取后端，或复用授权范围内的新鲜值 |
| 资源 TTL | 按原缓存截止时间返回剩余 `ttlMs`，并受 `MaxAge` 限制 |
| 修改成功 | 写入后端，再用 `Invalidate` 使对应授权范围的资源失效 |
| 资源通知 | SDK 向该授权范围的订阅者发送 `notifications/resources/updated` |
| 客户端重读 | SDK 在交付通知前使自己的资源缓存失效 |
| 衡量收益 | `Stats()` 统计服务端缓存决策；后端另行统计实际读取次数 |

[服务端代码](../examples/mcp/service.go)使用 2026-07-28 版本的无状态 Streamable HTTP 协议。
[客户端代码](../examples/mcp/client.go)先等待订阅确认，再修改资源，防止订阅尚未建立时错过通知。

## 新鲜期与并发修改

读取缓存不会重新计算一份完整 TTL。到达精确截止时间时，下次服务端读取会重新加载后端。
新鲜期为零或负数时返回 `ttlMs: 0`，SDK 下次使用时可以重新读取。

如果读取期间发生修改，失效标记也会作用于正在进行的加载。该请求之前取得的旧值
可能继续返回给原调用者，但会标记为立即过期，不阻止下一次读取加载新数据。

后端错误作为 MCP 错误返回，不转换为成功的资源内容。
HTTP/MCP 请求取消会传递给加载函数。

## 私有范围与资源释放

每次查缓存前都会验证凭证。缓存键包含凭证指纹和 URI，响应声明 `cacheScope: "private"`。
每个凭证拥有独立的 SDK 服务端和订阅集合，私有变化不会广播给其他用户。
共享缓存由当前端点实例拥有。

示例的凭证白名单接受 `alice` 和 `bob`；实际服务需要替换为自己的令牌验证和资源授权逻辑。
每个客户端连接使用固定授权上下文。示例缓存一个后端资源，不自动包装所有工具调用或需要额外输入的结果。

[入口代码](../examples/mcp/main.go)先关闭 MCP 客户端，再关闭 HTTP 服务和缓存，
终止订阅并释放保留数据。

## 验证

在 `examples/mcp` 中运行：

```sh
go test -race ./... -count=1
```

[HTTP MCP e2e](../examples/mcp/service_test.go)覆盖复用、变化通知、授权隔离、并发合并、
剩余 TTL、精确年龄边界、立即过期、失败、取消，以及读取期间的修改。
