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
- 通过查询与获取配置，按消费者覆盖新鲜时间、重试错误判断和超时。
- 单键、跨类型批量和按条件失效，可选择延后请求。
- 本地缓存更新、预加载、请求取消和闲置缓存自动删除。

## 安装

```sh
go get github.com/Waterkyuu/cacheq
```

导入路径为 `github.com/Waterkyuu/cacheq`，包名为 `cacheq`。需要 Go 1.22 或更新版本，没有外部依赖。

## 快速开始

```go
client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
defer client.Close()

query := cacheq.Query(client, "greeting", func(context.Context) (string, error) {
	return "hello", nil
})
defer query.Close()
```

`Query` 后台加载数据，同键查询共享缓存与请求。用 `Snapshot()` 读取状态，用 `Updates()` 接收变化；需要等待结果时使用 `Fetch`。

## 使用场景

| 场景 | 搭配方式 | 解决什么问题 |
| --- | --- | --- |
| Bubble Tea / TUI | `Query` + `Updates()` + `tea.Cmd` | 把加载、刷新和错误状态送入消息循环 |
| CLI / 后台任务 | `Fetch` + `Options.Timeout` | 等待结果，同进程复用缓存并限制加载时间 |
| HTTP 处理器 | 共享 `Client` + `Fetch(r.Context(), ...)` | 合并同键并发请求，响应请求取消 |
| MCP 资源 | `FetchWithExpiry` + `Invalidate` + `Stats()` | 跨连接复用资源、按 TTL 过期并在修改后通知重读 |

更新操作成功后用 `Invalidate` 使相关查询失效。HTTP 缓存键要包含影响结果的参数、用户或租户范围。各 API 的具体用法见[查询文档](docs/queries.zh-CN.md)与[失效文档](docs/invalidation.zh-CN.md)。

## Bubble Tea 示例

[完整代码与说明](examples/bubbletea/)展示查询状态如何进入 TUI 消息循环，包括后台刷新、失败后保留数据和退出清理。示例需要 Go 1.26+。

```sh
cd examples/bubbletea
go run .
```

## MCP 示例

[完整代码与说明](examples/mcp/)展示资源缓存、变化通知和授权隔离。示例需要 Go 1.25+。

```sh
cd examples/mcp
go run .
```

## 功能文档

在线阅读[中英文文档站](https://waterkyuu.github.io/cacheq/)。

| 文档 | 说明 |
| --- | --- |
| [查询与条件请求](docs/queries.zh-CN.md) | `Query`、状态、更新通知、条件开关、刷新、`Fetch` 和 HTTP 加载函数 |
| [查询配置](docs/query-options.zh-CN.md) | 按消费者覆盖默认配置、独立新鲜度、重试错误判断、超时和共享请求策略 |
| [缓存与生命周期](docs/cache.zh-CN.md) | 所有缓存读写、配置、自动删除、取消、删除、关闭及错误处理 |
| [缓存失效](docs/invalidation.zh-CN.md) | 单键、批量、条件失效及刷新模式 |
| [可观测性](docs/observability.zh-CN.md) | 缓存命中、请求合并、加载结果与耗时、重试和清理统计 |
| [MCP 资源缓存](docs/mcp.zh-CN.md) | 服务端资源复用、剩余 TTL、变化通知、授权隔离和 e2e |

完整集成程序在 `examples`，API 细节见功能文档。缓存值作为共享只读数据使用；修改切片或映射前先复制。不同类型不能复用同一个键。

## 版本与升级规则

cacheq 遵循[语义化版本](https://semver.org/spec/v2.0.0.html)。在 `v0` 阶段，补丁版本保持兼容；次版本可能包含破坏性变更，并提供迁移说明。从 `v1` 起，公开 API 的破坏性变更必须升级主版本。核心库目前支持 Go 1.22 及以上版本，示例模块可能要求更高的 Go 版本。

公开 API 边界、弃用策略、Go 版本支持和升级方式见[版本与升级规则](docs/versioning.zh-CN.md)。已发布的版本 tag 不会移动或复用。

## 参与贡献

本地环境、Go 代码规范、测试、文档和 PR 流程见[贡献指南（英文）](CONTRIBUTING.md)。欢迎通过 GitHub Issues 提交 Bug 报告和功能建议。

## 许可证

cacheq 使用 [MIT 许可证](LICENSE)。
