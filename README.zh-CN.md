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

更新操作成功后用 `Invalidate` 使相关查询失效。HTTP 缓存键要包含影响结果的参数、用户或租户范围。各 API 的具体用法见[查询文档](docs/queries.zh-CN.md)与[失效文档](docs/invalidation.zh-CN.md)。

## Bubble Tea 示例

[完整代码与说明](examples/bubbletea/)展示查询状态如何进入 TUI 消息循环，包括后台刷新、失败后保留数据和退出清理。示例需要 Go 1.26+。

```sh
cd examples/bubbletea
go run .
```

## 功能文档

在线阅读[中英文文档站](https://waterkyuu.github.io/cacheq/)。

| 文档 | 说明 |
| --- | --- |
| [查询与条件请求](docs/queries.zh-CN.md) | `Query`、状态、更新通知、条件开关、刷新、`Fetch` 和 HTTP 加载函数 |
| [缓存与生命周期](docs/cache.zh-CN.md) | 所有缓存读写、配置、自动删除、取消、删除、关闭及错误处理 |
| [缓存失效](docs/invalidation.zh-CN.md) | 单键、批量、条件失效及刷新模式 |
| [可观测性](docs/observability.zh-CN.md) | 缓存命中、请求合并、加载结果与耗时、重试和清理统计 |

完整 Bubble Tea 程序在 `examples/bubbletea`，API 细节见功能文档。缓存值作为共享只读数据使用；修改切片或映射前先复制。不同类型不能复用同一个键。

## 验证

```sh
task lint
task test
task build
```

`task test` 包含竞态检测和 [HTTP e2e 测试](e2e/query_lifecycle_test.go)。e2e 使用本地测试服务，覆盖真实修改、多类型缓存、条件请求、共享加载、重试和取消；不会访问外部服务。
