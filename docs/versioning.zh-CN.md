# 版本与升级规则

[English](versioning.en-US.md)

本文规定版本兼容性、弃用流程和升级要求。
soroi 遵循[语义化版本](https://semver.org/spec/v2.0.0.html)，目前处于 `v0` 开发阶段。

## 版本号

以 `v0.1.0` 为例，三个数字依次是主版本、次版本和补丁版本。

| 升级 | 含义 |
| --- | --- |
| `v0.1.0` → `v0.1.1` | 保持兼容的缺陷修复、文档或性能改进；原来受支持的用法继续有效 |
| `v0.1.x` → `v0.2.0` | 新功能或弃用；也可能包含破坏性变更，必须提供迁移说明 |
| `v0.x.y` → `v1.0.0` | 确立稳定的公开 API |
| `v1.0.0` → `v1.1.0` | 向后兼容的功能新增或弃用 |
| `v1.x.y` → `v2.0.0` | 公开 API 的破坏性变更；Go 还要求模块和导入路径带 `/v2` 后缀 |

所有阶段的补丁版本都保持兼容，不新增公开功能。即使在 `v0` 阶段，也优先采用
兼容改动；包含破坏性变更的版本必须在发布说明中写清受影响的 API 或行为，以及
用户如何迁移。

## 弃用策略

从 `v1` 起，弃用声明使用 Go 的 `Deprecated:` 注释，提供替代方式，并至少保留
一个次版本，才允许在后续主版本中删除。在 `v0` 阶段，条件允许时优先提前弃用；
删除功能必须发布次版本，并提供迁移说明。

## 兼容范围

承诺覆盖根目录 `soroi` 包的公开声明，以及源码注释和功能文档中明确说明的行为，
包括配置默认值、缓存新鲜期、请求共享、取消、失效和订阅。修改这些约定，导致
受支持的用法不再成立，属于破坏性变更；修正违背文档的行为属于缺陷修复。

使用带字段名的结构体字面量，以便兼容版本可以新增字段；用 `errors.Is` 判断公开
错误值。按位置初始化字段、错误消息的精确文本、内部实现、性能测量结果和未明确
约定的并发顺序不属于承诺。独立示例、文档站和发布工具不属于根包的 API 契约。

核心库目前要求 Go 1.22；CI 在 Linux、macOS 和 Windows 上测试 Go 1.22 和最新
稳定版 Go。提高最低 Go 版本必须发布 `v0` 次版本，或从 `v1` 起发布新主版本，
并在发布说明中记录。示例模块可以独立要求更高的 Go 版本。

## 升级流程

升级前阅读目标版本的发布说明，尤其是跨 `v0` 次版本升级。固定目标的已发布版本，例如 v0.3.1：

```sh
go get github.com/Waterkyuu/soroi@v0.3.1
```

升级后运行应用自身的测试。参考已安装版本对应 Git tag 下的源码注释和功能文档；`main` 和在线文档站可能包含
尚未发布的改动。

## 从 cacheq 迁移到 soroi

`v0.3.1` 将项目改名为 `soroi`。虽然版本号是 patch，本次发布仍会修改模块路径、
Go 包名和 Redis 默认命名空间，是 patch 兼容性规则的一次明确例外。

已有 Release 和 tag 保持原始内容。继续使用旧模块的应用可以固定旧名称下的最后版本：

```sh
go get github.com/Waterkyuu/cacheq@v0.3.0
```

迁移时，将导入路径中的 `github.com/Waterkyuu/cacheq` 替换为
`github.com/Waterkyuu/soroi`，包括 `/store/redis` 适配器路径；将 `cacheq.`
限定符替换为 `soroi.`，然后安装新版本：

```sh
go get github.com/Waterkyuu/soroi@v0.3.1
# Only for applications using the Redis adapter:
go get github.com/Waterkyuu/soroi/store/redis@v0.3.1
go mod tidy
go test ./...
```

核心库和适配器必须一起迁移。新旧模块路径中的类型和错误值身份不同，不能在同一套
集成中混用。导入别名可以保留本地的 `cacheq` 限定符，但不能保持模块或类型身份。
不要依赖旧模块路径的 `@latest` 跟随后续发布；GitHub 重定向不会改写 Go 模块声明。

Redis 适配器现在默认使用 `Prefix: "soroi"`。已有的 `cacheq` 记录不会被改名、
删除，也不会被新默认值读取。滚动迁移期间需要继续使用旧命名空间时，为所有新实例显式配置：

```go
store, err := redistore.New(remote, redistore.Options{Prefix: "cacheq"})
```

已自定义前缀的应用应继续使用原值。共享结果、标签和失效通知的所有实例必须使用
相同前缀和兼容的缓存格式。切换到 `soroi` 会启用独立命名空间，预热前可能增加
回源请求；旧的失效元数据需要单独清理。

仓库改名后，GitHub 会重定向旧仓库及 Release 链接。不要重新创建
`Waterkyuu/cacheq` 仓库，否则会覆盖重定向。GitHub Pages 项目地址不提供该重定向；
文档站使用 <https://waterkyuu.github.io/soroi/>。

## 已发布版本

已发布的 tag 和模块内容不可改写。发布出错时应发布新版本，不得移动、删除或复用
旧 tag。普通提交不会发布新版本。发布操作见
[发布指南](https://github.com/Waterkyuu/soroi/blob/main/.github/RELEASING.md)，模块路径规则见
[Go 版本说明](https://go.dev/doc/modules/version-numbers)。
