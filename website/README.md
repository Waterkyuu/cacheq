# 构建与发布文档站

文档站使用 Astro + Starlight 和 TypeScript。Go 从构建后的网页和公开 Go 声明生成本地搜索索引，不需要搜索服务或 API 密钥。

## 本地开发

需要 Node.js 24、pnpm 11.10.0，以及 Go 1.22 或更新版本。

```sh
pnpm --dir website install --frozen-lockfile
pnpm --dir website start
```

默认首页为英文，中文页面从同一个服务的 `/cacheq/zh-CN/` 访问。搜索索引在生产构建后生成，验证搜索请使用构建产物预览。

## 检查与预览构建产物

```sh
pnpm --dir website lint
pnpm --dir website fmt:check
pnpm --dir website typecheck
pnpm --dir website test
pnpm --dir website build
pnpm --dir website test:build
pnpm --dir website serve
```

`pnpm --dir website fmt` 使用 oxfmt 格式化 TypeScript、TSX、CSS 和配置文件。oxlint 检查前端代码，`astro check` 检查 Astro 模板和 TypeScript，`tsc` 检查脚本类型。仓库现有的 Go 检查也会覆盖搜索索引程序。

## 修改文档

直接编辑 `docs` 下按功能命名的中英文文档。`website/scripts/prepare-docs.mts` 为 Astro 生成被 Git 忽略的 Starlight 输入文档，保留代码块并转换源码链接。生成的文档、Astro 缓存、依赖目录、搜索 JSON 和网页产物不提交到 Git。

默认语言为英文，英文首页路径为 `/cacheq/`，功能文档路径为 `/cacheq/docs/`。中文首页为 `/cacheq/zh-CN/`，功能文档为 `/cacheq/zh-CN/docs/`。Go 使用构建后的真实路径和标题 ID 生成文档搜索结果，并索引根目录 Go 包的公开类型、函数、方法、常量和错误。API 搜索结果跳转到对应源码声明。

## 部署到 GitHub Pages

`Documentation` 工作流根据 pnpm 锁文件安装依赖，运行代码检查、格式检查、类型检查和测试，构建两种语言，生成 Go 搜索索引，验证站内链接，然后部署 Pages 产物。

推送到 `main` 或 `waterkyuu/feat/docs-site` 会发布。提交到 `main` 的 PR 只执行构建检查。GitHub Pages 使用 Actions 发布源，`github-pages` 环境允许发布分支部署。

公开地址为 <https://waterkyuu.github.io/cacheq/>。CI 构建用 `DOCS_REF` 选择源码链接指向的分支。

---

# Build and publish the documentation site

The website uses Astro + Starlight and TypeScript. Go generates a local search index from the rendered documentation and public Go declarations. No search service or API key is required.

## Run locally

Requires Node.js 24, pnpm 11.10.0, and Go 1.22 or later.

```sh
pnpm --dir website install --frozen-lockfile
pnpm --dir website start
```

Astro serves the default English site. The same dev server serves Chinese at `/cacheq/zh-CN/`. The search index is produced by the production build; use the built preview to test search.

## Check and preview the published build

```sh
pnpm --dir website lint
pnpm --dir website fmt:check
pnpm --dir website typecheck
pnpm --dir website test
pnpm --dir website build
pnpm --dir website test:build
pnpm --dir website serve
```

`pnpm --dir website fmt` formats TypeScript, TSX, CSS, and configuration with oxfmt. oxlint checks the frontend; `astro check` validates Astro templates and TypeScript, while `tsc` validates scripts. Repository Go checks also cover the search indexer.

## Edit documentation

Edit the named English and Chinese feature guides in `docs`. `website/scripts/prepare-docs.mts` creates ignored Starlight inputs, preserving code blocks and adapting source links. The generated documents, Astro cache, installed dependencies, search JSON, and rendered HTML are not committed.

The default locale is English, with the homepage at `/cacheq/` and guides under `/cacheq/docs/`. Chinese lives under `/cacheq/zh-CN/`, with guides under `/cacheq/zh-CN/docs/`. Go uses rendered page paths and heading IDs for documentation search, and indexes exported types, functions, methods, constants, and errors from the root Go package. API results open the exact source declaration.

## GitHub Pages deployment

The `Documentation` workflow installs from the pnpm lockfile, runs lint, formatting, type checks and tests, builds both locales, generates the Go search index, checks published links, and deploys the Pages artifact.

Pushes to `main` and `waterkyuu/feat/docs-site` publish changes. Pull requests targeting `main` run build checks without publishing. GitHub Pages uses the Actions publishing source, and the `github-pages` environment allows the publishing branches.

The public address is <https://waterkyuu.github.io/cacheq/>. `DOCS_REF` selects the branch for source links during CI builds.
