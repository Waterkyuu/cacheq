# 自动依赖更新

Dependabot 每周检查 Go 模块、网站依赖和 GitHub Actions。
所有 Dependabot 更新都可在必需 CI 检查通过后自动 squash 合并，包括主版本升级。
主版本更新仍可能引入现有测试未覆盖的行为变化。

`Dependabot auto-merge` 工作流会在 PR 的某个 CI 工作流成功结束后执行。
它只执行 `main` 分支上的合并脚本，并验证 PR 来自本仓库的 Dependabot，
且已完成检查的提交与 PR 当前提交一致。人工 PR、fork、过期检查、缺少必需检查
或检查未成功的情况都不会触发合并。检查尚未完成或失败时，PR 保持开放；
下一次 CI 成功结束时会再次检查。

## 仓库必需配置

为 `main` 启用分支保护，要求分支在合并前与主分支保持同步，并将以下
GitHub Actions 检查设为必需：

- `Lint and Format`
- `Go Vulnerability Check`
- `Bubble Tea example`
- `MCP example`
- `Test (ubuntu-latest, Go 1.22.x)`
- `Test (ubuntu-latest, Go stable)`
- `Test (macos-latest, Go 1.22.x)`
- `Test (macos-latest, Go stable)`
- `Test (windows-latest, Go 1.22.x)`
- `Test (windows-latest, Go stable)`
- `Website checks`
- `Docs build / Website checks`

网站 PR 检查会对每个 PR 执行，避免路径过滤导致必需检查缺失。
无需个人访问令牌，也无需机器人自动批准审查。合并任务使用仓库的
`GITHUB_TOKEN`，权限用于合并 PR、读取检查和触发工作流。
请保持分支保护开启：必需检查列表为空时，脚本会拒绝合并。

## 关联依赖与部署

`react` 和 `react-dom` 会分组更新，使两者版本保持一致。
更新检查失败时，PR 会保持开放；修复问题后重新运行 CI 即可。

合并成功后，脚本会显式触发 `main` 上的 `docs.yml`，重新构建并发布网站。
这是因为使用 `GITHUB_TOKEN` 合并不会触发通常的 push 工作流。
若合并后部署失败，重新运行 Docs CI；依赖更新此时已经合并。

需要暂停自动合并时，在 GitHub Actions 中禁用 `Dependabot auto-merge`。
Dependabot 仍会创建 PR，供人工处理。

参考 [GitHub 工作流安全指南](https://docs.github.com/en/actions/reference/security/secure-use)
和 [GITHUB_TOKEN 事件行为](https://docs.github.com/en/actions/concepts/security/github_token)。
