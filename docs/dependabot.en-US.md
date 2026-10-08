# Automatic dependency updates

Dependabot checks the Go modules, website packages, and GitHub Actions weekly.
Every Dependabot update, including major versions, is eligible for automatic
squash merging after all required CI checks succeed. Major updates can still
introduce behavior changes that existing tests do not cover.

The `Dependabot auto-merge` workflow runs after a pull request CI workflow
completes successfully. It executes the merge script from `main`, verifies the
PR belongs to Dependabot in this repository, and matches the completed run's
commit to the current PR head. Human PRs, forks, outdated runs, missing required
checks, and unsuccessful checks cannot trigger a merge. Pending or failed checks
leave the PR open; the next successful CI completion retries the gate.

## Required repository settings

Protect `main` with required checks from GitHub Actions and require branches to
be up to date before merging. Require all of these check names:

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

The website PR checks run for every PR so required checks cannot be omitted by
path filters. No personal access token or automatic review approval is needed.
The merge job uses the repository's `GITHUB_TOKEN` with permission to merge PRs,
read checks, and dispatch workflows. Keep branch protection enabled: the script
refuses to merge when the required check list is empty.

## Related packages and deployment

`react` and `react-dom` are grouped into one update so their versions stay aligned.
A failed update remains open for investigation; rerun CI after correcting it.

A successful merge explicitly dispatches `docs.yml` on `main`, which rebuilds and
publishes the website. This is necessary because merges using `GITHUB_TOKEN` do
not trigger the normal push workflows. If deployment fails after merging, rerun
Docs CI; the dependency update has already merged.

To pause automatic merging, disable `Dependabot auto-merge` in GitHub Actions.
Dependabot continues opening PRs for manual handling.

See [GitHub's workflow security guidance](https://docs.github.com/en/actions/reference/security/secure-use)
and [GITHUB_TOKEN event behavior](https://docs.github.com/en/actions/concepts/security/github_token).
