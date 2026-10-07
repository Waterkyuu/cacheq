# Contributing to cacheq

Bug reports, tests, documentation, examples, and focused code improvements are
welcome. Issues and pull request descriptions may be written in English or Chinese.
Source comments must be in English.

## Before making a change

Search existing issues and use the Bug report or Feature request form. Include a
small reproducer for bugs and a concrete use case for proposals. For substantial
API or architecture changes, an issue helps establish scope before implementation.

Read the surrounding implementation, tests, and call sites. Keep each change
focused on one problem. The detailed repository rules are in [AGENTS.md](AGENTS.md);
the [version policy](docs/versioning.en-US.md) defines compatibility expectations.

## Local setup

Fork the repository, clone your fork, and create a branch from an up-to-date `main`.
Use `waterkyuu/<type>/<description>`, for example:

```sh
git switch -c waterkyuu/fix/query-cancellation
```

Types are `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `chore`, `style`, or `ci`.

Install Go, Task, and golangci-lint. The root library supports Go 1.22 and later;
the lint job currently uses Go 1.25.x and golangci-lint v2.12.2. Match the tool
versions in [CI](.github/workflows/ci.yml) when setting up lint locally. The
Bubble Tea and MCP examples are separate modules requiring Go 1.26+ and Go 1.25+
respectively. Website development requires Node.js 24+ and the pnpm version in
[website/package.json](website/package.json).

To enable the repository's Git hooks, install Lefthook and run:

```sh
task hooks
```

Hooks format and fix selected Go issues, and may update staged files. Review those
changes before committing; hooks do not replace the complete checks below.

## Go code guidelines

Follow the Uber Go Style Guide and existing package patterns. The formatter and
linter configuration in [.golangci.yml](.golangci.yml) is authoritative for
automated checks; [.editorconfig](.editorconfig) supplies editor defaults.

- Keep code in the package that owns its behavior. Use descriptive snake_case
  file names and co-locate tests with the implementation.
- Prefer small, focused functions, early returns, and explicit ownership. Use
  dependency injection for clocks, loaders, and other external behavior.
- Use `any`, keyed struct literals, and interfaces defined by their consumers.
  Wrap errors with useful context while preserving `errors.Is` and `errors.As`.
- Prefer the standard library and existing dependencies. Add a dependency only
  when they are insufficient; keep the root module compatible with Go 1.22.
- Preserve documented defaults and public behavior. Explain affected callers
  and migration steps when a change must break compatibility.
- Every named function or method, struct and field, and constant needs an English
  declaration comment explaining its responsibility, contract, or invariant.
  Exported comments begin with the declaration name. Avoid filler comments.
- For review fixes, add an inline comment explaining the triggering case and why
  the fix works. Do not add unrelated refactoring or emojis to project content.

## Tests and documentation

Test observable behavior. Update tests when business behavior changes, and cover
success, failure, cancellation, and boundaries where relevant. Use table-driven
tests for cases with the same setup and assertions. Keep tests deterministic:
inject time and external behavior rather than relying on real services or timing.

New library features need paired `docs/<topic>.en-US.md` and
`docs/<topic>.zh-CN.md` examples. Fixes do not need new guides, but update existing
documentation when a documented API or behavior changes. Edit source guides under
`docs/`; generated website documentation is ignored and must not be committed.

## Verification

Run from the repository root. Format changed Go code, use focused tests while
iterating, then run the required checks in this order before committing:

```sh
task fmt
task lint
task test
task build
```

`task test` runs the complete root-module test suite with race detection. It does
not run the separate example modules. Run their checks from their own directories
when affected. For website changes, run the checks in
[Website CI](.github/workflows/website.yml). For release helper changes, run
`sh .github/scripts/release_test.sh`. Available root commands are defined in
[Taskfile.yml](Taskfile.yml).

Fix failures caused by your change. Report unrelated failures and checks you
could not run in the PR; leave unrelated code untouched.

## Commits and pull requests

Use English conventional commit messages with a summary and change bullets:

```text
fix(query): preserve cancellation during refresh
- Handle cancellation before starting another attempt
- Cover cancellation with a deterministic regression test
```

Each commit covers one responsibility and changes at most six files. Split larger
changes into focused commits. Inspect staged changes before committing, exclude
unrelated edits, and never force ignored files into Git.

Open a PR against `main` and fill in the [PR template](.github/pull_request_template.md).
Explain the problem, resulting behavior, compatibility, related issues, and
verification. Contributions are distributed under the project's [MIT license](LICENSE).
