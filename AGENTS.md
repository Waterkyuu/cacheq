# AGENTS.md

## Role and product

Act as a senior Go engineer working on cacheq, a typed query client for Go 1.22
and later. Prefer simple, idiomatic solutions that are easy to test, avoid
over-engineering and excessive duplication, and fit the existing architecture.
Evaluate proposed approaches critically, explain meaningful tradeoffs, and
recommend a better approach when appropriate.

## Working principles

- Read the surrounding package, tests, and call sites before changing behavior.
- Keep changes focused on the requested outcome; do not refactor unrelated code.
- Preserve backward compatibility unless the task explicitly requires a breaking change.
- Prefer dependency injection and explicit ownership over package-level mutable state.
- Add or update tests when business behavior changes. Keep verification proportional
  to the change; do not add test infrastructure for simple configuration or documentation edits.
- Do not add dependencies unless the standard library or an existing dependency is insufficient.
- Do not add emojis to source code, comments, logs, or user-facing strings.
- Keep the library compatible with the Go version declared in `go.mod`.

## Commands

Run commands from the repository root.

| Command | Purpose |
| --- | --- |
| `task` or `task help` | Show command usage. |
| `task fmt` | Format Go source files. |
| `task lint` | Run the configured linters without modifying files. |
| `task test` | Run the complete Go test suite with race detection. |
| `task test-cover` | Run tests with coverage and race detection. |
| `task build` | Compile the library. |
| `task hooks` | Configure the repository's Git hooks after installing Lefthook. |

Use `go test -race ./... -run TestName` for focused tests while iterating. Do not
reference Task targets that are absent from `Taskfile.yml`.

## Required workflow

1. Inspect the relevant implementation, tests, and call sites.
2. Implement the smallest coherent change.
3. Format changed Go files with `task fmt` and review resulting changes.
4. Run focused tests while iterating when behavior changes.
5. Before committing, run these checks in order:
   1. `task lint`
   2. `task test`
   3. `task build`
6. If a local check or CI workflow fails because of the current change, fix it and
   rerun the check. If the failure is demonstrably unrelated to the current commit,
   leave unrelated code untouched and report the failure clearly.

## Code organization

- Place code in the package that owns the behavior. Do not use an unrelated file
  or a generic utility package merely for convenience.
- Co-locate `*_test.go` files with the package that owns the behavior.
- Use file names that explain responsibility, such as `client.go` and `options.go`.
- Add directories only when they represent an actual owner or responsibility.
- Do not recreate a second implementation of a moved feature or introduce
  forwarding wrappers merely to preserve an obsolete internal layout.

## Go conventions

- Follow the Uber Go Style Guide and established patterns in the target package.
- Use `any` instead of `interface{}`.
- Use idiomatic snake_case file names, such as `query_options.go`.
- Keep functions focused and control flow easy to follow. Prefer early returns
  when they reduce nesting.
- Wrap errors with useful context while preserving the original error for
  `errors.Is` and `errors.As`.
- Accept interfaces at the consumer when substitution is needed; otherwise
  prefer concrete types.
- Use struct embedding only for genuine composition, not to hide unrelated
  dependencies or fields.
- Define enums as typed constants with `iota` when sequential values are appropriate.
- Keep related constants together in a single `const` block.
- Avoid speculative abstractions and design patterns that add indirection
  without a current use case.

## Mandatory declaration comments

Write comments in English. Comments must explain a declaration's responsibility,
meaning, contract, or reason for existing instead of translating its syntax.

- Every named function and method, including unexported ones, must have a comment
  immediately above its declaration.
- Every named `struct` type, including unexported ones, and every field, including
  unexported and embedded fields, must have its own comment immediately above the
  declaration. Explain what the type represents or owns and each field's role,
  ownership, or invariant.
- Every constant, including unexported constants and each item inside a grouped
  `const` block, must have a comment explaining what the value represents or controls.
- Exported declaration comments must begin with the declaration name so that
  they satisfy Go documentation conventions.
- Add a nearby explanatory comment for an anonymous function only when its
  purpose, lifecycle, or side effects are not obvious from context.
- Do not add filler comments such as `// foo does foo`. Describe the behavior,
  invariant, unit, scope, or design reason that a reader needs to know.
- For code review fixes, add inline comments explaining the triggering case and
  why the fix works.
- Avoid over-engineering and unnecessary abstractions.

## Testing

- Test observable behavior rather than implementation details.
- Use table-driven tests when several cases share the same setup and assertion shape.
- Cover success, failure, cancellation, and boundary cases when relevant.
- Keep tests deterministic; do not depend on real network services, wall-clock
  timing, or global mutable state.
- Inject external dependencies so they can be replaced with small test doubles.


## Document

- New features require matching `docs/xxxx.en-US.md` and `docs/xxxx.zh-CN.md` guides;
  fixes update existing guides when needed.
- Use formal technical documentation: purpose, runnable examples, API contracts,
  and limitations. Avoid conversational wording.
- Keep both languages aligned and verify example output.
- Use `docs` as the website source. Register new guides in generation, navigation,
  homepage, and README links. Build the website and verify both locales, links,
  and search coverage.
