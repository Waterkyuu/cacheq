# Versioning and upgrades

[简体中文](versioning.zh-CN.md)

These rules tell users whether upgrading cacheq requires changing their code.
cacheq follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and is
currently in the `v0` development phase.

## What the version number means

For `v0.1.0`, the three numbers are the major, minor, and patch versions.

| Upgrade | Meaning |
| --- | --- |
| `v0.1.0` → `v0.1.1` | Compatible bug fixes, documentation, or performance improvements; existing supported usage keeps working |
| `v0.1.x` → `v0.2.0` | New features or deprecations; may also contain breaking changes, with migration instructions |
| `v0.x.y` → `v1.0.0` | Establishes a stable public API |
| `v1.0.0` → `v1.1.0` | Backward-compatible additions or deprecations |
| `v1.x.y` → `v2.0.0` | Breaking public API changes; Go also requires a `/v2` module and import path suffix |

Patch releases preserve compatibility in every stage and do not introduce new
public features. Prefer compatible changes during `v0` too; any breaking release
must explain the affected APIs or behavior and how to migrate in its release notes.

From `v1`, mark deprecated declarations with Go's `Deprecated:` comments and
provide a replacement. Keep them for at least one minor release before removal
in a later major version. During `v0`, advance deprecation is preferred when
practical; removals require a minor release and migration instructions.

## What compatibility covers

The promise covers the root `cacheq` package's exported declarations and behavior
specified in its source comments and guides, including option defaults, cache
freshness, request sharing, cancellation, invalidation, and subscriptions.
Changing these in a way that breaks supported usage is a breaking change;
correcting behavior that contradicts the documentation is a bug fix.

Use keyed struct literals so compatible releases can add fields. Use `errors.Is`
for exported error values. Positional struct literals, exact error message text,
unexported internals, performance measurements, and undocumented concurrent
ordering are outside the promise. Standalone examples and website or release
tooling are outside the root package's API contract.

The core library currently requires Go 1.22; CI tests Go 1.22 and the latest
stable Go on Linux, macOS, and Windows. Raising the minimum Go version requires
a `v0` minor or, from `v1`, a major release and release notes. Example modules
may require newer Go versions independently.

## How to upgrade

Pin a published version, for example:

```sh
go get github.com/Waterkyuu/cacheq@v0.1.0
```

Before upgrading, read the target release's notes and run your application's
tests, especially across `v0` minor versions. Read source comments and guides
at the installed version's Git tag; `main` and the website may contain unreleased
changes.

Published tags and module contents are immutable. Fix a bad release with a new
version instead of moving, deleting, or reusing its tag. Ordinary commits do not
publish releases. See the [release guide](https://github.com/Waterkyuu/cacheq/blob/main/.github/RELEASING.md)
for publishing and [Go's version rules](https://go.dev/doc/modules/version-numbers)
for module paths.
