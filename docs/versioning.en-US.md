# Versioning and upgrades

[简体中文](versioning.zh-CN.md)

This guide defines release compatibility, deprecation, and upgrade requirements.
soroi follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and is
currently in the `v0` development phase.

## Version numbers

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

## Deprecation policy

From `v1`, mark deprecated declarations with Go's `Deprecated:` comments and
provide a replacement. Keep them for at least one minor release before removal
in a later major version. During `v0`, advance deprecation is preferred when
practical; removals require a minor release and migration instructions.

## Compatibility scope

The promise covers the root `soroi` package's exported declarations and behavior
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

## Upgrade procedure

Read the target release notes before upgrading, especially across `v0` minor
versions. Pin a published target version, for example v0.3.1:

```sh
go get github.com/Waterkyuu/soroi@v0.3.1
```

After upgrading, run your application's tests. Read source comments and guides
at the installed version's Git tag; `main` and the website may contain unreleased
changes.

## Migrating from cacheq to soroi

Version `v0.3.1` renames the project to `soroi`. This release intentionally changes
module paths, the Go package name, and the Redis default namespace despite its
patch version. It is an explicit exception to the patch compatibility policy.

Existing releases and tags retain their original contents. Applications staying
on the old module can pin the last release under that name:

```sh
go get github.com/Waterkyuu/cacheq@v0.3.0
```

To migrate, replace `github.com/Waterkyuu/cacheq` with
`github.com/Waterkyuu/soroi` in imports, including the `/store/redis` adapter
path. Replace `cacheq.` qualifiers with `soroi.`, then install the new release:

```sh
go get github.com/Waterkyuu/soroi@v0.3.1
# Only for applications using the Redis adapter:
go get github.com/Waterkyuu/soroi/store/redis@v0.3.1
go mod tidy
go test ./...
```

Migrate the core and adapter together. Types and error values from the old and
new module paths are distinct; do not mix them in one integration. An import
alias can retain a local `cacheq` qualifier, but it does not preserve module or
type identity. Do not depend on the old module path's `@latest` to follow future
releases; GitHub redirects do not rewrite Go module declarations.

The Redis adapter now defaults to `Prefix: "soroi"`. Existing `cacheq` records
are not renamed, deleted, or read by the new default. To keep using the old
namespace during a rolling migration, configure every new instance explicitly:

```go
store, err := redistore.New(remote, redistore.Options{Prefix: "cacheq"})
```

Retain any other application-defined prefix. All instances sharing results,
tags, and invalidation notifications must use the same prefix and compatible
cache formats. Switching to `soroi` starts a separate namespace and can increase
source loads until it warms; old invalidation metadata requires separate cleanup.

GitHub redirects old repository and release links after the repository rename.
Do not recreate a repository at `Waterkyuu/cacheq`, which would replace that
redirect. GitHub Pages project URLs do not receive this redirect; use
<https://waterkyuu.github.io/soroi/> for the documentation site.

## Published releases

Published tags and module contents are immutable. Fix a bad release with a new
version instead of moving, deleting, or reusing its tag. Ordinary commits do not
publish releases. See the [release guide](https://github.com/Waterkyuu/soroi/blob/main/.github/RELEASING.md)
for publishing and [Go's version rules](https://go.dev/doc/modules/version-numbers)
for module paths.
