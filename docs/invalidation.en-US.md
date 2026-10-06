# Single-key, batch, and predicate invalidation

[简体中文](invalidation.zh-CN.md) · [Queries and conditions](queries.en-US.md) · [Cache and lifecycle](cache.en-US.md)

Invalidation retains data but marks it stale. After updating a user, detail and list results can become stale together even though they return `User` and `[]User` respectively.

Snippets use the shared client and business fetchers `getUser` and `getUsers` from the [query guide](queries.en-US.md), inside a business function returning `error`.

## `Invalidate`: mark one key stale

```go
if err := client.Invalidate("user:42"); err != nil {
	return err
}
```

Data remains available, `Stale` becomes true, and handles are notified. An enabled handle supplies its fetcher for background refresh; keys without enabled handles are only marked stale.

Invalidation does not wait for refresh completion. Observe results through `Updates()`. Use handle `Refetch(ctx)` when a manual refresh must return its result.

## `InvalidateMany`: invalidate related results together

Create queries with different result types:

```go
detail := cacheq.Query(client, "user:42", getUser) // QueryHandle[User]
users := cacheq.Query(client, "users", getUsers)   // QueryHandle[[]User]
defer detail.Close()
defer users.Close()
```

After the business mutation succeeds:

```go
// Call the application's user-update API successfully before invalidating.
if err := client.InvalidateMany(
	[]any{"user:42", "users"},
	cacheq.InvalidateOptions{},
); err != nil {
	return err
}
```

One operation spans multiple result types in the shared client. Enabled consumers refresh by default; other keys simply become stale. Duplicate keys are processed once, avoiding duplicate work or invalidating a refresh just started by an earlier occurrence.

`[]any` contains keys, not untyped query data. It permits strings and comparable struct keys in the same batch. Each query's data remains typed.

All keys are validated before mutation. A non-comparable key returns `ErrInvalidKey` without partial invalidation. Empty batches have no effect. Closed clients return `ErrClosed`.

## `InvalidateOptions` and `RefetchMode`

| Mode | Behavior |
| --- | --- |
| `RefetchObserved` | Default and zero value: refresh keys with enabled handles |
| `RefetchNone` | Mark stale without starting new work |

Single-key invalidation accepts optional configuration:

```go
if err := client.Invalidate("user:42", cacheq.InvalidateOptions{
	Refetch: cacheq.RefetchNone,
}); err != nil {
	return err
}
```

Defer a group of related queries:

```go
if err := client.InvalidateMany(
	[]any{"user:42", "users"},
	cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone},
); err != nil {
	return err
}
```

Even enabled consumers retain stale data without starting a request. A later `Fetch`, new `Query`, transition from disabled to enabled, or manual `Refetch` may refresh it. Existing enabled handles do not poll simply because time passes.

## `InvalidateWhere`: select existing keys by a condition

Import `strings` to select all user details and the list:

```go
client.InvalidateWhere(func(key any) bool {
	name, ok := key.(string)
	return ok && (name == "users" || strings.HasPrefix(name, "user:"))
}, cacheq.InvalidateOptions{
	Refetch: cacheq.RefetchNone,
})
```

Only keys returning true become stale. This condition selects cached queries; `Enabled` instead controls permission to request data.

The predicate receives a key and runs outside the client lock, so it may call `Get`, `Set`, and other client operations. It evaluates a snapshot of existing keys once each, with no ordering guarantee. Newly added keys are excluded; keys removed before invalidation are skipped. Nil predicates and closed clients do nothing.

When inspecting data within a predicate, use the correct static result type for that key. Not every key necessarily contains `User`.

## Structured keys: select a tenant and resource

```go
// ResourceKey identifies a query within a tenant.
type ResourceKey struct {
	// Resource selects the query family.
	Resource string
	// Tenant identifies the data owner.
	Tenant int
}

client.InvalidateWhere(func(key any) bool {
	resource, ok := key.(ResourceKey)
	return ok && resource.Resource == "users" && resource.Tenant == 42
}, cacheq.InvalidateOptions{})
```

String prefixes have no built-in semantics. Match prefixes yourself, or use comparable structs and inspect their fields. Structs containing slices or maps are not valid keys.

## Invalidation during a request

An active same-key request is neither canceled nor duplicated. Its result remains stale, preventing pre-invalidation work from being treated as current. Completion does not automatically start a second request. Use the next `Fetch`, new `Query`, or `Refetch` when freshness is required.

`RefetchNone` does not cancel work already running; it only suppresses new work. Use `client.Cancel` to cancel, and `client.Remove` to discard data.

## Verify the mutation workflow

```sh
go test -race ./e2e -run 'Test(SharedClientMutation|DeferredPredicateInvalidation)' -count=1
```

[HTTP e2e tests](../e2e/query_lifecycle_test.go) perform a real PUT mutation, check mixed-type detail/list refresh and unrelated data isolation, and confirm deferred predicate invalidation issues no HTTP request.
