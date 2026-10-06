# MCP resource caching example

Run a local MCP server and three clients with the official Go SDK v1.8.0.
Requires Go 1.25+. This independent module uses the repository's cacheq through
`replace`, keeping MCP dependencies out of the core library.

```sh
cd examples/mcp
go run .
```

The program starts its own loopback HTTP endpoint and exits after the demo.
It needs no external server or real credentials. Expected output:

```text
alice: theme=light
alice: theme=light
bob: theme=blue
backend reads after three connections: 2
resource changed: config://app/settings
alice after update: theme=dark
backend reads=3 cache hits=1 cache misses=3 loads=3
```

The two Alice connections share a server-side cache entry; Bob has a separate
private value for the same URI. A tool writes Alice's settings, invalidates the
server cache, and notifies Alice's subscribed client to read again.

- [service.go](service.go) authenticates each request, keys cache entries by a
  credential fingerprint and URI, and uses `FetchWithExpiry` for backend reads.
  Cache hits advertise only the original deadline's remaining `ttlMs`, capped
  by `MaxAge`. Invalidated in-flight results advertise zero freshness.
- [client.go](client.go) gives each session an immutable authorization context
  and waits for subscription acknowledgment before issuing a mutation.
- [store.go](store.go) is the authoritative backend and counts actual reads.
- [main.go](main.go) owns the endpoint, clients, and shared cache.

The endpoint uses the 2026-07-28 stateless Streamable HTTP protocol. The SDK
already caches resource responses on each client; cacheq handles server-side
reuse across connections, concurrent request merging, capacity, age limits,
and statistics. SDK-local hits do not increment server cacheq counters.

Responses advertise `cacheScope: "private"`. Separate SDK servers per accepted
credential isolate notification subscriptions as well as resource values.
Authentication runs before cache lookup, including for warm entries.

The strings `alice` and `bob` are local demo credentials. For a deployed server,
replace this allowlist with token verification and resource authorization.
The example demonstrates one fixed resource and mutation; it is not a general
cache for arbitrary tool calls or multi-round-trip input results.

```sh
go test -race ./... -count=1
```

[HTTP MCP e2e tests](service_test.go) verify private scopes, notifications,
cross-connection reuse, merged requests, exact expiry, remaining TTL, immediate
staleness, failures, cancellation, and mutation during a pending read. Tests
use channels and an injected freshness clock rather than timed sleeps.

Protocol references: [MCP caching](https://modelcontextprotocol.io/specification/2026-07-28/server/utilities/caching),
[resource notifications](https://modelcontextprotocol.io/specification/2026-07-28/server/resources#subscriptions),
and the [official Go SDK](https://github.com/modelcontextprotocol/go-sdk/tree/v1.8.0).
