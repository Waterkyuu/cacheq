# MCP resource caching

[简体中文](mcp.zh-CN.md) · [Cache & lifecycle](cache.en-US.md) · [Observability](observability.en-US.md)

The [runnable MCP example](../examples/mcp/) caches private resource reads in
a server handler. It demonstrates cross-connection reuse, credential-scoped
keys, remaining TTL, mutation invalidation, and resource notifications.

## Requirements

The independent example module pins the official MCP Go SDK to v1.8.0 and requires
Go 1.25 or later. The core cacheq library supports Go 1.22 and has no external
dependencies. No external service or real credentials are needed.

## Run the example

From the repository root:

```sh
cd examples/mcp
go run .
```

The program starts a loopback HTTP server and three MCP clients. Two Alice
connections read the same settings; Bob reads a different private value under
the same URI. The backend is read twice. Alice then updates her settings through
a tool, receives a resource change notification, and reads the new value.

Expected output:

```text
alice: theme=light
alice: theme=light
bob: theme=blue
backend reads after three connections: 2
resource changed: config://app/settings
alice after update: theme=dark
backend reads=3 cache hits=1 cache misses=3 loads=3
```

## Server integration

The SDK version used by this example supports client-side TTL caching. The example places cacheq in
the server's resource handler, where it shares backend results across connections
and merges concurrent reads. A local SDK hit does not reach the server and does
not increment server-side statistics.

| Operation | Integration |
| --- | --- |
| `resources/read` | `FetchWithExpiry` loads the backend or reuses a fresh scoped value |
| Resource TTL | Return the remaining `ttlMs` from the original cache deadline, capped by `MaxAge` |
| Successful mutation | Write the backend, then `Invalidate` that scope's resource |
| Resource notification | The SDK sends `notifications/resources/updated` to that scope's subscribers |
| Client re-read | The SDK invalidates its local resource cache before delivering the notification |
| Measurement | `Stats()` reports server cache decisions; the backend separately counts actual reads |

The [server implementation](../examples/mcp/service.go) uses the 2026-07-28
stateless Streamable HTTP protocol. The [client](../examples/mcp/client.go) waits
for subscription acknowledgment before changing the resource, so a notification
cannot be missed simply because subscription setup is still in progress.

## Freshness and concurrent mutations

Reading cached data does not restart its TTL. At the exact deadline, the next
server-side read loads the backend again. Zero or negative freshness produces
`ttlMs: 0`, allowing the SDK to re-read on its next access.

An update during a pending read invalidates that operation. Its captured old
value may finish returning to its original caller, but is advertised with zero
freshness so it cannot prevent the next read from fetching new data.

## Errors and cancellation

Backend errors are returned as MCP errors. They are not converted into successful
resource content. Cancellation propagates from the HTTP/MCP request to the loader.

## Private scopes and ownership

Authentication runs before every cache lookup. Keys include a credential
fingerprint and URI, and responses use `cacheScope: "private"`. Each credential
has its own SDK server and subscription set, so private changes are not broadcast
to another user's subscribers. The shared cache belongs to this endpoint instance.

The demo allowlist accepts `alice` and `bob`; production code must supply its own
token validation and resource authorization. Sessions keep one immutable
authorization context. This example caches one backend resource, rather than
automatically wrapping all tools or input-required results.

The [entry point](../examples/mcp/main.go) closes the MCP clients before the HTTP
endpoint and cache. This terminates subscriptions and releases retained data.

## Verification

From `examples/mcp`:

```sh
go test -race ./... -count=1
```

[HTTP MCP e2e tests](../examples/mcp/service_test.go) cover reuse, notifications,
scoped isolation, concurrent loading, remaining TTL, exact age boundaries,
immediate staleness, failures, cancellation, and updates during a pending read.
