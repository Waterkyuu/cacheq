# Redis shared cache examples

Requires Go 1.26+ and a local `redis-server` executable. This independent module
uses local `replace` directives for the repository's cacheq core and Redis
adapter, keeping example dependencies separate from the library modules.

Start a disposable Redis server in one terminal:

```sh
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

From the repository root, run either program in another terminal:

```sh
cd examples/redis
REDIS_ADDR=127.0.0.1:16379 go run ./shared_cache
REDIS_ADDR=127.0.0.1:16379 go run ./shared_invalidation
```

`REDIS_ADDR` defaults to `127.0.0.1:6379`. Stop the disposable server with Ctrl+C
when finished. Use the reserved demonstration namespaces only for these examples.

| Program | Behavior | Guide and expected output |
| --- | --- | --- |
| [shared_cache](shared_cache/main.go) | Two local Clients reuse Redis results; key invalidation refreshes data; Redis failures fall back to the source | [Shared result cache](../../docs/shared-cache.en-US.md) · [简体中文](../../docs/shared-cache.zh-CN.md) |
| [shared_invalidation](shared_invalidation/main.go) | Two Clients reuse detail and list results; remote tag invalidation notifies the second Client and marks both results stale | [Shared invalidation](../../docs/shared-invalidation.en-US.md) · [简体中文](../../docs/shared-invalidation.zh-CN.md) |

Both programs use an atomic in-memory authoritative source to make changes and
source call counts observable. They reset their reserved demonstration key or
tag before each run. The first program disables remote notifications; the second
enables remote tags and notifications and waits for stale snapshots before
explicitly refreshing data.

The Redis adapter implementation and integration tests remain in
[store/redis](../../store/redis/). Build and inspect the example module separately
because root Go commands do not traverse nested modules:

```sh
go mod verify
go vet ./...
go build ./...
```
