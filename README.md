## Install

```sh
go get github.com/Waterkyuu/go-query@v0.1.0
```

# go-query

A small, dependency-free query cache for Go 1.22 and later.

- Typed keys and values using generics.
- In-memory reuse until an explicit expiration.
- One in-flight loader per key; different keys load independently.
- Context cancellation for loaders and waiting callers.
- Optional caching of failed results until a retry expiration.

## Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    query "github.com/Waterkyuu/go-query"
)

func main() {
    cache := query.New[string, string](time.Now)
    value, err := cache.Get(context.Background(), "greeting", func(ctx context.Context) (string, time.Time, error) {
        return "hello", time.Now().Add(time.Hour), nil
    })
    fmt.Println(value, err)
}
```

Reuse the same cache instance across requests. Loaders supply an absolute expiration so data restored from disk keeps its original freshness. To avoid caching a result, return a zero expiration. To delay retries after a failure, return the fallback value, a future expiration, and the error.

Values are shared and must be treated as immutable. Clone slices or maps before changing them. The cache owns no disk persistence, HTTP transport, retry loop, or background refresh. An expired result refreshes when requested.

The first caller's context controls its loader. A canceled waiter leaves that loader running. If the loader's caller cancels, other callers can retry with their own contexts. Inject a clock instead of `time.Now` for deterministic tests.
