# Getting started with cacheq

[简体中文](getting-started.zh-CN.md)

cacheq is a typed query client for Go. It provides in-process caching, same-key request sharing, and subscriptions to query state. It supports server handlers, background jobs, and interactive applications.

## Installation

Requires Go 1.22 or later. The cacheq core library has no external dependencies.

```sh
go get github.com/Waterkyuu/cacheq
```

Create a client at the application boundary, inject it into consumers, and close it when the application stops. The cache is in process memory; it does not synchronize across services.

## Quick start

Save this complete program as `main.go` in a module importing cacheq, then run `go run .`. The local loader requires no network service.

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// User contains the profile fields required by this application.
type User struct {
	// Name contains the user's display name.
	Name string
}

// main shares one cached result between a subscription and an imperative read.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()

	var calls atomic.Int32
	loadUser := func(context.Context) (User, error) {
		calls.Add(1)
		return User{Name: "Alice"}, nil
	}
	profile := cacheq.Query(client, "user:42", loadUser)
	defer profile.Close()

	user, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		loadUser,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(user.Name)
	fmt.Printf("loader calls: %d\n", calls.Load())
}
```

Expected output:

```text
Alice
loader calls: 1
```

`Query` returns a handle immediately and loads in the background. `Fetch` waits for fresh data: it joins an active same-key load or reuses its completed fresh result. The one-minute freshness window makes both calls use one loader invocation.

## Query lifecycle

| Cache state | When you use `Query` |
| --- | --- |
| No data yet | Load in the background and notify consumers |
| Fresh data | Reuse cached data immediately |
| Stale data | Keep previous data while refreshing |
| Refresh failed | Retain available data and report the error |

Read current state through `Snapshot()` and receive later updates through `Updates()`. A snapshot is a value, so it does not update itself.

## Related guides

- [Queries & conditions](queries.en-US.md): shared clients with different result types, conditional loading, updates, and manual refresh.
- [Cache & lifecycle](cache.en-US.md): reads, writes, prefetch, expiry, automatic cleanup, cancellation, and closing.
- [Invalidation](invalidation.en-US.md): invalidate related data together, select keys with predicates, and choose immediate or deferred refresh.

Each key has one static result type. Include parameters, tenant identity, and user identity that affect the result in your keys to avoid sharing unrelated data.
