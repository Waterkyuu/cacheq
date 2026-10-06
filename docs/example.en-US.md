# Unified invalidation example

[简体中文](example.zh-CN.md)

Copy the program into `main.go` in an application that imports cacheq, then run `go run .`. One `Invalidate` method accepts an exact key, a `[]any` key list, or a `func(any) bool` predicate. Cached data remains available after invalidation.

```go
package main

import (
	"fmt"
	"strings"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// main demonstrates the three selection forms of Invalidate with one shared client.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()

	if err := cacheq.Set(client, "user:42", "Alice"); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, "user:7", "Bob"); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, "users", []string{"Alice", "Bob"}); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, "settings", "dark"); err != nil {
		panic(err)
	}

	// An exact key selects only that result and retains its cached data.
	if err := client.Invalidate("user:42"); err != nil {
		panic(err)
	}
	detail := cacheq.Get[string](client, "user:42")
	fmt.Println("single:", detail.Data, detail.Stale)

	// A []any batch selects explicit keys; duplicates are invalidated once.
	if err := client.Invalidate([]any{"user:42", "users", "users"}); err != nil {
		panic(err)
	}
	fmt.Println("batch:", cacheq.Get[[]string](client, "users").Stale)

	// A predicate selects existing keys; RefetchNone suppresses new requests.
	if err := client.Invalidate(func(key any) bool {
		name, ok := key.(string)
		return ok && strings.HasPrefix(name, "user:")
	}, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
		panic(err)
	}
	fmt.Println("predicate:", cacheq.Get[string](client, "user:7").Stale)
	fmt.Println("unrelated:", cacheq.Get[string](client, "settings").Stale)
}
```

Expected output:

```text
single: Alice true
batch: true
predicate: true
unrelated: false
```

Omitting options refreshes keys with enabled query handles. This example has no query handles, so invalidation only marks data stale. `RefetchNone` also suppresses new requests when enabled handles exist. A nil target or predicate returns `ErrInvalidKey`; an empty or nil `[]any` batch does nothing. Other slice types such as `[]string` are not batches. Closed clients return `ErrClosed`.
