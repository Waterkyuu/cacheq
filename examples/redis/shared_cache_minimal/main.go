// Command shared_cache_minimal shows a source load, a shared hit, and a local hit in order.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Waterkyuu/cacheq"
	redistore "github.com/Waterkyuu/cacheq/store/redis"
	"github.com/redis/go-redis/v9"
)

// User is the query result encoded into Redis and restored by each Client.
type User struct {
	// Name is the display name supplied by the simulated database.
	Name string
}

// main runs three sequential reads and prints which cache layers they use.
// A fresh disposable Redis is recommended; setup clears only this demo's reserved shared key.
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	// 1. remote is the Redis connection; store adapts it to cacheq's storage operations.
	remote := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	defer func() { _ = remote.Close() }()
	if err := remote.Ping(ctx).Err(); err != nil {
		panic(err)
	}
	store, err := redistore.New(remote, redistore.Options{Prefix: "cacheq-minimal-demo"})
	if err != nil {
		panic(err)
	}

	// 2. shared holds rules for storing one User; construction does not access Redis.
	shared, err := cacheq.NewSharedCache(store, cacheq.SharedCacheOptions[User]{
		Key: "user:7", Format: "user.json.v1", TTL: time.Minute,
	})
	if err != nil {
		panic(err)
	}
	// Clear only this demo's shared key so repeat runs start with a shared miss.
	if err := shared.Invalidate(ctx); err != nil {
		panic(err)
	}

	// 3. Define the source callback now; cacheq runs it only when neither cache has fresh data.
	// Each actual source call prints a line and returns User{Name: "Alice"}.
	definition, err := cacheq.QueryDefinition("user:7", func(context.Context) (User, error) {
		fmt.Println("source: loading user 7")
		return User{Name: "Alice"}, nil
	})
	if err != nil {
		panic(err)
	}
	// Attaching shared makes reads consult Redis after the Client's own cache misses.
	definition.Shared = shared

	// 4. A and B own separate local caches. Fresh local results can be reused for one minute.
	a := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	b := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()

	// A has no local value and Redis has none: call the source, then fill both caches.
	first, err := definition.Fetch(ctx, a)
	if err != nil {
		panic(err)
	}
	fmt.Printf("A first: %s, shared hits: %d\n", first.Name, shared.Stats().Hits)

	// B has no local value: decode Redis data into a User and fill B's local cache.
	second, err := definition.Fetch(ctx, b)
	if err != nil {
		panic(err)
	}
	fmt.Printf("B first: %s, shared hits: %d\n", second.Name, shared.Stats().Hits)

	// A already has fresh local data: return it without accessing Redis or the source.
	third, err := definition.Fetch(ctx, a)
	if err != nil {
		panic(err)
	}
	fmt.Printf("A again: %s, shared hits: %d\n", third.Name, shared.Stats().Hits)
}
