// Command shared_cache demonstrates shared reads, explicit invalidation, and cache failure fallback.
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
	redistore "github.com/Waterkyuu/cacheq/store/redis"
	"github.com/redis/go-redis/v9"
)

// User represents the immutable result shared by the example's query clients.
type User struct {
	// Name is the display name returned by the simulated authoritative source.
	Name string
}

// main runs the example and reports setup or query failures to the shell.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run uses two clients and a simulated source to make each cache layer's effects observable.
func run() error {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	remote := redis.NewClient(&redis.Options{
		Addr: addr, ContextTimeoutEnabled: true, DialTimeout: time.Second,
		ReadTimeout: time.Second, WriteTimeout: time.Second, MaxRetries: -1,
	})
	defer func() { _ = remote.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := remote.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connect to example Redis: %w", err)
	}
	store, err := redistore.New(remote, redistore.Options{Prefix: "cacheq-demo"})
	if err != nil {
		return err
	}
	shared, err := cacheq.NewSharedCache[User](store, cacheq.SharedCacheOptions[User]{
		Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute,
		Timeout: 200 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	// Reset this reserved demonstration key so repeated runs have identical output.
	if err := shared.Invalidate(ctx); err != nil {
		return err
	}
	var source atomic.Value
	source.Store(User{Name: "Alice"})
	var calls atomic.Int64
	definition, err := cacheq.QueryDefinition("user:7", func(context.Context) (User, error) {
		calls.Add(1)
		return source.Load().(User), nil
	})
	if err != nil {
		return err
	}
	definition.Shared = shared
	a := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	b := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()
	if err := printRead(ctx, "A first", a, definition, &calls); err != nil {
		return err
	}
	if err := printRead(ctx, "B shared", b, definition, &calls); err != nil {
		return err
	}
	if err := printRead(ctx, "A local", a, definition, &calls); err != nil {
		return err
	}
	// A business write must succeed before invalidating its cached results.
	source.Store(User{Name: "Bob"})
	if err := definition.InvalidateShared(ctx, a, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
		return err
	}
	if err := printRead(ctx, "A refreshed", a, definition, &calls); err != nil {
		return err
	}
	if err := printRead(ctx, "B local", b, definition, &calls); err != nil {
		return err
	}
	if err := b.Invalidate(definition.Key, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
		return err
	}
	if err := printRead(ctx, "B refreshed", b, definition, &calls); err != nil {
		return err
	}
	stats := shared.Stats()
	fmt.Printf("shared hits: %d, writes: %d, bypasses: %d\n", stats.Hits, stats.Writes, stats.Bypasses)
	// Closing only the borrowed Redis connection simulates an unavailable cache without stopping the source.
	if err := remote.Close(); err != nil {
		return err
	}
	c := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer c.Close()
	if err := printRead(ctx, "C fallback", c, definition, &calls); err != nil {
		return err
	}
	fmt.Printf("shared read errors: %d\n", shared.Stats().ReadErrors)
	return nil
}

// printRead reports the typed result and actual source calls for one explicit query read.
func printRead(
	ctx context.Context,
	label string,
	client *cacheq.Client,
	definition cacheq.Definition[User],
	calls *atomic.Int64,
) error {
	value, err := definition.Fetch(ctx, client)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s, source calls: %d\n", label, value.Name, calls.Load())
	return nil
}
