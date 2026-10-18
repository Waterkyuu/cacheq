// Command shared_invalidation demonstrates typed remote tag invalidation and local notification delivery.
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
	redistore "github.com/Waterkyuu/soroi/store/redis"
	"github.com/redis/go-redis/v9"
)

// User represents the immutable source value shared by detail and list queries.
type User struct {
	// Name is the display name returned by the authoritative source.
	Name string
}

// main reports configuration and query errors to the shell.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run commits a simulated source change before remotely invalidating two related query result types.
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
		return err
	}
	store, err := redistore.New(
		remote,
		redistore.Options{Prefix: "soroi-invalidation-demo", EnableTags: true, EnableNotifications: true},
	)
	if err != nil {
		return err
	}
	detailShared, err := soroi.NewSharedCache(store, soroi.SharedCacheOptions[User]{
		Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute, ShareTags: true,
	})
	if err != nil {
		return err
	}
	listShared, err := soroi.NewSharedCache(store, soroi.SharedCacheOptions[[]User]{
		Key: "tenant:42:users:v1", Format: "users.json.v1", TTL: 5 * time.Minute, ShareTags: true,
	})
	if err != nil {
		return err
	}
	tags := []string{"tenant:42:users"}
	// Reset only the reserved demonstration namespace's label so repeated runs produce the same output.
	if _, err := store.InvalidateTags(ctx, tags); err != nil {
		return err
	}
	var source atomic.Value
	source.Store(User{Name: "Alice"})
	var calls atomic.Int64
	detail, err := soroi.QueryDefinition(
		"detail",
		func(context.Context) (User, error) { calls.Add(1); return source.Load().(User), nil },
		soroi.QueryOptions{Disable: true},
	)
	if err != nil {
		return err
	}
	detail.Tags = tags
	detail.Shared = detailShared
	list, err := soroi.QueryDefinition(
		"list",
		func(context.Context) ([]User, error) { calls.Add(1); return []User{source.Load().(User)}, nil },
		soroi.QueryOptions{Disable: true},
	)
	if err != nil {
		return err
	}
	list.Tags = tags
	list.Shared = listShared
	a := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	b := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()
	watchA, err := a.WatchShared(ctx, store, soroi.InvalidateOptions{Refetch: soroi.RefetchNone})
	if err != nil {
		return err
	}
	defer watchA.Close()
	watchB, err := b.WatchShared(ctx, store, soroi.InvalidateOptions{Refetch: soroi.RefetchNone})
	if err != nil {
		return err
	}
	defer watchB.Close()
	if err := detail.Prefetch(ctx, a); err != nil {
		return err
	}
	if err := list.Prefetch(ctx, a); err != nil {
		return err
	}
	if _, err := detail.Fetch(ctx, b); err != nil {
		return err
	}
	if _, err := list.Fetch(ctx, b); err != nil {
		return err
	}
	d, l := detail.Query(b), list.Query(b)
	defer d.Close()
	defer l.Close()
	fmt.Printf("initial detail: %s\n", d.Snapshot().Data.Name)
	fmt.Printf("initial list: %s\n", l.Snapshot().Data[0].Name)
	fmt.Printf("source calls: %d\n", calls.Load())
	// In production, this step is a successful database or API commit.
	source.Store(User{Name: "Bob"})
	if err := a.InvalidateSharedTags(
		ctx,
		store,
		tags,
		soroi.InvalidateOptions{Refetch: soroi.RefetchNone},
	); err != nil {
		return err
	}
	if err := waitStale(ctx, d); err != nil {
		return err
	}
	if err := waitStale(ctx, l); err != nil {
		return err
	}
	fmt.Printf("notified detail stale: %t\n", d.Snapshot().Stale)
	fmt.Printf("notified list stale: %t\n", l.Snapshot().Stale)
	fmt.Printf("retained detail: %s\n", d.Snapshot().Data.Name)
	newDetail, err := detail.Fetch(ctx, b)
	if err != nil {
		return err
	}
	newList, err := list.Fetch(ctx, b)
	if err != nil {
		return err
	}
	fmt.Printf("refreshed detail: %s\n", newDetail.Name)
	fmt.Printf("refreshed list: %s\n", newList[0].Name)
	fmt.Printf("source calls: %d\n", calls.Load())
	return nil
}

// waitStale waits for notification publication without sleeps, polling, or assumptions about delivery latency.
func waitStale[V any](ctx context.Context, query *soroi.QueryHandle[V]) error {
	for {
		select {
		case state := <-query.Updates():
			if state.HasData && state.Stale {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
