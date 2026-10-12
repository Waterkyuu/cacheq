package cacheq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestQueryDefinition reuses one definition across clients and loading entry points without owning a cache.
func TestQueryDefinition(t *testing.T) {
	hour, retries := time.Hour, 1
	calls := 0
	failure := errors.New("offline")
	definition := QueryDefinition[int]{
		Key: "user",
		Fetcher: func(context.Context) (int, error) {
			calls++
			if calls == 1 {
				return 0, failure
			}
			return calls, nil
		},
		Options: QueryOptions{
			Disable: true, StaleTime: &hour, Retry: &retries,
			RetryDelay: func(int) time.Duration { return 0 },
		},
	}
	if calls != 0 {
		t.Fatal("constructing a definition loaded data")
	}
	c, _ := newAgeClient(t, Options{})
	h := definition.Query(c)
	t.Cleanup(h.Close)
	if h.Snapshot().Fetching || calls != 0 {
		t.Fatal("definition ignored automatic loading permission")
	}
	if err := definition.Prefetch(context.Background(), c); err != nil || calls != 2 {
		t.Fatalf("definition prefetch = %v, calls=%d", err, calls)
	}
	value, err := definition.Fetch(context.Background(), c)
	if err != nil || value != 2 || calls != 2 || h.Snapshot().Stale || h.Snapshot().Data != 2 {
		t.Fatalf("shared definition = %d, %v, state=%+v", value, err, h.Snapshot())
	}
	// Creating a handle copies policy; modifying the definition does not change its subscription.
	definition.Options.Disable = false
	if err := c.Invalidate(definition.Key); err != nil || calls != 2 || h.Snapshot().Fetching {
		t.Fatalf("existing definition handle changed permission: %v, calls=%d", err, calls)
	}
	other, _ := newAgeClient(t, Options{})
	value, err = definition.Fetch(context.Background(), other)
	if err != nil || value != 3 || calls != 3 {
		t.Fatalf("independent Client = %d, %v, calls=%d", value, err, calls)
	}
}

// TestQueryDefinitionErrors delegates validation and cancellation to the existing query entry points.
func TestQueryDefinitionErrors(t *testing.T) {
	for _, api := range []string{"fetch", "prefetch", "query"} {
		t.Run(api, func(t *testing.T) {
			c, _ := newAgeClient(t, Options{})
			failure := errors.New("read rejected")
			calls := 0
			definition := QueryDefinition[int]{
				Key: "user",
				Fetcher: func(context.Context) (int, error) {
					calls++
					return 0, failure
				},
				Options: QueryOptions{Disable: true},
			}
			var err error
			switch api {
			case "fetch":
				_, err = definition.Fetch(context.Background(), c)
			case "prefetch":
				err = definition.Prefetch(context.Background(), c)
			case "query":
				h := definition.Query(c)
				t.Cleanup(h.Close)
				_, err = h.Refetch(context.Background())
			}
			if !errors.Is(err, failure) || calls != 1 {
				t.Fatalf("definition failure = %v, calls=%d", err, calls)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := definition.Fetch(ctx, c); !errors.Is(err, context.Canceled) || calls != 1 {
				t.Fatalf("canceled fetch = %v, calls=%d", err, calls)
			}
			if err := definition.Prefetch(ctx, c); !errors.Is(err, context.Canceled) || calls != 1 {
				t.Fatalf("canceled prefetch = %v, calls=%d", err, calls)
			}
			definition.Key = []int{1}
			if _, err := definition.Fetch(context.Background(), c); !errors.Is(err, ErrInvalidKey) {
				t.Fatal(err)
			}
			if err := definition.Prefetch(context.Background(), c); !errors.Is(err, ErrInvalidKey) {
				t.Fatal(err)
			}
			h := definition.Query(c)
			t.Cleanup(h.Close)
			if !errors.Is(h.Snapshot().Err, ErrInvalidKey) {
				t.Fatal(h.Snapshot().Err)
			}
		})
	}
}
