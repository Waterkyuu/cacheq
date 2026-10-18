package cacheq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestQueryOptionCounts accepts optional configuration and rejects extras before cache use or loading.
func TestQueryOptionCounts(t *testing.T) {
	for _, api := range []string{"fetch", "prefetch", "query"} {
		for count := range 3 {
			t.Run(fmt.Sprintf("%s/%d", api, count), func(t *testing.T) {
				c, _ := newAgeClient(t, Options{StaleTime: time.Hour})
				if err := Set(c, "key", 7); err != nil {
					t.Fatal(err)
				}
				load := func(context.Context) (int, error) {
					t.Error("configuration validation or inherited freshness started a load")
					return 0, nil
				}
				options := make([]QueryOptions, count)
				ctx := context.Background()
				var err error
				var query *QueryHandle[int]
				switch api {
				case "fetch":
					_, err = Fetch(ctx, c, "key", load, options...)
				case "prefetch":
					err = Prefetch(ctx, c, "key", load, options...)
				case "query":
					query = Query(c, "key", load, options...)
				}
				if query != nil {
					t.Cleanup(query.Close)
					err = query.Snapshot().Err
					initial := <-query.Updates()
					if count > 1 {
						if !errors.Is(initial.Err, ErrTooManyOptions) {
							t.Fatalf("initial error = %+v", initial)
						}
						if _, open := <-query.Updates(); open {
							t.Fatal("invalid query kept its updates channel open")
						}
						if _, err := query.Refetch(ctx); !errors.Is(err, ErrTooManyOptions) {
							t.Fatal(err)
						}
					} else if !initial.HasData || initial.Data != 7 {
						t.Fatalf("optional query = %+v", initial)
					}
				}
				if count > 1 {
					if !errors.Is(err, ErrTooManyOptions) {
						t.Fatalf("configuration error = %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if state := Get[int](c, "key"); state.Data != 7 || state.Stale {
					t.Fatalf("cache changed = %+v", state)
				}
			})
		}
	}
}

// TestMutationOptionCounts preserves budget inheritance and rejects extras before writing or synchronizing.
func TestMutationOptionCounts(t *testing.T) {
	for count := range 3 {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			budget, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
			if err != nil {
				t.Fatal(err)
			}
			c := NewClient(Options{Executor: budget})
			t.Cleanup(c.Close)
			writes, callbacks := 0, 0
			h := Mutation(c, func(context.Context, string) (int, error) {
				writes++
				if budget.Stats().Active != 1 {
					t.Error("lost inherited write budget")
				}
				return 7, nil
			}, func(context.Context, int, string) error {
				callbacks++
				if budget.Stats().Active != 0 {
					t.Error("synchronization retained write capacity")
				}
				return nil
			}, make([]MutationOptions, count)...)
			t.Cleanup(h.Close)
			result, err := h.Mutate(context.Background(), "input")
			if count > 1 {
				if !errors.Is(err, ErrTooManyOptions) || writes != 0 || callbacks != 0 {
					t.Fatalf("rejected mutation = %+v, %v, writes=%d callbacks=%d", result, err, writes, callbacks)
				}
				if state := <-h.Updates(); !errors.Is(state.Err, ErrTooManyOptions) {
					t.Fatalf("initial error = %+v", state)
				}
				if _, open := <-h.Updates(); open {
					t.Fatal("invalid mutation kept its updates channel open")
				}
				return
			}
			if err != nil || result.SyncErr != nil || result.Data != 7 || writes != 1 || callbacks != 1 {
				t.Fatalf("optional mutation = %+v, %v, writes=%d callbacks=%d", result, err, writes, callbacks)
			}
		})
	}
}

// TestInvalidationOptionCounts rejects extras before local invalidation, remote writes, or subscriptions.
func TestInvalidationOptionCounts(t *testing.T) {
	for _, api := range []string{"invalidate", "wait", "shared", "watch", "tags"} {
		for count := range 3 {
			t.Run(fmt.Sprintf("%s/%d", api, count), func(t *testing.T) {
				backend := newSharedTestStore()
				store := &sharedNotificationStore{SharedStore: backend, enableTags: true}
				definition := newSharedDefinition(t, store, func(context.Context) (int, error) { return 7, nil })
				definition.Tags = []string{"users"}
				c, _ := newAgeClient(t, Options{StaleTime: time.Hour})
				ctx := context.Background()
				if _, err := definition.Fetch(ctx, c); err != nil {
					t.Fatal(err)
				}
				before, err := backend.Read(ctx, definition.Shared.options.Key)
				if err != nil {
					t.Fatal(err)
				}
				options := make([]InvalidateOptions, count)
				switch api {
				case "invalidate":
					err = c.Invalidate(definition.Key, options...)
				case "wait":
					_, err = c.InvalidateAndWait(ctx, definition.Key, options...)
				case "shared":
					err = definition.InvalidateShared(ctx, c, options...)
				case "watch":
					var watcher *SharedWatcher
					watcher, err = c.WatchShared(ctx, store, options...)
					if watcher != nil {
						watcher.Close()
					}
				case "tags":
					err = c.InvalidateSharedTags(ctx, store, []string{"users"}, options...)
				}
				if count < 2 {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				if !errors.Is(err, ErrTooManyOptions) {
					t.Fatalf("configuration error = %v", err)
				}
				if state := Get[int](c, definition.Key); state.Stale || state.Data != 7 {
					t.Fatalf("cache changed = %+v", state)
				}
				after, readErr := backend.Read(ctx, definition.Shared.options.Key)
				if readErr != nil || before.Version != after.Version || len(store.streams) != 0 || store.sequence != 0 {
					t.Fatalf("rejected operation accessed remote state: %v", readErr)
				}
			})
		}
	}
}
