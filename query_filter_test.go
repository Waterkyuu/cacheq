package soroi

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestQueryFilterSelection applies one selection contract to invalidation, cancellation, and removal.
func TestQueryFilterSelection(t *testing.T) {
	for _, operation := range []string{"invalidate", "cancel", "remove"} {
		for _, tc := range []struct {
			// name identifies the selection boundary.
			name string
			// target supplies a reusable filter or direct selection.
			target any
			// selected identifies keys whose state should change.
			selected map[any]bool
		}{
			{name: "all", target: QueryFilter{}, selected: map[any]bool{"a": true, "b": true}},
			{name: "keys", target: QueryFilter{Keys: []any{"b", "b", "missing"}}, selected: map[any]bool{"b": true}},
			{name: "predicate", target: QueryFilter{Predicate: func(key any) bool { return key == "a" }},
				selected: map[any]bool{"a": true}},
			{name: "intersection", target: QueryFilter{
				Keys: []any{"a"}, Predicate: func(key any) bool { return key == "b" },
			}, selected: map[any]bool{}},
			{name: "empty keys", target: QueryFilter{Keys: []any{}}, selected: map[any]bool{}},
			{name: "empty batch", target: []any{}, selected: map[any]bool{}},
			{name: "nil batch", target: []any(nil), selected: map[any]bool{}},
			{name: "single", target: "a", selected: map[any]bool{"a": true}},
			{name: "batch", target: []any{"a", "a"}, selected: map[any]bool{"a": true}},
			{name: "direct predicate", target: func(key any) bool { return key == "b" }, selected: map[any]bool{"b": true}},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				c := newInvalidationClient(t)
				for _, key := range []string{"a", "b"} {
					if err := Set(c, key, 1); err != nil {
						t.Fatal(err)
					}
					if operation == "cancel" {
						started := make(chan struct{})
						query := Query(
							c,
							key,
							func(ctx context.Context) (int, error) {
								close(started)
								<-ctx.Done()
								return 0, ctx.Err()
							},
							QueryOptions{Disable: true},
						)
						t.Cleanup(query.Close)
						go func() { _, _ = query.Refetch(context.Background()) }()
						select {
						case <-started:
						case <-time.After(3 * time.Second):
							t.Fatal("load did not start")
						}
					}
				}
				if err := applyFilterOperation(c, operation, tc.target); err != nil {
					t.Fatal(err)
				}
				for _, key := range []string{"a", "b"} {
					state := Get[int](c, key)
					switch operation {
					case "invalidate":
						if state.Stale != tc.selected[key] || state.Data != 1 {
							t.Fatalf("key %s = %+v", key, state)
						}
					case "cancel":
						if state.Fetching == tc.selected[key] || state.Data != 1 {
							t.Fatalf("key %s = %+v", key, state)
						}
					case "remove":
						if state.HasData == tc.selected[key] {
							t.Fatalf("key %s = %+v", key, state)
						}
					}
				}
				seenMissing := false
				if err := c.Cancel(QueryFilter{Predicate: func(key any) bool {
					seenMissing = seenMissing || key == "missing"
					return false
				}}); err != nil || seenMissing {
					t.Fatalf("filter created a missing key: %v", err)
				}
			})
		}
	}
}

// applyFilterOperation exercises public selection while suppressing background refresh during assertions.
func applyFilterOperation(c *Client, operation string, target any) error {
	switch operation {
	case "invalidate":
		return c.Invalidate(target, InvalidateOptions{Refetch: RefetchNone})
	case "cancel":
		return c.Cancel(target)
	default:
		return c.Remove(target)
	}
}

// TestQueryFilterValidation rejects entire invalid selections before predicates or operation-owned changes.
func TestQueryFilterValidation(t *testing.T) {
	for _, operation := range []string{"invalidate", "cancel", "remove"} {
		for _, target := range []any{
			nil, []any{"a", nil}, []string{"a"}, (func(any) bool)(nil),
			QueryFilter{Keys: []any{"a", []int{1}}},
		} {
			c := newInvalidationClient(t)
			if err := Set(c, "a", 1); err != nil {
				t.Fatal(err)
			}
			if err := applyFilterOperation(c, operation, target); !errors.Is(err, ErrInvalidKey) {
				t.Fatalf("%s(%T) = %v", operation, target, err)
			}
			if state := Get[int](c, "a"); state.Stale || !state.HasData || state.Data != 1 {
				t.Fatalf("invalid %s changed data: %+v", operation, state)
			}
			c.Close()
			if err := applyFilterOperation(c, operation, QueryFilter{Predicate: func(any) bool {
				t.Fatal("closed client evaluated predicate")
				return true
			}}); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed %s = %v", operation, err)
			}
		}
	}
	c := newInvalidationClient(t)
	err := c.Remove(QueryFilter{Keys: []any{"a", nil}, Predicate: func(any) bool {
		t.Fatal("invalid keys evaluated predicate")
		return true
	}})
	if !errors.Is(err, ErrInvalidKey) {
		t.Fatal(err)
	}
}

// TestQueryFilterInvalidCancel preserves active work when a later selection key is invalid.
func TestQueryFilterInvalidCancel(t *testing.T) {
	c := newInvalidationClient(t)
	started := make(chan struct{})
	q := Query(c, "a", func(ctx context.Context) (int, error) {
		close(started)
		<-ctx.Done()
		return 0, ctx.Err()
	})
	t.Cleanup(q.Close)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("load did not start")
	}
	for _, target := range []any{[]any{"a", nil}, QueryFilter{Keys: []any{"a", nil}}} {
		if err := c.Cancel(target); !errors.Is(err, ErrInvalidKey) || !q.Snapshot().Fetching {
			t.Fatalf("invalid cancel changed active work: %v, %+v", err, q.Snapshot())
		}
	}
}

// TestQueryFilterPredicate permits Client calls and excludes additions and removed keys during matching.
func TestQueryFilterPredicate(t *testing.T) {
	for _, operation := range []string{"invalidate", "cancel", "remove"} {
		t.Run(operation, func(t *testing.T) {
			c := newInvalidationClient(t)
			for _, key := range []string{"a", "b"} {
				if err := Set(c, key, 1); err != nil {
					t.Fatal(err)
				}
			}
			seen := make(map[any]int)
			filter := QueryFilter{Keys: []any{"a", "b", "a", "missing"}, Predicate: func(key any) bool {
				seen[key]++
				_ = Get[int](c, "a")
				if err := Set(c, "added", 2); err != nil {
					t.Fatal(err)
				}
				if err := c.Remove("b"); err != nil {
					t.Fatal(err)
				}
				return true
			}}
			if err := applyFilterOperation(c, operation, filter); err != nil {
				t.Fatal(err)
			}
			if seen["a"] != 1 || seen["b"] != 1 || seen["missing"] != 0 || seen["added"] != 0 {
				t.Fatalf("predicate calls = %v", seen)
			}
			if state := Get[int](c, "added"); state.Stale || !state.HasData {
				t.Fatalf("new key affected: %+v", state)
			}
			if err := c.Cancel(QueryFilter{Predicate: func(key any) bool {
				if key == "b" {
					t.Fatal("removed key was recreated")
				}
				return false
			}}); err != nil {
				t.Fatal(err)
			}
			if err := applyFilterOperation(c, operation, QueryFilter{Predicate: func(any) bool {
				c.Close()
				return true
			}}); !errors.Is(err, ErrClosed) {
				t.Fatalf("closure while matching = %v", err)
			}
		})
	}
}

// TestQueryFilterWait preserves explicit order and covers deduplication, empty subscriptions, and load failures.
func TestQueryFilterWait(t *testing.T) {
	c := newInvalidationClient(t)
	key := testScopedKey{Resource: "users", Tenant: 42}
	if err := Set(c, key, 1); err != nil {
		t.Fatal(err)
	}
	q := Query(c, key, func(context.Context) (int, error) { return 2, nil })
	t.Cleanup(q.Close)
	failure := errors.New("backend unavailable")
	failed := Query(
		c,
		"failure",
		func(context.Context) (int, error) { return 0, failure },
		QueryOptions{Disable: true},
	)
	t.Cleanup(failed.Close)
	if err := failed.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	awaitState(t, failed.Updates(), func(s Snapshot[int]) bool { return s.Err != nil && !s.Fetching })
	disabled := Query(
		c,
		"empty",
		func(context.Context) (bool, error) { return true, nil },
		QueryOptions{Disable: true},
	)
	t.Cleanup(disabled.Close)
	filter := QueryFilter{Keys: []any{"empty", key, "failure", key, "unknown"}}
	results, err := c.InvalidateAndWait(context.Background(), filter)
	if !errors.Is(err, failure) || len(results) != 3 {
		t.Fatalf("results = %+v, %v", results, err)
	}
	if results[0].Key != "empty" || results[0].Refreshed || results[0].Err != nil {
		t.Fatalf("disabled result = %+v", results[0])
	}
	if results[1].Key != key || !results[1].Refreshed || q.Snapshot().Data != 2 {
		t.Fatalf("success result = %+v", results[1])
	}
	if results[2].Key != "failure" || !errors.Is(results[2].Err, failure) {
		t.Fatalf("failure result = %+v", results[2])
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.InvalidateAndWait(
		ctx,
		filter,
	); !errors.Is(err, context.Canceled) ||
		q.Snapshot().Stale {
		t.Fatalf("pre-canceled selection changed data: %v", err)
	}
}
