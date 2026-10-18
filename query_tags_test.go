package soroi

import (
	"context"
	"errors"
	"testing"
	"time"
)

// selectedTagKeys observes public tag selection without canceling loads or changing cached data.
func selectedTagKeys(t *testing.T, c *Client, filter QueryFilter) map[any]bool {
	t.Helper()
	selected := make(map[any]bool)
	predicate := filter.Predicate
	filter.Predicate = func(key any) bool {
		if predicate == nil || predicate(key) {
			selected[key] = true
		}
		return false
	}
	if err := c.Cancel(filter); err != nil {
		t.Fatal(err)
	}
	return selected
}

// TestQueryTagsSelection combines exact tags, keys, and predicates without changing result ownership.
func TestQueryTagsSelection(t *testing.T) {
	c, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	for key, tags := range map[string][]string{
		"a": {"users", "tenant:42"}, "b": {"users", "tenant:99"},
		"c": {"settings", "tenant:42"}, "empty": {""},
	} {
		definition, err := QueryDefinition(key, func(context.Context) (int, error) { return 1, nil })
		if err != nil {
			t.Fatal(err)
		}
		definition.Tags = tags
		if _, err := definition.Fetch(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		// name identifies the matching boundary.
		name string
		// filter supplies the selection fields being combined.
		filter QueryFilter
		// keys lists the expected distinct selected keys.
		keys []string
	}{
		{name: "one", filter: QueryFilter{Tags: []string{"users"}}, keys: []string{"a", "b"}},
		{name: "any", filter: QueryFilter{Tags: []string{"users", "settings"}}, keys: []string{"a", "b", "c"}},
		{name: "duplicate", filter: QueryFilter{Tags: []string{"users", "users"}}, keys: []string{"a", "b"}},
		{name: "exact", filter: QueryFilter{Tags: []string{"user"}}, keys: []string{}},
		{name: "empty list", filter: QueryFilter{Tags: []string{}}, keys: []string{}},
		{name: "empty label", filter: QueryFilter{Tags: []string{""}}, keys: []string{"empty"}},
		{name: "nil list", filter: QueryFilter{}, keys: []string{"a", "b", "c", "empty"}},
		{name: "keys intersection", filter: QueryFilter{Keys: []any{"a", "c"}, Tags: []string{"users"}},
			keys: []string{"a"}},
		{name: "predicate intersection", filter: QueryFilter{
			Tags: []string{"users"}, Predicate: func(key any) bool { return key == "b" },
		}, keys: []string{"b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := selectedTagKeys(t, c, tc.filter)
			if len(selected) != len(tc.keys) {
				t.Fatalf("selected = %v, want %v", selected, tc.keys)
			}
			for _, key := range tc.keys {
				if !selected[key] {
					t.Fatalf("missing key %s in %v", key, selected)
				}
			}
		})
	}
}

// TestQueryTagsRegistration copies tags and registers them even when an operation reuses fresh data.
func TestQueryTagsRegistration(t *testing.T) {
	for _, api := range []string{"fetch", "prefetch", "query"} {
		t.Run(api, func(t *testing.T) {
			c, _ := newAgeClient(t, Options{StaleTime: time.Hour})
			if err := Set(c, "key", 1); err != nil {
				t.Fatal(err)
			}
			tags := []string{"users", "users"}
			definition, err := QueryDefinition("key", func(context.Context) (int, error) {
				t.Error("registration replaced a fresh cached value")
				return 2, nil
			}, QueryOptions{Disable: true})
			if err != nil {
				t.Fatal(err)
			}
			definition.Tags = tags
			if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"users"}})) != 0 {
				t.Fatal("constructing a definition registered tags")
			}
			switch api {
			case "fetch":
				if _, err := definition.Fetch(context.Background(), c); err != nil {
					t.Fatal(err)
				}
			case "prefetch":
				if err := definition.Prefetch(context.Background(), c); err != nil {
					t.Fatal(err)
				}
			case "query":
				h := definition.Query(c)
				h.Close()
			}
			tags[0], tags[1] = "changed", "changed"
			if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"users"}})) != 1 {
				t.Fatal("tags were not retained after the operation")
			}
			if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"changed"}})) != 0 {
				t.Fatal("registered tags alias the definition's slice")
			}
			other, _ := newAgeClient(t, Options{})
			if len(selectedTagKeys(t, other, QueryFilter{Tags: []string{"users"}})) != 0 {
				t.Fatal("tags leaked to another Client")
			}
		})
	}
}

// TestQueryTagsRetention merges consumers' tags and preserves them through local writes and live removal.
func TestQueryTagsRetention(t *testing.T) {
	c, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	definition, err := QueryDefinition(
		"key",
		func(context.Context) (int, error) { return 1, nil },
		QueryOptions{Disable: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	definition.Tags = []string{"users"}
	first := definition.Query(c)
	t.Cleanup(first.Close)
	definition.Tags = []string{"admin"}
	second := definition.Query(c)
	second.Close()
	if err := Set(c, "key", 2); err != nil {
		t.Fatal(err)
	}
	if err := Update(c, "key", func(value int) (int, error) { return value + 1, nil }); err != nil {
		t.Fatal(err)
	}
	for _, tag := range []string{"users", "admin"} {
		if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{tag}})) != 1 {
			t.Fatalf("local writes or closing a consumer lost tag %s", tag)
		}
	}
	if err := c.Remove(QueryFilter{Tags: []string{"admin"}}); err != nil || first.Snapshot().HasData {
		t.Fatalf("tagged removal = %v", err)
	}
	if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"users"}})) != 1 {
		t.Fatal("live subscription lost its tags after removal")
	}
	if _, err := first.Refetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.Clear()
	if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"admin"}})) != 1 {
		t.Fatal("clear lost live query tags")
	}
	first.Close()
	if err := c.Remove("key"); err != nil {
		t.Fatal(err)
	}
	if err := Set(c, "key", 4); err != nil {
		t.Fatal(err)
	}
	if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"users", "admin"}})) != 0 {
		t.Fatal("deleted tags were restored by an untagged write")
	}
}

// TestQueryTagsValidation leaves registration unchanged when type, loader, or context validation fails.
func TestQueryTagsValidation(t *testing.T) {
	c, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	if err := Set(c, "typed", 1); err != nil {
		t.Fatal(err)
	}
	bad, err := QueryDefinition("typed", func(context.Context) (string, error) { return "", nil })
	if err != nil {
		t.Fatal(err)
	}
	bad.Tags = []string{"rejected"}
	if _, err := bad.Fetch(context.Background(), c); !errors.Is(err, ErrTypeMismatch) {
		t.Fatal(err)
	}
	h := bad.Query(c)
	t.Cleanup(h.Close)
	if !errors.Is(h.Snapshot().Err, ErrTypeMismatch) {
		t.Fatal(h.Snapshot().Err)
	}
	bad.Key, bad.Fetcher = "missing", nil
	if err := bad.Prefetch(context.Background(), c); !errors.Is(err, ErrNoFetcher) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	bad.Key = "typed"
	if _, err := bad.Fetch(ctx, c); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"rejected"}})) != 0 {
		t.Fatal("rejected operation registered tags")
	}
	failure := errors.New("offline")
	failed, err := QueryDefinition("failed", func(context.Context) (int, error) { return 0, failure })
	if err != nil {
		t.Fatal(err)
	}
	failed.Tags = []string{"failed"}
	if _, err := failed.Fetch(context.Background(), c); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"failed"}})) != 1 {
		t.Fatal("failed query lost its registered tags")
	}
	if err := c.Remove(QueryFilter{Keys: []any{"typed", nil}, Tags: []string{}}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("empty tags bypassed key validation: %v", err)
	}
}

// TestQueryTagsCapacity removes tags with an evicted key and does not transfer them to a replacement.
func TestQueryTagsCapacity(t *testing.T) {
	c, _ := newAgeClient(t, Options{StaleTime: time.Hour, MaxEntries: 1})
	definition, err := QueryDefinition("key", func(context.Context) (int, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	definition.Tags = []string{"users"}
	if _, err := definition.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if err := Set(c, "other", 2); err != nil {
		t.Fatal(err)
	}
	if err := Set(c, "key", 3); err != nil {
		t.Fatal(err)
	}
	if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"users"}})) != 0 {
		t.Fatal("evicted tags leaked to a replacement query")
	}
}

// TestQueryTagsGC releases labels with inactive cached state using a controlled cleanup callback.
func TestQueryTagsGC(t *testing.T) {
	c, scheduler := newGCClient(t, time.Minute)
	definition, err := QueryDefinition("key", func(context.Context) (int, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	definition.Tags = []string{"users"}
	if _, err := definition.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	timer := scheduler.latest(t)
	scheduler.now.Add(int64(time.Minute))
	timer.callback()
	if err := Set(c, "key", 2); err != nil {
		t.Fatal(err)
	}
	if len(selectedTagKeys(t, c, QueryFilter{Tags: []string{"users"}})) != 0 {
		t.Fatal("collected tags leaked to a replacement query")
	}
}

// TestQueryTagsSnapshot excludes labels registered after the operation has captured its candidates.
func TestQueryTagsSnapshot(t *testing.T) {
	c, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	definition, err := QueryDefinition("a", func(context.Context) (int, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	definition.Tags = []string{"users"}
	if _, err := definition.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	definition.Key, definition.Tags = "b", []string{"settings"}
	if _, err := definition.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	err = c.Remove(QueryFilter{Tags: []string{"users"}, Predicate: func(any) bool {
		definition.Tags = []string{"users"}
		if _, err := definition.Fetch(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		return true
	}})
	if err != nil || Get[int](c, "a").HasData || !Get[int](c, "b").HasData {
		t.Fatalf("tag snapshot included later registration: %v", err)
	}
	if !selectedTagKeys(t, c, QueryFilter{Tags: []string{"users"}})["b"] {
		t.Fatal("later tag registration was not retained")
	}
}

// TestQueryTagsMaxAge preserves query labels when data availability expires but query state remains.
func TestQueryTagsMaxAge(t *testing.T) {
	c, now := newAgeClient(t, Options{StaleTime: time.Hour, MaxAge: time.Minute})
	definition, err := QueryDefinition("key", func(context.Context) (int, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	definition.Tags = []string{"users"}
	if _, err := definition.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	now.Store(int64(time.Minute))
	if Get[int](c, "key").HasData {
		t.Fatal("over-age data remained available")
	}
	if !selectedTagKeys(t, c, QueryFilter{Tags: []string{"users"}})["key"] {
		t.Fatal("data expiration removed query labels")
	}
}
