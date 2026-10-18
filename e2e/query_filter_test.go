package e2e_test

import (
	"context"
	"errors"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// TestHTTPQueryFilter reuses one tenant selection for awaited HTTP refresh and cache removal.
func TestHTTPQueryFilter(t *testing.T) {
	api := newUserAPI(t)
	c := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	detail := cacheq.Query(
		c,
		"tenant:42:user",
		readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
		cacheq.QueryOptions{},
	)
	list := cacheq.Query(
		c,
		"tenant:42:users",
		readJSON[[]user](api.server.Client(), api.server.URL+"/users"),
		cacheq.QueryOptions{},
	)
	other := cacheq.Query(
		c,
		"tenant:99:user",
		readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
		cacheq.QueryOptions{},
	)
	t.Cleanup(detail.Close)
	t.Cleanup(list.Close)
	t.Cleanup(other.Close)
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	await(t, list, func(s cacheq.Snapshot[[]user]) bool { return s.HasData && !s.Fetching })
	await(t, other, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	filter := cacheq.QueryFilter{Keys: []any{"tenant:42:users", "tenant:42:user", "tenant:42:user"}}
	api.update(t, user{Name: "Bob"})
	results, err := c.InvalidateAndWait(context.Background(), filter, cacheq.InvalidateOptions{})
	if err != nil || len(results) != 2 || !results[0].Refreshed || !results[1].Refreshed {
		t.Fatalf("refresh = %+v, %v", results, err)
	}
	detailUpdated := detail.Snapshot().Data.Name == "Bob"
	listUpdated := list.Snapshot().Data[0].Name == "Bob"
	otherUnchanged := other.Snapshot().Data.Name == "Alice"
	if !detailUpdated || !listUpdated || !otherUnchanged {
		t.Fatal("refresh crossed tenant selection")
	}
	if api.count("/users") != 2 || api.count("/users/42") != 3 {
		t.Fatal("duplicate targets triggered extra HTTP requests")
	}
	if err := c.Remove(filter); err != nil {
		t.Fatal(err)
	}
	if detail.Snapshot().HasData || list.Snapshot().HasData || !other.Snapshot().HasData {
		t.Fatal("removal crossed tenant selection")
	}
	if value, err := detail.Refetch(context.Background()); err != nil || value.Name != "Bob" {
		t.Fatalf("removed subscription could not reload: %+v, %v", value, err)
	}
}

// TestHTTPQueryFilterCancellation cancels or removes selected HTTP work while retaining unrelated loads.
func TestHTTPQueryFilterCancellation(t *testing.T) {
	for _, operation := range []string{"cancel", "remove"} {
		t.Run(operation, func(t *testing.T) {
			first, second := newHeldHTTPResponse(t), newHeldHTTPResponse(t)
			api := newHeldListAPI(t, [2]*heldHTTPResponse{first, second})
			c := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
			t.Cleanup(c.Close)
			load := readJSON[[]user](api.server.Client(), api.server.URL+"/users")
			one := cacheq.Query(
				c,
				"one",
				load,
				cacheq.QueryOptions{Disable: true},
			)
			two := cacheq.Query(
				c,
				"two",
				load,
				cacheq.QueryOptions{Disable: true},
			)
			t.Cleanup(one.Close)
			t.Cleanup(two.Close)
			if err := cacheq.Set(c, "one", []user{{Name: "cached"}}); err != nil {
				t.Fatal(err)
			}
			finished := make(chan error, 1)
			go func() { _, err := one.Refetch(context.Background()); finished <- err }()
			first.awaitStarted(t)
			go func() { _, _ = two.Refetch(context.Background()) }()
			second.awaitStarted(t)
			filter := cacheq.QueryFilter{Predicate: func(key any) bool { return key == "one" }}
			var err error
			if operation == "cancel" {
				err = c.Cancel(filter)
			} else {
				err = c.Remove(filter)
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled HTTP load = %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("selected HTTP load did not stop")
			}
			state := one.Snapshot()
			if state.Fetching {
				t.Fatalf("selected load remained active: %+v", state)
			}
			if operation == "cancel" && (!state.HasData || state.Data[0].Name != "cached") {
				t.Fatalf("cancellation lost old data: %+v", state)
			}
			if operation == "remove" && state.HasData {
				t.Fatalf("removal retained old data: %+v", state)
			}
			if !two.Snapshot().Fetching {
				t.Fatal("unrelated HTTP load was canceled")
			}
			second.deliver()
			await(t, two, func(s cacheq.Snapshot[[]user]) bool { return s.HasData && !s.Fetching })
		})
	}
}
