package e2e_test

import (
	"context"
	"errors"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// TestHTTPCapacityEviction reuses recently read data and reloads evicted HTTP results without losing observers.
func TestHTTPCapacityEviction(t *testing.T) {
	api := newUserAPI(t)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour, MaxEntries: 2})
	t.Cleanup(client.Close)
	detailFetch := readJSON[user](api.server.Client(), api.server.URL+"/users/42")
	listFetch := readJSON[[]user](api.server.Client(), api.server.URL+"/users")
	detail := cacheq.Query(client, "user:42", detailFetch)
	t.Cleanup(detail.Close)
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	if _, err := cacheq.Fetch(
		context.Background(),
		client,
		"users",
		listFetch,
	); err != nil {
		t.Fatal(err)
	}
	// Reading the detail makes the list the eviction candidate, regardless of insertion order.
	if state := detail.Snapshot(); state.Data.Name != "Alice" {
		t.Fatalf("cached detail = %+v", state)
	}
	if _, err := cacheq.Fetch(
		context.Background(),
		client,
		"settings",
		readJSON[settings](api.server.Client(), api.server.URL+"/settings"),
	); err != nil {
		t.Fatal(err)
	}
	if cacheq.Get[[]user](client, "users").HasData || !cacheq.Get[settings](client, "settings").HasData {
		t.Fatal("capacity did not evict the least recently used HTTP result")
	}
	if _, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		detailFetch,
	); err != nil {
		t.Fatal(err)
	}
	if api.count("/users/42") != 1 {
		t.Fatal("recent HTTP data was fetched again")
	}
	if users, err := cacheq.Fetch(
		context.Background(),
		client,
		"users",
		listFetch,
	); err != nil || len(users) != 1 {
		t.Fatalf("reload evicted list = %+v, %v", users, err)
	}
	if api.count("/users") != 2 || api.count("/settings") != 1 {
		t.Fatal("evicted list did not issue exactly one replacement HTTP request")
	}
	// Reloading the list leaves the detail oldest; another result evicts it while its observer remains.
	if err := cacheq.Set(client, "extra", true); err != nil {
		t.Fatal(err)
	}
	state := await(t, detail, func(s cacheq.Snapshot[user]) bool { return !s.HasData && !s.Fetching })
	if state.Status != cacheq.Idle || api.count("/users/42") != 1 {
		t.Fatalf("eviction triggered HTTP work = %+v", state)
	}
	if err := cacheq.Set(client, "user:42", []user{}); !errors.Is(err, cacheq.ErrTypeMismatch) {
		t.Fatalf("evicted observer lost type protection: %v", err)
	}
	if updated, err := detail.Refetch(context.Background()); err != nil || updated.Name != "Alice" {
		t.Fatalf("refetch evicted detail = %+v, %v", updated, err)
	}
	if api.count("/users/42") != 2 || cacheq.Get[[]user](client, "users").HasData {
		t.Fatal("refetch did not respect capacity")
	}
}
