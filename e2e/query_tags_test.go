package e2e_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// TestHTTPQueryTags refreshes related result types by tag and retains unaffected tenant data.
func TestHTTPQueryTags(t *testing.T) {
	api := newUserAPI(t)
	c := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	detailDefinition := cacheq.QueryDefinition(
		"tenant:42:profile",
		readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
		cacheq.QueryOptions{},
	)
	detailDefinition.Tags = []string{"tenant:42:users"}
	listDefinition := cacheq.QueryDefinition(
		"tenant:42:list",
		readJSON[[]user](api.server.Client(), api.server.URL+"/users"),
		cacheq.QueryOptions{},
	)
	listDefinition.Tags = []string{"tenant:42:users", "lists"}
	otherDefinition := detailDefinition
	otherDefinition.Key, otherDefinition.Tags = "tenant:99:profile", []string{"tenant:99:users"}
	detail, list, other := detailDefinition.Query(c), listDefinition.Query(c), otherDefinition.Query(c)
	t.Cleanup(detail.Close)
	t.Cleanup(list.Close)
	t.Cleanup(other.Close)
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	await(t, list, func(s cacheq.Snapshot[[]user]) bool { return s.HasData && !s.Fetching })
	await(t, other, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	filter := cacheq.QueryFilter{Tags: []string{"tenant:42:users", "tenant:42:users"}}
	api.update(t, user{Name: "Bob"})
	results, err := c.InvalidateAndWait(context.Background(), filter, cacheq.InvalidateOptions{})
	if err != nil || len(results) != 2 {
		t.Fatalf("tagged refresh = %+v, %v", results, err)
	}
	for _, result := range results {
		if !result.Refreshed {
			t.Fatalf("refresh skipped tagged query: %+v", result)
		}
	}
	if detail.Snapshot().Data.Name != "Bob" || list.Snapshot().Data[0].Name != "Bob" {
		t.Fatal("related queries did not refresh together")
	}
	if other.Snapshot().Data.Name != "Alice" || api.count("/users") != 2 || api.count("/users/42") != 3 {
		t.Fatal("tagged refresh crossed tenant scope or repeated a request")
	}
	api.failNext(1)
	results, err = c.InvalidateAndWait(context.Background(), cacheq.QueryFilter{
		Keys: []any{detailDefinition.Key}, Tags: filter.Tags,
	}, cacheq.InvalidateOptions{})
	if err == nil || !strings.Contains(err.Error(), "HTTP status 503") || len(results) != 1 {
		t.Fatalf("tagged refresh failure = %+v, %v", results, err)
	}
	if detail.Snapshot().Data.Name != "Bob" || !detail.Snapshot().Stale {
		t.Fatal("failed refresh lost old data")
	}
	if err := c.Remove(filter); err != nil {
		t.Fatal(err)
	}
	if detail.Snapshot().HasData || list.Snapshot().HasData || !other.Snapshot().HasData {
		t.Fatal("tagged removal crossed tenant scope")
	}
}

// TestHTTPQueryTagsCancel cancels tagged shared work while leaving another tag's HTTP load active.
func TestHTTPQueryTagsCancel(t *testing.T) {
	first, second := newHeldHTTPResponse(t), newHeldHTTPResponse(t)
	api := newHeldListAPI(t, [2]*heldHTTPResponse{first, second})
	c := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	definition := cacheq.QueryDefinition(
		"one",
		readJSON[[]user](api.server.Client(), api.server.URL+"/users"),
		cacheq.QueryOptions{Disable: true},
	)
	definition.Tags = []string{"one"}
	one := definition.Query(c)
	t.Cleanup(one.Close)
	definition.Key, definition.Tags = "two", []string{"two"}
	two := definition.Query(c)
	t.Cleanup(two.Close)
	done := make(chan error, 1)
	go func() { _, err := one.Refetch(context.Background()); done <- err }()
	first.awaitStarted(t)
	go func() { _, _ = two.Refetch(context.Background()) }()
	second.awaitStarted(t)
	if err := c.Cancel(cacheq.QueryFilter{Tags: []string{"one"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("tagged cancel = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tagged HTTP request did not stop")
	}
	if one.Snapshot().Fetching || !two.Snapshot().Fetching {
		t.Fatal("tag selection canceled the wrong HTTP requests")
	}
	second.deliver()
	await(t, two, func(s cacheq.Snapshot[[]user]) bool { return s.HasData && !s.Fetching })
}
