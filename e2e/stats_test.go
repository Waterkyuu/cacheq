package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// TestHTTPStatsSharedRetry compares shared work, retry attempts, cache reuse, and manual refresh with HTTP admissions.
func TestHTTPStatsSharedRetry(t *testing.T) {
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-r.Context().Done():
				return
			case <-release:
				http.Error(w, "offline", http.StatusServiceUnavailable)
				return
			}
		}
		json.NewEncoder(w).Encode(user{Name: "Alice"})
	}))
	t.Cleanup(server.Close)
	client := cacheq.NewClient(cacheq.Options{
		StaleTime:  time.Hour,
		Retry:      1,
		RetryDelay: func(int) time.Duration { return 0 },
	})
	t.Cleanup(client.Close)
	fetch := readJSON[user](server.Client(), server.URL)
	first := cacheq.Query(client, "user", fetch)
	t.Cleanup(first.Close)
	<-started
	second := cacheq.Query(client, "user", fetch)
	t.Cleanup(second.Close)
	if state := client.Stats(); state.CacheMisses != 2 || state.MergedRequests != 1 || state.Loads != 1 ||
		state.LoadSuccesses != 0 || state.Retries != 0 || state.TotalLoadDuration != 0 {
		t.Fatalf("shared HTTP work in progress = %+v", state)
	}
	close(release)
	for _, query := range []*cacheq.QueryHandle[user]{first, second} {
		await(t, query, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	}
	if _, err := cacheq.Fetch(
		context.Background(),
		client,
		"user",
		fetch,
	); err != nil {
		t.Fatal(err)
	}
	first.Snapshot()
	cacheq.Get[user](client, "user")
	state := client.Stats()
	if calls.Load() != 2 || state.Loads != 1 || state.Retries != 1 || state.LoadSuccesses != 1 ||
		state.LoadFailures != 0 || state.CacheHits != 1 || state.CacheMisses != 2 || state.CacheEntries != 1 {
		t.Fatalf("HTTP attempts and cache savings = %+v, calls = %d", state, calls.Load())
	}
	if _, err := first.Refetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	state = client.Stats()
	if calls.Load() != 3 || state.Loads != 2 || state.LoadSuccesses != 2 || state.CacheHits != 1 ||
		state.CacheMisses != 2 {
		t.Fatalf("manual HTTP refresh changed cache decisions = %+v", state)
	}
}

// TestHTTPStatsFailures records final failures separately from retries and successful invalidation refreshes.
func TestHTTPStatsFailures(t *testing.T) {
	api := newUserAPI(t)
	client := cacheq.NewClient(cacheq.Options{
		StaleTime:  time.Hour,
		Retry:      1,
		RetryDelay: func(int) time.Duration { return 0 },
	})
	t.Cleanup(client.Close)
	api.failNext(2)
	detail := cacheq.Query(client, "user", readJSON[user](api.server.Client(), api.server.URL+"/users/42"))
	t.Cleanup(detail.Close)
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.Err != nil && !s.Fetching })
	if state := client.Stats(); state.LoadFailures != 1 || state.Retries != 1 || state.Loads != 1 ||
		state.CacheMisses != 1 || state.CacheEntries != 1 || api.count("/users/42") != 2 {
		t.Fatalf("failed HTTP statistics = %+v", state)
	}
	if _, err := detail.Refetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Invalidate("user"); err != nil {
		t.Fatal(err)
	}
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching && s.Err == nil })
	if state := client.Stats(); state.Loads != 3 || state.LoadSuccesses != 2 || state.LoadFailures != 1 ||
		state.Retries != 1 || state.CacheMisses != 1 || state.CacheHits != 0 || api.count("/users/42") != 4 {
		t.Fatalf("HTTP recovery and invalidation = %+v", state)
	}
}
