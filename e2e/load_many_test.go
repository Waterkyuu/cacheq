package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// TestHTTPLoadMany loads known IDs immediately in bounded batches and retains per-position HTTP outcomes.
func TestHTTPLoadMany(t *testing.T) {
	api := newBulkUserAPI(t)
	batcher, err := cacheq.NewBatcher(
		context.Background(),
		bulkHTTPFetcher(api.server),
		cacheq.BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(batcher.Close)
	out := make(chan []cacheq.BatchResult[user], 1)
	go func() { out <- batcher.LoadMany(context.Background(), []int{43, 42, 43, 99}) }()
	var results []cacheq.BatchResult[user]
	select {
	case results = <-out:
	case <-time.After(3 * time.Second):
		t.Fatal("LoadMany did not dispatch the final partial batch")
	}
	for index, name := range []string{"Alice:43", "Alice:42", "Alice:43"} {
		if results[index].Data.Name != name || results[index].Err != nil {
			t.Fatalf("HTTP result[%d] = %+v", index, results[index])
		}
	}
	if !errors.Is(results[3].Err, cacheq.ErrBatchResultMissing) || api.count("/bulk") != 2 {
		t.Fatalf("missing result = %+v; bulk calls = %d", results[3], api.count("/bulk"))
	}
	api.update(t, user{Name: "Bob"})
	results = batcher.LoadMany(context.Background(), []int{42})
	if results[0].Err != nil || results[0].Data.Name != "Bob:42" || api.count("/bulk") != 3 {
		t.Fatalf("subsequent uncached load = %+v; calls = %d", results, api.count("/bulk"))
	}
}

// TestHTTPLoadManyTermination propagates transport failures and releases active HTTP work on cancellation or closure.
func TestHTTPLoadManyTermination(t *testing.T) {
	for _, action := range []string{"failure", "cancel", "close"} {
		t.Run(action, func(t *testing.T) {
			entered, stopped := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				close(entered)
				if action == "failure" {
					http.Error(w, "bulk unavailable", http.StatusServiceUnavailable)
					return
				}
				<-r.Context().Done()
				close(stopped)
			}))
			t.Cleanup(server.Close)
			batcher, err := cacheq.NewBatcher(
				context.Background(),
				bulkHTTPFetcher(server),
				cacheq.BatchOptions{Wait: time.Hour, MaxBatchSize: 10},
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(batcher.Close)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := make(chan []cacheq.BatchResult[user], 1)
			go func() { out <- batcher.LoadMany(ctx, []int{42, 43, 42}) }()
			awaitBatchSignal(t, entered)
			switch action {
			case "cancel":
				cancel()
			case "close":
				batcher.Close()
			}
			var results []cacheq.BatchResult[user]
			select {
			case results = <-out:
			case <-time.After(3 * time.Second):
				t.Fatal("LoadMany did not release HTTP waiters")
			}
			for _, result := range results {
				if result.Err == nil || result.Data != (user{}) {
					t.Fatalf("terminal result = %+v", result)
				}
				if action == "failure" && !strings.Contains(result.Err.Error(), "HTTP status 503") {
					t.Fatalf("HTTP failure result = %+v", result)
				}
				if action == "cancel" && !errors.Is(result.Err, context.Canceled) ||
					action == "close" && !errors.Is(result.Err, cacheq.ErrBatcherClosed) {
					t.Fatalf("%s result = %+v", action, result)
				}
			}
			if action != "failure" {
				awaitBatchSignal(t, stopped)
			}
			if calls.Load() != 1 {
				t.Fatalf("HTTP calls = %d, want 1", calls.Load())
			}
		})
	}
}
