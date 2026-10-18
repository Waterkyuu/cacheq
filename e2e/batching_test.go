package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// newBulkUserAPI adds a bulk read endpoint to the mutable HTTP fixture used by application tests.
func newBulkUserAPI(t *testing.T) *userAPI {
	t.Helper()
	api := &userAPI{current: user{Name: "Alice"}, calls: make(map[string]int)}
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bulk" {
			api.serveHTTP(w, r)
			return
		}
		api.mu.Lock()
		api.calls["/bulk"]++
		current := api.current
		api.mu.Unlock()
		results := make(map[int]user)
		for _, text := range r.URL.Query()["id"] {
			id, err := strconv.Atoi(text)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if id == 42 || id == 43 {
				results[id] = user{Name: fmt.Sprintf("%s:%d", current.Name, id)}
			}
		}
		json.NewEncoder(w).Encode(results)
	}))
	t.Cleanup(api.server.Close)
	return api
}

// bulkHTTPFetcher translates one real bulk HTTP response into results matched to batch keys.
func bulkHTTPFetcher(server *httptest.Server) cacheq.BatchFunc[int, user] {
	return func(ctx context.Context, ids []int) (map[int]cacheq.BatchResult[user], error) {
		parameters := make(url.Values)
		for _, id := range ids {
			parameters.Add("id", strconv.Itoa(id))
		}
		load := readJSON[map[int]user](server.Client(), server.URL+"/bulk?"+parameters.Encode())
		values, err := load(ctx)
		if err != nil {
			return nil, err
		}
		results := make(map[int]cacheq.BatchResult[user], len(values))
		for id, value := range values {
			results[id] = cacheq.BatchResult[user]{Data: value}
		}
		return results, nil
	}
}

// awaitBatchSignal bounds HTTP synchronization without assuming a collection or transport delay.
func awaitBatchSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("expected batch HTTP transition did not occur")
	}
}

// TestHTTPBatchLifecycle combines distinct query keys into bulk reads and refreshes their separate caches after mutation.
func TestHTTPBatchLifecycle(t *testing.T) {
	api := newBulkUserAPI(t)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	batcher, err := cacheq.NewBatcher(
		context.Background(),
		bulkHTTPFetcher(api.server),
		cacheq.BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(batcher.Close)
	first := cacheq.Query(client, "user:42", batcher.Fetcher(42))
	second := cacheq.Query(client, "user:43", batcher.Fetcher(43))
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
	for index, handle := range []*cacheq.QueryHandle[user]{first, second} {
		state := await(t, handle, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
		if state.Err != nil || state.Data.Name != fmt.Sprintf("Alice:%d", 42+index) {
			t.Fatalf("initial query %d = %+v", index, state)
		}
	}
	if calls := api.count("/bulk"); calls != 1 {
		t.Fatalf("initial bulk HTTP calls = %d, want 1", calls)
	}
	duplicate := cacheq.Query(client, "user:42", batcher.Fetcher(42))
	t.Cleanup(duplicate.Close)
	value, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:43",
		batcher.Fetcher(43),
	)
	if err != nil || value.Name != "Alice:43" {
		t.Fatalf("cached fetch = %+v, %v", value, err)
	}
	if duplicate.Snapshot().Data.Name != "Alice:42" || api.count("/bulk") != 1 {
		t.Fatal("fresh consumers issued another bulk HTTP request")
	}

	api.update(t, user{Name: "Bob"})
	if first.Snapshot().Data.Name != "Alice:42" {
		t.Fatal("HTTP mutation changed cache before invalidation")
	}
	if err := client.Invalidate([]any{"user:42", "user:43"}); err != nil {
		t.Fatal(err)
	}
	for index, handle := range []*cacheq.QueryHandle[user]{first, second} {
		name := fmt.Sprintf("Bob:%d", 42+index)
		state := await(t, handle, func(s cacheq.Snapshot[user]) bool {
			return s.Data.Name == name && !s.Fetching
		})
		if state.Err != nil || state.Stale {
			t.Fatalf("refreshed query %d = %+v", index, state)
		}
	}
	await(t, duplicate, func(s cacheq.Snapshot[user]) bool { return s.Data.Name == "Bob:42" && !s.Fetching })
	if api.count("/bulk") != 2 || client.Stats().Loads != 4 {
		t.Fatalf("bulk HTTP calls = %d; per-key stats = %+v", api.count("/bulk"), client.Stats())
	}
}

// TestHTTPBatchMissingResult isolates an incomplete response to its key while keeping successful HTTP data cached.
func TestHTTPBatchMissingResult(t *testing.T) {
	api := newBulkUserAPI(t)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	batcher, err := cacheq.NewBatcher(
		context.Background(),
		bulkHTTPFetcher(api.server),
		cacheq.BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(batcher.Close)
	valid := cacheq.Query(client, "user:42", batcher.Fetcher(42))
	missing := cacheq.Query(client, "user:44", batcher.Fetcher(44))
	t.Cleanup(valid.Close)
	t.Cleanup(missing.Close)
	state := await(t, valid, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	if state.Err != nil || state.Data.Name != "Alice:42" {
		t.Fatalf("successful key = %+v", state)
	}
	failure := await(t, missing, func(s cacheq.Snapshot[user]) bool { return s.Err != nil && !s.Fetching })
	if failure.HasData || !errors.Is(failure.Err, cacheq.ErrBatchResultMissing) {
		t.Fatalf("omitted HTTP key = %+v", failure)
	}
	value, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		batcher.Fetcher(42),
	)
	if err != nil || value.Name != "Alice:42" {
		t.Fatalf("successful cached key = %+v, %v", value, err)
	}
	if api.count("/bulk") != 1 {
		t.Fatal("one missing key caused successful data to reload")
	}
}

// TestHTTPBatchCancellation preserves a live key's HTTP request and cancels transport on whole-batcher closure.
func TestHTTPBatchCancellation(t *testing.T) {
	for _, action := range []string{"cancel one key", "close batcher"} {
		t.Run(action, func(t *testing.T) {
			entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls, cancellations atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				close(entered)
				select {
				case <-r.Context().Done():
					cancellations.Add(1)
					close(stopped)
				case <-release:
					json.NewEncoder(w).Encode(map[int]user{42: {Name: "Alice:42"}, 43: {Name: "Alice:43"}})
				}
			}))
			t.Cleanup(server.Close)
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
			t.Cleanup(client.Close)
			batcher, err := cacheq.NewBatcher(
				context.Background(),
				bulkHTTPFetcher(server),
				cacheq.BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(batcher.Close)
			first := cacheq.Query(client, "user:42", batcher.Fetcher(42))
			second := cacheq.Query(client, "user:43", batcher.Fetcher(43))
			t.Cleanup(first.Close)
			t.Cleanup(second.Close)
			awaitBatchSignal(t, entered)
			if action == "close batcher" {
				batcher.Close()
				for _, handle := range []*cacheq.QueryHandle[user]{first, second} {
					state := await(t, handle, func(s cacheq.Snapshot[user]) bool { return s.Err != nil && !s.Fetching })
					if !errors.Is(state.Err, cacheq.ErrBatcherClosed) || state.HasData {
						t.Fatalf("closed bulk query = %+v", state)
					}
				}
				awaitBatchSignal(t, stopped)
				if cancellations.Load() != 1 || calls.Load() != 1 {
					t.Fatal("batcher closure did not cancel its single HTTP request")
				}
				return
			}
			if err := client.Cancel("user:42"); err != nil {
				t.Fatal(err)
			}
			await(t, first, func(s cacheq.Snapshot[user]) bool { return !s.Fetching })
			if err := cacheq.Set(client, "user:42", user{Name: "local"}); err != nil {
				t.Fatal(err)
			}
			unblock()
			state := await(t, second, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
			if state.Err != nil || state.Data.Name != "Alice:43" {
				t.Fatalf("surviving bulk query = %+v", state)
			}
			if first.Snapshot().Data.Name != "local" || cancellations.Load() != 0 {
				t.Fatal("canceling one key damaged surviving work or overwrote the local value")
			}
			if calls.Load() != 1 {
				t.Fatal("surviving key started a replacement HTTP request")
			}
		})
	}
}
