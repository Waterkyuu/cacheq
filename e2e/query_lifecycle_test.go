package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// user represents a detail response and an element of a list response.
type user struct {
	// Name identifies the version stored by the local HTTP service.
	Name string `json:"name"`
}

// settings represents an unrelated third result type sharing the application client.
type settings struct {
	// Theme controls the application's selected appearance.
	Theme string `json:"theme"`
}

// userAPI is an isolated local HTTP service with mutable user data and observable request counts.
type userAPI struct {
	// mu protects server state shared by concurrent HTTP handlers.
	mu sync.Mutex
	// current contains the user returned by both detail and list endpoints.
	current user
	// calls counts GET requests independently for each endpoint.
	calls map[string]int
	// failures injects a fixed number of server errors before normal responses resume.
	failures int
	// server owns the in-process HTTP transport and is closed by the test.
	server *httptest.Server
}

// newUserAPI creates a fresh HTTP service per test and registers its shutdown.
func newUserAPI(t *testing.T) *userAPI {
	t.Helper()
	api := &userAPI{current: user{Name: "Alice"}, calls: make(map[string]int)}
	api.server = httptest.NewServer(http.HandlerFunc(api.serveHTTP))
	t.Cleanup(api.server.Close)
	return api
}

// serveHTTP handles detail/list/config reads and an actual user update used by invalidation tests.
func (a *userAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPut && r.URL.Path == "/users/42" {
		var updated user
		if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		a.mu.Lock()
		a.current = updated
		a.mu.Unlock()
		json.NewEncoder(w).Encode(updated)
		return
	}
	a.mu.Lock()
	a.calls[r.URL.Path]++
	current := a.current
	fail := a.failures > 0
	if fail {
		a.failures--
	}
	a.mu.Unlock()
	if fail {
		http.Error(w, "offline", http.StatusServiceUnavailable)
		return
	}
	switch r.URL.Path {
	case "/users/42":
		json.NewEncoder(w).Encode(current)
	case "/users":
		json.NewEncoder(w).Encode([]user{current})
	case "/settings":
		json.NewEncoder(w).Encode(settings{Theme: "dark"})
	default:
		http.NotFound(w, r)
	}
}

// count reports completed handler admissions without relying on sleeps or transport timing.
func (a *userAPI) count(path string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls[path]
}

// failNext installs a bounded failure sequence used to verify retry and retained-data behavior.
func (a *userAPI) failNext(count int) { a.mu.Lock(); a.failures = count; a.mu.Unlock() }

// update performs a real HTTP mutation before the query client invalidates affected keys.
func (a *userAPI) update(t *testing.T, value user) {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPut,
		a.server.URL+"/users/42",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := a.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("update status = %d", response.StatusCode)
	}
}

// readJSON returns a typed HTTP loader whose request responds to the query's context cancellation.
func readJSON[V any](transport *http.Client, url string) cacheq.Fetcher[V] {
	return func(ctx context.Context) (V, error) {
		var value V
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			url,
			nil,
		)
		if err != nil {
			return value, fmt.Errorf("create request: %w", err)
		}
		response, err := transport.Do(request)
		if err != nil {
			return value, fmt.Errorf("perform request: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return value, fmt.Errorf("HTTP status %d", response.StatusCode)
		}
		if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
			return value, fmt.Errorf("decode response: %w", err)
		}
		return value, nil
	}
}

// await waits for a public query result with a deadlock guard rather than a business timing assumption.
func await[V any](
	t *testing.T,
	handle *cacheq.QueryHandle[V],
	matches func(cacheq.Snapshot[V]) bool,
) cacheq.Snapshot[V] {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case state, open := <-handle.Updates():
			if !open {
				t.Fatal("query closed before expected result")
			}
			if matches(state) {
				return state
			}
		case <-timer.C:
			t.Fatal("expected public query result was not published")
		}
	}
}

// TestSharedClientMutation refreshes heterogeneous detail/list data after a real mutation, leaving settings alone.
func TestSharedClientMutation(t *testing.T) {
	api := newUserAPI(t)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	detail := cacheq.Query(client, "user:42", readJSON[user](api.server.Client(), api.server.URL+"/users/42"))
	list := cacheq.Query(client, "users", readJSON[[]user](api.server.Client(), api.server.URL+"/users"))
	config := cacheq.Query(client, "settings", readJSON[settings](api.server.Client(), api.server.URL+"/settings"))
	t.Cleanup(detail.Close)
	t.Cleanup(list.Close)
	t.Cleanup(config.Close)
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	await(t, list, func(s cacheq.Snapshot[[]user]) bool { return s.HasData && !s.Fetching })
	await(t, config, func(s cacheq.Snapshot[settings]) bool { return s.HasData && !s.Fetching })
	api.update(t, user{Name: "Bob"})
	if detail.Snapshot().Data.Name != "Alice" {
		t.Fatal("mutation changed cache without invalidation")
	}
	if err := client.InvalidateMany([]any{"user:42", "users", "users"}, cacheq.InvalidateOptions{}); err != nil {
		t.Fatal(err)
	}
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.Data.Name == "Bob" && !s.Fetching })
	await(t, list, func(s cacheq.Snapshot[[]user]) bool { return s.HasData && s.Data[0].Name == "Bob" && !s.Fetching })
	if api.count("/users/42") != 2 || api.count("/users") != 2 || api.count("/settings") != 1 {
		t.Fatal("invalidation duplicated a request or refreshed unrelated data")
	}
	if err := cacheq.Set(client, "user:42", []user{}); !errors.Is(err, cacheq.ErrTypeMismatch) {
		t.Fatalf("wrong local type = %v", err)
	}
	if cacheq.Get[user](client, "user:42").Data.Name != "Bob" {
		t.Fatal("rejected update damaged detail cache")
	}
}

// TestDeferredPredicateInvalidation marks related data stale without HTTP work until its next use.
func TestDeferredPredicateInvalidation(t *testing.T) {
	api := newUserAPI(t)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	detailFetch := readJSON[user](api.server.Client(), api.server.URL+"/users/42")
	listFetch := readJSON[[]user](api.server.Client(), api.server.URL+"/users")
	detail := cacheq.Query(client, "user:42", detailFetch)
	list := cacheq.Query(client, "users", listFetch)
	t.Cleanup(detail.Close)
	t.Cleanup(list.Close)
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	await(t, list, func(s cacheq.Snapshot[[]user]) bool { return s.HasData && !s.Fetching })
	cacheq.Set(client, "settings", settings{Theme: "dark"})
	api.update(t, user{Name: "Bob"})
	client.InvalidateWhere(func(key any) bool {
		text, ok := key.(string)
		return ok && (text == "users" || strings.HasPrefix(text, "user:"))
	}, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone})
	if !detail.Snapshot().Stale || !list.Snapshot().Stale || detail.Snapshot().Data.Name != "Alice" ||
		api.count("/users/42") != 1 || api.count("/users") != 1 {
		t.Fatal("deferred invalidation issued HTTP work")
	}
	if cacheq.Get[settings](client, "settings").Stale {
		t.Fatal("predicate invalidated unrelated type")
	}
	updated, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		detailFetch,
	)
	if err != nil || updated.Name != "Bob" {
		t.Fatalf("next fetch = %+v, %v", updated, err)
	}
	reopened := cacheq.Query(client, "users", listFetch)
	t.Cleanup(reopened.Close)
	await(
		t,
		reopened,
		func(s cacheq.Snapshot[[]user]) bool { return s.HasData && s.Data[0].Name == "Bob" && !s.Fetching },
	)
	if api.count("/users/42") != 2 || api.count("/users") != 2 {
		t.Fatal("next use failed to refresh each type once")
	}
}

// TestConditionalConsumers shares data while keeping each consumer's loading permission independent.
func TestConditionalConsumers(t *testing.T) {
	api := newUserAPI(t)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	fetch := readJSON[user](api.server.Client(), api.server.URL+"/users/42")
	disabled := cacheq.Query(
		client,
		"user:42",
		fetch,
		cacheq.QueryOptions{Enabled: false},
	)
	t.Cleanup(disabled.Close)
	if disabled.Snapshot().Fetching || disabled.Snapshot().HasData || api.count("/users/42") != 0 {
		t.Fatal("disabled query issued HTTP work")
	}
	active := cacheq.Query(client, "user:42", fetch)
	await(t, disabled, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	await(t, active, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	active.Close()
	api.update(t, user{Name: "Bob"})
	client.Invalidate("user:42")
	if disabled.Snapshot().Fetching || api.count("/users/42") != 1 {
		t.Fatal("disabled consumer refreshed after invalidation")
	}
	if err := disabled.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	await(t, disabled, func(s cacheq.Snapshot[user]) bool { return s.Data.Name == "Bob" && !s.Fetching })
	disabled.SetEnabled(false)
	if _, err := disabled.Refetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if api.count("/users/42") != 3 {
		t.Fatal("manual refresh did not bypass disabled condition")
	}
	disabled.Close()
	if _, err := disabled.Refetch(context.Background()); !errors.Is(err, cacheq.ErrQueryClosed) {
		t.Fatalf("released refetch = %v", err)
	}
}

// TestHTTPRetriesAndRetainedData retries HTTP failures and preserves prior data after refresh exhaustion.
func TestHTTPRetriesAndRetainedData(t *testing.T) {
	api := newUserAPI(t)
	client := cacheq.NewClient(
		cacheq.Options{StaleTime: time.Hour, Retry: 2, RetryDelay: func(int) time.Duration { return 0 }},
	)
	t.Cleanup(client.Close)
	api.failNext(2)
	fetch := readJSON[user](api.server.Client(), api.server.URL+"/users/42")
	handle := cacheq.Query(client, "user:42", fetch)
	t.Cleanup(handle.Close)
	state := await(t, handle, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	if state.Err != nil || api.count("/users/42") != 3 {
		t.Fatal("HTTP retry did not recover")
	}
	api.failNext(3)
	if _, err := handle.Refetch(context.Background()); err == nil {
		t.Fatal("exhausted HTTP refresh succeeded")
	}
	state = handle.Snapshot()
	if !state.HasData || state.Data.Name != "Alice" || state.Err == nil || !state.Stale || state.Fetching ||
		api.count("/users/42") != 6 {
		t.Fatalf("retained refresh failure = %+v", state)
	}
}

// TestHTTPSharedBackgroundLoad merges consumers without hiding stale data during a gated HTTP refresh.
func TestHTTPSharedBackgroundLoad(t *testing.T) {
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(started)
		select {
		case <-r.Context().Done():
			return
		case <-release:
			json.NewEncoder(w).Encode(user{Name: "new"})
		}
	}))
	t.Cleanup(server.Close)
	now := time.Unix(0, 0)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour, Clock: func() time.Time { return now }})
	t.Cleanup(client.Close)
	cacheq.Set(client, "user", user{Name: "old"})
	now = now.Add(time.Hour)
	fetch := readJSON[user](server.Client(), server.URL)
	first := cacheq.Query(client, "user", fetch)
	t.Cleanup(first.Close)
	<-started
	second := cacheq.Query(client, "user", fetch)
	t.Cleanup(second.Close)
	if state := second.Snapshot(); state.Data.Name != "old" || !state.Stale || !state.Fetching {
		t.Fatalf("stale HTTP state = %+v", state)
	}
	close(release)
	for _, handle := range []*cacheq.QueryHandle[user]{first, second} {
		await(t, handle, func(s cacheq.Snapshot[user]) bool { return s.Data.Name == "new" && !s.Fetching })
	}
	if calls.Load() != 1 {
		t.Fatal("consumers did not share HTTP request")
	}
}

// TestHTTPCancellation prevents canceled responses from overwriting a local update or removed key.
func TestHTTPCancellation(t *testing.T) {
	for _, action := range []string{"cancel", "set", "remove", "close"} {
		t.Run(action, func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				<-r.Context().Done()
				close(stopped)
			}))
			t.Cleanup(server.Close)
			client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
			t.Cleanup(client.Close)
			finished := make(chan error, 1)
			go func() {
				_, err := cacheq.Fetch(
					context.Background(),
					client,
					"user",
					readJSON[user](server.Client(), server.URL),
				)
				finished <- err
			}()
			<-started
			switch action {
			case "cancel":
				client.Cancel("user")
			case "set":
				cacheq.Set(client, "user", user{Name: "local"})
			case "remove":
				client.Remove("user")
				cacheq.Set(client, "user", 42)
			case "close":
				client.Close()
			}
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled HTTP load = %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("HTTP fetch did not respond to cancellation")
			}
			select {
			case <-stopped:
			case <-time.After(3 * time.Second):
				t.Fatal("server request was not canceled")
			}
			if action == "set" && cacheq.Get[user](client, "user").Data.Name != "local" {
				t.Fatal("local value overwritten")
			}
			if action == "remove" && cacheq.Get[int](client, "user").Data != 42 {
				t.Fatal("new type overwritten")
			}
			if action == "close" && !errors.Is(cacheq.Get[user](client, "user").Err, cacheq.ErrClosed) {
				t.Fatal("closed client still readable")
			}
		})
	}
}

// TestPublicCacheOperations exercises prefetch, absolute expiry, removal, and cleanup using only exported APIs.
func TestPublicCacheOperations(t *testing.T) {
	now := time.Unix(0, 0)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour, Clock: func() time.Time { return now }})
	t.Cleanup(client.Close)
	if err := cacheq.Prefetch(
		context.Background(),
		client,
		"users",
		func(context.Context) ([]user, error) {
			return []user{{Name: "Alice"}}, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	value, err := cacheq.Fetch[[]user](
		context.Background(),
		client,
		"users",
		nil,
	)
	if err != nil || value[0].Name != "Alice" {
		t.Fatalf("prefetch cache = %+v, %v", value, err)
	}
	calls := 0
	load := func(context.Context) (settings, time.Time, error) {
		calls++
		return settings{Theme: "dark"}, now.Add(time.Minute), nil
	}
	if _, err := cacheq.FetchWithExpiry(
		context.Background(),
		client,
		"settings",
		load,
	); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if !cacheq.Get[settings](client, "settings").Stale {
		t.Fatal("absolute expiry boundary not respected")
	}
	if _, err := cacheq.FetchWithExpiry(
		context.Background(),
		client,
		"settings",
		load,
	); err != nil || calls != 2 {
		t.Fatalf("expired load = %v, calls=%d", err, calls)
	}
	client.Remove("settings")
	if state := cacheq.Get[settings](client, "settings"); state.HasData || state.Status != cacheq.Idle {
		t.Fatalf("remove = %+v", state)
	}
	client.Clear()
	if cacheq.Get[[]user](client, "users").HasData {
		t.Fatal("clear retained cached list")
	}
	client.Close()
	if err := cacheq.Set(client, "users", []user{}); !errors.Is(err, cacheq.ErrClosed) {
		t.Fatalf("closed cache write = %v", err)
	}
}
