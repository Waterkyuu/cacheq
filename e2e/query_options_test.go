package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// TestHTTPSharedQueryOptions applies one disabled configuration to subscriptions and explicit HTTP reads.
func TestHTTPSharedQueryOptions(t *testing.T) {
	for _, entry := range []string{"fetch", "prefetch", "refetch"} {
		for _, fail := range []bool{false, true} {
			t.Run(entry+"/"+map[bool]string{false: "retry success", true: "retry failure"}[fail], func(t *testing.T) {
				api := newUserAPI(t)
				client := cacheq.NewClient(cacheq.Options{})
				t.Cleanup(client.Close)
				hour, retries := time.Hour, 1
				options := cacheq.QueryOptions{
					Disable: true, StaleTime: &hour, Retry: &retries,
					RetryDelay: func(int) time.Duration { return 0 },
				}
				load := readJSON[user](api.server.Client(), api.server.URL+"/users/42")
				query := cacheq.Query(client, "user", load, options)
				t.Cleanup(query.Close)
				if err := client.Invalidate("user"); err != nil {
					t.Fatal(err)
				}
				if api.count("/users/42") != 0 || query.Snapshot().Fetching {
					t.Fatal("disabled subscription started an HTTP request")
				}
				failures := 1
				if fail {
					failures = 2
				}
				api.failNext(failures)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				var err error
				switch entry {
				case "fetch":
					_, err = cacheq.Fetch(ctx, client, "user", load, options)
				case "prefetch":
					err = cacheq.Prefetch(ctx, client, "user", load, options)
				case "refetch":
					_, err = query.Refetch(ctx)
				}
				if api.count("/users/42") != 2 || (err != nil) != fail {
					t.Fatalf("HTTP read = %v, requests=%d", err, api.count("/users/42"))
				}
				state := query.Snapshot()
				if fail {
					if !strings.Contains(err.Error(), "HTTP status 503") || state.Err == nil || state.HasData {
						t.Fatalf("failed HTTP policy = %+v, %v", state, err)
					}
					return
				}
				if !state.HasData || state.Data.Name != "Alice" || state.Stale || state.Fetching {
					t.Fatalf("shared HTTP result = %+v", state)
				}
				if err := cacheq.Prefetch(ctx, client, "user", load, options); err != nil {
					t.Fatal(err)
				}
				value, err := cacheq.Fetch(ctx, client, "user", load, options)
				if err != nil || value.Name != "Alice" || api.count("/users/42") != 2 {
					t.Fatalf("shared HTTP cache = %+v, %v, requests=%d", value, err, api.count("/users/42"))
				}
			})
		}
	}
}

// TestHTTPQueryDefinition shares a named HTTP query across prefetching, reading, and automatic refresh.
func TestHTTPQueryDefinition(t *testing.T) {
	api := newUserAPI(t)
	client := cacheq.NewClient(cacheq.Options{})
	t.Cleanup(client.Close)
	hour, retries := time.Hour, 1
	definition := cacheq.QueryDefinition[user]{
		Key:     "user",
		Fetcher: readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
		Options: cacheq.QueryOptions{
			Disable: true, StaleTime: &hour, Retry: &retries,
			RetryDelay: func(int) time.Duration { return 0 },
		},
	}
	query := definition.Query(client)
	t.Cleanup(query.Close)
	api.failNext(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := definition.Prefetch(ctx, client); err != nil {
		t.Fatal(err)
	}
	value, err := definition.Fetch(ctx, client)
	if err != nil || value.Name != "Alice" || query.Snapshot().Data.Name != "Alice" || api.count("/users/42") != 2 {
		t.Fatalf("definition HTTP cache = %+v, %v, state=%+v", value, err, query.Snapshot())
	}
	api.update(t, user{Name: "Bob"})
	if err := client.Invalidate(definition.Key); err != nil {
		t.Fatal(err)
	}
	if query.Snapshot().Fetching || api.count("/users/42") != 2 {
		t.Fatal("disabled definition refreshed automatically")
	}
	value, err = definition.Fetch(ctx, client)
	if err != nil || value.Name != "Bob" || query.Snapshot().Stale || api.count("/users/42") != 3 {
		t.Fatalf("definition refresh = %+v, %v, requests=%d", value, err, api.count("/users/42"))
	}
	definition.Options.Disable = false
	active := definition.Query(client)
	t.Cleanup(active.Close)
	api.update(t, user{Name: "Carol"})
	if err := client.Invalidate(definition.Key); err != nil {
		t.Fatal(err)
	}
	await(t, active, func(s cacheq.Snapshot[user]) bool { return s.Data.Name == "Carol" && !s.Fetching })
	if query.Snapshot().Data.Name != "Carol" || api.count("/users/42") != 4 {
		t.Fatal("definition subscriptions did not share the automatic refresh")
	}
}

// TestHTTPDefinitionCancellation propagates explicit read cancellation through a disabled definition.
func TestHTTPDefinitionCancellation(t *testing.T) {
	for _, entry := range []string{"fetch", "prefetch"} {
		t.Run(entry, func(t *testing.T) {
			response := newHeldHTTPResponse(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if response.hold(r.Context()) {
					json.NewEncoder(w).Encode(user{Name: "Alice"})
				}
			}))
			t.Cleanup(server.Close)
			client := cacheq.NewClient(cacheq.Options{})
			t.Cleanup(client.Close)
			definition := cacheq.QueryDefinition[user]{
				Key: "user", Fetcher: readJSON[user](server.Client(), server.URL),
				Options: cacheq.QueryOptions{Disable: true},
			}
			query := definition.Query(client)
			t.Cleanup(query.Close)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if entry == "prefetch" {
					done <- definition.Prefetch(ctx, client)
					return
				}
				_, err := definition.Fetch(ctx, client)
				done <- err
			}()
			response.awaitStarted(t)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled HTTP definition = %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("definition ignored caller cancellation")
			}
		})
	}
}
