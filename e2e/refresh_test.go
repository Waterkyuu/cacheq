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

	soroi "github.com/Waterkyuu/soroi"
)

// heldHTTPResponse delays delivery after a handler has captured its response data.
type heldHTTPResponse struct {
	// started closes when the handler has prepared the response being held.
	started chan struct{}
	// release allows the prepared response to reach the HTTP client.
	release chan struct{}
	// once makes test release and failure cleanup safe to repeat.
	once sync.Once
}

// newHeldHTTPResponse creates one controlled response and releases it during test cleanup.
func newHeldHTTPResponse(t *testing.T) *heldHTTPResponse {
	t.Helper()
	response := &heldHTTPResponse{started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(response.deliver)
	return response
}

// hold announces the prepared response and waits for delivery or transport cancellation.
func (r *heldHTTPResponse) hold(ctx context.Context) bool {
	close(r.started)
	select {
	case <-r.release:
		return true
	case <-ctx.Done():
		return false
	}
}

// deliver releases the response without depending on a timer or scheduling delay.
func (r *heldHTTPResponse) deliver() { r.once.Do(func() { close(r.release) }) }

// awaitStarted bounds deadlocks while ordering the test through an actual HTTP admission.
func (r *heldHTTPResponse) awaitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-r.started:
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP response was not prepared")
	}
}

// newHeldListAPI reuses the user service while controlling its first two list responses.
func newHeldListAPI(t *testing.T, responses [2]*heldHTTPResponse) *userAPI {
	t.Helper()
	api := &userAPI{current: user{Name: "Alice"}, calls: make(map[string]int)}
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/users" {
			api.serveHTTP(w, r)
			return
		}
		// Capture the old data before a PUT can commit, then delay only network delivery.
		recorded := httptest.NewRecorder()
		api.serveHTTP(recorded, r)
		call := api.count("/users")
		if call <= len(responses) && !responses[call-1].hold(r.Context()) {
			return
		}
		for key, values := range recorded.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(recorded.Code)
		w.Write(recorded.Body.Bytes())
	}))
	t.Cleanup(api.server.Close)
	return api
}

// writeHTTPUser commits a real PUT and returns the server-confirmed user for Mutation synchronization.
func writeHTTPUser(ctx context.Context, api *userAPI, value user) (user, error) {
	var committed user
	body, err := json.Marshal(value)
	if err != nil {
		return committed, fmt.Errorf("encode user: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		api.server.URL+"/users/42",
		bytes.NewReader(body),
	)
	if err != nil {
		return committed, fmt.Errorf("create update: %w", err)
	}
	response, err := api.server.Client().Do(request)
	if err != nil {
		return committed, fmt.Errorf("perform update: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return committed, fmt.Errorf("update HTTP status %d", response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(&committed); err != nil {
		return committed, fmt.Errorf("decode update: %w", err)
	}
	return committed, nil
}

// httpMutationResult preserves the committed outcome separately from business execution failure.
type httpMutationResult struct {
	// result contains server-confirmed data and any subsequent refresh failure.
	result soroi.MutationResult[user]
	// err reports write execution failure rather than refresh failure.
	err error
}

// awaitHTTPMutation bounds waiting for the complete public write-and-refresh call.
func awaitHTTPMutation(t *testing.T, done <-chan httpMutationResult) httpMutationResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP mutation did not complete")
		return httpMutationResult{}
	}
}

// TestHTTPMutationRefreshFollowup covers optional waiting and background follow-up after a real PUT.
func TestHTTPMutationRefreshFollowup(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(map[bool]string{false: "background", true: "wait"}[wait], func(t *testing.T) {
			old, next := newHeldHTTPResponse(t), newHeldHTTPResponse(t)
			api := newHeldListAPI(t, [2]*heldHTTPResponse{old, next})
			client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
			t.Cleanup(client.Close)
			detail := soroi.Query(
				client,
				"user:42",
				readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
			)
			list := soroi.Query(
				client,
				"users",
				readJSON[[]user](api.server.Client(), api.server.URL+"/users"),
			)
			config := soroi.Query(
				client,
				"settings",
				readJSON[settings](api.server.Client(), api.server.URL+"/settings"),
			)
			t.Cleanup(detail.Close)
			t.Cleanup(list.Close)
			t.Cleanup(config.Close)
			await(t, detail, func(s soroi.Snapshot[user]) bool { return s.HasData && !s.Fetching })
			await(t, config, func(s soroi.Snapshot[settings]) bool { return s.HasData && !s.Fetching })
			old.awaitStarted(t)
			events, err := client.SubscribeEvents(soroi.EventOptions{Key: "users", Buffer: 32})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(events.Close)
			var writes atomic.Int32
			mutation := soroi.Mutation(client, func(ctx context.Context, value user) (user, error) {
				writes.Add(1)
				return writeHTTPUser(ctx, api, value)
			}, func(ctx context.Context, value user, _ user) error {
				updateErr := soroi.Set(client, "user:42", value)
				var invalidateErr error
				if wait {
					_, invalidateErr = client.InvalidateAndWait(
						ctx,
						[]any{"users", "users"},
					)
				} else {
					invalidateErr = client.Invalidate(
						[]any{"users", "users"},
						soroi.InvalidateOptions{RefetchAfterLoad: true},
					)
				}
				return errors.Join(updateErr, invalidateErr)
			})
			t.Cleanup(mutation.Close)
			done := make(chan httpMutationResult, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			go func() {
				result, err := mutation.Mutate(ctx, user{Name: "Bob"})
				done <- httpMutationResult{result: result, err: err}
			}()
			for nextDiagnosticEvent(t, events).Kind != soroi.EventInvalidated {
			}
			// Read under the public Client lock after the invalidation event has been emitted.
			_ = client.Get("users")
			var outcome httpMutationResult
			if wait {
				if mutation.Snapshot().Status != soroi.Pending {
					t.Fatal("mutation finished before the old HTTP response was delivered")
				}
			} else {
				outcome = awaitHTTPMutation(t, done)
			}
			old.deliver()
			next.awaitStarted(t)
			if state := list.Snapshot(); !state.HasData || state.Data[0].Name != "Alice" || !state.Stale ||
				!state.Fetching {
				t.Fatalf("old HTTP data was marked current: %+v", state)
			}
			if wait {
				select {
				case result := <-done:
					t.Fatalf("returned before follow-up HTTP delivery: %+v", result)
				default:
				}
			}
			next.deliver()
			if wait {
				outcome = awaitHTTPMutation(t, done)
				if state := list.Snapshot(); state.Data[0].Name != "Bob" || state.Stale || state.Fetching {
					t.Fatalf("wait returned before cache synchronization: %+v", state)
				}
			}
			await(t, list, func(s soroi.Snapshot[[]user]) bool {
				return s.HasData && s.Data[0].Name == "Bob" && !s.Fetching
			})
			if outcome.err != nil || outcome.result.SyncErr != nil || outcome.result.Data.Name != "Bob" {
				t.Fatalf("HTTP mutation = %+v", outcome)
			}
			if detail.Snapshot().Data.Name != "Bob" || mutation.Snapshot().Status != soroi.Success ||
				writes.Load() != 1 {
				t.Fatal("confirmed write or detail synchronization was lost")
			}
			if api.count("/users") != 2 || api.count("/users/42") != 1 || api.count("/settings") != 1 {
				t.Fatal("follow-up duplicated HTTP reads or refreshed unrelated data")
			}
		})
	}
}

// TestHTTPInvalidateAndWaitResults returns mixed-type refresh results and preserves partial failures.
func TestHTTPInvalidateAndWaitResults(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "partial failure"}[fail], func(t *testing.T) {
			api := newUserAPI(t)
			client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
			t.Cleanup(client.Close)
			detail := soroi.Query(
				client,
				"user:42",
				readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
			)
			list := soroi.Query(
				client,
				"users",
				readJSON[[]user](api.server.Client(), api.server.URL+"/users"),
			)
			config := soroi.Query(
				client,
				"settings",
				readJSON[settings](api.server.Client(), api.server.URL+"/settings"),
			)
			t.Cleanup(detail.Close)
			t.Cleanup(list.Close)
			t.Cleanup(config.Close)
			await(t, detail, func(s soroi.Snapshot[user]) bool { return s.HasData && !s.Fetching })
			await(t, list, func(s soroi.Snapshot[[]user]) bool { return s.HasData && !s.Fetching })
			await(t, config, func(s soroi.Snapshot[settings]) bool { return s.HasData && !s.Fetching })
			soroi.Set(client, "unobserved", true)
			api.update(t, user{Name: "Bob"})
			if fail {
				api.failNext(1)
			}
			results, err := client.InvalidateAndWait(
				context.Background(),
				[]any{"user:42", "users", "users", "unobserved"},
			)
			if len(results) != 3 || (err != nil) != fail {
				t.Fatalf("HTTP refresh results = %+v, %v", results, err)
			}
			failures := 0
			for index, key := range []any{"user:42", "users"} {
				result := results[index]
				state := client.Get(key)
				if result.Key != key || state.Fetching || !state.HasData {
					t.Fatalf("unfinished target: result=%+v state=%+v", result, state)
				}
				name := detail.Snapshot().Data.Name
				if key == "users" {
					name = list.Snapshot().Data[0].Name
				}
				if result.Err != nil {
					failures++
					if result.Refreshed || !errors.Is(err, result.Err) || name != "Alice" || !state.Stale {
						t.Fatalf("partial failure = %+v, state=%+v", result, state)
					}
				} else if !result.Refreshed || name != "Bob" || state.Stale {
					t.Fatalf("successful refresh = %+v, state=%+v", result, state)
				}
			}
			if (failures == 1) != fail || failures > 1 {
				t.Fatalf("failed refresh count = %d", failures)
			}
			if skipped := results[2]; skipped.Key != "unobserved" || skipped.Refreshed || skipped.Err != nil {
				t.Fatalf("unobserved target = %+v", skipped)
			}
			if api.count("/users") != 2 || api.count("/users/42") != 2 || api.count("/settings") != 1 {
				t.Fatal("batch refresh duplicated HTTP reads or touched unrelated data")
			}
		})
	}
}

// TestHTTPRefreshWaitCancellation leaves Client-owned transport work running after a waiter cancels.
func TestHTTPRefreshWaitCancellation(t *testing.T) {
	response := newHeldHTTPResponse(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if response.hold(r.Context()) {
			json.NewEncoder(w).Encode(user{Name: "Bob"})
		}
	}))
	t.Cleanup(server.Close)
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	soroi.Set(client, "user", user{Name: "Alice"})
	query := soroi.Query(client, "user", readJSON[user](server.Client(), server.URL))
	t.Cleanup(query.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		results, err := client.InvalidateAndWait(ctx, "user")
		if len(results) != 1 || results[0].Refreshed || !errors.Is(results[0].Err, context.Canceled) {
			t.Errorf("canceled HTTP refresh results = %+v, %v", results, err)
		}
		done <- err
	}()
	response.awaitStarted(t)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("HTTP refresh wait = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP refresh wait ignored cancellation")
	}
	if state := query.Snapshot(); !state.Fetching || state.Data.Name != "Alice" || !state.Stale {
		t.Fatalf("wait cancellation altered shared HTTP work: %+v", state)
	}
	response.deliver()
	state := await(t, query, func(s soroi.Snapshot[user]) bool { return s.Data.Name == "Bob" && !s.Fetching })
	if state.Stale || state.Err != nil || calls.Load() != 1 {
		t.Fatalf("Client-owned HTTP completion = %+v, calls=%d", state, calls.Load())
	}
}

// TestHTTPMutationRefreshFailure keeps a committed PUT successful when the associated GET fails.
func TestHTTPMutationRefreshFailure(t *testing.T) {
	api := newUserAPI(t)
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	list := soroi.Query(
		client,
		"users",
		readJSON[[]user](api.server.Client(), api.server.URL+"/users"),
	)
	t.Cleanup(list.Close)
	await(t, list, func(s soroi.Snapshot[[]user]) bool { return s.HasData && !s.Fetching })
	api.failNext(1)
	writes := 0
	mutation := soroi.Mutation(client, func(ctx context.Context, name string) (user, error) {
		writes++
		return writeHTTPUser(ctx, api, user{Name: name})
	}, func(ctx context.Context, committed user, input string) error {
		if committed.Name != "Bob" || input != "Bob" {
			t.Fatalf("HTTP callback committed=%+v input=%q", committed, input)
		}
		_, err := client.InvalidateAndWait(ctx, "users")
		return err
	})
	t.Cleanup(mutation.Close)
	result, err := mutation.Mutate(context.Background(), "Bob")
	if err != nil || result.Data.Name != "Bob" || result.SyncErr == nil || writes != 1 {
		t.Fatalf("committed HTTP mutation = %+v, %v, writes=%d", result, err, writes)
	}
	if !strings.Contains(result.SyncErr.Error(), "HTTP status 503") {
		t.Fatalf("HTTP refresh cause was lost: %v", result.SyncErr)
	}
	if state := mutation.Snapshot(); state.Status != soroi.Success || state.Err != nil || state.SyncErr == nil {
		t.Fatalf("committed HTTP write state = %+v", state)
	}
	if state := list.Snapshot(); state.Data[0].Name != "Alice" || !state.Stale || state.Err == nil || state.Fetching {
		t.Fatalf("failed HTTP refresh lost old data: %+v", state)
	}
	stored, err := readJSON[user](api.server.Client(), api.server.URL+"/users/42")(context.Background())
	if err != nil || stored.Name != "Bob" || api.count("/users") != 2 {
		t.Fatalf("server commit or GET count = %+v, %v", stored, err)
	}
}
