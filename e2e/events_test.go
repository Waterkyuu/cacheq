package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// nextDiagnosticEvent bounds public event consumption and rejects premature subscription closure.
func nextDiagnosticEvent(t *testing.T, subscription *cacheq.EventSubscription) cacheq.Event {
	t.Helper()
	select {
	case event, open := <-subscription.Events():
		if !open {
			t.Fatal("diagnostic stream closed before expected event")
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("expected diagnostic event was not delivered")
		return cacheq.Event{}
	}
}

// expectDiagnosticEvent checks an operation's public classification without depending on duration or ID allocation.
func expectDiagnosticEvent(
	t *testing.T,
	subscription *cacheq.EventSubscription,
	expected cacheq.Event,
) cacheq.Event {
	t.Helper()
	event := nextDiagnosticEvent(t, subscription)
	sourceMatches := event.Kind == expected.Kind && event.Trigger == expected.Trigger
	causeMatches := event.Key == expected.Key && event.Reason == expected.Reason
	if !sourceMatches || !causeMatches {
		t.Fatalf("event = %+v; expected classification = %+v", event, expected)
	}
	return event
}

// TestHTTPDiagnosticLifecycle correlates real HTTP sharing, retry, mutation refresh, expiration, and removal.
func TestHTTPDiagnosticLifecycle(t *testing.T) {
	api := &userAPI{current: user{Name: "Alice"}, calls: make(map[string]int), failures: 1}
	entered, release := make(chan struct{}), make(chan struct{})
	var firstRequest sync.Once
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/users/42" {
			firstRequest.Do(func() {
				close(entered)
				select {
				case <-r.Context().Done():
				case <-release:
				}
			})
		}
		api.serveHTTP(w, r)
	}))
	t.Cleanup(api.server.Close)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var seconds atomic.Int64
	clock := func() time.Time { return time.Unix(seconds.Load(), 0) }
	client := cacheq.NewClient(cacheq.Options{
		StaleTime: time.Hour, Retry: 1, RetryDelay: func(int) time.Duration { return 0 }, Clock: clock,
	})
	t.Cleanup(client.Close)
	events, err := client.SubscribeEvents(cacheq.EventOptions{Key: "user:42", Buffer: 32})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(events.Close)
	var sequence uint64
	read := func(kind cacheq.EventKind, trigger cacheq.EventTrigger, reason cacheq.EventReason) cacheq.Event {
		event := expectDiagnosticEvent(t, events, cacheq.Event{
			Key: "user:42", Kind: kind, Trigger: trigger, Reason: reason,
		})
		if event.Sequence <= sequence || !event.Time.Equal(clock()) {
			t.Fatalf("event ordering or timestamp = %+v", event)
		}
		sequence = event.Sequence
		return event
	}
	load := readJSON[user](api.server.Client(), api.server.URL+"/users/42")
	first := cacheq.Query(client, "user:42", load)
	t.Cleanup(first.Close)
	start := read(cacheq.EventLoadStarted, cacheq.TriggerQuery, cacheq.ReasonMissing)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("initial diagnostic HTTP request did not start")
	}
	second := cacheq.Query(client, "user:42", load)
	t.Cleanup(second.Close)
	join := read(cacheq.EventLoadJoined, cacheq.TriggerQuery, cacheq.ReasonMissing)
	unblock()
	retry := read(cacheq.EventRetryStarted, cacheq.TriggerQuery, cacheq.ReasonMissing)
	end := read(cacheq.EventLoadFinished, cacheq.TriggerQuery, cacheq.ReasonMissing)
	idsMatch := start.LoadID != 0 && join.LoadID == start.LoadID && retry.LoadID == start.LoadID &&
		end.LoadID == start.LoadID
	if !idsMatch {
		t.Fatalf(
			"shared HTTP IDs = %d, %d, %d, %d",
			start.LoadID,
			join.LoadID,
			retry.LoadID,
			end.LoadID,
		)
	}
	if retry.Err == nil || retry.Attempts != 2 {
		t.Fatalf("HTTP retry record = %+v", retry)
	}
	applied := end.Applied && end.Err == nil && end.Attempts == 2 && end.Joined == 1 && end.Duration > 0
	if !applied || api.count("/users/42") != 2 {
		t.Fatalf("shared HTTP completion = %+v; requests = %d", end, api.count("/users/42"))
	}
	for _, handle := range []*cacheq.QueryHandle[user]{first, second} {
		state := await(t, handle, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
		if state.Data.Name != "Alice" || state.Err != nil {
			t.Fatalf("shared HTTP data = %+v", state)
		}
	}
	if _, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		load,
	); err != nil {
		t.Fatal(err)
	}
	if hit := read(cacheq.EventCacheHit, cacheq.TriggerFetch, cacheq.ReasonFresh); hit.LoadID != 0 {
		t.Fatalf("cache hit load ID = %d", hit.LoadID)
	}
	if api.count("/users/42") != 2 {
		t.Fatal("diagnostic cache hit performed HTTP work")
	}
	if err := cacheq.Prefetch(
		context.Background(),
		client,
		"settings",
		readJSON[settings](api.server.Client(), api.server.URL+"/settings"),
	); err != nil {
		t.Fatal(err)
	}
	first.Snapshot()
	cacheq.Get[user](client, "user:42")
	select {
	case event := <-events.Events():
		t.Fatalf("key filtering or passive reads produced %+v", event)
	default:
	}

	api.update(t, user{Name: "Bob"})
	if err := client.Invalidate("user:42"); err != nil {
		t.Fatal(err)
	}
	read(cacheq.EventInvalidated, cacheq.TriggerInvalidate, cacheq.ReasonInvalidated)
	refresh := read(cacheq.EventLoadStarted, cacheq.TriggerInvalidate, cacheq.ReasonInvalidated)
	refreshed := read(cacheq.EventLoadFinished, cacheq.TriggerInvalidate, cacheq.ReasonInvalidated)
	if refresh.LoadID == start.LoadID || refreshed.LoadID != refresh.LoadID {
		t.Fatalf("invalidation HTTP IDs = %d, %d", refresh.LoadID, refreshed.LoadID)
	}
	for _, handle := range []*cacheq.QueryHandle[user]{first, second} {
		await(t, handle, func(s cacheq.Snapshot[user]) bool { return s.Data.Name == "Bob" && !s.Fetching })
	}
	seconds.Add(3600)
	if _, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		load,
	); err != nil {
		t.Fatal(err)
	}
	expired := read(cacheq.EventLoadStarted, cacheq.TriggerFetch, cacheq.ReasonExpired)
	loaded := read(cacheq.EventLoadFinished, cacheq.TriggerFetch, cacheq.ReasonExpired)
	if expired.LoadID == refresh.LoadID || loaded.LoadID != expired.LoadID {
		t.Fatal("expired reload reused an earlier diagnostic ID")
	}
	if api.count("/users/42") != 4 {
		t.Fatal("invalidation or expiration performed unexpected HTTP work")
	}
	if err := client.Remove("user:42"); err != nil {
		t.Fatal(err)
	}
	read(cacheq.EventCacheRemoved, cacheq.TriggerNone, cacheq.ReasonRemove)
	if value, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		load,
	); err != nil || value.Name != "Bob" {
		t.Fatalf("HTTP load after removal = %+v, %v", value, err)
	}
	read(cacheq.EventLoadStarted, cacheq.TriggerFetch, cacheq.ReasonMissing)
	read(cacheq.EventLoadFinished, cacheq.TriggerFetch, cacheq.ReasonMissing)
	if api.count("/users/42") != 5 || events.Dropped() != 0 {
		t.Fatal("HTTP trace was incomplete or contained unexpected loads")
	}
	client.Close()
	expectDiagnosticEvent(t, events, cacheq.Event{Kind: cacheq.EventClientClosed, Reason: cacheq.ReasonClientClosed})
	if _, open := <-events.Events(); open {
		t.Fatal("client closure did not close the filtered diagnostic stream")
	}
}

// TestHTTPDiagnosticDiscard observes cancellation and late-result protection after a local write replaces HTTP work.
func TestHTTPDiagnosticDiscard(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		// Keep the server response late even after the client cancels its transport.
		<-release
		json.NewEncoder(w).Encode(user{Name: "old"})
	}))
	t.Cleanup(server.Close)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	events, err := client.SubscribeEvents(cacheq.EventOptions{Key: "user", Buffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(events.Close)
	handle := cacheq.Query(client, "user", readJSON[user](server.Client(), server.URL))
	t.Cleanup(handle.Close)
	start := expectDiagnosticEvent(t, events, cacheq.Event{
		Key: "user", Kind: cacheq.EventLoadStarted, Trigger: cacheq.TriggerQuery, Reason: cacheq.ReasonMissing,
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("replaced HTTP request did not start")
	}
	if err := cacheq.Set(client, "user", user{Name: "local"}); err != nil {
		t.Fatal(err)
	}
	expectDiagnosticEvent(t, events, cacheq.Event{
		Key: "user", Kind: cacheq.EventLocalWrite, Reason: cacheq.ReasonSet,
	})
	unblock()
	end := expectDiagnosticEvent(t, events, cacheq.Event{
		Key: "user", Kind: cacheq.EventLoadFinished, Trigger: cacheq.TriggerQuery, Reason: cacheq.ReasonMissing,
	})
	discard := expectDiagnosticEvent(t, events, cacheq.Event{
		Key: "user", Kind: cacheq.EventResultDiscarded, Trigger: cacheq.TriggerQuery, Reason: cacheq.ReasonSet,
	})
	idsMatch := end.LoadID == start.LoadID && discard.LoadID == start.LoadID
	if !idsMatch || end.Applied {
		t.Fatalf("replaced HTTP completion = %+v; discard = %+v", end, discard)
	}
	if !errors.Is(end.Err, context.Canceled) || discard.Sequence <= end.Sequence {
		t.Fatalf("cancellation diagnostics = %+v, %+v", end, discard)
	}
	if state := handle.Snapshot(); state.Data.Name != "local" || state.Fetching {
		t.Fatalf("late HTTP data replaced local state = %+v", state)
	}
}

// TestHTTPDiagnosticBackpressure lets HTTP loads finish with an unread bounded stream and accounts for terminal loss.
func TestHTTPDiagnosticBackpressure(t *testing.T) {
	api := newUserAPI(t)
	client := cacheq.NewClient(cacheq.Options{})
	t.Cleanup(client.Close)
	events, err := client.SubscribeEvents(cacheq.EventOptions{Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(events.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range 3 {
		value, err := cacheq.Fetch(
			ctx,
			client,
			"user",
			readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
		)
		if err != nil || value.Name != "Alice" {
			t.Fatalf("HTTP load with full diagnostic buffer = %+v, %v", value, err)
		}
	}
	if events.Dropped() != 5 || api.count("/users/42") != 3 {
		t.Fatalf("HTTP requests = %d; lost diagnostics = %d", api.count("/users/42"), events.Dropped())
	}
	client.Close()
	if events.Dropped() != 6 {
		t.Fatal("full buffer did not account for lost terminal event")
	}
	expectDiagnosticEvent(t, events, cacheq.Event{
		Key: "user", Kind: cacheq.EventLoadStarted, Trigger: cacheq.TriggerFetch, Reason: cacheq.ReasonMissing,
	})
	if _, open := <-events.Events(); open {
		t.Fatal("full diagnostic stream did not close after draining its retained event")
	}
}
