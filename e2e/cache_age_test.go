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

// TestHTTPMaxAgeAfterFailedRefresh expires retained HTTP data and renews successful replacements.
func TestHTTPMaxAgeAfterFailedRefresh(t *testing.T) {
	api := newUserAPI(t)
	var now atomic.Int64
	client := cacheq.NewClient(cacheq.Options{
		StaleTime: 30 * time.Second,
		MaxAge:    time.Minute,
		Clock:     func() time.Time { return time.Unix(0, now.Load()) },
	})
	t.Cleanup(client.Close)
	detail := cacheq.Query(client, "user:42", readJSON[user](api.server.Client(), api.server.URL+"/users/42"))
	t.Cleanup(detail.Close)
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.HasData && !s.Fetching })
	now.Store(int64(30 * time.Second))
	api.failNext(1)
	if _, err := detail.Refetch(context.Background()); err == nil {
		t.Fatal("failed HTTP refresh succeeded")
	}
	if state := detail.Snapshot(); !state.HasData || state.Data.Name != "Alice" || state.Err == nil {
		t.Fatalf("retained HTTP failure = %+v", state)
	}
	now.Store(int64(time.Minute))
	if state := detail.Snapshot(); state.HasData || state.Data.Name != "" || state.Err == nil || state.Fetching {
		t.Fatalf("over-age HTTP failure = %+v", state)
	}
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return !s.HasData && s.Err != nil && !s.Fetching })
	if api.count("/users/42") != 2 {
		t.Fatal("age expiration started an unsolicited HTTP request")
	}
	api.update(t, user{Name: "Bob"})
	if value, err := detail.Refetch(context.Background()); err != nil || value.Name != "Bob" {
		t.Fatalf("HTTP replacement = %+v, %v", value, err)
	}
	now.Store(int64(2*time.Minute - time.Nanosecond))
	if state := detail.Snapshot(); !state.HasData || state.Data.Name != "Bob" || state.Err != nil {
		t.Fatalf("renewed HTTP age window = %+v", state)
	}
	now.Add(1)
	if state := detail.Snapshot(); state.HasData || state.Data.Name != "" || api.count("/users/42") != 3 {
		t.Fatalf("renewed HTTP deadline = %+v", state)
	}
}

// TestHTTPMaxAgeDuringRefresh expires old data without canceling HTTP work and enforces capacity on completion.
func TestHTTPMaxAgeDuringRefresh(t *testing.T) {
	var now atomic.Int64
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(started)
		select {
		case <-r.Context().Done():
			return
		case <-release:
			json.NewEncoder(w).Encode(user{Name: "Bob"})
		}
	}))
	t.Cleanup(server.Close)
	client := cacheq.NewClient(cacheq.Options{
		MaxEntries: 1,
		MaxAge:     time.Minute,
		Clock:      func() time.Time { return time.Unix(0, now.Load()) },
	})
	t.Cleanup(client.Close)
	if err := cacheq.Set(client, "user", user{Name: "Alice"}); err != nil {
		t.Fatal(err)
	}
	detail := cacheq.Query(client, "user", readJSON[user](server.Client(), server.URL))
	t.Cleanup(detail.Close)
	<-started
	now.Store(int64(time.Minute))
	state := detail.Snapshot()
	if state.HasData || state.Data.Name != "" || !state.Fetching || state.Status != cacheq.Pending {
		t.Fatalf("over-age HTTP refresh = %+v", state)
	}
	await(t, detail, func(s cacheq.Snapshot[user]) bool { return !s.HasData && s.Fetching })
	if err := cacheq.Set(client, "settings", settings{Theme: "dark"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	state = await(t, detail, func(s cacheq.Snapshot[user]) bool { return s.Data.Name == "Bob" && !s.Fetching })
	if state.Err != nil || calls.Load() != 1 || cacheq.Get[settings](client, "settings").HasData {
		t.Fatalf("HTTP completion did not preserve work and enforce capacity = %+v", state)
	}
}
