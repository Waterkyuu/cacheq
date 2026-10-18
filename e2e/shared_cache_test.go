package e2e_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
)

// httpSharedStore represents one external cache shared by independently owned HTTP query clients.
type httpSharedStore struct {
	// mu protects the encoded value and invalidation generation.
	mu sync.Mutex
	// record holds one test resource's shared state.
	record soroi.SharedRecord
}

// Read returns a copied encoded result so clients do not share mutable decoded values.
func (s *httpSharedStore) Read(_ context.Context, _ string) (soroi.SharedRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.record.Version == "" {
		s.record.Version = "initial"
	}
	r := s.record
	r.Data = append([]byte{}, r.Data...)
	return r, nil
}

// Write checks the generation before replacing the resource.
func (s *httpSharedStore) Write(_ context.Context, _ string, w soroi.SharedWrite) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w.Version != s.record.Version {
		return false, nil
	}
	s.record.Data = append([]byte{}, w.Data...)
	return true, nil
}

// Invalidate changes the generation and removes the cached HTTP response.
func (s *httpSharedStore) Invalidate(_ context.Context, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record = soroi.SharedRecord{Version: s.record.Version + "/next"}
	return nil
}

// TestHTTPSharedCache exercises prefetch, subscription hydration, and refresh across two clients.
func TestHTTPSharedCache(t *testing.T) {
	api := newUserAPI(t)
	shared, err := soroi.NewSharedCache[user](&httpSharedStore{}, soroi.SharedCacheOptions[user]{
		Key: "tenant:42:user:42", Format: "user.json.v1", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := soroi.QueryDefinition(
		"user:42",
		readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
	)
	if err != nil {
		t.Fatal(err)
	}
	d.Shared = shared
	c1 := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	c2 := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	t.Cleanup(c1.Close)
	t.Cleanup(c2.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := d.Prefetch(ctx, c1); err != nil {
		t.Fatal(err)
	}
	h := d.Query(c2)
	t.Cleanup(h.Close)
	if value, err := d.Fetch(ctx, c2); err != nil || value.Name != "Alice" {
		t.Fatalf("shared = %+v, %v", value, err)
	}
	if state := h.Snapshot(); !state.HasData || state.Fetching || state.Stale {
		t.Fatalf("subscription = %+v", state)
	}
	if api.count("/users/42") != 1 {
		t.Fatal("second client repeated source request")
	}
	if _, err := h.Refetch(ctx); err != nil {
		t.Fatal(err)
	}
	if api.count("/users/42") != 2 {
		t.Fatal("refetch reused shared cache")
	}
	if err := d.InvalidateShared(ctx, c1, soroi.InvalidateOptions{Refetch: soroi.RefetchNone}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Fetch(ctx, c1); err != nil {
		t.Fatal(err)
	}
	if api.count("/users/42") != 3 {
		t.Fatal("invalidation reused old shared result")
	}
}

// unavailableHTTPSharedStore injects a cache read failure independently of the HTTP source.
type unavailableHTTPSharedStore struct {
	// SharedStore supplies the unchanged write and invalidation behavior.
	soroi.SharedStore
	// err is the original dependency error retained by shared diagnostics.
	err error
}

// Read returns the injected failure so the query must use its HTTP source.
func (s unavailableHTTPSharedStore) Read(context.Context, string) (soroi.SharedRecord, error) {
	return soroi.SharedRecord{}, s.err
}

// TestHTTPSharedCacheDiagnostics preserves source success and failure independently of cache diagnostics.
func TestHTTPSharedCacheDiagnostics(t *testing.T) {
	api := newUserAPI(t)
	cause := errors.New("cache connection unavailable")
	shared, err := soroi.NewSharedCache(unavailableHTTPSharedStore{
		SharedStore: &httpSharedStore{}, err: cause,
	}, soroi.SharedCacheOptions[user]{
		Key: "tenant:42:user:42", Format: "user.json.v1", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := soroi.QueryDefinition("user:42", readJSON[user](api.server.Client(), api.server.URL+"/users/42"))
	if err != nil {
		t.Fatal(err)
	}
	d.Shared = shared
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	t.Cleanup(client.Close)
	if value, err := d.Fetch(context.Background(), client); err != nil || value.Name != "Alice" {
		t.Fatalf("HTTP fallback = %+v, %v", value, err)
	}
	stats := shared.Stats()
	if !errors.Is(stats.LastError, cause) || stats.ReadErrors != 1 || stats.Writes != 0 {
		t.Fatalf("cache diagnostics = %+v", stats)
	}
	for _, part := range []string{"read", "tenant:42:user:42", "user.json.v1"} {
		if !strings.Contains(stats.LastError.Error(), part) {
			t.Fatalf("missing %q context: %v", part, stats.LastError)
		}
	}
	if _, err := d.Fetch(context.Background(), client); err != nil || api.count("/users/42") != 1 {
		t.Fatalf("local reuse = %v, HTTP calls = %d", err, api.count("/users/42"))
	}
	if shared.Stats().LastError != stats.LastError {
		t.Fatal("local hit erased diagnostic history")
	}
	api.failNext(1)
	peer := soroi.NewClient(soroi.Options{})
	t.Cleanup(peer.Close)
	if _, err := d.Fetch(
		context.Background(),
		peer,
	); err == nil || err.Error() != "HTTP status 503" ||
		errors.Is(err, cause) {
		t.Fatalf("source error replaced by cache error: %v", err)
	}
	if !errors.Is(shared.Stats().LastError, cause) || shared.Stats().ReadErrors != 2 {
		t.Fatalf("cache failure lost after source failure: %+v", shared.Stats())
	}
}
