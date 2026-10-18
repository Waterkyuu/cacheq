package e2e_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// httpSharedStore represents one external cache shared by independently owned HTTP query clients.
type httpSharedStore struct {
	// mu protects the encoded value and invalidation generation.
	mu sync.Mutex
	// record holds one test resource's shared state.
	record cacheq.SharedRecord
}

// Read returns a copied encoded result so clients do not share mutable decoded values.
func (s *httpSharedStore) Read(_ context.Context, _ string) (cacheq.SharedRecord, error) {
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
func (s *httpSharedStore) Write(_ context.Context, _ string, w cacheq.SharedWrite) (bool, error) {
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
	s.record = cacheq.SharedRecord{Version: s.record.Version + "/next"}
	return nil
}

// TestHTTPSharedCache exercises prefetch, subscription hydration, and refresh across two clients.
func TestHTTPSharedCache(t *testing.T) {
	api := newUserAPI(t)
	shared, err := cacheq.NewSharedCache[user](&httpSharedStore{}, cacheq.SharedCacheOptions[user]{
		Key: "tenant:42:user:42", Format: "user.json.v1", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	d := cacheq.QueryDefinition(
		"user:42",
		readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
	)
	d.Shared = shared
	c1 := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	c2 := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
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
	if err := d.InvalidateShared(ctx, c1, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Fetch(ctx, c1); err != nil {
		t.Fatal(err)
	}
	if api.count("/users/42") != 3 {
		t.Fatal("invalidation reused old shared result")
	}
}
