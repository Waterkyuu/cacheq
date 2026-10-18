package soroi

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sharedReadFuncStore injects controlled read behavior while delegating writes and invalidations.
type sharedReadFuncStore struct {
	// SharedStore owns the underlying conditional-write implementation.
	SharedStore
	// read replaces the read operation to exercise cancellation or observe request admission.
	read func(context.Context, string) (SharedRecord, error)
}

// Read runs the injected read callback without changing the delegated mutation contract.
func (s *sharedReadFuncStore) Read(ctx context.Context, key string) (SharedRecord, error) {
	return s.read(ctx, key)
}

// failingSharedCodec exercises serialization failures independently of source load success.
type failingSharedCodec struct{}

// Encode rejects values to verify best-effort persistence.
func (failingSharedCodec) Encode(int) ([]byte, error) { return nil, errors.New("encode failed") }

// Decode rejects stored values to verify source fallback.
func (failingSharedCodec) Decode([]byte) (int, error) { return 0, errors.New("decode failed") }

// sharedTestStore models atomic version checks without a server or wall-clock expiration.
type sharedTestStore struct {
	// mu protects records, generations, and injected failures.
	mu sync.Mutex
	// records owns encoded values keyed by their explicit shared identity.
	records map[string]SharedRecord
	// sequence assigns a never-reused generation in this test store.
	sequence int
	// readErr simulates an unavailable shared cache.
	readErr error
	// writeErr simulates a failed best-effort cache write.
	writeErr error
	// invalidateErr simulates a failed explicit remote operation.
	invalidateErr error
}

// newSharedTestStore allocates independently owned storage for each test.
func newSharedTestStore() *sharedTestStore {
	return &sharedTestStore{records: make(map[string]SharedRecord)}
}

// Read returns an owned snapshot and initializes a generation even when no value exists.
func (s *sharedTestStore) Read(ctx context.Context, key string) (SharedRecord, error) {
	if err := ctx.Err(); err != nil {
		return SharedRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readErr != nil {
		return SharedRecord{}, s.readErr
	}
	r := s.records[key]
	if r.Version == "" {
		s.sequence++
		r.Version = strconv.Itoa(s.sequence)
		s.records[key] = r
	}
	r.Data = append([]byte{}, r.Data...)
	return r, nil
}

// Write installs data only while its read generation still owns the key.
func (s *sharedTestStore) Write(ctx context.Context, key string, write SharedWrite) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr != nil {
		return false, s.writeErr
	}
	if s.records[key].Version != write.Version {
		return false, nil
	}
	s.records[key] = SharedRecord{Version: write.Version, Data: append([]byte{}, write.Data...)}
	return true, nil
}

// Invalidate advances the generation and removes data in one critical section.
func (s *sharedTestStore) Invalidate(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.invalidateErr != nil {
		return s.invalidateErr
	}
	s.sequence++
	s.records[key] = SharedRecord{Version: strconv.Itoa(s.sequence)}
	return nil
}

// newSharedDefinition supplies one explicit storage key and the default JSON codec.
func newSharedDefinition(t *testing.T, store SharedStore, load Fetcher[int]) Definition[int] {
	t.Helper()
	shared, err := NewSharedCache[int](store, SharedCacheOptions[int]{
		Key: "tenant:42:answer", Format: "answer.v1", TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	definition, err := QueryDefinition("local-answer", load)
	if err != nil {
		t.Fatal(err)
	}
	definition.Shared = shared
	return definition
}

// TestSharedCacheReuse restores typed data across clients without renewing its source freshness.
func TestSharedCacheReuse(t *testing.T) {
	store := newSharedTestStore()
	var calls atomic.Int32
	d := newSharedDefinition(t, store, func(context.Context) (int, error) { return int(calls.Add(1)), nil })
	c1, now1 := newAgeClient(t, Options{StaleTime: time.Minute})
	c2, now2 := newAgeClient(t, Options{StaleTime: time.Minute})
	ctx := context.Background()
	if value, err := d.Fetch(ctx, c1); err != nil || value != 1 {
		t.Fatalf("first = %d, %v", value, err)
	}
	now2.Add(int64(30 * time.Second))
	if value, err := d.Fetch(ctx, c2); err != nil || value != 1 {
		t.Fatalf("shared = %d, %v", value, err)
	}
	state := Get[int](c2, d.Key)
	if !state.ExpiresAt.Equal(time.Unix(0, now1.Load()).Add(time.Minute)) ||
		!state.UpdatedAt.Equal(time.Unix(0, now2.Load())) {
		t.Fatalf("restored timestamps = %+v", state)
	}
	if _, err := d.Fetch(ctx, c2); err != nil {
		t.Fatal(err)
	}
	if stats := d.Shared.Stats(); stats.Hits != 1 || stats.Misses != 1 || stats.Writes != 1 {
		t.Fatalf("shared stats = %+v", stats)
	}
	now2.Store(now1.Load() + int64(time.Minute))
	if value, err := d.Fetch(ctx, c2); err != nil || value != 2 {
		t.Fatalf("expired policy = %d, %v", value, err)
	}
}

// TestSharedCacheRefresh verifies bypass for both explicit refetch and local invalidation.
func TestSharedCacheRefresh(t *testing.T) {
	for _, operation := range []string{"refetch", "invalidate"} {
		t.Run(operation, func(t *testing.T) {
			var calls atomic.Int32
			d := newSharedDefinition(t, newSharedTestStore(), func(context.Context) (int, error) {
				return int(calls.Add(1)), nil
			})
			c := NewClient(Options{StaleTime: time.Hour})
			t.Cleanup(c.Close)
			ctx := context.Background()
			if _, err := d.Fetch(ctx, c); err != nil {
				t.Fatal(err)
			}
			d.Options.Disable = true
			h := d.Query(c)
			t.Cleanup(h.Close)
			var value int
			var err error
			if operation == "refetch" {
				value, err = h.Refetch(ctx)
			} else {
				if err := c.Invalidate(d.Key); err != nil {
					t.Fatal(err)
				}
				value, err = d.Fetch(ctx, c)
			}
			if err != nil || value != 2 || d.Shared.Stats().Bypasses != 1 {
				t.Fatalf("refresh = %d, %v, %+v", value, err, d.Shared.Stats())
			}
		})
	}
}

// TestSharedInvalidationFencesOldLoad controls the old-read/write ordering with channels.
func TestSharedInvalidationFencesOldLoad(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	d := newSharedDefinition(t, newSharedTestStore(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	})
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	done := make(chan error, 1)
	go func() { _, err := d.Fetch(context.Background(), c); done <- err }()
	<-started
	if err := d.InvalidateShared(context.Background(), c, InvalidateOptions{Refetch: RefetchNone}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if stats := d.Shared.Stats(); stats.WriteConflicts != 1 || stats.Writes != 0 {
		t.Fatalf("old write = %+v", stats)
	}
	if !Get[int](c, d.Key).Stale {
		t.Fatal("invalidated old load became fresh locally")
	}
	d.Fetcher = func(context.Context) (int, error) { return 2, nil }
	if value, err := d.Fetch(context.Background(), c); err != nil || value != 2 {
		t.Fatalf("replacement = %d, %v", value, err)
	}
}

// TestSharedCacheFailures distinguishes best-effort reads and writes from explicit invalidation errors.
func TestSharedCacheFailures(t *testing.T) {
	failure := errors.New("cache unavailable")
	for _, operation := range []string{"read", "write", "invalidate"} {
		t.Run(operation, func(t *testing.T) {
			s := newSharedTestStore()
			switch operation {
			case "read":
				s.readErr = failure
			case "write":
				s.writeErr = failure
			}
			d := newSharedDefinition(t, s, func(context.Context) (int, error) { return 7, nil })
			c := NewClient(Options{StaleTime: time.Hour})
			t.Cleanup(c.Close)
			if value, err := d.Fetch(context.Background(), c); err != nil || value != 7 {
				t.Fatalf("fallback = %d, %v", value, err)
			}
			stats := d.Shared.Stats()
			if operation == "read" && stats.ReadErrors != 1 {
				t.Fatalf("read errors = %+v", stats)
			}
			if operation == "write" && stats.WriteErrors != 1 {
				t.Fatalf("write errors = %+v", stats)
			}
			if operation == "invalidate" {
				s.invalidateErr = failure
				if err := d.InvalidateShared(context.Background(), c); !errors.Is(err, failure) {
					t.Fatal(err)
				}
				if Get[int](c, d.Key).Stale {
					t.Fatal("failed remote invalidation changed local state")
				}
			}
		})
	}
}

// TestSharedCacheLocalWrites preserves local-only Set and Update semantics after a shared load.
func TestSharedCacheLocalWrites(t *testing.T) {
	d := newSharedDefinition(t, newSharedTestStore(), func(context.Context) (int, error) { return 1, nil })
	c1, now := newAgeClient(t, Options{StaleTime: time.Minute})
	if _, err := d.Fetch(context.Background(), c1); err != nil {
		t.Fatal(err)
	}
	now.Add(int64(30 * time.Second))
	if err := Update(c1, d.Key, func(int) (int, error) { return 9, nil }); err != nil {
		t.Fatal(err)
	}
	if state := Get[int](
		c1,
		d.Key,
	); state.Data != 9 ||
		!state.ExpiresAt.Equal(time.Unix(0, now.Load()).Add(time.Minute)) {
		t.Fatalf("local update = %+v", state)
	}
	c2, _ := newAgeClient(t, Options{StaleTime: time.Minute})
	if value, err := d.Fetch(context.Background(), c2); err != nil || value != 1 {
		t.Fatalf("remote unaffected = %d, %v", value, err)
	}
}

// TestSharedCacheCancellation prevents pre-canceled reads and invalidations from changing storage.
func TestSharedCacheCancellation(t *testing.T) {
	d := newSharedDefinition(t, newSharedTestStore(), func(context.Context) (int, error) {
		t.Fatal("canceled request reached source")
		return 0, nil
	})
	c := NewClient(Options{})
	t.Cleanup(c.Close)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Fetch(ctx, c); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := d.InvalidateShared(ctx, c); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if stats := d.Shared.Stats(); stats != (SharedStats{}) {
		t.Fatalf("canceled stats = %+v", stats)
	}
}

// TestSharedCacheValidation rejects incomplete configurations before any storage interaction.
func TestSharedCacheValidation(t *testing.T) {
	valid := SharedCacheOptions[int]{Key: "key", Format: "int.v1", TTL: time.Minute}
	for _, field := range []string{"store", "key", "format", "ttl", "timeout"} {
		t.Run(field, func(t *testing.T) {
			options := valid
			var store SharedStore = newSharedTestStore()
			switch field {
			case "store":
				store = nil
			case "key":
				options.Key = ""
			case "format":
				options.Format = ""
			case "ttl":
				options.TTL = 0
			case "timeout":
				options.Timeout = -time.Second
			}
			if _, err := NewSharedCache(store, options); !errors.Is(err, ErrInvalidSharedCache) {
				t.Fatal(err)
			}
		})
	}
}

// TestSharedCacheRecordBoundaries covers shared deadlines, consumer policy, malformed data, and codec errors.
func TestSharedCacheRecordBoundaries(t *testing.T) {
	for _, scenario := range []string{"TTL", "zero freshness", "format", "envelope", "decode", "encode", "nil"} {
		t.Run(scenario, func(t *testing.T) {
			store := newSharedTestStore()
			d := newSharedDefinition(t, store, func(context.Context) (int, error) { return 1, nil })
			c1, _ := newAgeClient(t, Options{StaleTime: 2 * time.Hour})
			if _, err := d.Fetch(context.Background(), c1); err != nil {
				t.Fatal(err)
			}
			c2, now := newAgeClient(t, Options{StaleTime: 2 * time.Hour})
			d.Fetcher = func(context.Context) (int, error) { return 2, nil }
			switch scenario {
			case "TTL":
				now.Store(int64(time.Hour))
			case "zero freshness":
				zero := time.Duration(0)
				d.Options.StaleTime = &zero
			case "format":
				shared, err := NewSharedCache[int](
					store,
					SharedCacheOptions[int]{Key: d.Shared.options.Key, Format: "int.v2", TTL: time.Hour},
				)
				if err != nil {
					t.Fatal(err)
				}
				d.Shared = shared
			case "envelope":
				store.records[d.Shared.options.Key] = SharedRecord{Version: "corrupt", Data: []byte("invalid JSON")}
			case "decode", "encode":
				shared, err := NewSharedCache[int](store, SharedCacheOptions[int]{
					Key: d.Shared.options.Key, Format: "answer.v1", TTL: time.Hour, Codec: failingSharedCodec{},
				})
				if err != nil {
					t.Fatal(err)
				}
				d.Shared = shared
				if scenario == "encode" {
					store.records[d.Shared.options.Key] = SharedRecord{Version: "empty"}
				}
			case "nil":
				shared, err := NewSharedCache[*int](
					store,
					SharedCacheOptions[*int]{Key: "nil", Format: "ptr.v1", TTL: time.Hour},
				)
				if err != nil {
					t.Fatal(err)
				}
				definition, err := QueryDefinition(
					"nil",
					func(context.Context) (*int, error) { return nil, nil },
				)
				if err != nil {
					t.Fatal(err)
				}
				definition.Shared = shared
				if _, err := definition.Fetch(context.Background(), c1); err != nil {
					t.Fatal(err)
				}
				if _, err := definition.Fetch(context.Background(), c2); err != nil {
					t.Fatal(err)
				}
				if state := Get[*int](c2, "nil"); !state.HasData || state.Data != nil || shared.Stats().Hits != 1 {
					t.Fatalf("cached nil = %+v, %+v", state, shared.Stats())
				}
				return
			}
			if value, err := d.Fetch(context.Background(), c2); err != nil || value != 2 {
				t.Fatalf("fallback = %d, %v", value, err)
			}
			stats := d.Shared.Stats()
			if scenario == "encode" && stats.EncodeErrors != 1 {
				t.Fatalf("encode stats = %+v", stats)
			}
			if scenario == "decode" && stats.DecodeErrors != 1 {
				t.Fatalf("decode stats = %+v", stats)
			}
		})
	}
}

// TestSharedCacheTimeoutFallsBack bounds cache access without canceling the source's parent context.
func TestSharedCacheTimeoutFallsBack(t *testing.T) {
	store := &sharedReadFuncStore{
		SharedStore: newSharedTestStore(),
		read: func(ctx context.Context, _ string) (SharedRecord, error) {
			<-ctx.Done()
			return SharedRecord{}, ctx.Err()
		},
	}
	shared, err := NewSharedCache[int](
		store,
		SharedCacheOptions[int]{Key: "key", Format: "int.v1", TTL: time.Hour, Timeout: time.Nanosecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	d, err := QueryDefinition("key", func(ctx context.Context) (int, error) { return 7, ctx.Err() })
	if err != nil {
		t.Fatal(err)
	}
	d.Shared = shared
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	if value, err := d.Fetch(context.Background(), c); err != nil || value != 7 {
		t.Fatalf("fallback = %d, %v", value, err)
	}
	if stats := shared.Stats(); stats.ReadErrors != 1 || stats.Writes != 0 {
		t.Fatalf("timeout = %+v", stats)
	}
}

// TestSharedCacheCloseCancelsRead cancels active external work and prevents source fallback after closure.
func TestSharedCacheCloseCancelsRead(t *testing.T) {
	started := make(chan struct{})
	store := &sharedReadFuncStore{
		SharedStore: newSharedTestStore(),
		read: func(ctx context.Context, _ string) (SharedRecord, error) {
			close(started)
			<-ctx.Done()
			return SharedRecord{}, ctx.Err()
		},
	}
	d := newSharedDefinition(
		t,
		store,
		func(context.Context) (int, error) { t.Error("closed client reached source"); return 0, nil },
	)
	c := NewClient(Options{})
	t.Cleanup(c.Close)
	done := make(chan error, 1)
	go func() { _, err := d.Fetch(context.Background(), c); done <- err }()
	<-started
	c.Close()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if state := Get[int](c, d.Key); state.HasData || !errors.Is(state.Err, ErrClosed) {
		t.Fatalf("closed = %+v", state)
	}
}

// TestSharedCacheConcurrentMerge admits only one shared read and source callback per client key.
func TestSharedCacheConcurrentMerge(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	underlying := newSharedTestStore()
	store := &sharedReadFuncStore{
		SharedStore: underlying,
		read: func(ctx context.Context, key string) (SharedRecord, error) {
			reads.Add(1)
			return underlying.Read(ctx, key)
		},
	}
	d := newSharedDefinition(t, store, func(context.Context) (int, error) { close(started); <-release; return 7, nil })
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	first := make(chan error, 1)
	go func() { _, err := d.Fetch(context.Background(), c); first <- err }()
	<-started
	// Subscription construction synchronously joins the active load, giving a deterministic merge barrier.
	h := d.Query(c)
	t.Cleanup(h.Close)
	if c.Stats().MergedRequests != 1 {
		t.Fatal("subscription did not join load")
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 1 || h.Snapshot().Data != 7 {
		t.Fatal("merged request repeated shared work")
	}
}

// TestSharedCacheSourceFailureAndMaxAge preserves stale data on failure and applies local installation age.
func TestSharedCacheSourceFailureAndMaxAge(t *testing.T) {
	d := newSharedDefinition(t, newSharedTestStore(), func(context.Context) (int, error) { return 7, nil })
	c1, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	if _, err := d.Fetch(context.Background(), c1); err != nil {
		t.Fatal(err)
	}
	c2, now := newAgeClient(t, Options{StaleTime: time.Hour, MaxAge: time.Minute})
	now.Store(int64(30 * time.Second))
	if _, err := d.Fetch(context.Background(), c2); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("source failed")
	d.Fetcher = func(context.Context) (int, error) { return 0, failure }
	if err := c2.Invalidate(d.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Fetch(context.Background(), c2); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if state := Get[int](c2, d.Key); !state.HasData || state.Data != 7 || !state.Stale {
		t.Fatalf("stale fallback = %+v", state)
	}
	now.Store(int64(90 * time.Second))
	if state := Get[int](c2, d.Key); state.HasData {
		t.Fatalf("age boundary = %+v", state)
	}
	// Failed source results never replace the remote record with an error or zero-value fallback.
	record, err := d.Shared.store.Read(context.Background(), d.Shared.options.Key)
	if err != nil {
		t.Fatal(err)
	}
	var envelope sharedEnvelope
	if err := json.Unmarshal(record.Data, &envelope); err != nil {
		t.Fatal(err)
	}
	if value, err := (JSONCodec[int]{}).Decode(envelope.Data); err != nil || value != 7 {
		t.Fatalf("remote = %d, %v", value, err)
	}
}

// TestSharedCacheFailedRefreshDoesNotResurrect keeps the shared record bypassed after a failed refresh.
func TestSharedCacheFailedRefreshDoesNotResurrect(t *testing.T) {
	d := newSharedDefinition(t, newSharedTestStore(), func(context.Context) (int, error) { return 7, nil })
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	ctx := context.Background()
	if _, err := d.Fetch(ctx, c); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("source unavailable")
	d.Fetcher = func(context.Context) (int, error) { return 0, failure }
	if err := c.Invalidate(d.Key); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := d.Fetch(ctx, c); !errors.Is(err, failure) {
			t.Fatalf("invalidated record reused after failure: %v", err)
		}
	}
	if state := Get[int](c, d.Key); !state.Stale || state.Data != 7 || !errors.Is(state.Err, failure) {
		t.Fatalf("failed refresh state = %+v", state)
	}
}

// TestSharedCacheZeroClock preserves valid source timestamps even when an injected clock starts at zero.
func TestSharedCacheZeroClock(t *testing.T) {
	var ticks atomic.Int64
	options := Options{
		StaleTime: time.Minute,
		Clock:     func() time.Time { return time.Time{}.Add(time.Duration(ticks.Load())) },
	}
	d := newSharedDefinition(t, newSharedTestStore(), func(context.Context) (int, error) { return 7, nil })
	a, b := NewClient(options), NewClient(options)
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)
	if _, err := d.Fetch(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	ticks.Store(int64(30 * time.Second))
	if _, err := d.Fetch(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if state := Get[int](b, d.Key); !state.ExpiresAt.Equal(time.Time{}.Add(time.Minute)) || d.Shared.Stats().Hits != 1 {
		t.Fatalf("zero source clock = %+v, %+v", state, d.Shared.Stats())
	}
}
