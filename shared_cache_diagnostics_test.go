package cacheq

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// diagnosticCodec injects a specific codec failure while preserving normal JSON behavior otherwise.
type diagnosticCodec struct {
	// encodeErr replaces encoding when nonnil.
	encodeErr error
	// decodeErr replaces decoding when nonnil.
	decodeErr error
}

// Encode preserves the injected error identity for diagnostic assertions.
func (c diagnosticCodec) Encode(value int) ([]byte, error) {
	if c.encodeErr != nil {
		return nil, c.encodeErr
	}
	return JSONCodec[int]{}.Encode(value)
}

// Decode preserves the injected error identity for diagnostic assertions.
func (c diagnosticCodec) Decode(data []byte) (int, error) {
	if c.decodeErr != nil {
		return 0, c.decodeErr
	}
	return JSONCodec[int]{}.Decode(data)
}

// TestSharedCacheDiagnostics preserves failure causes without changing successful source results.
func TestSharedCacheDiagnostics(t *testing.T) {
	for _, operation := range []string{"read", "write", "encode", "decode", "invalidate"} {
		t.Run(operation, func(t *testing.T) {
			cause := &net.OpError{Op: "test", Net: "test", Err: errors.New("unavailable")}
			store := newSharedTestStore()
			d := newSharedDefinition(t, store, func(context.Context) (int, error) { return 7, nil })
			client, _ := newAgeClient(t, Options{StaleTime: time.Hour})
			if d.Shared.Stats().LastError != nil {
				t.Fatal("new cache has a diagnostic")
			}
			if _, err := d.Fetch(context.Background(), client); err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "read":
				store.readErr = cause
			case "write":
				store.writeErr = cause
			case "encode":
				d.Shared.options.Codec = diagnosticCodec{encodeErr: cause}
			case "decode":
				d.Shared.options.Codec = diagnosticCodec{decodeErr: cause}
			case "invalidate":
				store.invalidateErr = cause
			}
			if operation == "invalidate" {
				if err := d.InvalidateShared(context.Background(), client); !errors.Is(err, cause) {
					t.Fatalf("invalidation error = %v", err)
				}
				if Get[int](client, d.Key).Stale {
					t.Fatal("failed invalidation changed local freshness")
				}
			} else {
				peer, _ := newAgeClient(t, Options{StaleTime: time.Hour})
				if operation != "decode" {
					store.records[d.Shared.options.Key] = SharedRecord{Version: "next"}
				}
				if value, err := d.Fetch(context.Background(), peer); err != nil || value != 7 {
					t.Fatalf("source fallback = %d, %v", value, err)
				}
			}
			stats := d.Shared.Stats()
			var original *net.OpError
			if !errors.Is(stats.LastError, cause) || !errors.As(stats.LastError, &original) || original != cause {
				t.Fatalf("original error lost: %v", stats.LastError)
			}
			for _, part := range []string{operation, "tenant:42:answer", "answer.v1"} {
				if !strings.Contains(stats.LastError.Error(), part) {
					t.Fatalf("missing %q context: %v", part, stats.LastError)
				}
			}
			counts := map[string]uint64{
				"read":       stats.ReadErrors,
				"write":      stats.WriteErrors,
				"encode":     stats.EncodeErrors,
				"decode":     stats.DecodeErrors,
				"invalidate": stats.InvalidationErrors,
			}
			var total uint64
			for _, count := range counts {
				total += count
			}
			if total != 1 || counts[operation] != 1 {
				t.Fatalf("failure counted more than once: %+v", stats)
			}
			store.readErr, store.writeErr, store.invalidateErr = nil, nil, nil
			d.Shared.options.Codec = JSONCodec[int]{}
			if err := d.Shared.Invalidate(context.Background()); err != nil {
				t.Fatal(err)
			}
			peer, _ := newAgeClient(t, Options{StaleTime: time.Hour})
			if _, err := d.Fetch(context.Background(), peer); err != nil {
				t.Fatal(err)
			}
			if d.Shared.Stats().LastError != stats.LastError {
				t.Fatal("recovery erased diagnostic history")
			}
		})
	}
}

// TestSharedCacheRecordDiagnostics distinguishes malformed records from ordinary freshness misses.
func TestSharedCacheRecordDiagnostics(t *testing.T) {
	for _, scenario := range []string{"json", "format", "timestamps", "expired", "missing", "version"} {
		t.Run(scenario, func(t *testing.T) {
			store := newSharedTestStore()
			d := newSharedDefinition(t, store, func(context.Context) (int, error) { return 9, nil })
			client, _ := newAgeClient(t, Options{StaleTime: time.Hour})
			envelope := sharedEnvelope{
				Format:    "answer.v1",
				FetchedAt: time.Unix(0, 0),
				ExpiresAt: time.Unix(0, 0).Add(time.Hour),
				Data:      []byte("7"),
			}
			switch scenario {
			case "format":
				envelope.Format = "answer.v2"
			case "timestamps":
				envelope.ExpiresAt = envelope.FetchedAt
			case "expired":
				envelope.FetchedAt = envelope.FetchedAt.Add(-2 * time.Hour)
				envelope.ExpiresAt = envelope.ExpiresAt.Add(-2 * time.Hour)
			}
			data, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "json" {
				data = []byte("{")
			}
			if scenario == "missing" {
				data = nil
			}
			store.records[d.Shared.options.Key] = SharedRecord{Version: "initial", Data: data}
			if scenario == "version" {
				d.Shared.store = &sharedReadFuncStore{
					SharedStore: store,
					read: func(context.Context, string) (SharedRecord, error) {
						return SharedRecord{}, nil
					},
				}
			}
			if value, err := d.Fetch(context.Background(), client); err != nil || value != 9 {
				t.Fatalf("fallback = %d, %v", value, err)
			}
			diagnostic := d.Shared.Stats().LastError
			if scenario == "expired" || scenario == "missing" {
				if diagnostic != nil {
					t.Fatalf("ordinary miss recorded as failure: %v", diagnostic)
				}
				return
			}
			if diagnostic == nil {
				t.Fatal("record failure has no diagnostic")
			}
			if scenario == "json" {
				var syntax *json.SyntaxError
				if !errors.As(diagnostic, &syntax) {
					t.Fatalf("JSON cause lost: %v", diagnostic)
				}
			}
			if scenario == "format" && !strings.Contains(diagnostic.Error(), "answer.v2") {
				t.Fatalf("actual format missing: %v", diagnostic)
			}
		})
	}
}

// TestSharedCacheDiagnosticReplacement keeps snapshots stable during concurrent failures and reads.
func TestSharedCacheDiagnosticReplacement(t *testing.T) {
	store := newSharedTestStore()
	first := errors.New("first failure")
	store.invalidateErr = first
	d := newSharedDefinition(t, store, func(context.Context) (int, error) { return 1, nil })
	_ = d.Shared.Invalidate(context.Background())
	before := d.Shared.Stats()
	store.invalidateErr = context.DeadlineExceeded
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			_ = d.Shared.Invalidate(context.Background())
			_ = d.Shared.Stats()
		}()
	}
	group.Wait()
	after := d.Shared.Stats()
	if !errors.Is(before.LastError, first) || !errors.Is(after.LastError, context.DeadlineExceeded) {
		t.Fatalf("diagnostic snapshots = %v, %v", before.LastError, after.LastError)
	}
	if before.InvalidationErrors != 1 || after.InvalidationErrors != 9 {
		t.Fatalf("invalidation counts = %d, %d", before.InvalidationErrors, after.InvalidationErrors)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Shared.Invalidate(
		ctx,
	); !errors.Is(err, context.Canceled) ||
		d.Shared.Stats().LastError != after.LastError {
		t.Fatal("pre-canceled operation changed diagnostic history")
	}
}

// TestSharedCacheDiagnosticTimeout retains a store deadline without canceling the source query.
func TestSharedCacheDiagnosticTimeout(t *testing.T) {
	store := &sharedReadFuncStore{
		SharedStore: newSharedTestStore(),
		read: func(ctx context.Context, _ string) (SharedRecord, error) {
			<-ctx.Done()
			return SharedRecord{}, ctx.Err()
		},
	}
	d := newSharedDefinition(t, store, func(ctx context.Context) (int, error) { return 7, ctx.Err() })
	d.Shared.options.Timeout = time.Nanosecond
	client, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	if value, err := d.Fetch(context.Background(), client); err != nil || value != 7 {
		t.Fatalf("timeout fallback = %d, %v", value, err)
	}
	stats := d.Shared.Stats()
	if !errors.Is(stats.LastError, context.DeadlineExceeded) || stats.ReadErrors != 1 || stats.Writes != 0 {
		t.Fatalf("deadline diagnostic = %+v", stats)
	}
}

// TestSharedCacheDiagnosticConflict keeps an expected version conflict separate from operational errors.
func TestSharedCacheDiagnosticConflict(t *testing.T) {
	store := newSharedTestStore()
	d := newSharedDefinition(t, store, func(ctx context.Context) (int, error) {
		return 7, store.Invalidate(ctx, "tenant:42:answer")
	})
	client, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	if value, err := d.Fetch(context.Background(), client); err != nil || value != 7 {
		t.Fatalf("conflicting source result = %d, %v", value, err)
	}
	stats := d.Shared.Stats()
	if stats.LastError != nil || stats.WriteConflicts != 1 || !Get[int](client, d.Key).Stale {
		t.Fatalf("conflict diagnostic or freshness changed: %+v", stats)
	}
}

// TestSharedCacheDiagnosticStages retains the last failure when decoding and encoding both fail.
func TestSharedCacheDiagnosticStages(t *testing.T) {
	store := newSharedTestStore()
	d := newSharedDefinition(t, store, func(context.Context) (int, error) { return 7, nil })
	client, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	if _, err := d.Fetch(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	decodeErr, encodeErr := errors.New("cannot decode"), errors.New("cannot encode")
	d.Shared.options.Codec = diagnosticCodec{decodeErr: decodeErr, encodeErr: encodeErr}
	peer, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	if value, err := d.Fetch(context.Background(), peer); err != nil || value != 7 {
		t.Fatalf("codec fallback = %d, %v", value, err)
	}
	stats := d.Shared.Stats()
	if !errors.Is(stats.LastError, encodeErr) || stats.DecodeErrors != 1 || stats.EncodeErrors != 1 {
		t.Fatalf("latest stage diagnostic = %+v", stats)
	}
}

// TestSharedCacheEnvelopeEncodeDiagnostic preserves JSON errors from unrepresentable source timestamps.
func TestSharedCacheEnvelopeEncodeDiagnostic(t *testing.T) {
	d := newSharedDefinition(t, newSharedTestStore(), func(context.Context) (int, error) { return 7, nil })
	client := NewClient(Options{
		StaleTime: time.Hour,
		Clock:     func() time.Time { return time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC) },
	})
	t.Cleanup(client.Close)
	if value, err := d.Fetch(context.Background(), client); err != nil || value != 7 {
		t.Fatalf("source result = %d, %v", value, err)
	}
	stats := d.Shared.Stats()
	var original *json.MarshalerError
	if !errors.As(stats.LastError, &original) || stats.EncodeErrors != 1 || stats.Writes != 0 {
		t.Fatalf("envelope diagnostic = %+v", stats)
	}
	if !strings.Contains(stats.LastError.Error(), "encode envelope") {
		t.Fatalf("missing envelope context: %v", stats.LastError)
	}
}
