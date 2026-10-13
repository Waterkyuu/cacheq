package cacheq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrInvalidSharedCache indicates missing identity, storage, or a positive record lifetime.
var ErrInvalidSharedCache = errors.New("invalid shared cache configuration")

// Codec encodes and decodes one query's static result type; implementations must be concurrency-safe.
type Codec[V any] interface {
	// Encode returns an independent serialized representation of the supplied value.
	Encode(V) ([]byte, error)
	// Decode returns a typed value owned independently from the encoded input.
	Decode([]byte) (V, error)
}

// JSONCodec represents typed values using the standard library's JSON format.
// Values must support encoding/json; JSON does not preserve pointer identity or unexported fields.
type JSONCodec[V any] struct{}

// Encode produces an independent JSON representation of value.
func (JSONCodec[V]) Encode(value V) ([]byte, error) { return json.Marshal(value) }

// Decode restores a new typed value from a JSON representation.
func (JSONCodec[V]) Decode(data []byte) (V, error) {
	var value V
	err := json.Unmarshal(data, &value)
	return value, err
}

// SharedCacheOptions configures a shared record for one typed query identity.
type SharedCacheOptions[V any] struct {
	// Key is a stable cross-process identity including every parameter and tenant affecting the result.
	Key string
	// Format identifies the result schema and codec version; incompatible callers must use different keys.
	Format string
	// TTL bounds shared freshness and storage retention from source completion; it must be positive.
	TTL time.Duration
	// Timeout bounds each store call independently; zero selects one second and negative values are invalid.
	// It does not bound the source fetcher, which uses normal query timeout and cancellation rules.
	Timeout time.Duration
	// Codec converts V to bytes; nil selects JSONCodec[V].
	Codec Codec[V]
}

// SharedStats counts storage activity for one SharedCache across all clients using it.
// Local cache hits perform no shared operation and do not change these counters.
type SharedStats struct {
	// Hits counts fresh shared records decoded without invoking the source.
	Hits uint64
	// Misses counts readable records that are missing, expired, incompatible, or malformed.
	Misses uint64
	// Bypasses counts attempts ignoring shared data after invalidation, forced refresh, or failed refresh.
	Bypasses uint64
	// ReadErrors counts failed reads or store contract violations; these attempts fall back to the source.
	ReadErrors uint64
	// DecodeErrors counts malformed envelopes, incompatible formats, or failed value decoding.
	DecodeErrors uint64
	// Writes counts successful conditional writes following successful source loads.
	Writes uint64
	// WriteErrors counts failed store writes without turning source success into query failure.
	WriteErrors uint64
	// WriteConflicts counts writes rejected because an invalidation changed their generation.
	WriteConflicts uint64
	// EncodeErrors counts source results that could not be serialized; local data remains usable.
	EncodeErrors uint64
	// Invalidations counts completed explicit remote invalidations.
	Invalidations uint64
	// InvalidationErrors counts failed explicit remote invalidations, also returned to the caller.
	InvalidationErrors uint64
}

// SharedCache owns immutable typed storage configuration and independently synchronized counters.
// Attach it to QueryDefinition.Shared; it never owns the store's connection lifetime.
type SharedCache[V any] struct {
	// store owns the remote persistence mechanism and invalidation protocol.
	store SharedStore
	// options is copied during construction and must not be mutated afterward.
	options SharedCacheOptions[V]
	// mu protects cumulative counters without holding Client.mu during external work.
	mu sync.Mutex
	// stats accumulates operations across every client using this configuration.
	stats SharedStats
}

// NewSharedCache validates one shared query configuration without accessing storage.
func NewSharedCache[V any](store SharedStore, options SharedCacheOptions[V]) (*SharedCache[V], error) {
	invalidIdentity := options.Key == "" || options.Format == ""
	invalidTiming := options.TTL <= 0 || options.Timeout < 0
	if store == nil || invalidIdentity || invalidTiming {
		return nil, ErrInvalidSharedCache
	}
	if options.Timeout == 0 {
		options.Timeout = time.Second
	}
	if options.Codec == nil {
		options.Codec = JSONCodec[V]{}
	}
	return &SharedCache[V]{store: store, options: options}, nil
}

// Stats returns an independent snapshot of cumulative shared-cache activity.
func (s *SharedCache[V]) Stats() SharedStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Invalidate changes this key's remote generation and removes its shared record.
// It does not change any Client's local state; use QueryDefinition.InvalidateShared to update one client too.
// Other clients retain local results until normal freshness checks or explicit local invalidation.
func (s *SharedCache[V]) Invalidate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	operation, cancel := context.WithTimeout(ctx, s.options.Timeout)
	defer cancel()
	err := s.store.Invalidate(operation, s.options.Key)
	if err == nil {
		err = operation.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.stats.InvalidationErrors++
		return fmt.Errorf("invalidate shared key %q: %w", s.options.Key, err)
	}
	s.stats.Invalidations++
	return nil
}

// sharedEnvelope preserves source timestamps independently of local installation and codec format.
type sharedEnvelope struct {
	// Format prevents decoding another schema as the same query result.
	Format string
	// FetchedAt records successful source completion using the initiating client's clock.
	FetchedAt time.Time
	// ExpiresAt caps freshness and retention without restarting on another client's hydration.
	ExpiresAt time.Time
	// Data owns the codec's encoded value, including valid nils and zero values.
	Data []byte
}

// sharedResult carries source freshness through erased loaders without exposing it as user data.
type sharedResult struct {
	// value is the decoded or freshly loaded query value.
	value any
	// fetchedAt preserves the source completion time for per-consumer freshness decisions.
	fetchedAt time.Time
	// expiresAt is the absolute shared freshness cap.
	expiresAt time.Time
}

// sharedQuery is a typed callback erased at the query definition boundary.
type sharedQuery struct {
	// load runs storage and the original fetcher outside Client.mu.
	load func(context.Context, bool) (sharedResult, error)
}

// request captures the original fetcher and consumer policy for one admitted query operation.
func (s *SharedCache[V]) request(fetch Fetcher[V], options Options) *sharedQuery {
	return &sharedQuery{load: func(ctx context.Context, bypass bool) (sharedResult, error) {
		return s.load(ctx, fetch, options, bypass)
	}}
}

// read captures a generation even when bypassing data, allowing the later write to be fenced.
func (s *SharedCache[V]) read(ctx context.Context) (SharedRecord, error) {
	operation, cancel := context.WithTimeout(ctx, s.options.Timeout)
	defer cancel()
	record, err := s.store.Read(operation, s.options.Key)
	if err == nil {
		err = operation.Err()
	}
	if err == nil && record.Version == "" {
		err = errors.New("shared store returned an empty version")
	}
	return record, err
}

// load reuses a fresh record or executes the source and attempts a best-effort conditional write.
func (s *SharedCache[V]) load(
	ctx context.Context,
	fetch Fetcher[V],
	options Options,
	bypass bool,
) (sharedResult, error) {
	record, readErr := s.read(ctx)
	if err := ctx.Err(); err != nil {
		return sharedResult{}, err
	}
	if readErr != nil {
		s.count(func(stats *SharedStats) { stats.ReadErrors++ })
	}
	if bypass {
		s.count(func(stats *SharedStats) { stats.Bypasses++ })
	} else if readErr == nil {
		if result, ok := s.decode(record.Data, options); ok {
			s.count(func(stats *SharedStats) { stats.Hits++ })
			return result, nil
		}
		s.count(func(stats *SharedStats) { stats.Misses++ })
	}
	value, err := fetch(ctx)
	if err != nil {
		return sharedResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return sharedResult{}, err
	}
	fetchedAt := options.Clock()
	result := sharedResult{value: value, fetchedAt: fetchedAt, expiresAt: fetchedAt.Add(s.options.TTL)}
	if readErr == nil {
		s.write(ctx, record.Version, value, result, options.Clock)
	}
	return result, nil
}

// decode rejects expired or incompatible records before restoring the caller's static result type.
func (s *SharedCache[V]) decode(data []byte, options Options) (sharedResult, bool) {
	if len(data) == 0 {
		return sharedResult{}, false
	}
	var envelope sharedEnvelope
	err := json.Unmarshal(data, &envelope)
	validTimes := envelope.ExpiresAt.After(envelope.FetchedAt)
	if err != nil || envelope.Format != s.options.Format || !validTimes {
		s.count(func(stats *SharedStats) { stats.DecodeErrors++ })
		return sharedResult{}, false
	}
	now := options.Clock()
	deadline := envelope.FetchedAt.Add(options.StaleTime)
	if deadline.After(envelope.ExpiresAt) {
		deadline = envelope.ExpiresAt
	}
	// A clock ahead of this client's clock cannot establish a trustworthy age for the shared value.
	if envelope.FetchedAt.After(now) || !now.Before(deadline) {
		return sharedResult{}, false
	}
	value, err := s.options.Codec.Decode(envelope.Data)
	if err != nil {
		s.count(func(stats *SharedStats) { stats.DecodeErrors++ })
		return sharedResult{}, false
	}
	return sharedResult{value: value, fetchedAt: envelope.FetchedAt, expiresAt: envelope.ExpiresAt}, true
}

// write preserves the original deadline and reports storage failures separately from source outcomes.
func (s *SharedCache[V]) write(
	ctx context.Context,
	version string,
	value V,
	result sharedResult,
	clock func() time.Time,
) {
	data, err := s.options.Codec.Encode(value)
	if err != nil {
		s.count(func(stats *SharedStats) { stats.EncodeErrors++ })
		return
	}
	data, err = json.Marshal(sharedEnvelope{
		Format: s.options.Format, FetchedAt: result.fetchedAt, ExpiresAt: result.expiresAt, Data: data,
	})
	if err != nil {
		s.count(func(stats *SharedStats) { stats.EncodeErrors++ })
		return
	}
	ttl := result.expiresAt.Sub(clock())
	if ttl <= 0 || ctx.Err() != nil {
		return
	}
	operation, cancel := context.WithTimeout(ctx, s.options.Timeout)
	defer cancel()
	written, err := s.store.Write(operation, s.options.Key, SharedWrite{Version: version, Data: data, TTL: ttl})
	if err == nil {
		err = operation.Err()
	}
	s.count(func(stats *SharedStats) {
		switch {
		case err != nil:
			stats.WriteErrors++
		case written:
			stats.Writes++
		default:
			stats.WriteConflicts++
		}
	})
}

// count changes cumulative counters under their own lock, outside the client state machine.
func (s *SharedCache[V]) count(update func(*SharedStats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	update(&s.stats)
}
