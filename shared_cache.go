package soroi

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
	// ShareTags registers Definition.Tags remotely; the store must implement SharedTagStore.
	// False keeps query labels local while still honoring any tags already attached to the shared record.
	ShareTags bool
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
	// LastError retains the latest counted failure with its operation, shared key, and expected format.
	// Wrapping preserves errors.Is and errors.As; successful operations do not clear it.
	// Nil means no failure has been counted. Concurrent failures are ordered by statistics recording.
	// Treat retained errors as immutable; snapshots do not clone underlying errors.
	LastError error
}

// SharedCache owns immutable typed storage configuration and independently synchronized counters.
// Attach it to Definition.Shared; it never owns the store's connection lifetime.
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
	if options.ShareTags {
		if tagged, ok := store.(SharedTagStore); !ok || !tagged.TagsEnabled() {
			return nil, ErrSharedCapability
		}
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
// It does not change any Client's local state; use Definition.InvalidateShared to update one client too.
// Other clients retain local results until normal freshness checks or explicit local invalidation.
func (s *SharedCache[V]) Invalidate(ctx context.Context) error {
	_, err := s.invalidate(ctx)
	return err
}

// invalidate retains a committed receipt when the store can publish notifications.
func (s *SharedCache[V]) invalidate(ctx context.Context) (SharedInvalidation, error) {
	if err := ctx.Err(); err != nil {
		return SharedInvalidation{}, err
	}
	operation, cancel := context.WithTimeout(ctx, s.options.Timeout)
	defer cancel()
	var event SharedInvalidation
	var err error
	if store, ok := s.store.(SharedInvalidationStore); ok {
		event, err = store.InvalidateKey(operation, s.options.Key)
	} else {
		err = s.store.Invalidate(operation, s.options.Key)
	}
	if err == nil {
		err = operation.Err()
	}
	if err != nil {
		return SharedInvalidation{}, s.recordError("invalidate", err)
	}
	s.count(func(stats *SharedStats) { stats.Invalidations++ })
	return event, nil
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
	// tags preserves the remote membership accompanying the result's invalidation snapshot.
	tags []string
	// invalidated marks a locally usable result stale when remote conditional persistence rejects its snapshot.
	invalidated bool
}

// sharedQuery is a typed callback erased at the query definition boundary.
type sharedQuery struct {
	// scope identifies the optional process-local notification source.
	scope string
	// key identifies the remote result independently of its local Go key.
	key string
	// tags copies declared labels only when remote registration was requested.
	tags []string
	// load runs storage and the original fetcher outside Client.mu.
	load func(context.Context, bool, func([]string)) (sharedResult, error)
}

// request captures the original fetcher and consumer policy for one admitted query operation.
func (s *SharedCache[V]) request(fetch Fetcher[V], options Options, tags []string) *sharedQuery {
	request := &sharedQuery{key: s.options.Key, tags: []string{}}
	if store, ok := s.store.(SharedInvalidationStore); ok {
		request.scope = store.InvalidationScope()
	}
	if s.options.ShareTags {
		request.tags = append(request.tags, tags...)
	}
	request.load = func(ctx context.Context, bypass bool, register func([]string)) (sharedResult, error) {
		return s.load(ctx, fetch, options, bypass, request.tags, register)
	}
	return request
}

// read captures a generation even when bypassing data, allowing the later write to be fenced.
func (s *SharedCache[V]) read(ctx context.Context, tags []string) (SharedRecord, error) {
	operation, cancel := context.WithTimeout(ctx, s.options.Timeout)
	defer cancel()
	var record SharedRecord
	var err error
	if s.options.ShareTags {
		record, err = s.store.(SharedTagStore).ReadTagged(operation, s.options.Key, tags)
	} else {
		record, err = s.store.Read(operation, s.options.Key)
	}
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
	tags []string,
	register func([]string),
) (sharedResult, error) {
	record, readErr := s.read(ctx, tags)
	if readErr == nil && len(record.Tags) > 0 {
		register(record.Tags)
		// A tag notification can arrive before its previously unknown relationship is installed locally.
		// Confirm the snapshot after registration so that gap cannot restore an invalidated shared value.
		confirmed, err := s.read(ctx, tags)
		if err != nil {
			readErr = err
		} else {
			register(confirmed.Tags)
			if confirmed.Version != record.Version {
				confirmed.Data = nil
			}
			record = confirmed
		}
	}
	if err := ctx.Err(); err != nil {
		return sharedResult{}, err
	}
	if readErr != nil {
		_ = s.recordError("read", readErr)
	}
	if bypass {
		s.count(func(stats *SharedStats) { stats.Bypasses++ })
	} else if readErr == nil {
		if result, ok := s.decode(record.Data, options); ok {
			result.tags = append([]string{}, record.Tags...)
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
	result := sharedResult{
		value:     value,
		fetchedAt: fetchedAt,
		expiresAt: fetchedAt.Add(s.options.TTL),
		tags:      append([]string{}, record.Tags...),
	}
	if readErr == nil {
		result.invalidated = s.write(ctx, record.Version, value, result, options.Clock)
	}
	return result, nil
}

// decode rejects expired or incompatible records before restoring the caller's static result type.
func (s *SharedCache[V]) decode(data []byte, options Options) (sharedResult, bool) {
	if len(data) == 0 {
		return sharedResult{}, false
	}
	var envelope sharedEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		_ = s.recordError("decode", fmt.Errorf("decode envelope: %w", err))
		return sharedResult{}, false
	}
	if envelope.Format != s.options.Format {
		_ = s.recordError("decode", fmt.Errorf("incompatible record format %q", envelope.Format))
		return sharedResult{}, false
	}
	if !envelope.ExpiresAt.After(envelope.FetchedAt) {
		_ = s.recordError("decode", errors.New("invalid envelope timestamps: ExpiresAt must follow FetchedAt"))
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
		_ = s.recordError("decode", fmt.Errorf("decode value: %w", err))
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
) bool {
	data, err := s.options.Codec.Encode(value)
	if err != nil {
		_ = s.recordError("encode", fmt.Errorf("encode value: %w", err))
		return false
	}
	data, err = json.Marshal(sharedEnvelope{
		Format: s.options.Format, FetchedAt: result.fetchedAt, ExpiresAt: result.expiresAt, Data: data,
	})
	if err != nil {
		_ = s.recordError("encode", fmt.Errorf("encode envelope: %w", err))
		return false
	}
	ttl := result.expiresAt.Sub(clock())
	if ttl <= 0 || ctx.Err() != nil {
		return false
	}
	operation, cancel := context.WithTimeout(ctx, s.options.Timeout)
	defer cancel()
	written, err := s.store.Write(operation, s.options.Key, SharedWrite{Version: version, Data: data, TTL: ttl})
	if err == nil {
		err = operation.Err()
	}
	if err != nil {
		_ = s.recordError("write", err)
		return false
	}
	s.count(func(stats *SharedStats) {
		if written {
			stats.Writes++
		} else {
			stats.WriteConflicts++
		}
	})
	return !written
}

// recordError atomically retains one failure and its counter without changing source fallback policy.
func (s *SharedCache[V]) recordError(operation string, cause error) error {
	// Best-effort cache failures must remain inspectable even when the source query succeeds.
	err := fmt.Errorf("%s shared key %q (format %q): %w", operation, s.options.Key, s.options.Format, cause)
	s.count(func(stats *SharedStats) {
		switch operation {
		case "read":
			stats.ReadErrors++
		case "decode":
			stats.DecodeErrors++
		case "encode":
			stats.EncodeErrors++
		case "write":
			stats.WriteErrors++
		case "invalidate":
			stats.InvalidationErrors++
		}
		stats.LastError = err
	})
	return err
}

// count changes cumulative counters under their own lock, outside the client state machine.
func (s *SharedCache[V]) count(update func(*SharedStats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	update(&s.stats)
}
