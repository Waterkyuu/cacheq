package soroi

import (
	"context"
	"errors"
	"time"
)

// ErrInvalidBatchOptions indicates a negative collection window or a non-positive batch size.
var ErrInvalidBatchOptions = errors.New("invalid batch options")

// ErrBatchResultMissing indicates that a successful batch omitted a requested key.
// Applications should return an explicit per-key error for a missing business object.
var ErrBatchResultMissing = errors.New("batch result missing")

// ErrBatcherClosed indicates an operation attempted after the batcher was explicitly closed.
// Client loads treat this error as terminal, even when wrapped or RetryIf permits retries.
var ErrBatcherClosed = errors.New("batcher is closed")

// BatchOptions controls the collection window and the number of unique keys per callback.
type BatchOptions struct {
	// Wait starts with the first queued key; zero adds no intentional collection delay.
	// LoadMany dispatches queued work immediately instead of waiting for this window.
	Wait time.Duration
	// MaxBatchSize is the positive limit of unique keys per callback.
	// Reaching it dispatches immediately, without waiting for the collection window.
	MaxBatchSize int
	// Executor optionally bounds active and queued bulk callbacks, not individual keys.
	// Nil leaves batch execution unrestricted. Share a budget across batchers for one backend.
	Executor *Executor
}

// BatchResult represents per-key data and an optional error from a callback or LoadMany.
type BatchResult[V any] struct {
	// Data contains this key's result, including valid zero values or fallback data.
	Data V
	// Err reports this key's failure, including callback, admission, lifecycle, and cancellation errors.
	Err error
}

// BatchFunc loads unique keys and returns results matched by key rather than by position.
// A non-nil outer error fails every key and takes precedence over any returned map.
// Missing map entries become ErrBatchResultMissing; extra entries are ignored.
// Callbacks may run concurrently and must honor ctx, which derives from NewBatcher's lifetime.
// A batcher does not inspect whether the callback actually uses a bulk backend operation.
type BatchFunc[K comparable, V any] func(context.Context, []K) (map[K]BatchResult[V], error)
