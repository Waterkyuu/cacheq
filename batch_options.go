package cacheq

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
var ErrBatcherClosed = errors.New("batcher is closed")

// BatchOptions controls the collection window and the number of unique keys per callback.
type BatchOptions struct {
	// Wait starts with the first queued key; zero adds no intentional collection delay.
	Wait time.Duration
	// MaxBatchSize is the positive limit of unique keys per callback.
	// Reaching it dispatches immediately, without waiting for the collection window.
	MaxBatchSize int
}

// BatchResult represents a value and an optional application error for one requested key.
type BatchResult[V any] struct {
	// Data contains this key's result, including valid zero values or fallback data.
	Data V
	// Err reports a failure specific to this key, independently of other keys in the batch.
	Err error
}

// BatchFunc loads unique keys and returns results matched by key rather than by position.
// A non-nil outer error fails every key and takes precedence over any returned map.
// Missing map entries become ErrBatchResultMissing; extra entries are ignored.
// Callbacks may run concurrently and must honor ctx, which derives from NewBatcher's lifetime.
// A batcher does not inspect whether the callback actually uses a bulk backend operation.
type BatchFunc[K comparable, V any] func(context.Context, []K) (map[K]BatchResult[V], error)
