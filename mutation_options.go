package cacheq

import (
	"context"
	"errors"
)

// ErrNoMutator indicates a mutation handle constructed without a write function.
var ErrNoMutator = errors.New("mutation has no write function")

// ErrMutationClosed indicates an execution attempted through a released mutation handle.
var ErrMutationClosed = errors.New("mutation handle is closed")

// ErrMutationBusy indicates another execution, including cache synchronization, owns this handle.
var ErrMutationBusy = errors.New("mutation is already running")

// Mutator performs one business write. It must honor ctx and return success only after committing.
// Cancellation does not prove that a remote write was rolled back. Writes are never retried or deduplicated.
type Mutator[I, V any] func(context.Context, I) (V, error)

// MutationOptions declares cache synchronization after a successful write.
// Callbacks execute once outside internal locks, including when cancellation races with write success.
// Captured state and input/result references remain application-owned and require synchronization.
type MutationOptions[I, V any] struct {
	// OnSuccess installs or transforms local data after a committed write; nil skips this step.
	// Its error is reported separately from the write and does not prevent associated invalidation.
	OnSuccess func(I, V) error
	// Invalidate selects associated queries using Client.Invalidate's key, []any, or predicate contract.
	// Nil callbacks or nil returned targets skip invalidation. Include authorization scope in selected keys.
	Invalidate func(I, V) any
	// InvalidateOptions selects whether associated observed queries refresh in the background.
	// It does not wait for refresh completion or change in-flight invalidation semantics.
	InvalidateOptions InvalidateOptions
	// Executor optionally overrides the Client budget for the business write; nil inherits it.
	// Capacity is released before OnSuccess and invalidation, which may start their own loads.
	Executor *Executor
}

// MutationResult separates a committed business result from failures in subsequent cache synchronization.
type MutationResult[V any] struct {
	// Data is the business write's return value and remains available when SyncErr is non-nil.
	Data V
	// SyncErr combines local synchronization and invalidation errors after write success.
	// Repair cache state instead of repeating a committed write solely because this error is non-nil.
	SyncErr error
}

// MutationSnapshot describes the latest execution on one mutation handle.
// Data is immutable and shared with the execution result; Pending clears the previous outcome.
type MutationSnapshot[V any] struct {
	// Data holds the latest successful business write result.
	Data V
	// HasData distinguishes successful zero-valued results from missing or failed results.
	HasData bool
	// Status is Idle, Pending, Success, or Error; synchronization failure still leaves Success.
	Status Status
	// Err reports business execution failure, not post-write cache synchronization failure.
	Err error
	// SyncErr reports cache synchronization failure after a successful business write.
	SyncErr error
}
