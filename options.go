package cacheq

import (
	"context"
	"errors"
	"time"
)

// ErrClosed indicates that the client has released its resources and cannot start more queries.
var ErrClosed = errors.New("query client is closed")

// ErrQueryClosed indicates a request attempted through a released query handle.
var ErrQueryClosed = errors.New("query handle is closed")

// ErrTypeMismatch indicates a key was reused with a different static result type.
var ErrTypeMismatch = errors.New("query result type mismatch")

// ErrInvalidKey indicates a nil or non-comparable key, or a nil invalidation predicate.
var ErrInvalidKey = errors.New("query key must be non-nil and comparable")

// ErrNoFetcher indicates an operation requiring data has no loader to execute.
var ErrNoFetcher = errors.New("query has no fetcher")

// Status describes the most recent result independently of background fetching.
type Status uint8

const (
	// Idle indicates that the key has no completed result and no active load.
	Idle Status = iota
	// Pending indicates an initial load without previously available data.
	Pending
	// Success indicates that usable data was returned without an error.
	Success
	// Error indicates that the most recent load failed; earlier data may still be available.
	Error
)

// Options configures freshness and request policy for one shared client.
type Options struct {
	// StaleTime controls freshness for ordinary queries; zero makes completed data immediately stale.
	StaleTime time.Duration
	// GCTime removes cached state after this duration without reads, writes, loads, or subscriptions.
	// Zero or a negative duration disables automatic deletion; freshness is controlled by StaleTime.
	GCTime time.Duration
	// MaxEntries limits retained results and errors using least-recently-used eviction.
	// Eviction preserves subscriptions, active loads, and their type bindings; non-positive values disable it.
	MaxEntries int
	// Retry limits additional attempts after the initial load; zero disables retries.
	Retry int
	// RetryDelay supplies the delay before each additional attempt, numbered from one.
	// Nil uses exponential backoff capped at thirty seconds.
	RetryDelay func(attempt int) time.Duration
	// Timeout bounds the entire load, including retries; zero uses the caller's deadline.
	Timeout time.Duration
	// Clock supplies freshness and retention checks; nil uses time.Now.
	Clock func() time.Time
}

// QueryOptions controls automatic loading for one query handle without disabling other consumers.
// Query defaults to enabled when this optional configuration is omitted.
type QueryOptions struct {
	// Enabled allows initial loading and invalidation refreshes; false keeps the handle passive.
	Enabled bool
}

// RefetchMode controls whether invalidation starts background work for subscribed queries.
type RefetchMode uint8

const (
	// RefetchObserved refreshes queries with an enabled observer and loader when no load is active.
	RefetchObserved RefetchMode = iota
	// RefetchNone marks queries stale without initiating a new background load.
	RefetchNone
)

// InvalidateOptions configures refresh behavior when a group of related queries becomes stale.
type InvalidateOptions struct {
	// Refetch selects background refresh behavior; the zero value uses RefetchObserved.
	Refetch RefetchMode
}

// Snapshot exposes a query's data, freshness, request activity, and latest error.
// Data is shared and must be treated as immutable by all readers.
type Snapshot[V any] struct {
	// Data contains the current result or retained data from an earlier successful load.
	Data V
	// HasData distinguishes an available zero value from a query with no result.
	HasData bool
	// Status describes the last result or an initial pending load.
	Status Status
	// Err reports the last load error without discarding previously available data.
	Err error
	// Stale is true when data is missing, expired, or manually invalidated.
	Stale bool
	// Fetching reports an active load, including refreshes while Data remains available.
	Fetching bool
	// UpdatedAt records when Data was last installed in the cache.
	UpdatedAt time.Time
	// ExpiresAt is the absolute freshness deadline, or zero for invalidated data.
	ExpiresAt time.Time
}

// Loader returns data, an absolute expiration, and an optional error.
// A future expiration on an error explicitly caches the fallback data and error until that deadline.
type Loader[V any] func(context.Context) (V, time.Time, error)

// Fetcher returns remote data without requiring the caller to calculate an expiration.
type Fetcher[V any] func(context.Context) (V, error)

// retryDelay implements bounded exponential backoff for clients without a custom retry schedule.
func retryDelay(attempt int) time.Duration {
	if attempt >= 6 {
		return 30 * time.Second
	}
	return time.Second << (attempt - 1)
}
