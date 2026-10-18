package cacheq

import (
	"context"
	"time"
)

// SharedStore persists encoded query results and fences writes across explicit invalidations.
// Implementations must be safe for concurrent use and honor context cancellation.
// Read must atomically obtain the current version and data, initializing a nonempty version on a miss.
// Versions must never be reused; recreated metadata must discard orphaned data and receive a new version.
// Write compares the version and installs data with its TTL atomically; a conflict returns false, nil.
// Invalidate atomically changes the version and removes data, including for previously unseen keys.
// Store ownership remains with the application; Client.Close never closes a supplied store.
type SharedStore interface {
	// Read atomically snapshots a generation and its payload, initializing the generation on a miss.
	Read(context.Context, string) (SharedRecord, error)
	// Write installs a payload with its remaining TTL only if the generation still matches.
	Write(context.Context, string, SharedWrite) (bool, error)
	// Invalidate atomically replaces the generation and removes its payload.
	Invalidate(context.Context, string) error
}

// SharedRecord is an owned snapshot of a storage key's invalidation version and encoded result.
type SharedRecord struct {
	// Version identifies the invalidation generation, including when Data is empty.
	Version string
	// Data owns the serialized record; an empty slice denotes a cache miss.
	Data []byte
	// Tags owns the remote tag membership captured by the read; nil denotes an untagged record.
	Tags []string
}

// SharedWrite conditionally installs one encoded result without changing its invalidation version.
type SharedWrite struct {
	// Version must match the version obtained before loading the source.
	Version string
	// Data contains an owned encoded record; implementations must not retain a mutable caller slice.
	Data []byte
	// TTL is the positive remaining record lifetime, not a renewed freshness window.
	TTL time.Duration
}
