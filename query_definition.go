package cacheq

import "context"

// QueryDefinition describes a typed query without owning cached data or a subscription.
// Constructing a definition performs no loading. Clients own execution, caching, and lifetimes.
// Definitions are copied when used; captured loader state remains application-owned.
type QueryDefinition[V any] struct {
	// Key identifies the query and must include all parameters and identity scope affecting the result.
	// The same comparable-key and static-result-type rules as Query and Fetch apply.
	Key any
	// Fetcher loads the result using application-owned HTTP, database, or other dependencies.
	Fetcher Fetcher[V]
	// Options supplies the shared loading policy; Disable only affects Query's automatic loading.
	// Pointer values are read when an operation starts, not when the definition is constructed.
	Options QueryOptions
}

// Fetch waits for this definition's result using the supplied Client and caller context.
// Disable does not suppress this explicit read; validation and errors follow Fetch's contract.
func (d QueryDefinition[V]) Fetch(ctx context.Context, client *Client) (V, error) {
	return Fetch(ctx, client, d.Key, d.Fetcher, d.Options)
}

// Query creates a subscription using this definition's key, loader, and copied options.
// The caller owns the returned handle's Close; Disable controls automatic loading.
func (d QueryDefinition[V]) Query(client *Client) *QueryHandle[V] {
	return Query(client, d.Key, d.Fetcher, d.Options)
}

// Prefetch warms this definition's cached data and returns any loading error.
// Disable does not suppress this explicit read; it creates no persistent subscription.
func (d QueryDefinition[V]) Prefetch(ctx context.Context, client *Client) error {
	return Prefetch(ctx, client, d.Key, d.Fetcher, d.Options)
}
