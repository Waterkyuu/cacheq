package cacheq

import (
	"context"
	"reflect"
	"slices"
)

// QueryDefinition describes a typed query without owning cached data or a subscription.
// Constructing a definition performs no loading. Clients own execution, caching, and lifetimes.
// Definitions are copied when used; captured loader state remains application-owned.
type QueryDefinition[V any] struct {
	// Key identifies the query and must include all parameters and identity scope affecting the result.
	// The same comparable-key and static-result-type rules as Query and Fetch apply.
	Key any
	// Fetcher loads the result using application-owned HTTP, database, or other dependencies.
	Fetcher Fetcher[V]
	// Tags declares exact string labels to merge into this key when an operation is admitted.
	// Labels are copied, remain while the Client retains this query, and do not affect cache identity.
	Tags []string
	// Options supplies the shared loading policy; Disable only affects Query's automatic loading.
	// Pointer values are read when an operation starts, not when the definition is constructed.
	Options QueryOptions
}

// Fetch waits for this definition's result using the supplied Client and caller context.
// Disable does not suppress this explicit read; validation and errors follow Fetch's contract.
func (d QueryDefinition[V]) Fetch(ctx context.Context, client *Client) (V, error) {
	value, err := client.fetch(
		ctx,
		d.Key,
		reflect.TypeFor[V](),
		d.request(client.options, TriggerFetch),
		false,
		nil,
	)
	return typedValue[V](value), err
}

// Query creates a subscription using this definition's key, loader, and copied options.
// The caller owns the returned handle's Close; Disable controls automatic loading.
func (d QueryDefinition[V]) Query(client *Client) *QueryHandle[V] {
	return query(client, d)
}

// Prefetch warms this definition's cached data and returns any loading error.
// Disable does not suppress this explicit read; it creates no persistent subscription.
func (d QueryDefinition[V]) Prefetch(ctx context.Context, client *Client) error {
	_, err := client.fetch(
		ctx,
		d.Key,
		reflect.TypeFor[V](),
		d.request(client.options, TriggerPrefetch),
		false,
		nil,
	)
	return err
}

// request captures the definition's policy and labels for one read or subscription.
func (d QueryDefinition[V]) request(defaults Options, trigger EventTrigger) queryRequest {
	request := eraseFetcher(d.Fetcher, resolvePolicy(defaults, d.Options))
	request.trigger = trigger
	request.tags = slices.Clone(d.Tags)
	return request
}
