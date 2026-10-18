package cacheq

import (
	"context"
	"reflect"
	"slices"
)

// Definition describes a typed query without owning cached data or a subscription.
// Constructing a definition performs no loading. Clients own execution, caching, and lifetimes.
// Definitions are copied when used; captured loader state remains application-owned.
type Definition[V any] struct {
	// Key identifies the query and must include all parameters and identity scope affecting the result.
	// The same comparable-key and static-result-type rules as Query and Fetch apply.
	Key any
	// Fetcher loads the result using application-owned HTTP, database, or other dependencies.
	Fetcher Fetcher[V]
	// Shared optionally reuses serialized results across processes; nil keeps all caching local.
	// The shared key and format must consistently identify this result across every participating client.
	Shared *SharedCache[V]
	// Tags declares exact string labels to merge into this key when an operation is admitted.
	// Labels are copied, remain while the Client retains this query, and do not affect cache identity.
	Tags []string
	// Options supplies the shared loading policy; Disable only affects Query's automatic loading.
	// Pointer values are read when an operation starts, not when the definition is constructed.
	Options QueryOptions
}

// QueryDefinition creates a reusable typed query, inferring its result type from fetch.
// Construction performs no loading; extra configurations return a zero definition and ErrTooManyOptions immediately.
// Keys and result types are validated when an operation uses a Client.
// Omitted options use Client defaults; nil fields inherit their corresponding defaults at execution.
// Set Shared and Tags on the returned definition before use to configure shared caching and grouping.
func QueryDefinition[V any](key any, fetch Fetcher[V], options ...QueryOptions) (Definition[V], error) {
	option, err := singleOptions(options)
	if err != nil {
		return Definition[V]{}, err
	}
	return Definition[V]{Key: key, Fetcher: fetch, Options: option}, nil
}

// Fetch waits for this definition's result using the supplied Client and caller context.
// Disable does not suppress this explicit read; validation and errors follow Fetch's contract.
func (d Definition[V]) Fetch(ctx context.Context, client *Client) (V, error) {
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
func (d Definition[V]) Query(client *Client) *QueryHandle[V] {
	return query(client, d, nil)
}

// Prefetch warms this definition's cached data and returns any loading error.
// Disable does not suppress this explicit read; it creates no persistent subscription.
func (d Definition[V]) Prefetch(ctx context.Context, client *Client) error {
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
func (d Definition[V]) request(defaults Options, trigger EventTrigger) queryRequest {
	request := eraseFetcher(d.Fetcher, resolvePolicy(defaults, d.Options))
	request.trigger = trigger
	request.tags = slices.Clone(d.Tags)
	if d.Shared != nil && d.Fetcher != nil {
		request.shared = d.Shared.request(d.Fetcher, request.options, request.tags)
	}
	return request
}

// InvalidateShared invalidates this definition's remote key before marking one client's local query stale.
// A remote failure is returned without changing local state; remote success is not rolled back on local failure.
// Other clients receive notifications only through WatchShared; duplicate operation receipts are suppressed.
// Local refresh and same-key in-flight rules match Client.Invalidate.
// Omitted options use zero values; extras return ErrTooManyOptions without remote or local changes.
// Applications must commit their business write before calling this method.
func (d Definition[V]) InvalidateShared(
	ctx context.Context,
	client *Client,
	options ...InvalidateOptions,
) error {
	option, err := singleOptions(options)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.Shared == nil {
		return ErrInvalidSharedCache
	}
	client.mu.Lock()
	err = client.checkLocked(d.Key, reflect.TypeFor[V]())
	client.mu.Unlock()
	if err != nil {
		return err
	}
	event, err := d.Shared.invalidate(ctx)
	if err != nil {
		return err
	}
	return client.applySharedKey(d.Key, event, option)
}
