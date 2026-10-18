package soroi

import "fmt"

// QueryFilter selects existing cached, loading, or subscribed keys for Client operations.
// Keys, Tags, and Predicate are combined with AND. The zero value selects all existing queries.
// Selection does not create missing keys, change loading policy, or own query lifetimes.
type QueryFilter struct {
	// Keys limits selection to exact comparable keys, preserving their first-occurrence order.
	// Nil imposes no key restriction; a non-nil empty slice selects nothing.
	Keys []any
	// Tags requires at least one matching registered label, using exact string equality.
	// Nil imposes no tag restriction; a non-nil empty slice selects nothing.
	Tags []string
	// Predicate further restricts keys and runs once per candidate outside the Client lock.
	// Nil accepts every candidate. The caller owns synchronization of captured mutable state.
	Predicate func(key any) bool
}

// querySelection owns a validated, deduplicated snapshot for one Client operation.
type querySelection struct {
	// keys contains candidates in explicit key order or unspecified snapshot order.
	keys []any
	// predicate performs caller-owned matching before any operation-owned changes.
	predicate func(any) bool
	// existingOnly skips keys removed during matching and prevents creating missing filter targets.
	existingOnly bool
}

// withQueryTargets runs user matching outside mu and applies the operation under one lock.
// The operation callback must not call public Client methods or otherwise release mu.
func (c *Client) withQueryTargets(target any, apply func(any)) error {
	c.mu.Lock()
	selection, err := c.querySelectionLocked(target)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if selection.predicate != nil {
		c.mu.Unlock()
		matched := selection.keys[:0]
		for _, key := range selection.keys {
			if selection.predicate(key) {
				matched = append(matched, key)
			}
		}
		selection.keys = matched
		c.mu.Lock()
	}
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	for _, key := range selection.keys {
		if selection.existingOnly && !c.hasQueryLocked(key) {
			continue
		}
		apply(key)
	}
	return nil
}

// querySelectionLocked normalizes target forms and validates all keys before user matching or mutation.
func (c *Client) querySelectionLocked(target any) (querySelection, error) {
	if c.closed {
		return querySelection{}, ErrClosed
	}
	var selection querySelection
	var tags []string
	switch selected := target.(type) {
	case QueryFilter:
		tags = selected.Tags
		selection = querySelection{keys: selected.Keys, predicate: selected.Predicate, existingOnly: true}
		if selected.Keys == nil {
			selection.keys = c.queryKeysLocked()
		}
	case []any:
		selection.keys = selected
	case func(any) bool:
		if selected == nil {
			return querySelection{}, fmt.Errorf("%w: nil query predicate", ErrInvalidKey)
		}
		selection = querySelection{keys: c.queryKeysLocked(), predicate: selected, existingOnly: true}
	default:
		selection.keys = []any{target}
	}
	keys := make([]any, 0, len(selection.keys))
	seen := make(map[any]struct{}, len(selection.keys))
	for _, key := range selection.keys {
		if err := c.checkLocked(key, nil); err != nil {
			return querySelection{}, err
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		if selection.existingOnly && !c.hasQueryLocked(key) {
			continue
		}
		if tags != nil && !matchesQueryTags(c.entries[key].tags, tags) {
			continue
		}
		keys = append(keys, key)
	}
	selection.keys = keys
	return selection, nil
}

// queryKeysLocked snapshots distinct keys owned by cache state, a load, or a subscription.
func (c *Client) queryKeysLocked() []any {
	keys := make([]any, 0, len(c.entries)+len(c.pending)+len(c.observers))
	for key := range c.entries {
		keys = append(keys, key)
	}
	for key := range c.pending {
		if _, cached := c.entries[key]; !cached {
			keys = append(keys, key)
		}
	}
	for key := range c.observers {
		_, cached := c.entries[key]
		if !cached && c.pending[key] == nil {
			keys = append(keys, key)
		}
	}
	return keys
}

// hasQueryLocked reports whether a key belongs to this Client without reading or touching its data.
func (c *Client) hasQueryLocked(key any) bool {
	_, cached := c.entries[key]
	return cached || c.pending[key] != nil || len(c.observers[key]) > 0
}
