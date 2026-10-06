package cacheq

import "time"

// Stats is a per-client snapshot of cumulative activity and the current retained cache size.
// Counters survive Clear and Close; already-started loads may finish updating them after Close.
type Stats struct {
	// CacheHits counts non-forced Fetch calls and enabled consumer decisions that reuse fresh data.
	CacheHits uint64
	// CacheMisses counts those decisions without fresh data, including calls returning ErrNoFetcher.
	CacheMisses uint64
	// MergedRequests counts callers and enabled consumers joining existing loads, once per call or decision.
	MergedRequests uint64
	// Loads counts started shared operations, including manual refreshes and invalidation refreshes.
	Loads uint64
	// LoadSuccesses counts shared operations completed without an error, even if their result was detached.
	LoadSuccesses uint64
	// LoadFailures counts failed shared operations excluding cancellation and deadline errors.
	LoadFailures uint64
	// LoadCancellations counts shared operations ending with cancellation or deadline errors.
	LoadCancellations uint64
	// Retries counts actual loader attempts after the first, excluding canceled backoff waits.
	Retries uint64
	// TotalLoadDuration sums elapsed shared-operation time, including retries, backoff, and cancellation.
	TotalLoadDuration time.Duration
	// CacheEntries counts currently retained results or errors, excluding empty type bindings.
	// Over-age values not yet discovered by cache operations remain counted until cleanup.
	CacheEntries int
	// LRUEvictions counts retained results or errors discarded to enforce MaxEntries.
	LRUEvictions uint64
	// GCCollections counts keys removed by inactive-cache timers, excluding manual removal or clearing.
	GCCollections uint64
	// AgeExpirations counts values dropped after reaching MaxAge, once per installed value.
	AgeExpirations uint64
}

// Stats returns a coherent copy without reading query data, changing recency, or running cleanup.
// Get, Snapshot, disabled queries, and forced refreshes do not contribute cache hit or miss decisions.
// Loads still in progress contribute to Loads but not completed outcomes or TotalLoadDuration.
func (c *Client) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.stats
	for _, cached := range c.entries {
		if cached.hasData || cached.err != nil {
			state.CacheEntries++
		}
	}
	return state
}

// recordLookupLocked records one fresh-cache decision without counting later retries of the same caller.
func (c *Client) recordLookupLocked(fresh bool) {
	if fresh {
		c.stats.CacheHits++
		return
	}
	c.stats.CacheMisses++
}
