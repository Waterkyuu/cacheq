package cacheq

import "time"

// limitExpiry prevents a freshness deadline from extending beyond the configured availability limit.
func (c *Client) limitExpiry(updatedAt, expiresAt time.Time) time.Time {
	if c.options.MaxAge > 0 {
		deadline := updatedAt.Add(c.options.MaxAge)
		if expiresAt.After(deadline) {
			return deadline
		}
	}
	return expiresAt
}

// expireDataLocked drops an over-age value while preserving its result type, last error, and active load.
// Availability is checked on cache operations rather than by starting another timer for each key.
func (c *Client) expireDataLocked(key any) bool {
	cached := c.entries[key]
	if !cached.hasData || c.options.MaxAge <= 0 {
		return false
	}
	if c.options.Clock().Before(cached.updatedAt.Add(c.options.MaxAge)) {
		return false
	}
	// A failed refresh changes the error and freshness deadline, but not UpdatedAt.
	// Checking the installation time prevents retries or repeated reads from extending old data's life.
	cached.value, cached.hasData, cached.expiresAt = nil, false, time.Time{}
	c.entries[key] = cached
	if cached.err == nil {
		c.forgetCapacityLocked(key)
	}
	return true
}
