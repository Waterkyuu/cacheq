package cacheq

// touchCapacityLocked records use of a completed result or error and enforces the configured capacity.
func (c *Client) touchCapacityLocked(key any) {
	if c.closed || c.options.MaxEntries <= 0 {
		return
	}
	cached := c.entries[key]
	if !cached.hasData && cached.err == nil {
		c.forgetCapacityLocked(key)
		return
	}
	if position := c.recencyEntries[key]; position != nil {
		c.recency.MoveToBack(position)
	} else {
		c.recencyEntries[key] = c.recency.PushBack(key)
	}
	for c.recency.Len() > c.options.MaxEntries {
		oldest := c.recency.Front().Value
		// Retain the type binding while a handle or load still owns this key.
		// Eviction clears only cached state and never cancels shared work or starts a refresh.
		c.discardLocked(oldest)
		c.stopGCLocked(oldest)
		c.notifyLocked(oldest)
	}
}

// forgetCapacityLocked releases a key's recency record when its retained state is discarded.
func (c *Client) forgetCapacityLocked(key any) {
	if position := c.recencyEntries[key]; position != nil {
		c.recency.Remove(position)
		delete(c.recencyEntries, key)
	}
}
