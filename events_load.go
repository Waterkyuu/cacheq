package cacheq

// loadReason describes an already-stale decision without re-evaluating a consumer's clock or policy.
func loadReason(cached entry) EventReason {
	if cached.staleReason != ReasonNone {
		return cached.staleReason
	}
	if !cached.hasData {
		return ReasonMissing
	}
	return ReasonExpired
}

// event copies operation metadata while the Client lock protects mutable diagnostic counters.
func (pending *flight) event(key any, kind EventKind) Event {
	return Event{
		Key: key, Kind: kind, LoadID: pending.id,
		Trigger: pending.request.trigger, Reason: pending.reason,
		Joined: pending.joined, Attempts: pending.attempts,
	}
}

// joinLoadLocked records a join to one operation independently of aggregate Stats counting.
func (c *Client) joinLoadLocked(key any, pending *flight, request queryRequest) {
	pending.joined++
	event := pending.event(key, EventLoadJoined)
	event.Trigger, event.Reason = request.trigger, loadReason(c.entries[key])
	c.emitEventLocked(event)
}
