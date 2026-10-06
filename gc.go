package cacheq

import "time"

// gcTask identifies one inactive key's retention deadline and owns its scheduled cleanup.
type gcTask struct {
	// expiresAt is the inactivity deadline measured by the client's clock.
	expiresAt time.Time
	// stop releases the timer when the key is reused or the client closes.
	stop func()
}

// scheduleGC schedules one cleanup callback without creating a permanent background worker.
func scheduleGC(delay time.Duration, cleanup func()) func() {
	timer := time.AfterFunc(delay, cleanup)
	return func() { timer.Stop() }
}

// touchGCLocked restarts retention after use and suspends cleanup for subscribed or loading keys.
func (c *Client) touchGCLocked(key any) {
	c.stopGCLocked(key)
	if c.closed || c.options.GCTime <= 0 {
		return
	}
	if _, exists := c.entries[key]; !exists {
		return
	}
	if len(c.observers[key]) > 0 || c.pending[key] != nil {
		return
	}
	task := &gcTask{expiresAt: c.options.Clock().Add(c.options.GCTime)}
	c.gcTasks[key] = task
	task.stop = c.gcAfterFunc(c.options.GCTime, func() { c.collectGC(key, task) })
}

// collectGC deletes only the inactive state belonging to the current cleanup task.
func (c *Client) collectGC(key any, task *gcTask) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Stop cannot retract a callback that has already started. Comparing task
	// identities prevents that callback from deleting a reused or recreated key.
	if c.gcTasks[key] != task {
		return
	}
	if remaining := task.expiresAt.Sub(c.options.Clock()); remaining > 0 {
		task.stop = c.gcAfterFunc(remaining, func() { c.collectGC(key, task) })
		return
	}
	delete(c.gcTasks, key)
	if len(c.observers[key]) == 0 && c.pending[key] == nil {
		delete(c.entries, key)
	}
}

// stopGCLocked detaches a key's cleanup before stopping its timer.
func (c *Client) stopGCLocked(key any) {
	if task := c.gcTasks[key]; task != nil {
		delete(c.gcTasks, key)
		task.stop()
	}
}

// clearGCLocked releases every cleanup timer when cached state is discarded.
func (c *Client) clearGCLocked() {
	for key := range c.gcTasks {
		c.stopGCLocked(key)
	}
}
