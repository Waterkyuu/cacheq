package cacheq

import (
	"context"
	"errors"
	"fmt"
)

// ErrRefreshInvalidated indicates another invalidation occurred during the awaited refresh.
// The result remains stale; a bounded follow-up may require an explicit subsequent refresh.
var ErrRefreshInvalidated = errors.New("refresh was invalidated while loading")

// ErrRefreshSuperseded indicates a local write replaced the awaited refresh.
var ErrRefreshSuperseded = errors.New("refresh was superseded by a local write")

// RefreshResult describes the outcome for one distinct invalidation target.
type RefreshResult struct {
	// Key identifies the selected query, retaining its original dynamic type.
	Key any
	// Refreshed means a post-invalidation load succeeded without being invalidated or replaced.
	// False with nil Err means no enabled observer or RefetchNone skipped the refresh.
	Refreshed bool
	// Err reports refresh failure, supersession, or cancellation of this caller's wait.
	Err error
}

// refreshCompletion publishes a single load outcome without depending on lossy subscriptions.
type refreshCompletion struct {
	// done closes after the outcome has been set under Client.mu.
	done chan struct{}
	// result contains the immutable outcome once done closes.
	result RefreshResult
	// finished prevents double publication when explicit cancellation races with completion.
	finished bool
}

// newRefreshCompletion allocates the receipt shared by coalesced invalidation waiters.
func newRefreshCompletion(key any) *refreshCompletion {
	return &refreshCompletion{done: make(chan struct{}), result: RefreshResult{Key: key}}
}

// finish publishes the outcome once; callers hold Client.mu.
func (r *refreshCompletion) finish(refreshed bool, err error) {
	if r == nil || r.finished {
		return
	}
	r.result.Refreshed, r.result.Err = refreshed, err
	r.finished = true
	close(r.done)
}

// InvalidateAndWait invalidates targets and waits for their post-invalidation refresh outcomes.
// It accepts the same exact keys, batches, predicates, and QueryFilter as Invalidate.
// It enables RefetchAfterLoad and accepts exactly one configuration.
// Only enabled observed queries refresh; RefetchNone and unobserved targets return skipped results.
// All selected refreshes are scheduled before waiting. Results retain explicit key order and exclude duplicates.
// Selection without explicit keys has unspecified order. Per-key errors are wrapped and joined in the returned error.
// Canceling ctx ends only this wait, not Client-owned refreshes. A pre-canceled ctx makes no changes.
// Success describes load completion, not time-based freshness or an atomic multi-key snapshot.
func (c *Client) InvalidateAndWait(
	ctx context.Context,
	target any,
	options InvalidateOptions,
) ([]RefreshResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.invalidateAndWait(ctx, target, options)
}

// invalidateAndWait schedules selected refreshes before collecting their completion results.
func (c *Client) invalidateAndWait(
	ctx context.Context,
	target any,
	options InvalidateOptions,
) ([]RefreshResult, error) {
	options.RefetchAfterLoad = true
	refreshes, err := c.invalidate(target, options, ctx)
	if err != nil {
		return nil, err
	}
	results := make([]RefreshResult, 0, len(refreshes))
	var combined error
	for _, refresh := range refreshes {
		result := waitRefresh(ctx, refresh)
		results = append(results, result)
		if result.Err != nil {
			combined = errors.Join(combined, fmt.Errorf("refresh key %v: %w", result.Key, result.Err))
		}
	}
	return results, combined
}

// waitRefresh prefers a completed receipt, including skipped targets, over simultaneous cancellation.
func waitRefresh(ctx context.Context, refresh *refreshCompletion) RefreshResult {
	select {
	case <-refresh.done:
		return refresh.result
	default:
	}
	select {
	case <-refresh.done:
		return refresh.result
	case <-ctx.Done():
		select {
		case <-refresh.done:
			return refresh.result
		default:
			return RefreshResult{Key: refresh.result.Key, Err: ctx.Err()}
		}
	}
}

// scheduleRefreshLocked uses enabled observers and bounds each active chain to one follow-up.
func (c *Client) scheduleRefreshLocked(
	key any,
	pending *flight,
	options InvalidateOptions,
	waitCtx context.Context,
) *refreshCompletion {
	if options.Refetch != RefetchObserved {
		return nil
	}
	request := c.observedRequestLocked(key)
	if request.load == nil {
		return nil
	}
	if waitCtx != nil && request.options.Executor.heldBy(waitCtx) {
		// Background loads use Client context, so reject waiting on caller-held capacity explicitly.
		refresh := newRefreshCompletion(key)
		refresh.finish(false, ErrExecutionReentry)
		return refresh
	}
	if pending == nil {
		request.trigger = TriggerInvalidate
		pending = c.startLocked(c.ctx, key, request)
		if waitCtx != nil {
			pending.refresh = newRefreshCompletion(key)
		}
		return pending.refresh
	}
	if !options.RefetchAfterLoad {
		return nil
	}
	if pending.followupLoad {
		return pending.refresh
	}
	if pending.followup == nil {
		pending.followup = newRefreshCompletion(key)
	}
	return pending.followup
}

// finishRefreshLocked settles this load's receipt and starts at most one coalesced successor.
func (c *Client) finishRefreshLocked(key any, pending *flight, applied bool) {
	err := pending.err
	if err == nil && pending.invalidated {
		err = ErrRefreshInvalidated
	}
	if err == nil && !applied {
		err = ErrRefreshSuperseded
	}
	pending.refresh.finish(err == nil, err)
	if pending.followup == nil {
		return
	}
	// Set, Remove, Cancel, Clear, and Close settle scheduled work when detaching the flight.
	if pending.detached || c.closed {
		return
	}
	request := c.observedRequestLocked(key)
	if request.load == nil {
		pending.followup.finish(false, nil)
		return
	}
	request.trigger = TriggerInvalidate
	next := c.startLocked(c.ctx, key, request)
	next.refresh = pending.followup
	next.followupLoad = true
}

// cancelRefreshLocked releases waiters immediately even if a detached loader ignores cancellation.
func (c *Client) cancelRefreshLocked(pending *flight, reason EventReason) {
	err := error(context.Canceled)
	switch reason {
	case ReasonSet:
		err = ErrRefreshSuperseded
	case ReasonClientClosed:
		err = ErrClosed
	}
	pending.refresh.finish(false, err)
	pending.followup.finish(false, err)
}
