package cacheq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// awaitEventKind consumes earlier transitions until the requested lifecycle record arrives.
func awaitEventKind(t *testing.T, s *EventSubscription, kind EventKind) Event {
	t.Helper()
	for {
		event := receiveTestEvent(t, s.Events())
		if event.Kind == kind {
			return event
		}
	}
}

// waitEventCall bounds asynchronous query completion without sleep-based coordination.
func waitEventCall(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for observed query")
		return nil
	}
}

// TestEventsLoadDecisions identifies missing data, consumer-specific expiration, and fresh reuse.
func TestEventsLoadDecisions(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	c := NewClient(Options{StaleTime: time.Minute, Clock: func() time.Time { return now }})
	t.Cleanup(c.Close)
	var ticks atomic.Int64
	c.loadNow = func() time.Time { return time.Unix(0, 0).Add(time.Duration(ticks.Add(1)) * time.Millisecond) }
	s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	load := func(context.Context) (int, error) { return 7, nil }
	if _, err := Fetch(context.Background(), c, "a", load); err != nil {
		t.Fatal(err)
	}
	start := receiveTestEvent(t, s.Events())
	end := receiveTestEvent(t, s.Events())
	if start.Kind != EventLoadStarted || start.Reason != ReasonMissing || start.Trigger != TriggerFetch ||
		start.LoadID == 0 {
		t.Fatalf("initial start = %+v", start)
	}
	if end.Kind != EventLoadFinished || end.LoadID != start.LoadID || end.Attempts != 1 || !end.Applied {
		t.Fatalf("initial completion = %+v", end)
	}
	if end.Duration != time.Millisecond || c.Stats().TotalLoadDuration != end.Duration {
		t.Fatalf("elapsed duration = %v; stats = %+v", end.Duration, c.Stats())
	}
	now = now.Add(2 * time.Minute)
	hour := time.Hour
	if _, err := FetchWithOptions(context.Background(), c, "a", load, FetchOptions{StaleTime: &hour}); err != nil {
		t.Fatal(err)
	}
	if hit := receiveTestEvent(
		t,
		s.Events(),
	); hit.Kind != EventCacheHit || hit.Reason != ReasonFresh ||
		hit.LoadID != 0 {
		t.Fatalf("long-window decision = %+v", hit)
	}
	handle := Query(c, "a", load)
	t.Cleanup(handle.Close)
	start = receiveTestEvent(t, s.Events())
	end = receiveTestEvent(t, s.Events())
	if start.Reason != ReasonExpired || start.Trigger != TriggerQuery || start.LoadID != 2 || end.LoadID != 2 {
		t.Fatalf("default-window start = %+v; completion = %+v", start, end)
	}
}

// TestEventsTriggers preserves the public API source independently of the shared loader function.
func TestEventsTriggers(t *testing.T) {
	load := func(context.Context) (int, error) { return 7, nil }
	for _, tc := range []struct {
		// name identifies the public entry point under observation.
		name string
		// trigger is the expected initiating API source.
		trigger EventTrigger
		// reason is the expected stale or forced-load cause.
		reason EventReason
		// call invokes the public entry point without altering the shared loader.
		call func(*testing.T, *Client) error
	}{
		{name: "fetch", trigger: TriggerFetch, reason: ReasonMissing, call: func(t *testing.T, c *Client) error {
			_, err := Fetch(context.Background(), c, "a", load)
			return err
		}},
		{name: "fetch options", trigger: TriggerFetch, reason: ReasonMissing, call: func(t *testing.T, c *Client) error {
			_, err := FetchWithOptions(context.Background(), c, "a", load, FetchOptions{})
			return err
		}},
		{name: "expiry", trigger: TriggerFetch, reason: ReasonMissing, call: func(t *testing.T, c *Client) error {
			_, err := FetchWithExpiry(context.Background(), c, "a", func(context.Context) (int, time.Time, error) {
				return 7, time.Unix(0, 0).Add(time.Hour), nil
			})
			return err
		}},
		{name: "prefetch", trigger: TriggerPrefetch, reason: ReasonMissing, call: func(t *testing.T, c *Client) error {
			return Prefetch(context.Background(), c, "a", load)
		}},
		{name: "query", trigger: TriggerQuery, reason: ReasonMissing, call: func(t *testing.T, c *Client) error {
			h := Query(c, "a", load)
			t.Cleanup(h.Close)
			return nil
		}},
		{name: "enable", trigger: TriggerEnable, reason: ReasonMissing, call: func(t *testing.T, c *Client) error {
			h := Query(c, "a", load, QueryOptions{Disable: true})
			t.Cleanup(h.Close)
			return h.SetEnabled(true)
		}},
		{name: "refetch", trigger: TriggerRefetch, reason: ReasonRefetch, call: func(t *testing.T, c *Client) error {
			h := Query(c, "a", load, QueryOptions{Disable: true})
			t.Cleanup(h.Close)
			_, err := h.Refetch(context.Background())
			return err
		}},
		{name: "invalidate", trigger: TriggerInvalidate, reason: ReasonInvalidated, call: func(t *testing.T, c *Client) error {
			if err := Set(c, "a", 1); err != nil {
				return err
			}
			h := Query(c, "a", load)
			t.Cleanup(h.Close)
			return c.Invalidate("a")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(Options{StaleTime: time.Hour, Clock: func() time.Time { return time.Unix(0, 0) }})
			t.Cleanup(c.Close)
			s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
			if err := tc.call(t, c); err != nil {
				t.Fatal(err)
			}
			start := awaitEventKind(t, s, EventLoadStarted)
			end := awaitEventKind(t, s, EventLoadFinished)
			if start.Trigger != tc.trigger || start.Reason != tc.reason || end.Trigger != tc.trigger ||
				end.LoadID != start.LoadID {
				t.Fatalf("start = %+v; completion = %+v", start, end)
			}
		})
	}
}

// TestEventsSharing counts joins to a load, including a caller that later cancels, but not passive subscribers.
func TestEventsSharing(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	load := func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-release:
			return 7, nil
		}
	}
	first := Query(c, "a", load)
	t.Cleanup(first.Close)
	start := receiveTestEvent(t, s.Events())
	second := Query(c, "a", load)
	t.Cleanup(second.Close)
	if join := receiveTestEvent(
		t,
		s.Events(),
	); join.Kind != EventLoadJoined || join.Joined != 1 ||
		join.LoadID != start.LoadID {
		t.Fatalf("second consumer = %+v", join)
	}
	third := Query(c, "a", load, QueryOptions{Disable: true})
	t.Cleanup(third.Close)
	if err := third.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	if join := receiveTestEvent(t, s.Events()); join.Trigger != TriggerEnable || join.Joined != 2 {
		t.Fatalf("enabled consumer = %+v", join)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := Fetch(ctx, c, "a", load)
		done <- err
	}()
	if join := receiveTestEvent(t, s.Events()); join.Trigger != TriggerFetch || join.Joined != 3 {
		t.Fatalf("imperative consumer = %+v", join)
	}
	cancel()
	if err := waitEventCall(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter = %v", err)
	}
	unblock()
	end := receiveTestEvent(t, s.Events())
	if end.Kind != EventLoadFinished || end.Joined != 3 || end.Trigger != TriggerQuery || end.LoadID != start.LoadID {
		t.Fatalf("shared completion = %+v", end)
	}
	Get[int](c, "a")
	third.Snapshot()
	select {
	case event := <-s.Events():
		t.Fatalf("passive snapshot produced an event: %+v", event)
	default:
	}
}

// TestEventsRetry preserves one operation ID, reports the prior error, and counts actual attempts.
func TestEventsRetry(t *testing.T) {
	failure := errors.New("transient backend error")
	c := NewClient(Options{Retry: 1, RetryDelay: func(int) time.Duration { return 0 }})
	t.Cleanup(c.Close)
	s := subscribeTestEvents(t, c, EventOptions{})
	calls := 0
	_, err := Fetch(context.Background(), c, "a", func(context.Context) (int, error) {
		calls++
		if calls == 1 {
			return 0, failure
		}
		return 7, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	start := receiveTestEvent(t, s.Events())
	retry := receiveTestEvent(t, s.Events())
	end := receiveTestEvent(t, s.Events())
	if retry.Kind != EventRetryStarted || retry.LoadID != start.LoadID || retry.Attempts != 2 ||
		!errors.Is(retry.Err, failure) {
		t.Fatalf("retry event = %+v", retry)
	}
	if end.Kind != EventLoadFinished || end.LoadID != start.LoadID || end.Attempts != 2 || end.Err != nil {
		t.Fatalf("retried completion = %+v", end)
	}
}

// TestEventsFailureReasons distinguishes stale failures from a reusable explicitly cached fallback.
func TestEventsFailureReasons(t *testing.T) {
	failure := errors.New("backend unavailable")
	now := time.Unix(0, 0)
	c := NewClient(Options{Clock: func() time.Time { return now }})
	t.Cleanup(c.Close)
	s := subscribeTestEvents(t, c, EventOptions{})
	_, _ = Fetch(context.Background(), c, "a", func(context.Context) (int, error) { return 0, failure })
	receiveTestEvent(t, s.Events())
	if end := receiveTestEvent(t, s.Events()); !errors.Is(end.Err, failure) || !end.Applied {
		t.Fatalf("failed outcome = %+v", end)
	}
	_, _ = Fetch(context.Background(), c, "a", func(context.Context) (int, error) { return 7, nil })
	if start := receiveTestEvent(t, s.Events()); start.Reason != ReasonPreviousFailure {
		t.Fatalf("load after failure = %+v", start)
	}
	receiveTestEvent(t, s.Events())
	_, _ = FetchWithExpiry(context.Background(), c, "fallback", func(context.Context) (int, time.Time, error) {
		return -1, now.Add(time.Minute), failure
	})
	receiveTestEvent(t, s.Events())
	receiveTestEvent(t, s.Events())
	_, _ = Fetch[int](context.Background(), c, "fallback", nil)
	if hit := receiveTestEvent(t, s.Events()); hit.Kind != EventCacheHit || !errors.Is(hit.Err, failure) {
		t.Fatalf("fresh fallback = %+v", hit)
	}
	now = now.Add(time.Minute)
	_, _ = Fetch(context.Background(), c, "fallback", func(context.Context) (int, error) { return 7, nil })
	if start := receiveTestEvent(t, s.Events()); start.Reason != ReasonExpired {
		t.Fatalf("expired fallback = %+v", start)
	}
}

// TestEventsDiscardedResult records why detached work could not overwrite a local write or removed state.
func TestEventsDiscardedResult(t *testing.T) {
	for _, tc := range []struct {
		// name identifies the explicit detachment operation.
		name string
		// reason is the expected discard cause.
		reason EventReason
		// detach performs the public operation while the loader is blocked.
		detach func(*Client) error
	}{
		{name: "set", reason: ReasonSet, detach: func(c *Client) error { return Set(c, "a", 99) }},
		{name: "remove", reason: ReasonRemove, detach: func(c *Client) error { return c.Remove("a") }},
		{name: "clear", reason: ReasonClear, detach: func(c *Client) error { c.Clear(); return nil }},
		{name: "cancel", reason: ReasonCancel, detach: func(c *Client) error { return c.Cancel("a") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(Options{StaleTime: time.Hour})
			t.Cleanup(c.Close)
			s := subscribeTestEvents(t, c, EventOptions{})
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			h := Query(c, "a", func(context.Context) (int, error) {
				close(started)
				<-release
				return 7, nil
			})
			t.Cleanup(h.Close)
			start := receiveTestEvent(t, s.Events())
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("loader did not start")
			}
			if err := tc.detach(c); err != nil {
				t.Fatal(err)
			}
			unblock()
			end := awaitEventKind(t, s, EventLoadFinished)
			discard := receiveTestEvent(t, s.Events())
			if end.Applied || discard.Kind != EventResultDiscarded || discard.Reason != tc.reason ||
				discard.LoadID != start.LoadID {
				t.Fatalf("completion = %+v; discard = %+v", end, discard)
			}
			if tc.reason == ReasonSet && Get[int](c, "a").Data != 99 {
				t.Fatal("detached result overwrote local data")
			}
		})
	}
}

// TestEventsLoadBackpressure lets queries finish while an unread stream repeatedly drops records.
func TestEventsLoadBackpressure(t *testing.T) {
	c := NewClient(Options{})
	t.Cleanup(c.Close)
	s := subscribeTestEvents(t, c, EventOptions{Buffer: 1})
	done := make(chan error, 1)
	go func() {
		for range 10 {
			if _, err := Fetch(
				context.Background(),
				c,
				"a",
				func(context.Context) (int, error) { return 7, nil },
			); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	if err := waitEventCall(t, done); err != nil {
		t.Fatal(err)
	}
	if s.Dropped() != 19 || receiveTestEvent(t, s.Events()).Kind != EventLoadStarted {
		t.Fatalf("slow reader drops = %d", s.Dropped())
	}
}

// TestEventsOwnerReplacement counts each join under its actual load ID while preserving Stats' per-call convention.
func TestEventsOwnerReplacement(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	entered := make(chan error, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls atomic.Int32
	load := func(ctx context.Context) (int, error) {
		if calls.Add(1) == 1 {
			entered <- nil
			<-ctx.Done()
			return 0, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-release:
			return 7, nil
		}
	}
	fetch := func(ctx context.Context) <-chan error {
		done := make(chan error, 1)
		go func() {
			value, err := Fetch(ctx, c, "a", load)
			if err == nil && value != 7 {
				err = errors.New("replacement returned an unexpected value")
			}
			done <- err
		}()
		return done
	}
	ownerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner := fetch(ownerCtx)
	firstStart := receiveTestEvent(t, s.Events())
	if err := waitEventCall(t, entered); err != nil {
		t.Fatal(err)
	}
	firstWaiter := fetch(context.Background())
	awaitEventKind(t, s, EventLoadJoined)
	secondWaiter := fetch(context.Background())
	awaitEventKind(t, s, EventLoadJoined)
	cancel()
	if err := waitEventCall(t, owner); !errors.Is(err, context.Canceled) {
		t.Fatalf("owner outcome = %v", err)
	}
	oldEnd := awaitEventKind(t, s, EventLoadFinished)
	if oldEnd.Joined != 2 || oldEnd.LoadID != firstStart.LoadID || oldEnd.Applied {
		t.Fatalf("canceled operation = %+v", oldEnd)
	}
	awaitEventKind(t, s, EventResultDiscarded)
	newStart := awaitEventKind(t, s, EventLoadStarted)
	newJoin := awaitEventKind(t, s, EventLoadJoined)
	if newStart.LoadID == firstStart.LoadID || newJoin.LoadID != newStart.LoadID || newJoin.Joined != 1 {
		t.Fatalf("replacement start = %+v; join = %+v", newStart, newJoin)
	}
	unblock()
	for _, done := range []<-chan error{firstWaiter, secondWaiter} {
		if err := waitEventCall(t, done); err != nil {
			t.Fatal(err)
		}
	}
	end := receiveTestEvent(t, s.Events())
	if end.Joined != 1 || end.LoadID != newStart.LoadID || c.Stats().MergedRequests != 2 {
		t.Fatalf("replacement completion = %+v; stats = %+v", end, c.Stats())
	}
}

// TestEventsMidflightSubscription correlates work that started before diagnostics were enabled.
func TestEventsMidflightSubscription(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	entered := make(chan error, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	load := func(ctx context.Context) (int, error) {
		entered <- nil
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-release:
			return 7, nil
		}
	}
	first := Query(c, "a", load)
	t.Cleanup(first.Close)
	if err := waitEventCall(t, entered); err != nil {
		t.Fatal(err)
	}
	s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	second := Query(c, "a", load)
	t.Cleanup(second.Close)
	join := receiveTestEvent(t, s.Events())
	if join.Kind != EventLoadJoined || join.LoadID == 0 || join.Joined != 1 {
		t.Fatalf("midflight join = %+v", join)
	}
	unblock()
	end := receiveTestEvent(t, s.Events())
	if end.Kind != EventLoadFinished || end.LoadID != join.LoadID || end.Attempts != 1 {
		t.Fatalf("midflight completion = %+v", end)
	}
}
