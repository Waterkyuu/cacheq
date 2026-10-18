package soroi

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// refreshWaitResult transports one public waiting call's outcomes to its test owner.
type refreshWaitResult struct {
	// results preserves the per-key outcomes returned by InvalidateAndWait.
	results []RefreshResult
	// err combines selection, waiting, and per-key failures.
	err error
}

// startRefreshWait executes a waiting invalidation without blocking the test's channel orchestration.
func startRefreshWait(
	c *Client,
	ctx context.Context,
	target any,
	options InvalidateOptions,
) <-chan refreshWaitResult {
	done := make(chan refreshWaitResult, 1)
	go func() {
		results, err := c.InvalidateAndWait(ctx, target, options)
		done <- refreshWaitResult{results: results, err: err}
	}()
	return done
}

// receiveRefreshWait bounds deadlocks without using timing to order library operations.
func receiveRefreshWait(t *testing.T, done <-chan refreshWaitResult) refreshWaitResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("refresh wait did not complete")
		return refreshWaitResult{}
	}
}

// awaitInvalidation observes a public invalidation event and waits for its locked transition to finish.
func awaitInvalidation(t *testing.T, c *Client, events *EventSubscription) {
	t.Helper()
	for {
		event := receiveTestEvent(t, events.Events())
		if event.Kind == EventInvalidated {
			_ = c.Get(event.Key)
			return
		}
	}
}

// TestInvalidateAndWaitResults refreshes heterogeneous keys concurrently and reports partial failure.
func TestInvalidateAndWaitResults(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "success", 1)
	Set(c, "failure", "old")
	Set(c, "unobserved", true)
	started := make(chan string, 2)
	release := make(chan struct{})
	failure := errors.New("backend unavailable")
	success := Query(c, "success", func(ctx context.Context) (int, error) {
		started <- "success"
		select {
		case <-release:
			return 2, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	failed := Query(c, "failure", func(ctx context.Context) (string, error) {
		started <- "failure"
		select {
		case <-release:
			return "", failure
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	t.Cleanup(success.Close)
	t.Cleanup(failed.Close)
	done := startRefreshWait(
		c,
		context.Background(),
		[]any{"success", "failure", "success", "unobserved"},
		InvalidateOptions{},
	)
	<-started
	<-started
	select {
	case result := <-done:
		t.Fatalf("returned before loads completed: %+v", result)
	default:
	}
	close(release)
	result := receiveRefreshWait(t, done)
	if !errors.Is(result.err, failure) || len(result.results) != 3 {
		t.Fatalf("outcomes = %+v", result)
	}
	if first := result.results[0]; first.Key != "success" || !first.Refreshed || first.Err != nil {
		t.Fatalf("success = %+v", first)
	}
	if second := result.results[1]; second.Key != "failure" || second.Refreshed || !errors.Is(second.Err, failure) {
		t.Fatalf("failure = %+v", second)
	}
	if third := result.results[2]; third.Key != "unobserved" || third.Refreshed || third.Err != nil {
		t.Fatalf("skipped = %+v", third)
	}
	if state := failed.Snapshot(); state.Data != "old" || !state.Stale || !errors.Is(state.Err, failure) {
		t.Fatalf("failed refresh lost old data: %+v", state)
	}
}

// TestInvalidateAndWaitValidation preserves all-or-nothing selection and canceled-call behavior.
func TestInvalidateAndWaitValidation(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "key", 1)
	for _, target := range []any{nil, []int{1}, []any{"key", []int{1}}, (func(any) bool)(nil)} {
		results, err := c.InvalidateAndWait(context.Background(), target)
		if results != nil || !errors.Is(err, ErrInvalidKey) || Get[int](c, "key").Stale {
			t.Fatalf("invalid target %T: results=%+v err=%v", target, results, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.InvalidateAndWait(
		ctx,
		"key",
	); !errors.Is(err, context.Canceled) ||
		Get[int](c, "key").Stale {
		t.Fatalf("pre-canceled invalidation = %v", err)
	}
	results, err := c.InvalidateAndWait(context.Background(), []any{})
	if err != nil || len(results) != 0 {
		t.Fatalf("empty batch = %+v, %v", results, err)
	}
	c.Close()
	if _, err := c.InvalidateAndWait(context.Background(), []any{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// TestInvalidateFollowupPolicy preserves the default and coalesces opted-in requests into one follow-up.
func TestInvalidateFollowupPolicy(t *testing.T) {
	for _, mode := range []string{"default", "followup", "none"} {
		t.Run(mode, func(t *testing.T) {
			c := newInvalidationClient(t)
			var backend, calls atomic.Int32
			backend.Store(1)
			started, release := make(chan struct{}), make(chan struct{})
			q := Query(c, "key", func(ctx context.Context) (int32, error) {
				value := backend.Load()
				if calls.Add(1) == 1 {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return 0, ctx.Err()
					}
				}
				return value, nil
			})
			t.Cleanup(q.Close)
			<-started
			backend.Store(2)
			options := InvalidateOptions{RefetchAfterLoad: mode != "default"}
			if mode == "none" {
				options.Refetch = RefetchNone
			}
			for range 3 {
				if err := c.Invalidate("key", options); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			state := awaitState(t, q.Updates(), func(s Snapshot[int32]) bool { return s.HasData && !s.Fetching })
			if mode == "followup" {
				if state.Data != 2 || state.Stale || calls.Load() != 2 {
					t.Fatalf("follow-up = %+v, calls=%d", state, calls.Load())
				}
			} else if state.Data != 1 || !state.Stale || calls.Load() != 1 {
				t.Fatalf("original contract = %+v, calls=%d", state, calls.Load())
			}
		})
	}
}

// TestInvalidateAndWaitDuringLoad shares one post-invalidation load even when the old load fails.
func TestInvalidateAndWaitDuringLoad(t *testing.T) {
	for _, oldFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[oldFailure], func(t *testing.T) {
			// Zero StaleTime must not turn a successfully completed receipt into an infinite refresh loop.
			c := NewClient(Options{})
			t.Cleanup(c.Close)
			events := subscribeTestEvents(t, c, EventOptions{Buffer: 64})
			var calls atomic.Int32
			started := make(chan int32, 2)
			releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
			q := Query(c, "key", func(ctx context.Context) (int32, error) {
				call := calls.Add(1)
				started <- call
				select {
				case <-releases[call-1]:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
				if oldFailure && call == 1 {
					return 0, errors.New("old request failed")
				}
				return call, nil
			})
			t.Cleanup(q.Close)
			<-started
			first := startRefreshWait(c, context.Background(), "key", InvalidateOptions{})
			awaitInvalidation(t, c, events)
			second := startRefreshWait(c, context.Background(), "key", InvalidateOptions{})
			awaitInvalidation(t, c, events)
			close(releases[0])
			if call := <-started; call != 2 {
				t.Fatalf("follow-up call = %d", call)
			}
			for _, done := range []<-chan refreshWaitResult{first, second} {
				select {
				case result := <-done:
					t.Fatalf("returned the pre-invalidation result: %+v", result)
				default:
				}
			}
			close(releases[1])
			for _, done := range []<-chan refreshWaitResult{first, second} {
				result := receiveRefreshWait(t, done)
				if result.err != nil || len(result.results) != 1 || !result.results[0].Refreshed {
					t.Fatalf("refresh = %+v", result)
				}
			}
			if state := q.Snapshot(); state.Data != 2 || calls.Load() != 2 {
				t.Fatalf("state = %+v, calls=%d", state, calls.Load())
			}
		})
	}
}

// TestInvalidateFollowupBound reports repeated invalidation rather than creating an unbounded chain.
func TestInvalidateFollowupBound(t *testing.T) {
	c := newInvalidationClient(t)
	events := subscribeTestEvents(t, c, EventOptions{Buffer: 64})
	var calls atomic.Int32
	started := make(chan int32, 2)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
	q := Query(c, "key", func(ctx context.Context) (int32, error) {
		call := calls.Add(1)
		started <- call
		select {
		case <-releases[call-1]:
			return call, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	t.Cleanup(q.Close)
	<-started
	first := startRefreshWait(c, context.Background(), "key", InvalidateOptions{})
	awaitInvalidation(t, c, events)
	close(releases[0])
	<-started
	second := startRefreshWait(c, context.Background(), "key", InvalidateOptions{})
	awaitInvalidation(t, c, events)
	close(releases[1])
	for _, done := range []<-chan refreshWaitResult{first, second} {
		result := receiveRefreshWait(t, done)
		if !errors.Is(result.err, ErrRefreshInvalidated) || result.results[0].Refreshed {
			t.Fatalf("invalidated follow-up = %+v", result)
		}
	}
	if state := q.Snapshot(); !state.Stale || state.Fetching || calls.Load() != 2 {
		t.Fatalf("bounded state = %+v, calls=%d", state, calls.Load())
	}
}

// TestInvalidateWaitCancellation stops only the waiter and preserves Client-owned refreshing.
func TestInvalidateWaitCancellation(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "key", 1)
	started, release := make(chan struct{}), make(chan struct{})
	q := Query(c, "key", func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-release:
			return 2, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	t.Cleanup(q.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startRefreshWait(c, ctx, "key", InvalidateOptions{})
	<-started
	cancel()
	result := receiveRefreshWait(t, done)
	if !errors.Is(result.err, context.Canceled) || result.results[0].Refreshed {
		t.Fatalf("canceled wait = %+v", result)
	}
	close(release)
	state := awaitState(t, q.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	if state.Stale {
		t.Fatalf("wait cancellation canceled shared work: %+v", state)
	}
}

// TestInvalidateRefreshDetachment releases waiters and discards scheduled work after explicit replacement.
func TestInvalidateRefreshDetachment(t *testing.T) {
	for _, operation := range []string{"set", "cancel", "remove", "clear", "close"} {
		t.Run(operation, func(t *testing.T) {
			c := newInvalidationClient(t)
			events := subscribeTestEvents(t, c, EventOptions{Buffer: 64})
			started, release := make(chan struct{}), make(chan struct{})
			// Deliberately delayed delivery verifies detachment releases waiters without waiting for the loader.
			q := Query(c, "key", func(context.Context) (int, error) {
				close(started)
				<-release
				return 1, nil
			})
			t.Cleanup(q.Close)
			defer close(release)
			<-started
			done := startRefreshWait(c, context.Background(), "key", InvalidateOptions{})
			awaitInvalidation(t, c, events)
			want := error(context.Canceled)
			switch operation {
			case "set":
				Set(c, "key", 2)
				want = ErrRefreshSuperseded
			case "cancel":
				c.Cancel("key")
			case "remove":
				c.Remove("key")
			case "clear":
				c.Clear()
			case "close":
				c.Close()
				want = ErrClosed
			}
			if result := receiveRefreshWait(t, done); !errors.Is(result.err, want) {
				t.Fatalf("%s outcome = %+v; want %v", operation, result, want)
			}
		})
	}
}

// TestInvalidateFollowupObserverLoss skips scheduled work when all enabled observers leave.
func TestInvalidateFollowupObserverLoss(t *testing.T) {
	for _, disable := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "disable"}[disable], func(t *testing.T) {
			c := newInvalidationClient(t)
			events := subscribeTestEvents(t, c, EventOptions{Buffer: 64})
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			q := Query(c, "key", func(ctx context.Context) (int, error) {
				calls.Add(1)
				close(started)
				select {
				case <-release:
					return 1, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			})
			t.Cleanup(q.Close)
			<-started
			done := startRefreshWait(c, context.Background(), "key", InvalidateOptions{})
			awaitInvalidation(t, c, events)
			if disable {
				q.SetEnabled(false)
			} else {
				q.Close()
			}
			close(release)
			result := receiveRefreshWait(t, done)
			if result.err != nil || result.results[0].Refreshed || calls.Load() != 1 {
				t.Fatalf("observer loss = %+v, calls=%d", result, calls.Load())
			}
		})
	}
}

// TestInvalidateAndWaitPredicate keeps matching outside locks and respects RefetchNone over follow-up.
func TestInvalidateAndWaitPredicate(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "selected", 1)
	Set(c, "other", true)
	q := Query(
		c,
		"selected",
		func(context.Context) (int, error) { t.Fatal("unexpected load"); return 0, nil },
	)
	t.Cleanup(q.Close)
	results, err := c.InvalidateAndWait(context.Background(), func(key any) bool {
		_ = c.Get(key)
		return key == "selected"
	}, InvalidateOptions{Refetch: RefetchNone, RefetchAfterLoad: true})
	if err != nil || len(results) != 1 || results[0].Key != "selected" || results[0].Refreshed {
		t.Fatalf("predicate results = %+v, %v", results, err)
	}
	if !q.Snapshot().Stale || Get[bool](c, "other").Stale {
		t.Fatal("predicate changed the wrong keys")
	}
}

// TestInvalidateFollowupOwnerCancellation gives the successor a Client lifetime instead of the old caller's.
func TestInvalidateFollowupOwnerCancellation(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "key", int32(0))
	events := subscribeTestEvents(t, c, EventOptions{Buffer: 64})
	started := make(chan struct{})
	var calls atomic.Int32
	q := Query(c, "key", func(ctx context.Context) (int32, error) {
		call := calls.Add(1)
		if call == 1 {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return call, nil
	})
	t.Cleanup(q.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	old := make(chan error, 1)
	go func() { _, err := q.Refetch(ctx); old <- err }()
	<-started
	done := startRefreshWait(c, context.Background(), "key", InvalidateOptions{})
	awaitInvalidation(t, c, events)
	cancel()
	if err := <-old; !errors.Is(err, context.Canceled) {
		t.Fatalf("old owner = %v", err)
	}
	result := receiveRefreshWait(t, done)
	if result.err != nil || !result.results[0].Refreshed || q.Snapshot().Data != 2 || calls.Load() != 2 {
		t.Fatalf("replacement = %+v, calls=%d", result, calls.Load())
	}
}

// TestInvalidateRefreshBudget returns overload through per-key results without retrying rejected refreshes.
func TestInvalidateRefreshBudget(t *testing.T) {
	budget, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(Options{StaleTime: time.Hour, Retry: 3, Executor: budget})
	t.Cleanup(c.Close)
	started := make(chan struct{})
	busy := Query(c, "busy", func(ctx context.Context) (int, error) {
		close(started)
		<-ctx.Done()
		return 0, ctx.Err()
	})
	t.Cleanup(busy.Close)
	<-started
	Set(c, "key", 1)
	q := Query(
		c,
		"key",
		func(context.Context) (int, error) { t.Error("rejected loader ran"); return 2, nil },
	)
	t.Cleanup(q.Close)
	results, err := c.InvalidateAndWait(context.Background(), "key")
	if !errors.Is(err, ErrQueueFull) || len(results) != 1 || !errors.Is(results[0].Err, ErrQueueFull) {
		t.Fatalf("overload = %+v, %v", results, err)
	}
	if state := q.Snapshot(); state.Data != 1 || !state.Stale || state.Fetching {
		t.Fatalf("overload state = %+v", state)
	}
}

// TestInvalidateFollowupSelectsCurrentObserver retains automatic selection and policy overrides at dispatch.
func TestInvalidateFollowupSelectsCurrentObserver(t *testing.T) {
	c := newInvalidationClient(t)
	events := subscribeTestEvents(t, c, EventOptions{Buffer: 64})
	started, release := make(chan struct{}), make(chan struct{})
	first := Query(c, "key", func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-release:
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	t.Cleanup(first.Close)
	<-started
	var attempts atomic.Int32
	failure := errors.New("try again")
	retries := 1
	second := Query(c, "key", func(context.Context) (int, error) {
		if attempts.Add(1) == 1 {
			return 0, failure
		}
		return 2, nil
	}, QueryOptions{Retry: &retries, RetryDelay: func(int) time.Duration { return 0 }})
	t.Cleanup(second.Close)
	done := startRefreshWait(c, context.Background(), "key", InvalidateOptions{})
	awaitInvalidation(t, c, events)
	first.SetEnabled(false)
	close(release)
	result := receiveRefreshWait(t, done)
	if result.err != nil || !result.results[0].Refreshed || second.Snapshot().Data != 2 || attempts.Load() != 2 {
		t.Fatalf("current observer policy = %+v, attempts=%d", result, attempts.Load())
	}
}

// TestInvalidateWaitExecutionReentry rejects waiting on the calling callback's shared budget.
func TestInvalidateWaitExecutionReentry(t *testing.T) {
	budget, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(Options{StaleTime: time.Hour, Executor: budget})
	t.Cleanup(c.Close)
	Set(c, "key", 1)
	q := Query(
		c,
		"key",
		func(context.Context) (int, error) { t.Error("recursive refresh ran"); return 2, nil },
	)
	t.Cleanup(q.Close)
	_, err = Fetch(context.Background(), c, "outer", func(ctx context.Context) (int, error) {
		results, err := c.InvalidateAndWait(ctx, "key")
		if len(results) != 1 || !errors.Is(results[0].Err, ErrExecutionReentry) {
			t.Errorf("recursive results = %+v, %v", results, err)
		}
		return 0, err
	})
	if !errors.Is(err, ErrExecutionReentry) {
		t.Fatalf("recursive wait = %v", err)
	}
	if stats := budget.Stats(); stats.Active != 0 || stats.Queued != 0 || stats.Rejected != 0 {
		t.Fatalf("recursive wait changed capacity: %+v", stats)
	}
}
