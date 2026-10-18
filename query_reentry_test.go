package cacheq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestQueryJoinExecutionReentry rejects waits on the initiator's caller-held budget before joining.
func TestQueryJoinExecutionReentry(t *testing.T) {
	for _, operation := range []string{"fetch", "prefetch", "refetch"} {
		for _, override := range []string{"inherited", "other", "disabled"} {
			t.Run(operation+"/"+override, func(t *testing.T) {
				budget, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
				if err != nil {
					t.Fatal(err)
				}
				other, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
				if err != nil {
					t.Fatal(err)
				}
				c := NewClient(Options{Executor: budget, Retry: 3,
					RetryIf: func(error) bool { t.Error("reentry was retried"); return true },
				})
				defer c.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				options := QueryOptions{Disable: true}
				switch override {
				case "other":
					options.Executor = other
				case "disabled":
					options.DisableExecutor = true
				}
				load := func(context.Context) (int, error) {
					t.Error("joining caller replaced the initiator's loader")
					return 9, nil
				}
				h := Query(c, "inner", load, options)
				defer h.Close()
				events, err := c.SubscribeEvents(EventOptions{Key: "inner"})
				if err != nil {
					t.Fatal(err)
				}
				defer events.Close()
				holding, proceed := make(chan struct{}), make(chan struct{})
				nestedCancel := make(chan context.CancelFunc, 1)
				outerDone := make(chan error, 1)
				go func() {
					_, err := Fetch(ctx, c, "outer", func(ctx context.Context) (int, error) {
						close(holding)
						select {
						case <-proceed:
						case <-ctx.Done():
							return 0, ctx.Err()
						}
						nested, stop := context.WithCancel(ctx)
						defer stop()
						nestedCancel <- stop
						var nestedErr error
						switch operation {
						case "fetch":
							_, nestedErr = Fetch(nested, c, "inner", load, options)
						case "prefetch":
							nestedErr = Prefetch(nested, c, "inner", load, options)
						case "refetch":
							_, nestedErr = h.Refetch(nested)
						}
						return 0, fmt.Errorf("nested read: %w", nestedErr)
					})
					outerDone <- err
				}()
				select {
				case <-holding:
				case <-ctx.Done():
					t.Fatal("outer callback did not acquire its budget")
				}
				firstDone := make(chan error, 1)
				go func() {
					_, err := Fetch(ctx, c, "inner", func(context.Context) (int, error) { return 7, nil })
					firstDone <- err
				}()
				awaitExecution(t, budget, 1, 1)
				close(proceed)
				var stop context.CancelFunc
				select {
				case stop = <-nestedCancel:
				case <-ctx.Done():
					t.Fatal("nested read did not start")
				}
			wait:
				for {
					select {
					case event := <-events.Events():
						if event.Kind == EventLoadJoined {
							// Observing the unsafe join establishes the cycle before cancellation breaks it.
							stop()
						}
					case err = <-outerDone:
						break wait
					case <-ctx.Done():
						t.Fatal("nested read did not complete")
					}
				}
				if !errors.Is(err, ErrExecutionReentry) {
					t.Fatalf("nested %s = %v, want ErrExecutionReentry", operation, err)
				}
				select {
				case err := <-firstDone:
					if err != nil {
						t.Fatalf("independent flight = %v", err)
					}
				case <-ctx.Done():
					t.Fatal("independent flight did not complete after budget release")
				}
				if stats := c.Stats(); stats.MergedRequests != 0 || stats.Retries != 0 {
					t.Fatalf("rejected dependency joined or retried: %+v", stats)
				}
				if budget.Stats().Rejected != 0 {
					t.Fatal("reentry was counted as overload")
				}
			})
		}
	}
}

// TestQueryJoinIndependentExecutor permits waiting when the initiator does not need the caller-held budget.
func TestQueryJoinIndependentExecutor(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("disabled=%t", disabled), func(t *testing.T) {
			budget, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
			if err != nil {
				t.Fatal(err)
			}
			other, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
			if err != nil {
				t.Fatal(err)
			}
			c := NewClient(Options{Executor: budget})
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			events, err := c.SubscribeEvents(EventOptions{Key: "inner"})
			if err != nil {
				t.Fatal(err)
			}
			defer events.Close()
			started, release := make(chan struct{}), make(chan struct{})
			firstDone, outerDone := make(chan error, 1), make(chan error, 1)
			go func() {
				_, err := Fetch(ctx, c, "inner", func(ctx context.Context) (int, error) {
					close(started)
					select {
					case <-release:
						return 7, nil
					case <-ctx.Done():
						return 0, ctx.Err()
					}
				}, QueryOptions{Executor: other, DisableExecutor: disabled})
				firstDone <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("independent loader did not start")
			}
			go func() {
				value, err := Fetch(ctx, c, "outer", func(ctx context.Context) (int, error) {
					return Fetch(ctx, c, "inner", func(context.Context) (int, error) {
						t.Error("joining caller replaced independent loader")
						return 9, nil
					})
				})
				if err == nil && value != 7 {
					t.Errorf("joined value = %d, want 7", value)
				}
				outerDone <- err
			}()
		wait:
			for {
				select {
				case event := <-events.Events():
					if event.Kind == EventLoadJoined {
						close(release)
						break wait
					}
				case err := <-outerDone:
					t.Fatalf("nested read returned before joining independent work: %v", err)
				case <-ctx.Done():
					t.Fatal("nested read did not join independent work")
				}
			}
			for _, done := range []<-chan error{outerDone, firstDone} {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("joined work did not complete")
				}
			}
		})
	}
}

// TestQueryReentryFreshCache allows nested cache hits without acquiring another execution slot.
func TestQueryReentryFreshCache(t *testing.T) {
	budget, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(Options{Executor: budget, StaleTime: time.Hour})
	defer c.Close()
	if err := Set(c, "inner", 7); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	value, err := Fetch(ctx, c, "outer", func(ctx context.Context) (int, error) {
		load := func(context.Context) (int, error) {
			t.Error("fresh cache hit invoked loader")
			return 0, nil
		}
		if err := Prefetch(ctx, c, "inner", load); err != nil {
			return 0, err
		}
		return Fetch(ctx, c, "inner", load)
	})
	if err != nil || value != 7 || c.Stats().Loads != 1 {
		t.Fatalf("nested cache hit = %d, %v; stats = %+v", value, err, c.Stats())
	}
}
