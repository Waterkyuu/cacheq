package cacheq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestExecutionOptions resolves inheritance, replacement, and disabling per initiator.
func TestExecutionOptions(t *testing.T) {
	for _, api := range []string{"fetch", "prefetch", "query"} {
		for _, mode := range []string{"inherit", "override", "disable", "disable wins"} {
			t.Run(api+"/"+mode, func(t *testing.T) {
				shared, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
				if err != nil {
					t.Fatal(err)
				}
				separate, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
				if err != nil {
					t.Fatal(err)
				}
				c := NewClient(Options{Executor: shared})
				t.Cleanup(c.Close)
				release, err := shared.acquire(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				options := QueryOptions{}
				switch mode {
				case "override":
					options = QueryOptions{Executor: separate}
				case "disable":
					options = QueryOptions{DisableExecutor: true}
				case "disable wins":
					options = QueryOptions{Executor: separate, DisableExecutor: true}
				}
				calls := 0
				load := func(context.Context) (int, error) {
					calls++
					wantActive := 0
					if mode == "override" {
						wantActive = 1
					}
					if shared.Stats().Active != 1 || separate.Stats().Active != wantActive {
						t.Errorf("wrong budget: shared=%+v separate=%+v", shared.Stats(), separate.Stats())
					}
					return 42, nil
				}
				var value int
				switch api {
				case "fetch":
					value, err = Fetch(context.Background(), c, "key", load, options)
				case "prefetch":
					err = Prefetch(context.Background(), c, "key", load, options)
					value = Get[int](c, "key").Data
				case "query":
					options.Disable = true
					h := Query(c, "key", load, options)
					t.Cleanup(h.Close)
					// Mutation of the source options must not reconfigure this handle's future loads.
					options.Executor = shared
					options.DisableExecutor = false
					value, err = h.Refetch(context.Background())
				}
				if mode == "inherit" {
					if !errors.Is(err, ErrQueueFull) || calls != 0 {
						t.Fatalf("inherited budget = %d, %v, calls=%d", value, err, calls)
					}
				} else if err != nil || value != 42 || calls != 1 {
					t.Fatalf("overridden budget = %d, %v, calls=%d", value, err, calls)
				}
				awaitExecution(t, separate, 0, 0)
			})
		}
	}
}

// TestExecutionOptionsSharedRequest keeps the initiator's budget when a differently configured caller joins.
func TestExecutionOptionsSharedRequest(t *testing.T) {
	shared, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	separate, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(Options{Executor: shared, StaleTime: time.Hour})
	t.Cleanup(c.Close)
	release, err := shared.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	started, finish := make(chan struct{}), make(chan struct{})
	h := Query(c, "key", func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-finish:
			return 42, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}, QueryOptions{Executor: separate})
	t.Cleanup(h.Close)
	<-started
	result := make(chan error, 1)
	go func() {
		value, err := Fetch(context.Background(), c, "key", func(context.Context) (int, error) {
			t.Error("joining caller replaced the loader")
			return 0, nil
		}, QueryOptions{DisableExecutor: true})
		if err == nil && value != 42 {
			err = errors.New("joining caller did not receive shared data")
		}
		result <- err
	}()
	awaitStats(t, c, func(s Stats) bool { return s.MergedRequests == 1 })
	if shared.Stats().Active != 1 || separate.Stats().Active != 1 {
		t.Fatal("joining caller changed active budgets")
	}
	close(finish)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	awaitExecution(t, separate, 0, 0)
}
