package cacheq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestExecutionOptions resolves inheritance, replacement, disabling, and last-option precedence per initiator.
func TestExecutionOptions(t *testing.T) {
	for _, api := range []string{"fetch", "query"} {
		for _, mode := range []string{"inherit", "override", "disable", "disable wins", "last inherits", "last disables"} {
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
				options := []FetchOptions{{}}
				switch mode {
				case "override":
					options = []FetchOptions{{Executor: separate}}
				case "disable":
					options = []FetchOptions{{DisableExecutor: true}}
				case "disable wins":
					options = []FetchOptions{{Executor: separate, DisableExecutor: true}}
				case "last inherits":
					options = []FetchOptions{{DisableExecutor: true}, {}}
				case "last disables":
					options = []FetchOptions{{Executor: separate}, {DisableExecutor: true}}
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
				if api == "fetch" {
					value, err = Fetch(context.Background(), c, "key", load, options...)
				} else {
					queryOptions := make([]QueryOptions, 0, len(options))
					for _, option := range options {
						queryOptions = append(queryOptions, QueryOptions{
							Disable: true, Executor: option.Executor, DisableExecutor: option.DisableExecutor,
						})
					}
					h := Query(c, "key", load, queryOptions...)
					t.Cleanup(h.Close)
					// Mutation of the source options must not reconfigure this handle's future loads.
					queryOptions[len(queryOptions)-1].Executor = shared
					queryOptions[len(queryOptions)-1].DisableExecutor = false
					value, err = h.Refetch(context.Background())
				}
				if mode == "inherit" || mode == "last inherits" {
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
		}, FetchOptions{DisableExecutor: true})
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
