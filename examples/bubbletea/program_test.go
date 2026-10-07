package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	cacheq "github.com/Waterkyuu/cacheq"
)

// loadResult lets the test finish a real query operation without sleep-based timing.
type loadResult struct {
	// value becomes the shared query's data when the controlled load succeeds.
	value []task
	// err selects the final load outcome.
	err error
}

// receive waits for a test event, using the context deadline only as a deadlock guard.
func receive[V any](t *testing.T, ctx context.Context, events <-chan V) V {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatal("timed out waiting for the Bubble Tea program")
		var zero V
		return zero
	}
}

// awaitViews waits until all three consumers have received the expected phase.
func awaitViews(
	t *testing.T,
	ctx context.Context,
	states <-chan snapshotMsg,
	matches func(cacheq.Snapshot[[]task]) bool,
) {
	t.Helper()
	var seen [3]bool
	for seen != [3]bool{true, true, true} {
		update := receive(t, ctx, states)
		seen[update.view] = matches(update.state)
	}
}

// TestProgramRefreshLifecycle covers shared loading, mutation, retained failures, and recovery on the actual UI loop.
func TestProgramRefreshLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	store := newTaskStore()
	requests := make(chan chan loadResult, 1)
	fetch := func(ctx context.Context) ([]task, error) {
		reply := make(chan loadResult, 1)
		select {
		case requests <- reply:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case result := <-reply:
			return result.value, result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	input, keys := io.Pipe()
	t.Cleanup(func() { input.Close(); keys.Close() })
	states := make(chan snapshotMsg, 64)
	finished := make(chan error, 1)
	go func() {
		finished <- run(
			client,
			store,
			fetch,
			tea.WithContext(ctx),
			tea.WithInput(input),
			tea.WithOutput(io.Discard),
			tea.WithoutRenderer(),
			tea.WithoutSignalHandler(),
			tea.WithFilter(func(_ tea.Model, msg tea.Msg) tea.Msg {
				if update, ok := msg.(snapshotMsg); ok {
					select {
					case states <- update:
					case <-ctx.Done():
					}
				}
				return msg
			}),
		)
	}()
	reply := receive(t, ctx, requests)
	awaitViews(t, ctx, states, func(s cacheq.Snapshot[[]task]) bool { return s.Fetching && !s.HasData })
	if stats := client.Stats(); stats.Loads != 1 || stats.MergedRequests != 2 {
		t.Fatalf("three consumers did not share the initial load: %+v", stats)
	}
	data, err := store.list()
	if err != nil {
		t.Fatal(err)
	}
	reply <- loadResult{value: data}
	awaitViews(t, ctx, states, func(s cacheq.Snapshot[[]task]) bool { return s.HasData && !s.Fetching })
	if _, err := io.WriteString(keys, " "); err != nil {
		t.Fatal(err)
	}
	reply = receive(t, ctx, requests)
	awaitViews(t, ctx, states, func(s cacheq.Snapshot[[]task]) bool {
		return s.Fetching && s.HasData && !s.Data[0].done
	})
	data, err = store.list()
	if err != nil || !data[0].done {
		t.Fatalf("toggle did not change backend state: %v", err)
	}
	reply <- loadResult{value: data}
	awaitViews(t, ctx, states, func(s cacheq.Snapshot[[]task]) bool {
		return s.HasData && !s.Fetching && s.Data[0].done
	})
	if _, err := io.WriteString(keys, "f"); err != nil {
		t.Fatal(err)
	}
	reply = receive(t, ctx, requests)
	awaitViews(t, ctx, states, func(s cacheq.Snapshot[[]task]) bool { return s.Fetching && s.HasData })
	_, failure := store.list()
	if failure == nil {
		t.Fatal("f did not inject a backend failure")
	}
	reply <- loadResult{err: failure}
	awaitViews(t, ctx, states, func(s cacheq.Snapshot[[]task]) bool {
		retained := !s.Fetching && s.HasData && s.Data[0].done
		return retained && errors.Is(s.Err, failure)
	})
	if _, err := io.WriteString(keys, "r"); err != nil {
		t.Fatal(err)
	}
	reply = receive(t, ctx, requests)
	data, err = store.list()
	if err != nil {
		t.Fatal(err)
	}
	reply <- loadResult{value: data}
	awaitViews(t, ctx, states, func(s cacheq.Snapshot[[]task]) bool {
		return !s.Fetching && s.HasData && s.Data[0].done && s.Err == nil
	})
	if _, err := io.WriteString(keys, "q"); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, ctx, finished); err != nil {
		t.Fatal(err)
	}
	stats := client.Stats()
	correctOutcomes := stats.Loads == 4 && stats.LoadSuccesses == 3 && stats.LoadFailures == 1
	if !correctOutcomes {
		t.Fatalf("terminal actions did not match shared query work: %+v", stats)
	}
	if err := cacheq.Get[[]task](client, "tasks").Err; !errors.Is(err, cacheq.ErrClosed) {
		t.Fatalf("program did not release its client: %v", err)
	}
}

// TestProgramQuitDuringLoad releases all waiting subscriptions and cancels application-owned work.
func TestProgramQuitDuringLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	client := cacheq.NewClient(cacheq.Options{})
	t.Cleanup(client.Close)
	started, stopped := make(chan struct{}), make(chan struct{})
	input, keys := io.Pipe()
	t.Cleanup(func() { input.Close(); keys.Close() })
	finished := make(chan error, 1)
	go func() {
		finished <- run(
			client,
			newTaskStore(),
			func(ctx context.Context) ([]task, error) {
				close(started)
				<-ctx.Done()
				close(stopped)
				return nil, ctx.Err()
			},
			tea.WithContext(ctx),
			tea.WithInput(input),
			tea.WithOutput(io.Discard),
			tea.WithoutRenderer(),
			tea.WithoutSignalHandler(),
		)
	}()
	receive(t, ctx, started)
	if _, err := io.WriteString(keys, "q"); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, ctx, finished); err != nil {
		t.Fatal(err)
	}
	receive(t, ctx, stopped)
}

// TestProgramStartupFailure releases the client even when the terminal program cannot start.
func TestProgramStartupFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := cacheq.NewClient(cacheq.Options{})
	err := run(
		client,
		newTaskStore(),
		func(context.Context) ([]task, error) { return nil, nil },
		tea.WithContext(ctx),
		tea.WithInput(nil),
		tea.WithOutput(io.Discard),
		tea.WithoutRenderer(),
		tea.WithoutSignalHandler(),
	)
	if err == nil {
		t.Fatal("canceled startup succeeded")
	}
	if err := cacheq.Get[[]task](client, "tasks").Err; !errors.Is(err, cacheq.ErrClosed) {
		t.Fatalf("startup failure did not release the client: %v", err)
	}
}
