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

// loadResult lets the test finish a real query operation without using sleep-based timing.
type loadResult struct {
	// value becomes the query's data when the controlled load succeeds.
	value string
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

// awaitSnapshot skips unrelated notifications until the expected observable state is delivered.
func awaitSnapshot(
	t *testing.T,
	ctx context.Context,
	states <-chan cacheq.Snapshot[string],
	matches func(cacheq.Snapshot[string]) bool,
) {
	t.Helper()
	for {
		if matches(receive(t, ctx, states)) {
			return
		}
	}
}

// TestProgramRefreshLifecycle exercises actual terminal input, query commands, and the Bubble Tea event loop.
func TestProgramRefreshLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	requests := make(chan chan loadResult, 1)
	fetch := func(ctx context.Context) (string, error) {
		reply := make(chan loadResult, 1)
		select {
		case requests <- reply:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		select {
		case result := <-reply:
			return result.value, result.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	input, keys := io.Pipe()
	t.Cleanup(func() { input.Close(); keys.Close() })
	states := make(chan cacheq.Snapshot[string], 16)
	finished := make(chan error, 1)
	go func() {
		finished <- run(
			client,
			fetch,
			tea.WithContext(ctx),
			tea.WithInput(input),
			tea.WithOutput(io.Discard),
			tea.WithoutRenderer(),
			tea.WithoutSignalHandler(),
			tea.WithFilter(func(_ tea.Model, msg tea.Msg) tea.Msg {
				if update, ok := msg.(snapshotMsg); ok {
					states <- update.state
				}
				return msg
			}),
		)
	}()
	reply := receive(t, ctx, requests)
	awaitSnapshot(
		t,
		ctx,
		states,
		func(s cacheq.Snapshot[string]) bool { return s.Fetching && !s.HasData },
	)
	reply <- loadResult{value: "first"}
	awaitSnapshot(
		t,
		ctx,
		states,
		func(s cacheq.Snapshot[string]) bool { return s.HasData && !s.Fetching },
	)
	if _, err := io.WriteString(keys, "r"); err != nil {
		t.Fatal(err)
	}
	reply = receive(t, ctx, requests)
	awaitSnapshot(
		t,
		ctx,
		states,
		func(s cacheq.Snapshot[string]) bool { return s.Fetching && s.Data == "first" },
	)
	failure := errors.New("offline")
	reply <- loadResult{err: failure}
	awaitSnapshot(
		t,
		ctx,
		states,
		func(s cacheq.Snapshot[string]) bool {
			retained := !s.Fetching && s.Data == "first"
			return retained && errors.Is(s.Err, failure)
		},
	)
	if _, err := io.WriteString(keys, "r"); err != nil {
		t.Fatal(err)
	}
	reply = receive(t, ctx, requests)
	reply <- loadResult{value: "recovered"}
	awaitSnapshot(
		t,
		ctx,
		states,
		func(s cacheq.Snapshot[string]) bool {
			recovered := !s.Fetching && s.Data == "recovered"
			return recovered && s.Err == nil
		},
	)
	if _, err := io.WriteString(keys, "q"); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, ctx, finished); err != nil {
		t.Fatal(err)
	}
	state := client.Stats()
	correctOutcomes := state.Loads == 3 && state.LoadSuccesses == 2 && state.LoadFailures == 1
	if !correctOutcomes {
		t.Fatalf("terminal refreshes did not match query work: %+v", state)
	}
	if err := cacheq.Get[string](client, "greeting").Err; !errors.Is(err, cacheq.ErrClosed) {
		t.Fatalf("program did not release its client: %v", err)
	}
}

// TestProgramQuitDuringLoad releases a waiting subscription and cancels application-owned work.
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
			func(ctx context.Context) (string, error) {
				close(started)
				<-ctx.Done()
				close(stopped)
				return "", ctx.Err()
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
		func(context.Context) (string, error) { return "unused", nil },
		tea.WithContext(ctx),
		tea.WithInput(nil),
		tea.WithOutput(io.Discard),
		tea.WithoutRenderer(),
		tea.WithoutSignalHandler(),
	)
	if err == nil {
		t.Fatal("canceled startup succeeded")
	}
	if err := cacheq.Get[string](client, "greeting").Err; !errors.Is(err, cacheq.ErrClosed) {
		t.Fatalf("startup failure did not release the client: %v", err)
	}
}
