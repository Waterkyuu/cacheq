package main

import (
	"context"
	"fmt"
	"os"
	"time"

	tea "charm.land/bubbletea/v2"
	cacheq "github.com/Waterkyuu/cacheq"
)

// main starts a local task board that needs no network service or credentials.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	store := newTaskStore()
	if err := run(client, store, demoFetcher(store)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run owns the client and three subscriptions, releasing them even if terminal startup fails.
func run(client *cacheq.Client, store *taskStore, fetch cacheq.Fetcher[[]task], options ...tea.ProgramOption) error {
	defer client.Close()
	initial := model{client: client, store: store}
	for view := range initial.queries {
		initial.queries[view] = cacheq.Query(client, "tasks", fetch)
		initial.states[view] = initial.queries[view].Snapshot()
	}
	defer initial.closeQueries()
	if _, err := tea.NewProgram(initial, options...).Run(); err != nil {
		return fmt.Errorf("run Bubble Tea example: %w", err)
	}
	return nil
}

// demoFetcher delays each backend read so shared loading and retained data are visible.
func demoFetcher(store *taskStore) cacheq.Fetcher[[]task] {
	return func(ctx context.Context) ([]task, error) {
		timer := time.NewTimer(750 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		return store.list()
	}
}
