package main

import (
	"context"
	"fmt"
	"os"
	"time"

	tea "charm.land/bubbletea/v2"
	soroi "github.com/Waterkyuu/soroi"
)

// main starts a local task board that needs no network service or credentials.
func main() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	store := newTaskStore()
	if err := run(client, store, demoFetcher(store)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run owns the client and three subscriptions, releasing them even if terminal startup fails.
func run(client *soroi.Client, store *taskStore, fetch soroi.Fetcher[[]task], options ...tea.ProgramOption) error {
	defer client.Close()
	initial := model{client: client, store: store}
	for view := range initial.queries {
		initial.queries[view] = soroi.Query(client, "tasks", fetch)
		initial.states[view] = initial.queries[view].Snapshot()
	}
	defer initial.closeQueries()
	if _, err := tea.NewProgram(initial, options...).Run(); err != nil {
		return fmt.Errorf("run Bubble Tea example: %w", err)
	}
	return nil
}

// demoFetcher delays each backend read so shared loading and retained data are visible.
func demoFetcher(store *taskStore) soroi.Fetcher[[]task] {
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
