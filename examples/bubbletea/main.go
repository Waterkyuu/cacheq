package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	tea "charm.land/bubbletea/v2"
	cacheq "github.com/Waterkyuu/cacheq"
)

// main starts a local demonstration that needs no network service or credentials.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	if err := run(client, demoFetcher()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run owns the supplied client and query, releasing both even if terminal startup fails.
func run(client *cacheq.Client, fetch cacheq.Fetcher[string], options ...tea.ProgramOption) error {
	defer client.Close()
	query := cacheq.Query(client, "greeting", fetch)
	defer query.Close()
	initial := model{client: client, query: query, state: query.Snapshot()}
	if _, err := tea.NewProgram(initial, options...).Run(); err != nil {
		return fmt.Errorf("run Bubble Tea example: %w", err)
	}
	return nil
}

// demoFetcher simulates cancellable work and fails every third load to demonstrate retained data.
func demoFetcher() cacheq.Fetcher[string] {
	var calls atomic.Int32
	return func(ctx context.Context) (string, error) {
		attempt := calls.Add(1)
		timer := time.NewTimer(750 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
		}
		if attempt%3 == 0 {
			return "", errors.New("simulated load failure; press r to retry")
		}
		return fmt.Sprintf("Result from load %d", attempt), nil
	}
}
