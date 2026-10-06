package main

import (
	"fmt"

	tea "charm.land/bubbletea/v2"
	cacheq "github.com/Waterkyuu/cacheq"
)

// snapshotMsg transports one query notification into Bubble Tea's serialized update loop.
type snapshotMsg struct {
	// state is immutable query state captured by the subscription command.
	state cacheq.Snapshot[string]
}

// queryClosedMsg ends the subscription loop when its handle or client has been released.
type queryClosedMsg struct{}

// model owns display state; query commands never mutate it from background goroutines.
type model struct {
	// client supplies invalidation and statistics for the application-owned cache.
	client *cacheq.Client
	// query supplies the typed subscription and is released when the UI quits.
	query *cacheq.QueryHandle[string]
	// state is the latest snapshot processed by Update.
	state cacheq.Snapshot[string]
	// actionErr reports a rejected refresh separately from the loader's result error.
	actionErr error
	// quitting prevents late messages from restarting a closed subscription.
	quitting bool
}

// Init starts exactly one asynchronous wait for the subscription's next notification.
func (m model) Init() tea.Cmd {
	return waitForUpdate(m.query.Updates())
}

// Update applies snapshots on the UI loop and turns refresh keys into background invalidation.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if m.quitting {
		return m, nil
	}
	switch msg := msg.(type) {
	case snapshotMsg:
		m.state = msg.state
		return m, waitForUpdate(m.query.Updates())
	case queryClosedMsg:
		m.quitting = true
		return m, tea.Quit
	case tea.KeyPressMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			// Closing the handle releases a command already waiting on Updates during shutdown.
			m.query.Close()
			return m, tea.Quit
		case "r":
			// Invalidate starts background work; Update never waits for the fetcher to finish.
			m.actionErr = m.client.Invalidate("greeting")
		}
	}
	return m, nil
}

// View renders retained data alongside request activity and errors without blocking on a load.
func (m model) View() tea.View {
	value, status := "No data yet", "Idle"
	if m.state.HasData {
		value, status = m.state.Data, "Ready"
	}
	if m.state.Err != nil {
		status = "Load failed"
	}
	if m.state.Fetching {
		status = "Loading"
		if m.state.HasData {
			status = "Refreshing (keeping previous data)"
		}
	}
	content := fmt.Sprintf(
		"cacheq + Bubble Tea\n\n%s\nStatus: %s\nLoads: %d\n",
		value,
		status,
		m.client.Stats().Loads,
	)
	if m.state.Err != nil {
		content += fmt.Sprintf("Error: %v\n", m.state.Err)
	}
	if m.actionErr != nil {
		content += fmt.Sprintf("Refresh error: %v\n", m.actionErr)
	}
	content += "\nr: refresh    q / ctrl+c: quit\n"
	return tea.NewView(content)
}

// waitForUpdate consumes one state asynchronously; Update schedules the next wait after handling it.
func waitForUpdate(updates <-chan cacheq.Snapshot[string]) tea.Cmd {
	return func() tea.Msg {
		state, ok := <-updates
		if !ok {
			return queryClosedMsg{}
		}
		return snapshotMsg{state: state}
	}
}
