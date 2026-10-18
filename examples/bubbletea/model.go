package main

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	cacheq "github.com/Waterkyuu/cacheq"
)

// viewID identifies the independent consumer receiving a shared query notification.
type viewID int

const (
	// listView displays every task and owns selection navigation.
	listView viewID = iota
	// detailView displays the selected record from its own subscription.
	detailView
	// summaryView derives completion counts from its own subscription.
	summaryView
)

// snapshotMsg transports one consumer's notification into Bubble Tea's serialized update loop.
type snapshotMsg struct {
	// view routes the notification to the consumer that scheduled the wait.
	view viewID
	// state is an immutable snapshot captured by that consumer's subscription command.
	state cacheq.Snapshot[[]task]
}

// queryClosedMsg ends the subscription loops when a handle or client has been released.
type queryClosedMsg struct{}

// model owns UI state for three consumers of the same task query.
type model struct {
	// client supplies shared query ownership, invalidation, and load statistics.
	client *cacheq.Client
	// store owns backend mutations separately from the immutable cache.
	store *taskStore
	// queries are independently owned subscriptions for the list, detail, and summary.
	queries [3]*cacheq.QueryHandle[[]task]
	// states are the latest snapshots processed for each consumer on the UI loop.
	states [3]cacheq.Snapshot[[]task]
	// selected is the list position shown by the detail view; the demo keeps record ordering stable.
	selected int
	// actionErr reports a rejected mutation or invalidation separately from load failures.
	actionErr error
	// quitting prevents late messages from restarting closed subscription loops.
	quitting bool
}

// Init starts one asynchronous wait per consumer without blocking the UI loop.
func (m model) Init() tea.Cmd {
	commands := make([]tea.Cmd, 0, len(m.queries))
	for view, query := range m.queries {
		commands = append(commands, waitForUpdate(viewID(view), query.Updates()))
	}
	return tea.Batch(commands...)
}

// Update applies consumer snapshots and turns local backend changes into shared invalidation.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if m.quitting {
		return m, nil
	}
	switch msg := msg.(type) {
	case snapshotMsg:
		m.states[msg.view] = msg.state
		return m, waitForUpdate(msg.view, m.queries[msg.view].Updates())
	case queryClosedMsg:
		m.quitting = true
		m.closeQueries()
		return m, tea.Quit
	case tea.KeyPressMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			m.closeQueries()
			return m, tea.Quit
		case "j", "down":
			if m.selected+1 < len(m.states[listView].Data) {
				m.selected++
			}
		case "k", "up":
			if m.selected > 0 {
				m.selected--
			}
		case "r", "f", "space", "enter":
			// Read current shared activity because key input may arrive before a queued UI snapshot.
			state := m.queries[listView].Snapshot()
			if state.Fetching {
				return m, nil
			}
			m.actionErr = nil
			switch msg.String() {
			case "f":
				m.store.fail()
			case "space", "enter":
				if !state.HasData || m.selected >= len(state.Data) {
					return m, nil
				}
				m.actionErr = m.store.toggle(state.Data[m.selected].id)
			}
			if m.actionErr == nil {
				// Mutate the backend first; one invalidation refreshes all three subscribers.
				m.actionErr = m.client.Invalidate("tasks")
			}
		}
	}
	return m, nil
}

// View renders three independently notified views while preserving previous results during refresh.
func (m model) View() tea.View {
	var content strings.Builder
	stats := m.client.Stats()
	fmt.Fprintf(&content, "cacheq task board\n3 subscriptions, 1 shared query\nBackend loads: %d\n\n", stats.Loads)
	list := m.states[listView]
	fmt.Fprintf(&content, "LIST [%s]\n", queryStatus(list))
	if !list.HasData {
		content.WriteString("No data yet\n")
	}
	for i, task := range list.Data {
		cursor, mark := " ", " "
		if i == m.selected {
			cursor = ">"
		}
		if task.done {
			mark = "x"
		}
		fmt.Fprintf(&content, "%s [%s] %s\n", cursor, mark, task.title)
	}
	detail := m.states[detailView]
	fmt.Fprintf(&content, "\nDETAIL [%s]\n", queryStatus(detail))
	if detail.HasData && m.selected < len(detail.Data) {
		selected := detail.Data[m.selected]
		fmt.Fprintf(&content, "Task #%d: %s\nCompleted: %t\n", selected.id, selected.title, selected.done)
	} else {
		content.WriteString("No data yet\n")
	}
	summary := m.states[summaryView]
	fmt.Fprintf(&content, "\nSUMMARY [%s]\n", queryStatus(summary))
	if summary.HasData {
		completed := 0
		for _, task := range summary.Data {
			if task.done {
				completed++
			}
		}
		fmt.Fprintf(&content, "%d / %d completed\n", completed, len(summary.Data))
	} else {
		content.WriteString("No data yet\n")
	}
	if list.Err != nil {
		fmt.Fprintf(&content, "\nError: %v\n", list.Err)
	}
	if m.actionErr != nil {
		fmt.Fprintf(&content, "\nAction error: %v\n", m.actionErr)
	}
	content.WriteString("\nj/k: select  space: toggle  r: refresh  f: fail next refresh  q: quit\n")
	return tea.NewView(content.String())
}

// closeQueries releases every wait command during quit or canceled terminal startup.
func (m model) closeQueries() {
	for _, query := range m.queries {
		if query != nil {
			query.Close()
		}
	}
}

// queryStatus distinguishes initial loading, retained-data refresh, failure, and ready state.
func queryStatus(state cacheq.Snapshot[[]task]) string {
	if state.Fetching {
		if state.HasData {
			return "Refreshing (keeping previous data)"
		}
		return "Loading"
	}
	if state.Err != nil {
		return "Load failed"
	}
	if state.HasData {
		return "Ready"
	}
	return "Idle"
}

// waitForUpdate consumes one notification; Update schedules this consumer's next wait.
func waitForUpdate(view viewID, updates <-chan cacheq.Snapshot[[]task]) tea.Cmd {
	return func() tea.Msg {
		state, ok := <-updates
		if !ok {
			return queryClosedMsg{}
		}
		return snapshotMsg{view: view, state: state}
	}
}
