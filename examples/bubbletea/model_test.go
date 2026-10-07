package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	cacheq "github.com/Waterkyuu/cacheq"
)

// TestSubscriptionCommands routes notifications independently and releases every wait on closure.
func TestSubscriptionCommands(t *testing.T) {
	client := cacheq.NewClient(cacheq.Options{})
	t.Cleanup(client.Close)
	initial := model{client: client}
	for view := range initial.queries {
		initial.queries[view] = cacheq.Query(
			client,
			"tasks",
			func(context.Context) ([]task, error) { return nil, nil },
			cacheq.QueryOptions{Disable: true},
		)
	}
	t.Cleanup(initial.closeQueries)
	commands := initial.Init()().(tea.BatchMsg)
	next := make([]tea.Cmd, len(commands))
	for view, command := range commands {
		updated, wait := initial.Update(command())
		initial = updated.(model)
		next[view] = wait
		if initial.states[view].HasData || wait == nil {
			t.Fatal("initial idle state did not schedule another subscription wait")
		}
	}
	data := []task{{id: 1, title: "installed"}}
	if err := cacheq.Set(client, "tasks", data); err != nil {
		t.Fatal(err)
	}
	for view, command := range next {
		updated, wait := initial.Update(command())
		initial = updated.(model)
		next[view] = wait
		if state := initial.states[view]; !state.HasData || !slices.Equal(state.Data, data) {
			t.Fatalf("view %d did not receive installed data: %+v", view, state)
		}
	}
	initial.queries[listView].Close()
	updated, quit := initial.Update(next[listView]())
	if !updated.(model).quitting || quit == nil {
		t.Fatal("closed subscription did not quit")
	}
	if _, ok := quit().(tea.QuitMsg); !ok {
		t.Fatal("closed subscription returned another wait")
	}
	for _, view := range []viewID{detailView, summaryView} {
		if _, ok := next[view]().(queryClosedMsg); !ok {
			t.Fatalf("view %d still waits after shutdown", view)
		}
	}
	if _, next := updated.Update(snapshotMsg{}); next != nil {
		t.Fatal("late state restarted a closed subscription")
	}
}

// TestViewKeepsData shows the same retained task and load phase in all three consumers.
func TestViewKeepsData(t *testing.T) {
	client := cacheq.NewClient(cacheq.Options{})
	t.Cleanup(client.Close)
	previous := []task{{id: 1, title: "previous", done: true}}
	for _, tc := range []struct {
		// name identifies the user-visible query phase.
		name string
		// state supplies retained data and request activity to each view.
		state cacheq.Snapshot[[]task]
		// want lists text required to explain the current phase.
		want []string
	}{
		{name: "idle", want: []string{"No data yet", "[Idle]"}},
		{name: "initial load", state: cacheq.Snapshot[[]task]{Fetching: true},
			want: []string{"No data yet", "[Loading]"}},
		{name: "ready", state: cacheq.Snapshot[[]task]{HasData: true, Data: previous},
			want: []string{"[x] previous", "Completed: true", "1 / 1 completed", "[Ready]"}},
		{name: "refresh", state: cacheq.Snapshot[[]task]{HasData: true, Data: previous, Fetching: true},
			want: []string{"previous", "Refreshing (keeping previous data)"}},
		{name: "failed refresh", state: cacheq.Snapshot[[]task]{HasData: true, Data: previous, Err: errors.New("offline")},
			want: []string{"previous", "[Load failed]", "Error: offline"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := model{client: client}
			for view := range m.states {
				m.states[view] = tc.state
			}
			view := m.View().Content
			for _, want := range tc.want {
				if !strings.Contains(view, want) {
					t.Fatalf("view does not contain %q: %s", want, view)
				}
			}
			for _, label := range []string{"LIST", "DETAIL", "SUMMARY"} {
				if !strings.Contains(view, label) {
					t.Fatalf("missing %s consumer: %s", label, view)
				}
			}
		})
	}
}

// TestSelectionBounds keeps keyboard navigation within the stable task list.
func TestSelectionBounds(t *testing.T) {
	client := cacheq.NewClient(cacheq.Options{})
	t.Cleanup(client.Close)
	m := model{client: client}
	m.states[listView].Data = []task{{id: 1}, {id: 2}}
	for _, tc := range []struct {
		// key moves the cursor up or down.
		key rune
		// want is the bounded position after applying this key.
		want int
	}{
		{key: 'k', want: 0},
		{key: 'j', want: 1},
		{key: 'j', want: 1},
		{key: 'k', want: 0},
	} {
		updated, _ := m.Update(tea.KeyPressMsg{Code: tc.key})
		m = updated.(model)
		if m.selected != tc.want {
			t.Fatalf("after %c: selected=%d, want=%d", tc.key, m.selected, tc.want)
		}
	}
}

// TestTaskStoreIsolation preserves cached snapshots across mutations and consumes failures once.
func TestTaskStoreIsolation(t *testing.T) {
	store := newTaskStore()
	previous, err := store.list()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.toggle(previous[0].id); err != nil {
		t.Fatal(err)
	}
	current, err := store.list()
	if err != nil || previous[0].done || !current[0].done {
		t.Fatalf("mutation changed a cached slice or did not reach the backend: %v", err)
	}
	current[0].title = "caller modification"
	if next, _ := store.list(); next[0].title == current[0].title {
		t.Fatal("backend shares mutable data with consumers")
	}
	if err := store.toggle(999); err == nil {
		t.Fatal("unknown task mutation succeeded")
	}
	store.fail()
	if _, err := store.list(); err == nil {
		t.Fatal("injected read failure succeeded")
	}
	if _, err := store.list(); err != nil {
		t.Fatalf("backend did not recover after one failure: %v", err)
	}
}
