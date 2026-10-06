package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	cacheq "github.com/Waterkyuu/cacheq"
)

// TestSubscriptionCommands delivers successive notifications and stops waiting after the handle closes.
func TestSubscriptionCommands(t *testing.T) {
	client := cacheq.NewClient(cacheq.Options{})
	t.Cleanup(client.Close)
	query := cacheq.Query(
		client,
		"greeting",
		func(context.Context) (string, error) { return "unused", nil },
		cacheq.QueryOptions{Enabled: false},
	)
	t.Cleanup(query.Close)
	initial := model{client: client, query: query}
	msg := initial.Init()()
	updated, next := initial.Update(msg)
	if updated.(model).state.HasData || next == nil {
		t.Fatal("initial idle state did not schedule another subscription wait")
	}
	if err := cacheq.Set(client, "greeting", "installed"); err != nil {
		t.Fatal(err)
	}
	updated, next = updated.Update(next())
	if state := updated.(model).state; !state.HasData || state.Data != "installed" {
		t.Fatalf("subscription did not deliver installed data: %+v", state)
	}
	query.Close()
	updated, quit := updated.Update(next())
	if !updated.(model).quitting || quit == nil {
		t.Fatal("closed subscription did not quit")
	}
	if _, ok := quit().(tea.QuitMsg); !ok {
		t.Fatal("closed subscription returned another wait")
	}
	if _, next := updated.Update(snapshotMsg{}); next != nil {
		t.Fatal("late state restarted the closed subscription")
	}
}

// TestViewKeepsData distinguishes initial loading, refreshes, and failed refreshes in the display.
func TestViewKeepsData(t *testing.T) {
	client := cacheq.NewClient(cacheq.Options{})
	t.Cleanup(client.Close)
	for _, tc := range []struct {
		// name identifies the user-visible query phase.
		name string
		// state supplies the retained data and request activity to render.
		state cacheq.Snapshot[string]
		// want lists text required to explain the current phase.
		want []string
	}{
		{name: "idle", want: []string{"No data yet", "Status: Idle"}},
		{name: "initial load", state: cacheq.Snapshot[string]{Fetching: true},
			want: []string{"No data yet", "Status: Loading"}},
		{name: "ready", state: cacheq.Snapshot[string]{HasData: true, Data: "previous"},
			want: []string{"previous", "Status: Ready"}},
		{name: "refresh", state: cacheq.Snapshot[string]{HasData: true, Data: "previous", Fetching: true},
			want: []string{"previous", "Refreshing (keeping previous data)"}},
		{name: "failed refresh", state: cacheq.Snapshot[string]{HasData: true, Data: "previous", Err: errors.New("offline")},
			want: []string{"previous", "Status: Load failed", "Error: offline"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := model{client: client, state: tc.state}.View().Content
			for _, want := range tc.want {
				if !strings.Contains(view, want) {
					t.Fatalf("view does not contain %q: %s", want, view)
				}
			}
		})
	}
}
