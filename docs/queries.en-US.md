# Queries, shared data, and conditional loading

[简体中文](queries.zh-CN.md) · [Cache and lifecycle](cache.en-US.md) · [Invalidation](invalidation.en-US.md)

One `Client` manages different result types. `cacheq.Query` is the primary subscription API, and its result type is inferred from the fetcher. Go cannot declare type parameters on individual methods, so typed operations are package functions; shared cache controls remain client methods.

## One client, different result types

This complete program can be copied into `main.go`. The fixed loaders demonstrate typing; an actual HTTP loader appears below.

```go
package main

import (
	"context"
	"fmt"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// User is a detail result and an element of a list result.
type User struct {
	// Name contains the user's display name.
	Name string
}

// Settings contains application configuration independent of user data.
type Settings struct {
	// Theme selects the application's appearance.
	Theme string
}

// main shares one application-owned client across differently typed queries.
func main() {
	client := cacheq.NewClient(cacheq.Options{
		StaleTime: time.Minute,
		GCTime:    5 * time.Minute,
	})
	defer client.Close()

	getUser := func(context.Context) (User, error) {
		return User{Name: "Alice"}, nil
	}
	getUsers := func(context.Context) ([]User, error) {
		return []User{{Name: "Alice"}}, nil
	}
	getSettings := func(context.Context) (Settings, error) {
		return Settings{Theme: "dark"}, nil
	}
	detail := cacheq.Query(client, "user:42", getUser)
	users := cacheq.Query(client, "users", getUsers)
	settings := cacheq.Query(client, "settings", getSettings)
	defer detail.Close()
	defer users.Close()
	defer settings.Close()

	// Fetch waits for the same load already started by Query.
	user, err := cacheq.Fetch(
		context.Background(),
		client,
		"user:42",
		getUser,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(user.Name)
	listState := users.Snapshot()        // Snapshot[[]User]
	settingsState := settings.Snapshot() // Snapshot[Settings]
	_ = listState
	_ = settingsState
}
```

Each key has one static result type: `"user:42"` stores `User`; `"users"` stores `[]User`. Include all parameters, tenants, and identities affecting a result in its key. Strings, integers, and comparable structs are supported. Nil keys, slices, and maps are rejected. Sharing is in-process; this is not automatic cross-service synchronization.

## `Query`: current data and background loading

```go
users := cacheq.Query(client, "users", getUsers)
defer users.Close()
state := users.Snapshot()
```

Missing data starts an initial request. Fresh data is reused. Stale data remains available while a background request runs. Neither `Query` nor `Snapshot()` waits for HTTP completion. A snapshot is a value; a previously returned variable does not update itself.

Omitting options enables loading. Explicit `cacheq.QueryOptions{}` disables it because `Enabled` is false. If multiple options are supplied, the last wins. Construction failures appear in `Snapshot().Err` and the initial `Updates()` value; the failed handle's channel then closes.

## `Updates`: receive the latest state

```go
for state := range users.Updates() {
	if state.Err != nil {
		// Report the error while retaining any available old data.
	}
	if state.HasData {
		// Forward state.Data to the application's UI or event loop.
	}
}
```

The channel provides initial and subsequent latest states. Request transitions, writes, and invalidation can publish updates. Slow consumers may skip intermediate states, so this is not an event log. The application must release the handle from an independent shutdown path: a `defer` after a blocking `range` cannot itself end that range.

## `SetEnabled`: load only when a condition permits

```go
users := cacheq.Query(
	client,
	"users",
	getUsers,
	cacheq.QueryOptions{
		Enabled: false,
	},
)
defer users.Close()

// Called by the application's login-state change handler.
if err := users.SetEnabled(true); err != nil {
	return err
}
```

Disabled handles do not start initial or invalidation-triggered requests. They still read cached data and receive updates produced by other consumers. Enabling loads missing or stale data. Go does not watch a boolean variable, so update permission explicitly. Each handle has its own permission; disabling one neither disables another nor cancels an existing request.

## `Refetch`: manually refresh with the retained fetcher

```go
updatedUsers, err := users.Refetch(ctx)
if err != nil {
	return err
}
_ = updatedUsers
```

This waits for a result regardless of freshness or enablement, sharing an existing same-key request if one is running. Released handles return `ErrQueryClosed`.

## `Fetch`: wait for fresh data without a subscription

```go
users, err := cacheq.Fetch(
	ctx,
	client,
	"users",
	getUsers,
)
if err != nil {
	return err
}
_ = users // []User
```

Fresh cached results return immediately; stale or missing results wait for a load. Use this in HTTP handlers, jobs, or business steps requiring the result. It shares data and in-flight work with `Query`. `Fetcher[V]` is your `func(context.Context) (V, error)` loader. Create the client once at the application boundary, rather than once per request.

## Snapshot fields

| Field | Meaning |
| --- | --- |
| `Data` | Current or retained previous data |
| `HasData` | Distinguishes usable zero values from absent data |
| `Status` | `Idle`, `Pending`, `Success`, or `Error` |
| `Err` | Latest failure; previous usable data can remain available |
| `Stale` | Missing, expired, or explicitly invalidated data |
| `Fetching` | Active initial load or background refresh |
| `UpdatedAt` | Last installation of usable data |
| `ExpiresAt` | Absolute freshness deadline; zero after invalidation |

`Pending` means an initial request is running without old data. A disabled missing query is `Idle`. Result status and request activity are separate: background refresh can be `Success` with `Fetching: true`. Becoming stale does not itself start a request or publish a timed update; this is not polling.

## HTTP cancellation

The following loader needs `context`, `encoding/json`, `fmt`, and `net/http`. `endpoint` is the application's user-list URL.

```go
getUsers := func(ctx context.Context) ([]User, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		endpoint,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create users request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch users: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch users: HTTP %d", response.StatusCode)
	}
	users := []User{}
	if err := json.NewDecoder(response.Body).Decode(&users); err != nil {
		return nil, fmt.Errorf("decode users: %w", err)
	}
	return users, nil
}
```

Automatic `Query` work belongs to the client. Loads started by `Fetch` or manual `Refetch` belong to the starting caller's context. A waiting caller's cancellation ends only its wait. If the owner cancels, remaining callers may start a replacement request. Closing a handle releases its subscription without canceling shared work; `client.Cancel` and `client.Close` cancel requests. Loaders must honor their context. `Options.Timeout` bounds the complete load, including retries and backoff.

## Bubble Tea: connect subscriptions to the message loop

The complete runnable code lives in `examples/bubbletea`. It uses Bubble Tea v2 and requires Go 1.26+. Its independent Go module keeps TUI dependencies separate from the core library, which still supports Go 1.22.

```sh
cd examples/bubbletea
go run .
```

The [model](../examples/bubbletea/model.go) starts a `tea.Cmd` in `Init` to wait for one notification from `Updates()`. It returns that snapshot as a message; `Update` applies it and schedules the next wait. Only the message loop mutates display state. Slow consumers may skip intermediate notifications and receive the latest snapshot.

Press `r` to invalidate the query and refresh in the background while keeping previous data visible. Every third load simulates a failure; press `r` again to recover. Press `q` or `ctrl+c` to close the subscription and release its waiting command. The [entry point](../examples/bubbletea/main.go) closes the client after normal exit or startup failure, canceling remaining work; closing only a query handle does not cancel shared loads.

From the example directory, run `go test -race ./... -count=1` to verify refresh, failure, recovery, and exit cleanup through the actual Bubble Tea message loop.

## Run real HTTP workflows

```sh
go test -race ./e2e -run 'Test(SharedClientMutation|ConditionalConsumers|HTTPSharedBackgroundLoad)' -count=1
```

[The lifecycle e2e suite](../e2e/query_lifecycle_test.go) runs an isolated local HTTP service and verifies heterogeneous results, mutation refresh, enablement, and request sharing.
