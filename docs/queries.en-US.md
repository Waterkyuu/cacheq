# Queries, shared data, and conditional loading

[简体中文](queries.zh-CN.md) · [Cache and lifecycle](cache.en-US.md) · [Invalidation](invalidation.en-US.md)

One `Client` manages different result types. `Query` subscribes to cached state and background loading; `Fetch` waits for fresh data without retaining a subscription. Both infer their result type from the loader and share data and active requests for the same key.

## Quick start: multiple result types

Save this complete program as `main.go` in a module importing cacheq, then run `go run .`. The fixed loaders demonstrate typing; an actual HTTP loader appears below.

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

	// Fetch reuses fresh data or joins the active load started by Query.
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

Expected output:

```text
Alice
```

Each key has one static result type: `"user:42"` stores `User`; `"users"` stores `[]User`. Include all parameters, tenants, and identities affecting a result in its key. Strings, integers, and comparable structs are supported. Nil keys, slices, and maps are rejected. Sharing is in-process; this is not automatic cross-service synchronization.

## API snippet prerequisites

The following partial snippets use `client`, `getUser`, and `getUsers` from the quick start and a non-nil `ctx`. Place snippets containing `return err` inside a function returning `error`. Each snippet is independent; add the imports it uses.

## `Query`: current data and background loading

```go
users := cacheq.Query(client, "users", getUsers)
defer users.Close()
state := users.Snapshot()
```

Missing data starts an initial request. Fresh data is reused. Stale data remains available while a background request runs. Neither `Query` nor `Snapshot()` waits for HTTP completion. A snapshot is a value; a previously returned variable does not update itself.

Omitting options, supplying an empty `cacheq.QueryOptions{}`, or leaving `Disable` false enables loading. Set `Disable: true` to disable automatic loading. If multiple options are supplied, the last wins. Construction failures appear in `Snapshot().Err` and the initial `Updates()` value; the failed handle's channel then closes.

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
		Disable: true,
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

Fresh cached results return immediately; stale or missing results wait for a load. Use this in HTTP handlers, jobs, or business steps requiring the result. It shares data and in-flight work with `Query`. `Fetcher[V]` is a `func(context.Context) (V, error)` business loader. Create the client once at the application boundary and inject it into request handlers.

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

## Bubble Tea integration

The [Bubble Tea guide](bubbletea.en-US.md) documents the runnable task board,
keyboard controls, message-loop integration, and cleanup. It uses Bubble Tea v2
in an independent module requiring Go 1.26; the core library supports Go 1.22.

The [model](../examples/bubbletea/model.go) waits for one `Updates()` notification
in a `tea.Cmd`, sends the snapshot to `Update`, then schedules the next wait.
Only the message loop mutates display state.

## Verification

```sh
go test -race ./e2e -run 'Test(SharedClientMutation|ConditionalConsumers|HTTPSharedBackgroundLoad)' -count=1
```

[The lifecycle e2e suite](../e2e/query_lifecycle_test.go) runs an isolated local HTTP service and verifies heterogeneous results, mutation refresh, enablement, and request sharing.
