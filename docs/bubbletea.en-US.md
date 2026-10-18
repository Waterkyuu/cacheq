# Shared task views with Bubble Tea

[简体中文](bubbletea.zh-CN.md)

The [task board example](../examples/bubbletea/) gives a list, selected detail,
and completion summary their own subscriptions to one task query. It demonstrates
the code that a UI can delegate to cacheq: shared loads, query activity, retained
data after failure, and refresh notifications.

## Running the example

The independent example module uses Bubble Tea v2 and requires Go 1.26 or later. The core library supports Go 1.22. The example uses an in-memory backend and needs no external service or credentials.

```sh
git clone https://github.com/Waterkyuu/cacheq.git
cd cacheq/examples/bubbletea
go run .
```

After the initial load, all three views display the same dataset and the backend
load count is one. Each refresh adds one backend load, regardless of the number
of subscribers. Local reads include a cancellable 750 ms delay to expose the
loading state.

## Keyboard controls

| Key | Action |
| --- | --- |
| `j` / down, `k` / up | Select a task without fetching |
| Space / enter | Toggle the selected task in the backend and invalidate the query |
| `r` | Invalidate the query and refresh |
| `f` | Fail the next backend read once and refresh |
| `q` / `ctrl+c` | Close subscriptions and exit |

Mutation and refresh controls are ignored while the shared query has an active load.

## Example files

| File | Responsibility |
| --- | --- |
| [main.go](../examples/bubbletea/main.go) | Own the client and terminal program; clean up on exit or startup failure |
| [model.go](../examples/bubbletea/model.go) | Subscribe each view and process snapshots in the message loop |
| [store.go](../examples/bubbletea/store.go) | Own mutable task data and return copies through cancellable reads |

## Data and ownership

The application owns one Client and closes it on exit. Each view owns a
`QueryHandle[[]task]` for the same `"tasks"` key and closes its subscription when
the program stops. All fetchers represent the same dataset; independent views
derive selection and counts from that shared result.

The local backend owns its mutable task slice. Reads return a copy; cached
slices are immutable. Selecting a task only changes UI state and does not fetch.
This small dataset does not need a separate query for each task detail.

## Message-loop integration

Each view schedules one command waiting for its next `Updates()` notification.
The message includes the view identity and its snapshot. `Update` stores that
snapshot and schedules the next wait for the same view.

The commands never mutate the UI model. All display changes happen on Bubble
Tea's serialized loop. Notifications hold the initial and latest snapshots;
slow consumers can skip intermediate phases. Views receive the same result
through separate messages, so they can briefly display different phases while
the loop processes those messages.

## Mutate and refresh

Space or enter changes the selected record in the local backend, then calls
`client.Invalidate("tasks")`. Invalidation marks the shared data stale and starts
one observed refresh. The list, detail, and summary retain their previous data
until the refreshed copy arrives.

The example uses a short synchronous local mutation. A real HTTP or database
mutation belongs in a `tea.Cmd`; send its completion back to `Update` and
invalidate only after success. Separate query keys need explicit invalidation
of all affected keys; cacheq does not infer relationships between datasets.

The `f` key injects a single backend read failure and starts a refresh. All views
show their previous tasks alongside the failure. The next `r` refresh succeeds.
The deliberate failure has no automatic retry, so the error phase stays visible.

## Cleanup and verification

Quit closes every subscription to release waiting commands, then the application
closes its Client to cancel shared work. Startup failure follows the same cleanup
path. Closing just one handle does not cancel a load owned by the Client.

From `examples/bubbletea`:

```sh
go vet ./...
go test -race ./... -count=1
go build -o /tmp/cacheq-bubbletea .
```

The event-loop tests coordinate loads with channels and check notifications for
all three views. They verify request sharing, mutation, retained-data failures,
recovery, and cancellation without relying on the demo's wall-clock delay.
