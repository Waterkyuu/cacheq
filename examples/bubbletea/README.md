# Bubble Tea task board

A local task board built with cacheq and Bubble Tea v2. Requires Go 1.26 or later.
The example has its own module; `replace` uses the repository's cacheq
implementation without adding TUI dependencies to the core library.

From the repository root:

```sh
cd examples/bubbletea
go run .
```

The list, selected detail, and completion summary each own a
`QueryHandle[[]task]` for the same `"tasks"` key on one Client. The detail and
summary derive their display from the shared task list; they do not issue
separate backend requests. The first load supplies all three consumers.
The displayed backend load count makes this reuse visible.

| Key | Action |
| --- | --- |
| `j` / down, `k` / up | Select a task; selection does not load data |
| space / enter | Toggle the selected task in the local backend, then invalidate the shared query |
| `r` | Refresh in the background while retaining previous data |
| `f` | Fail the next refresh once; all views keep their previous data |
| `q` / ctrl+c | Quit and release the subscriptions and Client |

Try this sequence:

1. Wait for the first result: three views, one backend load.
2. Press space: the list checkbox, detail completion flag, and summary update
   after one additional load.
3. Press `f`: inspect the refresh phase and then the retained result with a
   failure in every view.
4. Press `r`: recover from the failure using one shared refresh.

Refresh and mutation keys are ignored during an active load. Backend reads
simulate a cancellable 750 ms delay. No server or credentials are needed;
task changes last only for this process.

- [main.go](main.go) owns the Client and subscriptions, supplies the delayed
  fetcher, and releases resources after normal exit or terminal startup failure.
- [model.go](model.go) schedules one `Updates()` wait per consumer and routes
  notifications into Bubble Tea's serialized update loop.
- [store.go](store.go) owns mutable backend records and returns detached slices,
  so a mutation cannot change the cached result before refresh completes.

Only `Update` changes display state. Subscription commands wait without blocking
the UI. Slow consumers may skip intermediate notifications. Closing a handle
does not cancel shared work; this application owns its Client and closes it at
exit. The local backend mutation is short and synchronous; use a `tea.Cmd`
for a real network mutation and invalidate after it succeeds.

The [English walkthrough](../../docs/bubbletea.en-US.md) and
[中文实践文档](../../docs/bubbletea.zh-CN.md) explain the integration.

```sh
go vet ./...
go test -race ./... -count=1
go build -o /tmp/cacheq-bubbletea .
```

Tests run the actual Bubble Tea event loop with pipe input and no renderer.
They verify one initial load for three subscribers, mutation updates in every
view, refresh failures and recovery, quit during a load, closed subscriptions,
and canceled-startup cleanup. Controlled loads use channels instead of the
demo's delay.
