# Bubble Tea example

A local, runnable cacheq integration using Bubble Tea v2. Requires Go 1.26 or
later. The example has its own module; `replace` uses the repository's cacheq
implementation without adding TUI dependencies to the core library.

From the repository root:

```sh
cd examples/bubbletea
go run .
```

The first load starts automatically. Press `r` to invalidate the query and
refresh it in the background, keeping the previous result visible. Every third
load fails so that the error and retained result can be seen together. Press
`r` again to recover; press `q` or `ctrl+c` to quit.

The fetcher simulates a 750 ms delay locally and observes context cancellation.
No server or credentials are needed.

- [main.go](main.go) owns the client and query, supplies the demo fetcher, and
  closes both after normal exit or terminal startup failure.
- [model.go](model.go) converts `Updates()` into one Bubble Tea message at a time.
  `Init` starts the first wait; `Update` applies a snapshot and schedules the
  next wait. Closing the subscription ends this chain.

Only `Update` changes the display state. Commands wait for notifications without
blocking the UI. Notifications retain the latest snapshot, so slow consumers
can skip intermediate transitions. Closing a handle alone does not cancel
shared work; this application owns its client and closes it at exit.

```sh
go test -race ./... -count=1
go build -o /tmp/cacheq-bubbletea .
```

Tests run the actual Bubble Tea event loop with pipe input and no renderer.
They cover successive notifications, refresh failures and recovery, quit during
a load, closed subscriptions, and cleanup after canceled startup. Test loads
use channels instead of the demo's delay.
