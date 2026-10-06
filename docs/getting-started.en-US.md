# Getting started with cacheq

cacheq is a typed query client for Go. Share one client across different result types, reuse in-flight requests, and refresh stale data in the background.

> One process. One shared client. Every query keeps its own result type.

## Installation

Requires Go 1.22 or later. The cacheq core library has no external dependencies.

```sh
go get github.com/Waterkyuu/cacheq
```

Create a client at the application boundary, inject it into consumers, and close it when the application stops. The cache is in process memory; it does not synchronize across services.

## Your first query

This complete program requests a public GitHub profile and shares its request with an imperative fetch. Save it as `main.go` and run it.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "time"

    "github.com/Waterkyuu/cacheq"
)

// User contains the GitHub profile fields this application needs.
type User struct {
    // Login identifies the account returned by GitHub.
    Login string `json:"login"`
    // PublicRepos counts the account's public repositories.
    PublicRepos int `json:"public_repos"`
}

// loadUser requests a public profile while honoring caller cancellation.
func loadUser(ctx context.Context) (User, error) {
    var user User
    request, err := http.NewRequestWithContext(
        ctx,
        http.MethodGet,
        "https://api.github.com/users/Waterkyuu",
        nil,
    )
    if err != nil {
        return user, err
    }
    response, err := http.DefaultClient.Do(request)
    if err != nil {
        return user, err
    }
    defer response.Body.Close()
    if response.StatusCode != http.StatusOK {
        return user, fmt.Errorf("load user: HTTP %d", response.StatusCode)
    }
    err = json.NewDecoder(response.Body).Decode(&user)
    return user, err
}

// main shares one client and waits for the profile needed by its business logic.
func main() {
    client := cacheq.NewClient(cacheq.Options{
        StaleTime: time.Minute,
        GCTime:    5 * time.Minute,
        Timeout:   10 * time.Second,
    })
    defer client.Close()

    profile := cacheq.Query(client, "github:user:Waterkyuu", loadUser)
    defer profile.Close()

    user, err := cacheq.Fetch(
        context.Background(),
        client,
        "github:user:Waterkyuu",
        loadUser,
    )
    if err != nil {
        fmt.Println(err)
        return
    }
    fmt.Printf("%s: %d public repositories\n", user.Login, user.PublicRepos)
}
```

`Query` returns a handle immediately. `Fetch` waits for fresh data. These calls share the same key and therefore the same in-flight request.

## How data flows

| Cache state | When you use `Query` |
| --- | --- |
| No data yet | Load in the background and notify consumers |
| Fresh data | Reuse cached data immediately |
| Stale data | Keep previous data while refreshing |
| Refresh failed | Retain available data and report the error |

Read current state through `Snapshot()` and receive later updates through `Updates()`. A snapshot is a value, so it does not update itself.

## Find your next step

- [Queries & conditions](queries.en-US.md): shared clients with different result types, conditional loading, updates, and manual refresh.
- [Cache & lifecycle](cache.en-US.md): reads, writes, prefetch, expiry, automatic cleanup, cancellation, and closing.
- [Invalidation](invalidation.en-US.md): invalidate related data together, select keys with predicates, and choose immediate or deferred refresh.

Each key has one static result type. Include parameters, tenant identity, and user identity that affect the result in your keys to avoid sharing unrelated data.
