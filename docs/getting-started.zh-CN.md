# 开始使用 cacheq

cacheq 是 Go 的类型安全查询客户端。一个客户端管理多种数据，让重复请求共享结果，让过期数据在后台刷新，让业务代码专注于业务。

> 同一个进程，共享一个客户端。不同的查询，保留各自的数据类型。

## 安装

需要 Go 1.22 或更新版本。cacheq 核心库没有外部依赖。

```sh
go get github.com/Waterkyuu/cacheq
```

在应用入口创建客户端，将它传给需要数据的业务模块，并在应用退出时关闭。缓存保存在本进程内存中，不会自动跨服务同步。

## 第一个查询

下面的完整程序请求一个用户资料，并复用同一个查询的请求结果。将它保存为 `main.go` 后运行。

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

`Query` 立即返回查询对象，`Fetch` 等待新鲜结果。上面两次使用同一个键，会共享正在进行的请求。

## 数据怎样流动

| 缓存状态 | 使用 `Query` 时的行为 |
| --- | --- |
| 还没有数据 | 后台加载，完成后通知查询对象 |
| 数据仍然新鲜 | 立即使用缓存 |
| 数据已经过期 | 保留旧数据，同时后台刷新 |
| 刷新失败 | 保留已有数据，并提供错误状态 |

用 `Snapshot()` 读取当前状态，用 `Updates()` 接收后续变化。快照是一次读取的值，不会自行更新。

## 选择你需要的功能

- [查询与条件请求](queries.zh-CN.md)：多个类型共用客户端、满足条件才请求、获取更新、手动刷新。
- [缓存与生命周期](cache.zh-CN.md)：读写缓存、预取、过期时间、自动清理、取消和关闭。
- [缓存失效](invalidation.zh-CN.md)：更新数据后让相关缓存一起作废，按条件筛选，选择立即刷新或下次再查。

同一个键只对应一个静态数据类型。查询键应包含影响结果的参数、租户和用户身份，避免不同请求错误地共享数据。
