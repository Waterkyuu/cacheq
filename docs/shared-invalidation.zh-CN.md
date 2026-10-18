# 共享失效通知与远端标签

[English](shared-invalidation.en-US.md) · [共享结果缓存](shared-cache.zh-CN.md) · [查询标签](query-tags.zh-CN.md)

## 概述

`Client.WatchShared` 接收远端失效通知，并使绑定到同一个存储实例的本地查询失效。已启用订阅按照 watcher 策略刷新；禁用的查询保留数据并发布 stale 快照。通知是可选能力，没有 watcher 的 Client 继续依赖正常的新鲜度截止时间。

设置 `redistore.Options.EnableNotifications: true` 后，存储发布变更并允许 watcher。该字段默认为 false，保持原有纯存储命令与 ACL 契约；需要及时传播通知的写入方均应启用。

远端标签可以将不同共享 key 和结果类型关联成组。存储启用 `redistore.Options.EnableTags`，各参与查询启用 `SharedCacheOptions.ShareTags`，由 `Definition.Tags` 提供远端登记的标签。`Client.InvalidateSharedTags` 更换远端标签版本，并使发起 Client 中的匹配查询失效。同一远端命名空间中的其他 Client 通过 watcher 接收该操作。

权威数据库或 API 写入必须先成功提交，再使缓存失效。通知投递不提供分布式事务、跨实例同步完成屏障或持久化事件重放。

## 安装

核心契约继续兼容 Go 1.22。可选 Redis 模块要求 Go 1.26，依赖版本由自身 go.mod 声明。在共享缓存原有命令之外，通知需要 `PUBLISH` 和 `SUBSCRIBE`，标签需要 `HGET`、`HSET`、`HGETALL` 和 `HEXISTS`。应使用可写主节点连接，配置相应 ACL 权限、context 和网络操作超时。

在仓库检出的目录中，在一个终端启动不启用持久化的临时 Redis：

```sh
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

在另一个终端，从仓库根目录准备已有的示例模块。检入的 `go.mod` 通过本地替换引用核心库和 Redis 适配器，不要求适配器已发布：

```sh
cd examples/redis
go mod download
```

## 使用方式

以下片段扩展应用中返回 `error` 的 `run()` 函数，不是独立程序。复用[创建 Redis 存储](shared-cache.zh-CN.md#创建-redis-存储)中配置的 `remote`（`*redis.Client`）及服务生命周期 context `ctx`。应用拥有该连接并负责配置超时。所需导入为 `context`、`fmt`、`time`、`github.com/Waterkyuu/soroi` 和 `redistore "github.com/Waterkyuu/soroi/store/redis"`。下方完整程序包含全部初始化和导入。

已有查询返回以下资料类型，在包级别声明：

```go
// User represents the immutable source value shared by detail and list queries.
type User struct {
	// Name is the display name returned by the authoritative source.
	Name string
}
```

### 为存储启用通知

使用以下配置替换应用原有的 `redistore.New` 调用。`EnableNotifications` 默认为 false；设为 true 后，存储发布失效并允许订阅。生产环境应保留应用已有的 Prefix；`soroi-notification-usage` 是示例命名空间。需要传播通知的写入方均须启用发布。在[共享缓存命令](shared-cache.zh-CN.md#安装)之外，Redis ACL 还需要 `PUBLISH` 和 `SUBSCRIBE` 权限。

```go
store, err := redistore.New(remote, redistore.Options{
	Prefix: "soroi-notification-usage", EnableNotifications: true,
})
if err != nil {
	return err
}
```

返回的 `store` 借用 `remote`；构造函数只验证配置，不执行 Redis I/O。同一个 Client 的查询绑定和 watcher 必须复用这个存储对象。不同进程可以为同一远端命名空间各自创建对象。启用通知不要求启用远端标签。

### 将已有查询绑定到通知存储

复用应用按照[定义查询](shared-cache.zh-CN.md#定义使用共享缓存的查询)创建的 `definition`，其类型为 `soroi.Definition[User]`。其中的 Fetcher 从应用数据源读取已提交的用户资料。`User` 是上方声明的资料结果类型。

`NewSharedCache` 从 `SharedCacheOptions[User]` 推断结果类型 `User`。共享键包含租户、记录身份和模式版本；`Format` 指定 JSON 模式，`TTL` 将共享新鲜期限制为五分钟。更新配置时应保留应用原有的共享键、格式和 TTL。通过 Client 使用定义之前，将返回的共享缓存赋给该定义。

```go
shared, err := soroi.NewSharedCache(store, soroi.SharedCacheOptions[User]{
	Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute,
})
if err != nil {
	return err
}
definition.Shared = shared
```

构造和赋值均不执行 Redis I/O。后续加载会将查询的本地键绑定到这个存储及共享键。按键失效不需要标签。

### 为 Client 与存储建立一个 watcher

复用应用中长期存在的 `client`，类型为 `*soroi.Client`（完整示例中的 `a`）。在启动时，为这个 Client 与存储组合调用一次 `WatchShared`。该调用执行订阅 I/O，等待订阅确认，并返回 `*SharedWatcher`；建立失败时返回错误，不保留读取协程。已有的存储绑定查询会变为 stale，以修复订阅建立之前的区间。

```go
watch, err := client.WatchShared(ctx, store, soroi.InvalidateOptions{
	Refetch: soroi.RefetchNone,
})
if err != nil {
	return err
}
defer watch.Close()
```

watcher 使用服务生命周期 `ctx`，不能使用短暂的请求 context；Client 关闭也会结束它的生命周期。示例选择 `RefetchNone`，因此通知保留数据并标记 stale，不自动加载。省略配置时使用 `RefetchObserved`，允许已启用的订阅刷新。其他进程在各自的 Client 和存储上建立自己的 watcher。

### 加载绑定的查询

通过同一个 Client 加载已有定义。`Fetch` 返回 `User` 或错误；新鲜本地命中不执行 Redis I/O，需要加载时可能读取 Redis 并调用数据源 Fetcher。返回的用户资料可以立即使用；通知向其他实例的投递仍然是异步的。

```go
user, err := definition.Fetch(ctx, client)
if err != nil {
	return err
}
fmt.Printf("loaded user: %s\n", user.Name)
```

### 在业务提交后使一个共享键失效

权威数据库或 API 写入成功后，使用同一个 Client 调用 `definition.InvalidateShared`。该方法先执行远端按键失效并发布通知，再将定义对应的精确本地键标记为 stale。`RefetchNone` 将加载留给下一次读取；调用不会等待其他实例的 watcher 或刷新完成。

```go
// The database or API write has already committed successfully.
if err := definition.InvalidateShared(ctx, client, soroi.InvalidateOptions{
	Refetch: soroi.RefetchNone,
}); err != nil {
	return fmt.Errorf("invalidate committed user change: %w", err)
}
```

远端失败时返回错误，本地状态不变；超时可能导致远端结果不确定。应独立处理缓存失效失败，不能仅因失效失败而重试已提交的业务写入，否则可能重复产生业务效果。`shared.Invalidate(ctx)` 是只操作远端的替代方法，不会同步处理发起 Client 的本地失效。

### 启用可选的远端标签

按键失效已满足需求时可跳过本节。需要将详情和列表记录关联成组时，创建 `EnableTags: true` 的存储并建立它的 watcher。独立的示例前缀使可选流程与上方按键流程互不干扰。标签模式使用独立 Redis 布局和通知 channel；同一标签命名空间中的参与方必须采用一致的 Prefix 和 EnableTags。标签额外需要 `HGET`、`HSET`、`HGETALL` 和 `HEXISTS` 权限。

```go
tagStore, err := redistore.New(remote, redistore.Options{
	Prefix: "soroi-tag-usage", EnableNotifications: true, EnableTags: true,
})
if err != nil {
	return err
}
tagWatch, err := client.WatchShared(ctx, tagStore, soroi.InvalidateOptions{
	Refetch: soroi.RefetchNone,
})
if err != nil {
	return err
}
defer tagWatch.Close()
```

存储构造不执行 I/O；`WatchShared` 按前述确认和生命周期规则订阅。每个标签共享缓存复用 `tagStore`，这个 Client 与存储组合复用 `tagWatch`。启用标签不会迁移原有按键布局中的记录。

### 登记查询定义的远端标签

在 `SharedCacheOptions[User]` 中启用 `ShareTags`，并在加载前设置 `Definition.Tags`。`ShareTags` 请求远端登记；不启用时，新声明的标签仅保留在本地。存储无法登记标签时，构造返回 `ErrSharedCapability`。

以下代码复制已有定义并保留其数据源 Fetcher，使用新的本地键，防止新鲜本地结果跳过首次共享读取。标签包含租户身份，将详情记录与声明同一标签的列表记录关联。`NewSharedCache` 仅验证配置，不执行 I/O；`tagged.Fetch` 执行共享读取并登记关系，即使命中 Redis 缓存也会登记。

```go
tagShared, err := soroi.NewSharedCache(tagStore, soroi.SharedCacheOptions[User]{
	Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute,
	ShareTags: true,
})
if err != nil {
	return err
}
tagged := definition
tagged.Key = "tagged-detail"
tagged.Shared = tagShared
tagged.Tags = []string{"tenant:42:users"}
if _, err := tagged.Fetch(ctx, client); err != nil {
	return err
}
```

构造定义、创建禁用的查询对象或命中新鲜本地数据都不会登记远端关系。为已经加载的本地键添加标签时，应使用更新后的定义创建查询对象，并显式调用 `Refetch(ctx)` 强制登记。远端关系采用追加方式；调整分组可能需要新的版本化共享键或命名空间。

### 在业务提交后使已登记标签失效

`client.InvalidateSharedTags` 接收同一个 `tagStore` 和待失效标签。它先执行远端 I/O，再使当前 Client 中匹配的绑定查询失效。匹配采用区分大小写的精确 OR 规则：任意一个标签匹配即可，空列表不执行操作。其他 Client 通过各自的 watcher 异步接收通知。

```go
// The business write affecting the tagged records has already committed.
operation, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()
if err := client.InvalidateSharedTags(
	operation,
	tagStore,
	[]string{"tenant:42:users"},
	soroi.InvalidateOptions{Refetch: soroi.RefetchNone},
); err != nil {
	return fmt.Errorf("invalidate committed users change: %w", err)
}
```

两秒的操作截止时间限制本次标签调用；与共享键失效不同，该方法直接使用传入的 context。远端错误不会改变本地状态；已成功的远端操作不会因后续本地处理失败而回滚。应将这种失败与业务写入分开处理。

### 检查 watcher 诊断并释放订阅

`watch.Stats()` 读取统计，不执行 Redis I/O。`LastError` 在恢复后仍保留最近一次连接或解码错误；结合 `Errors` 和 `Resyncs` 可以诊断通知中断。处理结束并释放订阅资源后，`Done()` 关闭。事件流意外关闭会终止 watcher；如果应用需要恢复监听，应先排查失败原因再重新创建。

```go
stats := watch.Stats()
if stats.LastError != nil {
	fmt.Printf("watcher errors: %d, last error: %v\n", stats.Errors, stats.LastError)
}
select {
case <-watch.Done():
	fmt.Println("watcher stopped")
default:
}
watch.Close()
```

`Close()` 取消处理并等待订阅释放。该方法幂等，显式停机清理可以与延迟清理共存。关闭或取消 watcher 不会关闭它的 Client 或借用的 Redis 连接。停机时，应先释放 watcher 和 Client，再关闭 `remote`；查询对象仍需要独立清理。

## 完整示例

下方程序结合远端标签和两个 Client，在提交数据源变更后使用户详情及列表失效。程序维护于 [examples/redis/shared_invalidation/main.go](../examples/redis/shared_invalidation/main.go)。如需运行修改后的副本，在已配置的 `examples/redis` 模块中新建 `invalidation-guide` 目录，将程序保存为其中的 `main.go`，然后从该模块执行 `REDIS_ADDR=127.0.0.1:16379 go run ./invalidation-guide`。

示例只重置保留命名空间 `soroi-invalidation-demo` 中的示例标签，因此重复运行输出一致。元数据保留到临时 Redis 停止。`RefetchNone` 和禁用的查询对象确保回源次数确定；生产应用可以采用默认的已观察查询刷新策略。

```go
// Command shared_invalidation demonstrates typed remote tag invalidation and local notification delivery.
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
	redistore "github.com/Waterkyuu/soroi/store/redis"
	"github.com/redis/go-redis/v9"
)

// User represents the immutable source value shared by detail and list queries.
type User struct {
	// Name is the display name returned by the authoritative source.
	Name string
}

// main reports configuration and query errors to the shell.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run commits a simulated source change before remotely invalidating two related query result types.
func run() error {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	remote := redis.NewClient(&redis.Options{
		Addr: addr, ContextTimeoutEnabled: true, DialTimeout: time.Second,
		ReadTimeout: time.Second, WriteTimeout: time.Second, MaxRetries: -1,
	})
	defer func() { _ = remote.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := remote.Ping(ctx).Err(); err != nil {
		return err
	}
	store, err := redistore.New(
		remote,
		redistore.Options{Prefix: "soroi-invalidation-demo", EnableTags: true, EnableNotifications: true},
	)
	if err != nil {
		return err
	}
	detailShared, err := soroi.NewSharedCache(store, soroi.SharedCacheOptions[User]{
		Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute, ShareTags: true,
	})
	if err != nil {
		return err
	}
	listShared, err := soroi.NewSharedCache(store, soroi.SharedCacheOptions[[]User]{
		Key: "tenant:42:users:v1", Format: "users.json.v1", TTL: 5 * time.Minute, ShareTags: true,
	})
	if err != nil {
		return err
	}
	tags := []string{"tenant:42:users"}
	// Reset only the reserved demonstration namespace's label so repeated runs produce the same output.
	if _, err := store.InvalidateTags(ctx, tags); err != nil {
		return err
	}
	var source atomic.Value
	source.Store(User{Name: "Alice"})
	var calls atomic.Int64
	detail, err := soroi.QueryDefinition(
		"detail",
		func(context.Context) (User, error) { calls.Add(1); return source.Load().(User), nil },
		soroi.QueryOptions{Disable: true},
	)
	if err != nil {
		return err
	}
	detail.Tags = tags
	detail.Shared = detailShared
	list, err := soroi.QueryDefinition(
		"list",
		func(context.Context) ([]User, error) { calls.Add(1); return []User{source.Load().(User)}, nil },
		soroi.QueryOptions{Disable: true},
	)
	if err != nil {
		return err
	}
	list.Tags = tags
	list.Shared = listShared
	a := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	b := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()
	watchA, err := a.WatchShared(ctx, store, soroi.InvalidateOptions{Refetch: soroi.RefetchNone})
	if err != nil {
		return err
	}
	defer watchA.Close()
	watchB, err := b.WatchShared(ctx, store, soroi.InvalidateOptions{Refetch: soroi.RefetchNone})
	if err != nil {
		return err
	}
	defer watchB.Close()
	if err := detail.Prefetch(ctx, a); err != nil {
		return err
	}
	if err := list.Prefetch(ctx, a); err != nil {
		return err
	}
	if _, err := detail.Fetch(ctx, b); err != nil {
		return err
	}
	if _, err := list.Fetch(ctx, b); err != nil {
		return err
	}
	d, l := detail.Query(b), list.Query(b)
	defer d.Close()
	defer l.Close()
	fmt.Printf("initial detail: %s\n", d.Snapshot().Data.Name)
	fmt.Printf("initial list: %s\n", l.Snapshot().Data[0].Name)
	fmt.Printf("source calls: %d\n", calls.Load())
	// In production, this step is a successful database or API commit.
	source.Store(User{Name: "Bob"})
	if err := a.InvalidateSharedTags(
		ctx,
		store,
		tags,
		soroi.InvalidateOptions{Refetch: soroi.RefetchNone},
	); err != nil {
		return err
	}
	if err := waitStale(ctx, d); err != nil {
		return err
	}
	if err := waitStale(ctx, l); err != nil {
		return err
	}
	fmt.Printf("notified detail stale: %t\n", d.Snapshot().Stale)
	fmt.Printf("notified list stale: %t\n", l.Snapshot().Stale)
	fmt.Printf("retained detail: %s\n", d.Snapshot().Data.Name)
	newDetail, err := detail.Fetch(ctx, b)
	if err != nil {
		return err
	}
	newList, err := list.Fetch(ctx, b)
	if err != nil {
		return err
	}
	fmt.Printf("refreshed detail: %s\n", newDetail.Name)
	fmt.Printf("refreshed list: %s\n", newList[0].Name)
	fmt.Printf("source calls: %d\n", calls.Load())
	return nil
}

// waitStale waits for notification publication without sleeps, polling, or assumptions about delivery latency.
func waitStale[V any](ctx context.Context, query *soroi.QueryHandle[V]) error {
	for {
		select {
		case state := <-query.Updates():
			if state.HasData && state.Stale {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
```

临时 Redis 运行期间，从仓库根目录执行维护中的示例：

```sh
cd examples/redis
REDIS_ADDR=127.0.0.1:16379 go run ./shared_invalidation
```

预期输出：

```text
initial detail: Alice
initial list: Alice
source calls: 2
notified detail stale: true
notified list stale: true
retained detail: Alice
refreshed detail: Bob
refreshed list: Bob
source calls: 4
```

两个 Client 通过 Redis 复用 `User` 详情和 `[]User` 列表。A 提交数据源修改后使共有标签失效。B 收到两个结果类型的 stale 快照，在 stale 期间保留 Alice，再通过显式读取获取 Bob。只有显式读取后，回源次数才从二次增加到四次。

## API 契约

| API | 契约 |
| --- | --- |
| `client.WatchShared(ctx, store, options...)` | 确认订阅建立，将已有绑定查询标记为 stale，并返回 `*SharedWatcher`，使用零份或一份刷新配置 |
| `watch.Close()` | 取消处理并等待 watcher 自己拥有的订阅释放；幂等，支持并发关闭 |
| `watch.Done()` | 事件处理停止且资源释放后关闭 |
| `watch.Stats()` | 返回累计投递与恢复统计 |
| `definition.InvalidateShared(ctx, client, options...)` | 先提交远端按 key 失效，再处理指定本地 key；发布一个操作回执 |
| `shared.Invalidate(ctx)` | 只执行远端按 key 失效；活动 watcher 仍可能异步使本地查询失效 |
| `client.InvalidateSharedTags(ctx, store, tags, options...)` | 提交远端 OR 标签失效，再处理当前 Client 的绑定查询；本地失败不回滚远端操作 |
| `store.InvalidateTags(ctx, tags)` | 返回已提交的标签回执，不直接操作 Client |

同一个 Client 的查询配置和 watcher 应复用同一个 `Store` 对象。`InvalidationScope()` 是进程内路由身份，不是跨进程缓存 key。不同进程可以各自创建 Store，使用相同的 Redis 地址、Prefix 和 EnableTags 配置，通过同一个远端 channel 收到通知。不同本地 Store 对象具有不同路由身份，即使它们的远端配置恰好相同。

每个 Client 与 Store 组合创建一个 watcher。EnableNotifications 为 true 时，默认 Redis 布局支持按 key 通知。禁用通知的存储在订阅前拒绝 WatchShared，普通按 key 失效不需要 PUBLISH 权限。远端标签操作要求 EnableTags，查询通过 ShareTags 请求将定义中的标签登记到远端。配置的存储无法登记标签时，构造函数返回 `ErrSharedCapability`。只实现 Eval 的自定义 RedisClient 可以继续提供共享存储；WatchShared 需要额外的 Subscribe 能力。

通知传输支持单个 Redis 主节点、Sentinel 客户端和 Redis Cluster。go-redis Ring 由独立服务器组成，不具备集群范围的 Pub/Sub 转发；对 Ring 启用通知会返回 ErrSharedCapability。其 P0 纯存储用法保持支持。

watcher 建立过程受调用方 context 和 Client 生命周期约束。Redis 适配器将首次订阅确认限制为一秒，命令客户端的连接选项也会生效。标签失效直接使用调用方 context，应为操作设置合适截止时间。按 key 失效继续使用 SharedCacheOptions.Timeout。这些显式失效调用不会自动重试业务写入。

## 通知策略与本地生命周期

`WatchShared` 接受零份或一份 `InvalidateOptions`。省略配置和使用零值均采用 `RefetchObserved`，且 `RefetchAfterLoad: false`：匹配结果变为 stale，没有执行中加载时，已启用订阅启动刷新。显式设置 `RefetchAfterLoad: true` 后，执行中的加载最多合并一次补刷。多份配置会在建立订阅或将本地查询标记过期之前返回 `ErrTooManyOptions`。`RefetchNone` 发布 stale 状态而不启动新工作。

通知保留 HasData 和已有值，不启用禁用的查询对象，也不为未观察的查询创建订阅。后续显式 Fetch 可以加载替代值。QueryHandle 的 Close 仍由调用方独立管理，不随 watcher 关闭而自动关闭。

本地 Set、Update、数据年龄到期，以及仍有查询对象或活动状态拥有的结果删除，保留远端绑定。一个查询 key 被完全释放后，绑定也被释放，不再参与后续通知。Set、Update 仍不执行远端写入；Remove、Clear、GC 和 Client.Close 不删除 Redis 记录或发布远端失效。

操作 ID 用于抑制最近一个回执的重复应用，覆盖失效辅助方法的本地应用与自身广播竞争。回执不定义业务事务顺序，也不保证跨重连的严格一次投递。延迟到达的旧操作可能保守地触发另一次刷新。

调用方取消或 Client.Close 会停止 watcher。Client.Close 发起取消；`watch.Close()` 或等待 Done 提供资源释放屏障。借用的 Redis 命令客户端保持可用。事件流意外关闭时，执行最后一次存储范围重置、记录错误并结束 watcher；自定义通知源出现终止故障时，应观察 Done 并重新建立 watcher。

## 远端标签登记与匹配

远端标签采用本地已有的精确字符串和 OR 规则：区分大小写，不按前缀匹配，不规范化空白，重复标签不增加效果，空字符串是普通标签。空失效列表不执行任何操作。租户边界必须包含在标签或 Redis 命名空间中，例如 `tenant:42:users`。

远端登记发生在共享读取期间，包括共享命中。构造定义、创建禁用的查询对象或复用新鲜本地结果，不进行 Redis I/O，因此不会登记新增远端关系。本地标签仍可用于 QueryFilter。如果新增远端标签需要立即登记，可通过使用更新后定义创建的查询对象执行显式 Refetch。

每个共享 key 的远端关系只增加，不替换。标签存储的 Read 检查全部已记录标签，包括其他 Client 登记的标签。读取同一记录的其他查询，即使自身 ShareTags 为 false，也会继承该记录已有的远端关系。RegisteredTags 通知将新增关系同步到已有本地绑定，不使数据变为 stale，使后续由其他实例声明的标签失效能够作用于同一共享 key 的本地缓存。

类型化查询获知远端依赖后，会在复用带标签结果前再次确认版本快照。快照变化时丢弃缓存数据并回源，封闭“通知到达早于未知标签在本地登记”的区间。带标签复用增加一次确认读取，未带标签记录保持原来的一次读取路径。

按 key 失效、数据到期与本地删除不移除远端关系。标签关系和版本元数据在命名空间内持续保留。数据结构或分组变化需要不同标签集时，应使用新的带版本共享 key 或命名空间。查询 predicate 与本地 QueryFilter 不会被序列化为远端扫描条件。

## 原子版本与 Redis Cluster 布局

EnableTags 为 false 时，每条记录保持独立 key 的 hash slot。EnableTags 为 true 时，同一命名空间中的全部记录和标签版本 hash 使用相同 hash tag：

```text
<prefix>:tagged:{<SHA-256 of prefix>}:<SHA-256 of logical key>:version
<prefix>:tagged:{<SHA-256 of prefix>}:<SHA-256 of logical key>:data
<prefix>:tagged:{<SHA-256 of prefix>}:<SHA-256 of logical key>:version:tags
<prefix>:tagged:{<SHA-256 of prefix>}:tag-generations
```

不透明 Version 快照组合 key 版本和全部已登记标签版本。新增关系会更换 key 版本，防止早期写入遗漏新关系。条件写入原子比较所有已捕获版本。InvalidateTags 原子更新全部目标标签并发布一个回执，不枚举记录。失效的数据在下一次读取时延迟删除。

条件写入被拒绝的回源加载仍返回成功值，但本地结果保持 stale，WriteConflicts 增加。即使通知丢失，也不会把旧版本快照标记为新鲜，同时保留可用数据。

key、关系或标签版本元数据丢失，不会重新授权旧写入恢复有效数据。随机令牌、关系初始化检查与版本快照确认处理元数据重建。恢复旧 Redis 快照或故障切换丢失已确认写入，仍可能回退版本；协议不增加 Redis 之外的持久性保证。

标签模式中，一个命名空间占用一个 cluster slot，能够原子校验跨 key 的标签关系，但该命名空间流量集中到一个主节点。需要分布负载时，可按独立租户拆分命名空间；一个标签操作不能原子跨越这些命名空间。启用标签采用不同物理布局和通知 channel，相当于建立新的缓存命名空间，不会就地迁移默认布局。所有参与实例必须使用一致的 Prefix 和 EnableTags。

内部关系标记不会与编码后的标签冲突。标签和通知身份使用 base64 JSON 表示保留任意 Go 字符串字节；业务查询继续使用原始精确字符串。

## 连接中断与新鲜期兜底

Pub/Sub 是异步投递，没有重放日志。确认首次订阅后，执行一次本地存储范围重置，修复订阅建立前的区间。检测到连接故障时，对这一断连区间执行一次重置；成功重新订阅后，再执行一次重置，使通知不可用期间安装的结果失效。无法解析的消息也触发重置，不会静默丢弃。

适配器缓冲 32 个事件，通过可取消的背压处理后续事件，不使用 go-redis 中可能在投递超时后丢失消息的 channel API。消费者过慢仍可能触发 Redis 服务端输出缓冲限制并导致连接中断，重连后的重置修复该区间。网络故障检测并非即时，新鲜度仍由 StaleTime 和共享绝对截止时间限制，作为通知延迟或不可用时的兜底。

读取与写入还会独立检查标签版本。通知加速本地失效，不是唯一正确性机制。时间经过本身不发布更新，也不启动轮询。系统没有所有实例的同步确认或分布式锁。

## 可观测性、错误与测试

| watcher 统计 | 含义 |
| --- | --- |
| `Invalidations` | 普通远端操作实际改变的匹配本地查询数；重复回执和纯关系登记消息不增加计数 |
| `Resyncs` | 建立订阅与连接区间重置次数，包括没有保留查询的重置 |
| `Errors` | 通知源报告的连接、解码或事件流意外关闭故障 |
| `LastError` | 最新诊断错误，恢复后仍保留 |

SharedCache.Stats 继续描述结果复用与存储写入，Client.Stats 继续描述本地加载与缓存活动。watcher 统计属于单个 watcher，不跨进程自动聚合。

远端失效失败时返回错误，不改变本地状态。连接超时可能导致远端结果不确定；再次失效会产生另一个安全回执。Client 在本地应用前关闭，不回滚已经提交的远端操作。watcher 建立失败不会保留读取协程，缺少能力返回 ErrSharedCapability。不能仅因缓存同步失败就重试已经成功提交的数据库写入。

执行根模块的 lint、race 测试与构建，并独立执行 Redis 模块的 vet、lint、race 测试与构建。测试覆盖 key 和标签投递、禁用与启用消费者、回执去重、远端标签并集传播、租户隔离、依赖登记竞争、取消、断连恢复、损坏消息、二进制身份和元数据丢失。Redis 测试使用关闭持久化的临时本地服务器，CI 要求 redis-server 可用。
