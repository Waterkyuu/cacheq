# 共享失效通知与远端标签

[English](shared-invalidation.en-US.md) · [共享结果缓存](shared-cache.zh-CN.md) · [查询标签](query-tags.zh-CN.md)

## 概述

`Client.WatchShared` 接收远端失效通知，并使绑定到同一个存储实例的本地查询失效。已启用订阅按照 watcher 策略刷新；禁用的查询保留数据并发布 stale 快照。通知是可选能力，没有 watcher 的 Client 继续依赖正常的新鲜度截止时间。

设置 `redistore.Options.EnableNotifications: true` 后，存储发布变更并允许 watcher。该字段默认为 false，保持原有纯存储命令与 ACL 契约；需要及时传播通知的写入方均应启用。

远端标签可以将不同共享 key 和结果类型关联成组。存储启用 `redistore.Options.EnableTags`，各参与查询启用 `SharedCacheOptions.ShareTags`，由 `QueryDefinition.Tags` 提供远端登记的标签。`Client.InvalidateSharedTags` 更换远端标签版本，并使发起 Client 中的匹配查询失效。同一远端命名空间中的其他 Client 通过 watcher 接收该操作。

权威数据库或 API 写入必须先成功提交，再使缓存失效。通知投递不提供分布式事务、跨实例同步完成屏障或持久化事件重放。

## 环境要求与快速开始

核心契约继续兼容 Go 1.22。可选 Redis 模块要求 Go 1.26，依赖版本由自身 go.mod 声明。在共享缓存原有命令之外，通知需要 `PUBLISH` 和 `SUBSCRIBE`，标签需要 `HGET`、`HSET`、`HGETALL` 和 `HEXISTS`。应使用可写主节点连接，配置相应 ACL 权限、context 和网络操作超时。

在一个终端启动不启用持久化的临时 Redis：

```sh
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

在另一个终端，从仓库根目录执行：

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

两个 Client 通过 Redis 复用 `User` 详情和 `[]User` 列表。A 提交数据源修改后，使二者共有的标签失效。B 无须手动本地失效，就能收到两个不同类型结果的 stale 快照；在 stale 期间保留 Alice，再通过显式读取获取 Bob。示例使用 `RefetchNone` 和禁用的查询对象，确保回源次数确定。生产应用可以采用默认的已观察查询刷新策略。

示例只重置保留命名空间 `cacheq-invalidation-demo` 中的示例标签。元数据保留到这个临时 Redis 停止。完整程序维护于 [examples/redis/shared_invalidation/main.go](../examples/redis/shared_invalidation/main.go)。

```go
// Command shared_invalidation demonstrates typed remote tag invalidation and local notification delivery.
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
	redistore "github.com/Waterkyuu/cacheq/store/redis"
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
		redistore.Options{Prefix: "cacheq-invalidation-demo", EnableTags: true, EnableNotifications: true},
	)
	if err != nil {
		return err
	}
	detailShared, err := cacheq.NewSharedCache[User](store, cacheq.SharedCacheOptions[User]{
		Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute, ShareTags: true,
	})
	if err != nil {
		return err
	}
	listShared, err := cacheq.NewSharedCache[[]User](store, cacheq.SharedCacheOptions[[]User]{
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
	detail := cacheq.QueryDefinition[User]{
		Key: "detail", Tags: tags, Shared: detailShared, Options: cacheq.QueryOptions{Disable: true},
		Fetcher: func(context.Context) (User, error) { calls.Add(1); return source.Load().(User), nil },
	}
	list := cacheq.QueryDefinition[[]User]{
		Key: "list", Tags: tags, Shared: listShared, Options: cacheq.QueryOptions{Disable: true},
		Fetcher: func(context.Context) ([]User, error) { calls.Add(1); return []User{source.Load().(User)}, nil },
	}
	a := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	b := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()
	watchA, err := a.WatchShared(ctx, store, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone})
	if err != nil {
		return err
	}
	defer watchA.Close()
	watchB, err := b.WatchShared(ctx, store, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone})
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
		cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone},
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
func waitStale[V any](ctx context.Context, query *cacheq.QueryHandle[V]) error {
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

## API 契约

| API | 契约 |
| --- | --- |
| `client.WatchShared(ctx, store, options...)` | 确认订阅建立，将已有绑定查询标记为 stale，并返回 `*SharedWatcher`；最后一份完整配置生效 |
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

不传 watcher 配置时，采用 `RefetchObserved`，并启用 `RefetchAfterLoad: true`。匹配结果变为 stale，已启用订阅启动刷新，执行中的加载最多合并一次补刷。传入配置时，整份配置替换默认值；未填写字段采用零值。`RefetchNone` 发布 stale 状态而不启动新工作。

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
