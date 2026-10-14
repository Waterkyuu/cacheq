# 使用 Redis 共享查询结果

[English](shared-cache.en-US.md) · [缓存与生命周期](cache.zh-CN.md) · [失效与刷新](invalidation.zh-CN.md)

## 概述

cacheq 的 `Client` 在进程内部维护类型安全的查询状态。两个服务实例各自创建 Client 后，本地缓存相互独立：实例 A 的缓存命中不会填充实例 B。因此，即使 A 已经缓存了新鲜结果，B 仍可能再次访问数据库或 HTTP 服务，读取同一个资源。

可选共享缓存为成功结果增加一层复用位置。A 依次检查本地缓存、Redis 和真实数据源；B 采用相同路径，可以复用 A 已经写入 Redis 的结果。这能够减少多个实例之间重复的数据源读取，也有助于新启动的实例复用已有结果。

真实数据源仍然是权威来源。Redis 保存可丢弃的序列化结果。cacheq 继续负责本地订阅、加载状态、类型检查、请求合并、重试和本地保留策略。Redis 不保存 goroutine、回调、订阅 channel、Go 类型身份或错误对象。

多个实例反复读取相同资源，并且业务允许明确的新鲜度窗口时，共享缓存具有实际意义。只有一个进程，或者每次读取都必须访问数据源的结果，可能无法获得收益。Redis 也增加网络与序列化成本，启用前应与真实数据源成本比较。

## 读取与刷新流程

```text
Definition.Fetch / Definition.Prefetch / Definition.Query
  |
  +-- 本地结果新鲜 --> 返回本地结果
  |
  +-- 启动或加入当前 Client 的同 key 加载
        |
        +-- 普通读取：取得共享版本与记录
        |     +-- 记录新鲜且格式兼容 --> 解码 --> 发布到本地
        |     +-- 未命中、记录过期或缓存故障 --> 访问数据源
        |
        +-- 强制刷新或失效刷新：取得版本，忽略共享数据 --> 访问数据源
              |
              +-- 数据源失败 --> 使用已有的错误与本地 stale 数据语义
              +-- 数据源成功 --> 编码 --> 按版本条件写入共享缓存 --> 发布到本地
```

存储访问与 Codec 调用都在 Client 锁外执行。已有 Executor 的一个执行槽覆盖整次尝试，包括共享缓存访问；存储操作超时与数据源超时分别配置。重试沿用发起消费者的策略。单独的共享缓存读写失败不会触发查询重试，数据源失败仍可以触发重试。

强制刷新仍然遵守现有规则：如果同 key 已经有加载，就加入该加载，不升级或重启正在读取 Redis 的请求。只有新启动的强制刷新才绕过共享数据。本地失效发生在加载期间时，最终安装的结果仍然 stale，并遵循已有 `RefetchAfterLoad` 补刷规则。刷新失败后，新启动的加载继续绕过共享数据，直到成功加载或本地写入替换失效状态。

## 环境要求与快速开始

核心模块要求 Go 1.22 或更高版本。可选 Redis 模块使用 go-redis v9.23.0，要求 Go 1.26 或更高版本。Redis 需要支持 `EVAL`、`GET`、带 `PX` 的 `SET` 和 `DEL`；适配器使用可写主节点连接。

以下步骤使用当前检出的实现。示例是独立模块，通过本地 `replace` 引用仓库根模块和 Redis 适配器，不假设已经发布可安装的适配器版本。

在一个终端中，使用空闲端口启动不启用持久化的本地 Redis：

```sh
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

在另一个终端中，从仓库根目录执行：

```sh
cd examples/redis
REDIS_ADDR=127.0.0.1:16379 go run ./shared_cache
```

预期输出：

```text
A first: Alice, source calls: 1
B shared: Alice, source calls: 1
A local: Alice, source calls: 1
A refreshed: Bob, source calls: 2
B local: Alice, source calls: 2
B refreshed: Bob, source calls: 3
shared hits: 1, writes: 3, bypasses: 2
C fallback: Bob, source calls: 4
shared read errors: 1
```

示例使用支持并发访问的内存数据源代替数据库，使每次数据变化和真实回源次数都可以直接观察。A、B 使用不同的本地 Client，但共享同一条 Redis 记录。

首次读取访问数据源，B 的首次读取从 Redis 恢复结果，A 的第二次读取直接命中本地缓存。共享失效后 A 获取 Bob，B 仍返回本地 Alice，符合未启用 WatchShared 时不接收通知的默认契约。B 的显式本地失效会绕过共享数据并回源。关闭 Redis 连接后，新 Client C 回源成功，ReadErrors 增加。

示例使用保留的 `cacheq-demo` 命名空间，每次运行前重置该示例 key。生产记录不应使用这个命名空间。数据过期后仍会保留一个小型版本元数据 key；示例完成后可以使用 Ctrl+C 停止这个临时 Redis。

完整程序维护于 [examples/redis/shared_cache/main.go](../examples/redis/shared_cache/main.go)，包含连接配置、类型化数据源、本地 Client、显式失效和资源清理。

```go
// Command shared_cache demonstrates shared reads, explicit invalidation, and cache failure fallback.
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

// User represents the immutable result shared by the example's query clients.
type User struct {
	// Name is the display name returned by the simulated authoritative source.
	Name string
}

// main runs the example and reports setup or query failures to the shell.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run uses two clients and a simulated source to make each cache layer's effects observable.
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
		return fmt.Errorf("connect to example Redis: %w", err)
	}
	store, err := redistore.New(remote, redistore.Options{Prefix: "cacheq-demo"})
	if err != nil {
		return err
	}
	shared, err := cacheq.NewSharedCache[User](store, cacheq.SharedCacheOptions[User]{
		Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute,
		Timeout: 200 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	// Reset this reserved demonstration key so repeated runs have identical output.
	if err := shared.Invalidate(ctx); err != nil {
		return err
	}
	var source atomic.Value
	source.Store(User{Name: "Alice"})
	var calls atomic.Int64
	definition := cacheq.QueryDefinition[User]{
		Key: "user:7", Shared: shared,
		Fetcher: func(context.Context) (User, error) {
			calls.Add(1)
			return source.Load().(User), nil
		},
	}
	a := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	b := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()
	if err := printRead(ctx, "A first", a, definition, &calls); err != nil {
		return err
	}
	if err := printRead(ctx, "B shared", b, definition, &calls); err != nil {
		return err
	}
	if err := printRead(ctx, "A local", a, definition, &calls); err != nil {
		return err
	}
	// A business write must succeed before invalidating its cached results.
	source.Store(User{Name: "Bob"})
	if err := definition.InvalidateShared(ctx, a, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
		return err
	}
	if err := printRead(ctx, "A refreshed", a, definition, &calls); err != nil {
		return err
	}
	if err := printRead(ctx, "B local", b, definition, &calls); err != nil {
		return err
	}
	if err := b.Invalidate(definition.Key, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
		return err
	}
	if err := printRead(ctx, "B refreshed", b, definition, &calls); err != nil {
		return err
	}
	stats := shared.Stats()
	fmt.Printf("shared hits: %d, writes: %d, bypasses: %d\n", stats.Hits, stats.Writes, stats.Bypasses)
	// Closing only the borrowed Redis connection simulates an unavailable cache without stopping the source.
	if err := remote.Close(); err != nil {
		return err
	}
	c := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer c.Close()
	if err := printRead(ctx, "C fallback", c, definition, &calls); err != nil {
		return err
	}
	fmt.Printf("shared read errors: %d\n", shared.Stats().ReadErrors)
	return nil
}

// printRead reports the typed result and actual source calls for one explicit query read.
func printRead(
	ctx context.Context,
	label string,
	client *cacheq.Client,
	definition cacheq.QueryDefinition[User],
	calls *atomic.Int64,
) error {
	value, err := definition.Fetch(ctx, client)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s, source calls: %d\n", label, value.Name, calls.Load())
	return nil
}
```

后续片段使用程序中的查询定义、本地 Client `a` 和非 nil 的 `ctx`，位于返回 error 的业务函数中。

## 配置、key 与序列化

| 配置 | 职责 | 默认值或校验 |
| --- | --- | --- |
| `QueryDefinition.Key` | 一个 Client 内的查询身份与静态类型绑定 | 沿用已有的可比较 key 规则 |
| `QueryDefinition.Shared` | 可选类型化共享缓存配置 | nil：仅使用本地缓存 |
| `SharedCacheOptions.Key` | 跨进程稳定的结果身份 | 必须是非空字符串 |
| `SharedCacheOptions.Format` | 数据结构与 Codec 的格式版本 | 必须是非空字符串 |
| `SharedCacheOptions.TTL` | 从数据源完成时间开始计算的共享新鲜度上限与记录保留时间 | 必须为正数 |
| `SharedCacheOptions.Timeout` | 每次存储操作的截止时间 | 0：一秒；负数：配置错误 |
| `SharedCacheOptions.ShareTags` | 将 QueryDefinition.Tags 登记到远端 | false；要求启用的 SharedTagStore |
| `SharedCacheOptions.Codec` | 类型化结果编码与解码 | nil：`JSONCodec[V]` |
| `redistore.Options.Prefix` | 区分应用和环境的 Redis 命名空间 | 空字符串：`cacheq`；禁止大括号 |

本地 key 可以是可比较的结构体或其他受支持 Go 值，不会自动转换成远端 key。共享 key 必须显式提供，并包含影响结果的租户、资源、请求参数、授权范围及必要的数据结构版本。不要使用指针地址、任意 Go 值的 `fmt.Sprint` 或包含秘密的数据作为身份。

共享同一个远端 key 的客户端必须在结果语义、Format 和 Codec 上保持一致。不兼容的数据结构或编码变更应使用新 key。Format 检查可以防止错误解码，但不负责迁移旧数据；不兼容调用方使用同一个 key 时，会在回源后反复覆盖彼此的记录。

外层记录封装保存 Format、数据源完成时间、共享绝对到期时间与编码后的字节。默认结果 Codec 使用标准 JSON。导出字段、nil 指针、空值和零值沿用 `encoding/json` 规则；缓存的 nil 仍然有外层记录，因此可以与未命中区分。函数、channel、不支持的循环引用、指针身份和未导出字段不能保留。其他编码方式可以实现 `Codec[V]`，方法必须支持并发调用。

cacheq 继续在本地保护结果静态类型，没有引入跨进程反射类型注册表。同一个本地 key 的定义必须使用一致的加载器和共享配置；加入已有加载时，保留发起者的配置。

## 新鲜度、保留时间与时钟

共享缓存分别保留两个时间概念：

- **FetchedAt**：成功的数据源回调完成时间，随共享记录传递，用于计算消费者新鲜度。
- **UpdatedAt**：当前 Client 安装可用结果的时间，继续作为本地 Snapshot 时间和本地 MaxAge 起点。

成功共享查询的有效新鲜期在以下时间中取最早值：

```text
FetchedAt + 当前消费者的 StaleTime
共享记录的绝对 ExpiresAt
本地 UpdatedAt + MaxAge（启用 MaxAge 时）
```

例如，A 在 12:00:00 回源，B 在 12:00:30 恢复数据，StaleTime 都为一分钟，那么 B 只会新鲜到 12:01:00，而不是 12:01:30。如果共享 TTL 为五分钟，消费者即使使用十分钟 StaleTime，也不能把该共享结果的新鲜期延长到 12:05:00 之后。StaleTime 为零或负数时，数据仍然立即 stale，普通读取不会复用共享记录。

Redis 数据 TTL 使用绝对到期时间的剩余时长，包括编码消耗的时间。Redis 的亚毫秒 TTL 向上取整，但复用前仍会检查记录中的绝对截止时间。命中不会续期。不同机器需要合理同步时钟；如果记录时间晚于读取客户端的当前时间，则按未命中处理。

共享记录到期结束的是新鲜期，不代表本地结果立即不可用。本地 stale 数据仍可在刷新期间展示，直到 MaxAge 或其他本地删除策略将其释放。GCTime 和 LRU 只影响本地 Client，不删除 Redis 数据。MaxAge 保持已有的本地安装年龄语义，不能作为从源头开始计算的端到端数据年龄保证。

## 业务写入与显式失效

先成功提交数据库或 API 写入，再使缓存失效：

```go
// The business write has already committed successfully.
if err := definition.InvalidateShared(ctx, a, cacheq.InvalidateOptions{
    Refetch: cacheq.RefetchNone,
}); err != nil {
    return err
}
_, err := definition.Fetch(ctx, a)
return err
```

`InvalidateShared` 先检查本地 key、类型、Client 状态和 context，再使远端记录失效，最后调用本地 Invalidate。远端失败时不修改本地状态。如果远端已经成功，而 Client 在本地失效前关闭，远端操作不会回滚。超时或连接错误可能意味着远端结果不确定：重新执行远端失效可以安全地产生另一个版本，但不能因此盲目重试业务写入。

不使用 RefetchNone 时，已启用订阅按照现有规则刷新。这个方法不等待刷新完成；需要观察完成结果时，使用已有本地刷新 API。其他 Client 仅在配置 WatchShared 后接收失效。远端标签采用 InvalidateSharedTags，参见[共享失效通知与标签](shared-invalidation.zh-CN.md)。

`shared.Invalidate(ctx)` 只执行远端操作。`client.Invalidate(key)` 只执行本地失效，不过新启动的刷新会绕过共享数据，并可能写入新的共享结果。

`Set`、`Update` 继续是本地写入，包括共享加载之后的写入。它们正常重置本地新鲜度，不自动发布序列化数据。`Get`、`Snapshot`、`Remove`、`Clear`、GC、容量淘汰与 Client.Close 都不执行远端删除，使网络故障和连接生命周期由应用边界明确管理。

## 并发失效契约

考虑下面的受控执行顺序：

```text
A 读取远端版本 G1，开始获取旧的数据源结果
B 提交业务修改
B 执行共享失效：原子地将 G1 替换为 G2，同时删除数据
A 完成旧加载，尝试使用 G1 写回 Redis
Redis 比较 G1 与 G2，拒绝旧写入
```

被拒绝的写入增加 WriteConflicts。A 的数据源调用仍可成功返回，因为缓存版本冲突不是数据源失败。如果同一个 Client 在加载期间也发生本地失效，安装后的结果按照已有规则保持 stale。

保护范围是：在成功远端失效前已经取得版本令牌的加载。它不负责业务事务排序，不比较不同数据源副本的真实数据年龄，也不阻止同一版本内的两个加载按相反顺序完成。请求合并仍只覆盖一个 Client，没有引入分布式锁或跨实例 singleflight。

## Redis 存储结构与运行边界

每个逻辑共享 key 对应两个 Redis key：

```text
<prefix>:{<SHA-256 of logical key>}:version
<prefix>:{<SHA-256 of logical key>}:data
```

相同的 hash tag 使两个 key 位于 Redis Cluster 的同一个 slot。读取、条件写入与失效通过原子 Lua 脚本执行。读取也可能初始化元数据，因此只读副本或只有 GET 权限的凭据不足以运行。key 布局支持 Redis Cluster；当前集成测试使用单机本地 Redis，不覆盖真实 Cluster 或 Sentinel 故障切换。

版本令牌使用随机值，版本 key 不设置 TTL。每个访问过的查询身份需要保留一个小型元数据 key，即使数据已经过期。无限增长的 key 身份需要应用制定容量或命名空间生命周期策略。如果版本元数据丢失，下一次读取生成新令牌并删除孤立数据；版本不存在或已经变化的写入被拒绝。恢复较旧 Redis 快照，或者故障切换丢失已确认写入，可能回退版本，不属于这个协议提供的持久性保证。

应用需要注入支持并发访问的 go-redis 客户端，并按整体预算配置 `ContextTimeoutEnabled`、连接、读取、写入和重试选项。context 取消是协作式的，cacheq 不能强制终止忽略 context 的驱动或回调。外部连接由应用关闭，Client.Close 不关闭注入的存储。

## 故障处理与统计

| 条件 | 查询行为 | 共享统计 |
| --- | --- | --- |
| 本地结果新鲜 | 返回本地值，无网络访问 | 不改变共享计数 |
| 共享记录新鲜 | 解码并安装到本地 | Hits |
| 记录不存在、到期或超出消费者新鲜期 | 回源 | Misses |
| 外层记录损坏、Format 不兼容或 Codec 解码失败 | 回源 | Misses 与 DecodeErrors |
| 读取失败、读取超时或返回空版本 | 回源；没有有效令牌时跳过共享写入 | ReadErrors |
| 强制刷新或失效刷新 | 忽略共享数据，取得令牌并回源 | Bypasses |
| 编码或存储写入失败 | 仍返回成功的数据源结果 | EncodeErrors 或 WriteErrors |
| 回源期间版本发生变化 | 返回数据源结果，拒绝远端写入，本地结果保持 stale | WriteConflicts |
| 数据源失败 | 正常错误、重试与本地 stale 数据行为，不写入远端错误记录 | 已有 Client 统计 |
| 调用方取消或查询超时 | 按已有规则取消查询 | 取消前完成的操作可能已计数 |
| 显式远端失效失败 | 返回包装错误，保留 errors.Is 能力 | InvalidationErrors |

`shared.Stats()` 属于这个 SharedCache 对象，累计所有使用它的 Client 的操作，不是 Redis 服务端全局指标，也不会自动合并不同进程的统计。Client Stats 继续统计本地缓存决策与加载；共享命中发生在一次本地 load 内。应用可以将两种快照接入已有监控系统。

## 实现存储适配器

新适配器实现 SharedStore 的 Read、Write 和 Invalidate。即使未命中，Read 也必须返回非空版本。版本比较与数据安装相对于失效必须是原子的；无条件返回 Write 成功会破坏旧写入保护。返回的 Data 必须由接收方独立拥有，存储回调需要遵守 context 并支持并发调用。

核心代码以及只使用核心契约的自定义适配器可以继续支持 Go 1.22。Redis 依赖的 Go 版本要求隔离在自身 go.mod 中。根目录检查不会自动遍历嵌套模块，因此 CI 单独检查适配器模块元数据、lint、race 测试、构建与依赖。

## 测试与限制

从仓库根目录执行：

```sh
task fmt
task lint
task test
task build
cd store/redis
go vet ./...
golangci-lint run --config ../../.golangci.yml --timeout=5m
CACHEQ_REQUIRE_REDIS=1 go test -race ./... -count=1
go build ./...
```

核心单元测试覆盖跨 Client 复用、原始新鲜期、TTL 边界、nil 数据、Codec 故障、数据源失败、本地写入、取消、Close、请求合并顺序和旧写入拒绝。HTTP 测试验证预加载、订阅发布、真实回源次数和刷新。Redis 适配器测试启动关闭持久化的临时 Redis 进程，运行实际 Lua 脚本、检查数据过期，并模拟元数据丢失。没有 redis-server 时会明确跳过；设置 CACHEQ_REQUIRE_REDIS=1 后缺失服务器会失败，CI 使用这个要求。

共享缓存支持一层共享结果存储、Redis、JSON，以及按 key 的失效版本保护。可选通知和远端标签版本参见[共享失效通知与标签](shared-invalidation.zh-CN.md)。当前不支持任意缓存链、错误缓存、分布式锁、Redis 原子 Update 或持久化查询状态。
