# 使用 Redis 共享查询结果

[English](shared-cache.en-US.md)

两个服务实例读取同一个用户资料时，可以通过 `store/redis` 复用查询结果，减少重复访问数据库或 API。本指南先实现 A 读取资料、B 命中 Redis 的最小程序，再说明业务接入、修改数据后的失效操作和配置方法。

## 安装

需要 Go 1.26+、当前 cacheq 仓库和已安装的 `redis-server`。核心库只要求 Go 1.22+；`store/redis` 是独立模块，其 go-redis 依赖要求 Go 1.26+。

以下使用本地源码接入，不假定 Redis 适配器已有可安装的发布版本。在 **cacheq 仓库根目录**执行，创建独立的临时示例模块：

```sh
CACHEQ_SOURCE_DIR="$(pwd)"
CACHEQ_DEMO_DIR="$(mktemp -d)"
cd "$CACHEQ_DEMO_DIR"
go mod init example.com/cacheq-redis-demo
go mod edit -replace="github.com/Waterkyuu/cacheq=$CACHEQ_SOURCE_DIR"
go mod edit -replace="github.com/Waterkyuu/cacheq/store/redis=$CACHEQ_SOURCE_DIR/store/redis"
go get github.com/Waterkyuu/cacheq/store/redis@v0.0.0
```

两个 `replace` 分别指向核心库和 Redis 适配器的本地源码。接入已有项目时，在项目的 `go.mod` 中配置这两个路径并添加依赖，无需重新执行 `go mod init`。

另外打开一个终端，启动临时 Redis，保持该终端运行：

```sh
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

程序连接 `127.0.0.1:16379`。Redis 连接必须指向可写主节点，允许 `EVAL` 以及脚本内的 `GET`、`SET`（含 `PX`）和 `DEL`；读取也可能初始化缓存元数据。

## 快速开始

在前面创建的临时模块目录（`$CACHEQ_DEMO_DIR`）中，将以下完整程序保存为 `main.go`。程序用内存变量模拟数据库，通过调用次数验证 B 是否复用了 A 的结果。

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
	redistore "github.com/Waterkyuu/cacheq/store/redis"
	"github.com/redis/go-redis/v9"
)

// User contains the profile fields shared between service instances.
type User struct {
	// Name is the user's display name.
	Name string
}

// main demonstrates a source read followed by reuse in a separate local Client.
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The application owns the Redis connection and closes it at shutdown.
	remote := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:16379", ContextTimeoutEnabled: true,
		DialTimeout: time.Second, ReadTimeout: time.Second,
		WriteTimeout: time.Second, MaxRetries: -1,
	})
	defer func() { _ = remote.Close() }()
	if err := remote.Ping(ctx).Err(); err != nil {
		panic(err)
	}
	store, err := redistore.New(remote, redistore.Options{Prefix: "cacheq-quickstart"})
	if err != nil {
		panic(err)
	}

	// This configuration describes one shared user profile, not the entire cache.
	shared, err := cacheq.NewSharedCache[User](store, cacheq.SharedCacheOptions[User]{
		Key: "tenant:42:user:7:v1", Format: "user.json.v1",
		TTL: 5 * time.Minute, Timeout: 200 * time.Millisecond,
	})
	if err != nil {
		panic(err)
	}
	// Reset only this demonstration record so repeated runs have the same output.
	if err := shared.Invalidate(ctx); err != nil {
		panic(err)
	}

	// A concurrency-safe in-memory source stands in for a database or API.
	var source atomic.Value
	source.Store(User{Name: "Alice"})
	var calls atomic.Int64
	definition := cacheq.QueryDefinition[User]{
		Key: "tenant:42:user:7", Shared: shared,
		Fetcher: func(context.Context) (User, error) {
			calls.Add(1)
			return source.Load().(User), nil
		},
	}

	a := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	b := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()

	first, err := definition.Fetch(ctx, a)
	if err != nil {
		panic(err)
	}
	fmt.Printf("A: %s, source calls: %d\n", first.Name, calls.Load())

	second, err := definition.Fetch(ctx, b)
	if err != nil {
		panic(err)
	}
	fmt.Printf("B: %s, source calls: %d\n", second.Name, calls.Load())
	fmt.Printf("shared hits: %d\n", shared.Stats().Hits)
}
```

在保存 `main.go` 的目录执行：

```sh
go run .
```

预期输出：

```text
A: Alice, source calls: 1
B: Alice, source calls: 1
shared hits: 1
```

A 没有本地或共享结果，因此执行 `Fetcher` 并将结果写入 Redis。B 的本地缓存也是空的，但从 Redis 读到了 Alice，数据源调用次数仍为 1。后续对 A 或 B 的新鲜结果读取直接命中各自的本地缓存。

程序每次运行前只重置 `cacheq-quickstart` 命名空间中的示例记录，以便输出一致。这是演示准备步骤，业务启动时不应清空缓存；生产数据也不要使用该命名空间。完成后，在 Redis 终端按 Ctrl+C 停止临时服务。

## 使用方式

### 接入业务项目

按程序的创建顺序接入：

1. 在应用启动时创建 go-redis 客户端 `remote`，再通过 `redistore.New` 创建 `store`。它实现 cacheq 的 `SharedStore` 接口，负责 Redis 读写；同一个存储对象可以用于多个查询。`redistore.New` 不访问服务器，示例中的 `Ping` 用于提前发现连接问题。
2. 为每一种查询身份创建 `NewSharedCache[V]`，其中 `V` 是结果类型。示例中的 `shared` 只对应租户 42 的用户 7；查询其他用户时应使用不同的 key。配置可复用，不需要每次请求重新创建。
3. 将 `shared` 赋给 `QueryDefinition.Shared`。将示例的 `Fetcher` 替换成实际的数据库或 API 读取函数，使用传入的 context，并返回 `User` 和错误。`source`、`calls` 只是演示数据和计数器，业务代码不需要保留。
4. 在请求处理中调用 `definition.Fetch(ctx, client)`，取得 `User` 或错误。`ctx` 使用请求的 context；应用内复用同一个 `Client`，让不同请求共享本地结果。示例的 A、B 用于模拟两个独立实例。

不同进程可以各自创建连接、存储和查询配置，但共享结果需要一致的 Redis 地址、数据库、`Prefix`、`EnableTags`、共享 key、`Format` 和序列化方式。不要把不同租户、权限或查询参数对应的结果放在同一个 key 下。

应用退出时关闭 `Client`，再关闭应用持有的 Redis 连接。`Client.Close()` 不会关闭 `remote`，也不会删除 Redis 数据。示例中的 `defer` 按这个顺序执行。

### 修改数据后使缓存失效

先完成数据库或 API 写入，再调用 `definition.InvalidateShared`。该方法先使 Redis 记录失效，再使指定 Client 中的查询失效；随后读取会重新访问数据源。

将下面片段放在快速开始的 `main` 函数末尾、最后一条输出之后。它复用已有的 `source`、`definition`、`ctx`、`a` 和 `calls`，用修改内存数据模拟一次成功的业务写入：

```go
// Simulate a successful database or API write before invalidating the cache.
source.Store(User{Name: "Bob"})
if err := definition.InvalidateShared(ctx, a, cacheq.InvalidateOptions{
	Refetch: cacheq.RefetchNone,
}); err != nil {
	panic(err)
}
updated, err := definition.Fetch(ctx, a)
if err != nil {
	panic(err)
}
fmt.Printf("A refreshed: %s, source calls: %d\n", updated.Name, calls.Load())
```

新增输出为：

```text
A refreshed: Bob, source calls: 2
```

`RefetchNone` 只标记失效，不自动启动刷新；这里由紧接着的 `Fetch` 发起读取。省略该选项时，已观察且启用的查询可以按默认规则刷新，但 `InvalidateShared` 本身不等待刷新完成。

此时 B 仍可能返回本地的 Alice，直到新鲜期结束或本地失效。本例没有启用跨实例通知。若需要其他实例及时处理失效，按[共享失效通知与标签](shared-invalidation.zh-CN.md)配置 `EnableNotifications` 和 `WatchShared`；关联多个共享 key 时再启用远端标签。

### 只刷新当前 Client

如果只需要让 B 重新读取数据，在上一个片段之后插入以下代码。它复用已有的 `b` 和查询定义：

```go
if err := b.Invalidate(definition.Key, cacheq.InvalidateOptions{
	Refetch: cacheq.RefetchNone,
}); err != nil {
	panic(err)
}
refreshed, err := definition.Fetch(ctx, b)
if err != nil {
	panic(err)
}
fmt.Printf("B refreshed: %s, source calls: %d\n", refreshed.Name, calls.Load())
```

新增输出为：

```text
B refreshed: Bob, source calls: 3
```

本地失效后新启动的读取会绕过 Redis 中的数据并回源，因此调用次数增加。这种操作不会主动通知其他 Client。

选择失效 API 时，注意作用范围：

| API | 作用 |
| --- | --- |
| `definition.InvalidateShared(ctx, client)` | 先使共享记录失效，再使指定 Client 的查询失效；启用通知后其他 watcher 可异步收到事件 |
| `shared.Invalidate(ctx)` | 只执行共享失效，不直接操作任何 Client；启用通知后 watcher 仍可异步处理 |
| `client.Invalidate(key)` | 只使当前 Client 的查询失效；之后新启动的加载绕过共享数据 |

## 配置说明

### 查询身份与序列化

| 字段 | 用途 | 必填或默认值 | 使用约束 |
| --- | --- | --- | --- |
| `QueryDefinition.Key` | 当前 Client 内的查询身份 | 必填 | 使用受支持的可比较值，包含影响结果的租户、权限和参数；相同 key 使用一致的结果类型和加载配置 |
| `QueryDefinition.Shared` | 将查询连接到共享缓存 | 默认 `nil`，只使用本地缓存 | `SharedCache[V]` 的 `V` 与查询结果类型一致 |
| `SharedCacheOptions.Key` | 跨实例共享记录的身份 | 必填非空字符串 | 不会从本地 key 自动生成；示例为 `tenant:42:user:7:v1` |
| `SharedCacheOptions.Format` | 结果的数据结构和编码版本 | 必填非空字符串 | 示例为 `user.json.v1`；不兼容升级应同时使用新的共享 key |
| `SharedCacheOptions.Codec` | 对结果编码和解码 | 默认 `JSONCodec[V]` | JSON 只保留可序列化字段；自定义 `Codec[V]` 的方法必须支持并发调用 |

`Key` 决定“是不是同一份数据”，`Format` 决定“如何解释这份数据”。两个不兼容的调用方共用一个 key，即使 Format 不同，也会互相替换记录并反复回源。不要把 Format 校验当作数据迁移。

默认 JSON 支持可编码的零值、空值和 nil；缓存的 nil 仍然是有效结果。未导出字段、函数、channel 和指针身份不会被保留。共享 key 不应包含密钥，也不应依赖指针地址或任意值的 `fmt.Sprint` 输出。

### 新鲜期与超时

| 字段 | 用途 | 必填或默认值 | 使用约束 |
| --- | --- | --- | --- |
| `Options.StaleTime` | 允许复用数据源结果的新鲜时长 | 默认 `0`，立即 stale | 要复用共享记录，需为 Client 或查询消费者配置正值；示例为 1 分钟 |
| `SharedCacheOptions.TTL` | 共享结果从数据源加载完成起的最长新鲜期和存储时长 | 必填正时长 | 示例为 5 分钟；命中不续期 |
| `SharedCacheOptions.Timeout` | 每次共享存储调用的超时 | `0` 使用 1 秒；负值无效 | 示例为 200 毫秒；不控制 Fetcher 的超时 |

例如，A 在 12:00:00 从数据源读取 Alice。B 在 12:00:30 命中 Redis，`StaleTime` 为 1 分钟，则 B 只新鲜到 12:01:00，不会从命中时间重新计时。即使这条 Redis 记录的 TTL 还有剩余，此后也不能将它作为新鲜结果复用。如果 `StaleTime` 为 10 分钟而共享 TTL 为 5 分钟，新鲜期仍在 12:05:00 截止。

`GCTime` 和容量淘汰只管理本地缓存，不删除 Redis 记录。`MaxAge` 从本地安装结果时计时，也可能提前删除本地数据；它不是从数据源读取起计算的数据年龄保证。详见[缓存与生命周期](cache.zh-CN.md)。

`Timeout` 分别约束每次存储调用，不是整个查询的总耗时。请求 context 和查询的超时策略仍控制加载生命周期。为 go-redis 同时配置 context、连接、读写和重试预算；示例启用了 `ContextTimeoutEnabled`，并通过 `MaxRetries: -1` 禁用驱动重试。

### Redis 命名空间与可选能力

| 字段 | 用途 | 必填或默认值 | 使用约束 |
| --- | --- | --- | --- |
| `redistore.Options.Prefix` | 隔离应用或环境的 Redis 命名空间 | 空值使用 `cacheq` | 不允许 `{` 或 `}`；生产项目使用自己的命名空间 |
| `redistore.Options.EnableNotifications` | 发布失效事件并允许 watcher 订阅 | 默认 `false` | 需要额外的 Pub/Sub 权限；所有发布方都应启用 |
| `redistore.Options.EnableTags` | 启用跨 key 的远端标签失效 | 默认 `false` | 使用独立的数据布局；一个命名空间集中在一个 Cluster slot |
| `SharedCacheOptions.ShareTags` | 将 `QueryDefinition.Tags` 登记到远端 | 默认 `false` | 要求存储启用标签能力；仍会遵守同一共享记录上已登记的标签 |

通知和标签的接入示例、权限、watcher 清理及 Cluster 取舍见[共享失效通知与标签](shared-invalidation.zh-CN.md)。

## 错误处理与常见问题

`Fetch` 仍以数据源结果为准：Redis 读取失败会回源，编码或共享写入失败不会把成功的数据源读取变成查询错误。数据源本身失败则返回查询错误，并按配置处理重试和本地旧数据。调用方取消或查询超时仍可终止查询。

显式失效不采用静默降级：`InvalidateShared` 的远端操作失败时返回错误，并保留本地状态。业务写入已成功时，应单独处理缓存同步失败，不能因此盲目重试业务写入。传输超时可能意味着远端操作已经执行；可以重试失效操作，但已提交的远端失效不会因后续本地处理失败而回滚。

| 现象 | 检查原因 | 处理方式 |
| --- | --- | --- |
| `go run` 报找不到适配器或要求更新 Go | 本地替换路径错误，或 Go 版本不足 | 检查两个 `replace` 是否指向当前源码，使用 Go 1.26+ |
| 快速开始的 `Ping` 报错 | Redis 未启动、地址或权限错误 | 确认服务监听 `127.0.0.1:16379`；该检查是演示的启动检查，不是 cacheq 强制要求 |
| B 再次回源，`ReadErrors` 增加 | Redis 连接、ACL 或操作超时 | 检查连接选项、可写主节点和脚本权限；用下方统计区分故障和普通未命中 |
| `Hits` 为 0，但没有读错误 | `StaleTime` 为 0、记录已过期，或始终命中本地缓存 | 配置正新鲜期，用新的 Client 验证共享命中；本地命中不会增加共享 Hits |
| `DecodeErrors` 增加并回源 | Format 不匹配、记录损坏或 Codec 解码失败 | 对齐类型、Format 和 Codec；不兼容结果改用新 key |
| 数据源读取成功，但 `EncodeErrors` 或 `WriteErrors` 增加 | 结果不能编码，或 Redis 写入失败 | 检查结果字段和 Codec，再检查连接、超时及权限；本地结果仍可用 |
| A 失效后 B 仍读到旧值 | B 的本地结果仍新鲜，且未启用 watcher | 显式使 B 本地失效，或配置 `WatchShared` |
| 构造配置时报错 | key、Format 为空，TTL 非正数，或缺少标签能力 | 按配置表检查；无效共享配置返回 `ErrInvalidSharedCache`，缺少能力返回 `ErrSharedCapability` |

在快速开始的 `main` 函数末尾加入以下片段，读取已有 `shared` 的统计：

```go
stats := shared.Stats()
fmt.Printf("hits=%d misses=%d read_errors=%d write_errors=%d\n",
	stats.Hits, stats.Misses, stats.ReadErrors, stats.WriteErrors)
```

`shared.Stats()` 只累计这个 `SharedCache` 对象参与的操作，不是 Redis 服务端或所有进程的全局指标。`Bypasses` 表示强制刷新或失效后绕过共享数据的尝试；`WriteConflicts` 表示加载期间发生失效、共享写入被拒绝。Client 自身的统计继续描述本地缓存和加载。

## 行为与限制

- 读取顺序为本地新鲜结果、Redis 新鲜记录、数据源。新启动的强制刷新或失效刷新跳过 Redis 数据，但仍读取版本以约束后续写入。如果同 key 已经在加载，刷新仍加入现有加载，不会重启或升级它。刷新失败后的新加载继续绕过共享数据，直到成功加载或本地写入替换失效状态。
- 共享读取保留原始数据源完成时间。命中不延长 TTL；共享新鲜期结束不等于立即删除本地旧数据。跨机器应合理同步时钟，时间晚于读取方当前时间的记录按未命中处理。
- 某次加载取得共享版本后，如果记录在写回前被失效，这次加载不能把旧结果重新写回 Redis。被拒绝的写入增加 `WriteConflicts`；成功的数据源结果仍可返回，但本地结果保持 stale。这个保护不替应用排序数据库事务，也不解决同一版本内多个加载的完成顺序。
- 同 key 请求合并只发生在单个 Client 内，没有跨实例的分布式锁。多个实例同时未命中时，可能同时回源。共享缓存也不保存查询错误、订阅或持久化查询状态。
- `Set`、`Update` 只修改本地数据，不自动写入 Redis。`Get`、`Snapshot` 不读取 Redis；`Remove`、`Clear`、GC 和 `Client.Close()` 不删除共享记录或发布失效事件。
- 默认 Redis 布局为每个共享 key 保存一条会过期的数据 key 和一条不设置 TTL 的版本元数据 key。即使数据过期，元数据仍占空间，应按查询身份数量规划容量。丢失元数据后，下一次读取会创建新版本并丢弃孤立数据。
- 适配器使用原子 Lua 脚本，默认布局把同一记录的物理 key 放在同一个 Cluster slot。测试使用单机 Redis，不覆盖真实 Cluster 或 Sentinel 故障切换。旧快照恢复或故障切换丢失已确认写入时，不提供超出 Redis 本身的持久性保证。
- 存储和 Codec 调用在 Client 锁外执行；Executor 的一个槽覆盖整次加载。共享缓存故障本身不触发查询重试，数据源错误仍遵守查询重试策略。取消是协作式的，驱动和 Fetcher 都需要响应 context。

## 完整示例与相关文档

- [完整 Redis 示例与运行说明](../examples/redis/)：运行 `shared_cache` 验证共享读取、显式失效和 Redis 故障回源；运行 `shared_invalidation` 验证标签及跨实例通知。
- [共享失效通知与标签](shared-invalidation.zh-CN.md)：让其他实例及时标记本地结果 stale，并对详情、列表等多个查询统一失效。
- [缓存与生命周期](cache.zh-CN.md)：配置本地新鲜期、数据保留与资源清理。
- [失效与刷新](invalidation.zh-CN.md)：选择自动刷新策略并等待本地刷新完成。
- [SharedStore 契约](../shared_store.go)：实现自定义存储时所需的原子读写、版本校验、context 和数据所有权要求。
