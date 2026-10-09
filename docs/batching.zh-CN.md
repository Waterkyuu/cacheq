# 自动批量加载

[English](batching.en-US.md)

`Batcher[K, V]` 将不同 key 的并发加载请求收集为一批，并调用业务提供的 `BatchFunc`。该功能适用于支持批量查询的数据源，例如 SQL `WHERE id IN (...)` 或批量 HTTP 接口。

业务负责实现批量查询；cacheq 不探测后端能力，也不生成 SQL。批量函数内部逐个调用单对象接口时，后端调用次数不会减少。

## 快速开始

以下示例通过两个 `Query` 加载不同商品，并共享一次批量回调。将代码保存为已导入 cacheq 的 Go 模块中的 `main.go`，运行 `go run .`。

示例使用 `Wait: time.Hour` 和 `MaxBatchSize: 2`，由达到数量上限触发执行，使输出不依赖短暂的计时窗口。实际配置应使用符合加载延迟要求的收集时间。

```go
package main

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main groups different query keys and reuses their individually cached results.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	var calls atomic.Int32
	products, err := cacheq.NewBatcher(
		context.Background(),
		func(_ context.Context, ids []int) (map[int]cacheq.BatchResult[string], error) {
			calls.Add(1)
			sort.Ints(ids)
			fmt.Printf("bulk load: %v\n", ids)
			results := make(map[int]cacheq.BatchResult[string], len(ids))
			for _, id := range ids {
				results[id] = cacheq.BatchResult[string]{Data: fmt.Sprintf("product %d", id)}
			}
			return results, nil
		},
		cacheq.BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	if err != nil {
		panic(err)
	}
	defer products.Close()

	first := cacheq.Query(client, "product:42", products.Fetcher(42))
	defer first.Close()
	second := cacheq.Query(client, "product:43", products.Fetcher(43))
	defer second.Close()
	for _, handle := range []*cacheq.QueryHandle[string]{first, second} {
		for state := range handle.Updates() {
			if state.Err != nil {
				panic(state.Err)
			}
			if state.HasData && !state.Fetching {
				fmt.Println(state.Data)
				break
			}
		}
	}
	value, err := cacheq.Fetch(context.Background(), client, "product:42", products.Fetcher(42))
	if err != nil {
		panic(err)
	}
	fmt.Printf("cached: %s; bulk calls: %d\n", value, calls.Load())
}
```

输出：

```text
bulk load: [42 43]
product 42
product 43
cached: product 42; bulk calls: 1
```

两个 `Query` 在后台发起加载，ID 42 和 43 进入同一批次。最后的 `Fetch` 命中商品 42 的缓存，不再调用批量函数。

## 接入查询 API

`products.Fetcher(42)` 返回绑定批量 key `42` 的 `Fetcher[V]`，创建该函数不会启动加载。它可传给 `Fetch`、`FetchWithOptions`、`Query` 和 `Prefetch`。

Client 先判断是否可复用缓存；需要加载时，才调用该 Fetcher 并进入批量收集窗口。每个 Client key 保留独立的缓存条目和订阅。

| key 类型 | 示例 | 用途 |
| --- | --- | --- |
| Client 缓存 key | `"product:42"` | 标识缓存数据、订阅和失效范围 |
| 批量 key | `42` | 传给业务批量函数，用于匹配结果 |

不使用 Client 缓存时，可直接调用 `products.Load(ctx, id)`。批量加载器仅合并排队和执行中的请求，不保存已完成的结果。

## API 与配置

| API | 说明 |
| --- | --- |
| `NewBatcher(ctx, load, options)` | 创建批量加载器；`ctx` 管理其生命周期，`load` 为业务批量函数 |
| `Load(ctx, key)` | 加入或创建该 key 的加载，并等待对应结果 |
| `Fetcher(key)` | 返回供现有查询 API 调用的加载函数，不立即执行 |
| `Close()` | 关闭加载器，释放等待者并取消执行中的批次；可重复调用 |

`BatchFunc[K, V]` 的签名为 `func(context.Context, []K) (map[K]BatchResult[V], error)`。构造函数和 `Load` 必须接收非 nil 的 context。

| `BatchOptions` 字段 | 默认值或约束 | 说明 |
| --- | --- | --- |
| `Wait` | 零值为 0，不能为负数 | 从空窗口收到首个唯一 key 起计时；0 表示不主动增加收集延迟 |
| `MaxBatchSize` | 必须大于 0，无默认值 | 单批唯一 key 的数量上限，达到上限立即执行 |

`MaxBatchSize` 不限制批次并发数。不同批次可以并发执行，业务回调必须支持并发调用。串行请求逐次等待结果时，不能合并为同一批。

## 返回结果

业务函数返回 `map[K]BatchResult[V]`，按 key 匹配结果，不依赖响应顺序。

| `BatchResult[V]` 字段 | 说明 |
| --- | --- |
| `Data` | 对应 key 的数据；零值也是有效结果 |
| `Err` | 对应 key 的错误，只影响该 key |

批量函数返回整体错误时，所有请求 key 收到该错误，结果 map 被忽略。整体成功时，缺失的请求 key 返回包装后的 `ErrBatchResultMissing`；额外返回的 key 被忽略。

不存在的业务对象应返回明确的逐 key 业务错误。遗漏条目表示批量响应不完整，不等同于对象不存在。

## 错误处理

| 条件 | 错误或结果 |
| --- | --- |
| 批量函数为 nil | `ErrNoFetcher` |
| `Wait` 为负数或 `MaxBatchSize` 非正数 | `ErrInvalidBatchOptions` |
| 构造时父 context 已取消 | 父 context 的错误 |
| key 为 nil、运行时不可比较或不与自身相等，例如 NaN | `ErrInvalidKey` |
| 成功响应缺少请求 key | 包装后的 `ErrBatchResultMissing` |
| 调用者取消或截止时间到达 | `context.Canceled` 或 `context.DeadlineExceeded` |
| 加载器已显式关闭 | `ErrBatcherClosed` |
| 批量函数或某个条目返回业务错误 | 对应业务错误 |

通过 `errors.Is` 判断库错误和业务错误。传入单对象函数而非 `BatchFunc` 时，Go 在编译期报告类型不匹配；运行时不会自动改用单对象接口。

## 取消与生命周期

构造函数的 context 决定批量回调的上下文值、截止时间和取消。单个调用者的 context 只控制自己的等待，不将其上下文值或截止时间传入共享回调。

- 一个调用者取消，不影响其他调用者；排队 key 的所有等待者退出后，该 key 被移出收集窗口。
- 执行中批次的所有 key 都失去等待者时，批次回调被取消。
- `Close()` 使等待者收到 `ErrBatcherClosed`，并取消执行中的回调，但不等待忽略取消信号的函数退出。
- 父 context 取消时，等待者收到父 context 的错误。
- 批次被放弃或关闭后返回的结果被丢弃。

按数据源和权限范围分别创建加载器。Client key 和批量 key 应包含影响结果的身份及查询参数，不能依赖某个调用者的私有 context。共享返回数据应视为不可变。

## 与缓存策略的关系

缓存新鲜度、重试和每个 key 的加载超时由 Client 管理。收集窗口等待计入该 key 的超时；失败 key 可进入后续批次重试，成功 key 不会因此重新加载。

`ErrBatcherClosed` 对 Client 加载是终止错误，包括包装后的错误。关闭批量器后，受影响的加载会结束，不会继续重试、等待退避或调用 `RetryIf`。后续后端请求需要使用新批量器；重试已关闭的调度器无法恢复。

失效操作可以让多个被订阅的 key 通过同一加载器刷新。失效一个 key 不会连带失效同批其他 key。`Set`、`Remove` 和取消继续保护缓存，避免迟到结果覆盖新状态。

`Stats()` 和[诊断事件](events.zh-CN.md)统计逐 key 加载，不代表批量回调或数据库调用次数；需要记录实际批量次数时，在业务回调中计数。
