# 可观测性与客户端统计

[English](observability.en-US.md) · [缓存与生命周期](cache.zh-CN.md) · [查询与条件请求](queries.zh-CN.md)

通过 `client.Stats()` 查看缓存复用、请求合并、加载结果和清理情况。以下程序可以直接运行，两次读取同一个键只执行一次加载。

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// main demonstrates one load followed by a fresh cache hit.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	load := func(context.Context) (string, error) {
		return "hello", nil
	}
	for i := 0; i < 2; i++ {
		if _, err := cacheq.Fetch(
			context.Background(),
			client,
			"greeting",
			load,
		); err != nil {
			log.Fatal(err)
		}
	}
	state := client.Stats()
	fmt.Printf(
		"hits=%d misses=%d loads=%d entries=%d\n",
		state.CacheHits,
		state.CacheMisses,
		state.Loads,
		state.CacheEntries,
	)
}
```

输出为 `hits=1 misses=1 loads=1 entries=1`。业务中可以定期读取统计，记录日志或接入现有监控系统。

## `Stats`：查看缓存收益与加载统计

```go
state := client.Stats()
fmt.Printf(
	"hits=%d misses=%d merged=%d loads=%d retries=%d entries=%d\n",
	state.CacheHits,
	state.CacheMisses,
	state.MergedRequests,
	state.Loads,
	state.Retries,
	state.CacheEntries,
)
completed := state.LoadSuccesses + state.LoadFailures + state.LoadCancellations
if completed > 0 {
	fmt.Println("average load duration:", state.TotalLoadDuration/time.Duration(completed))
}
```

`Stats()` 返回当前客户端的一致值快照。计数从创建客户端开始累计，`Clear` 和 `Close` 不会重置。读取统计不会请求数据、更新使用顺序、重启 GC 定时器或清理超龄数据。不同客户端各自统计。

| 字段 | 含义 |
| --- | --- |
| `CacheHits` | 合法的非强制 `Fetch`、`FetchWithExpiry`、`Prefetch` 调用，以及创建启用的 `Query` 或从禁用变为启用时，复用了新鲜数据 |
| `CacheMisses` | 上述操作没有新鲜数据，每次调用或启用转换只计一次，包括返回 `ErrNoFetcher` 的情况 |
| `MergedRequests` | 加入已有加载的调用或启用消费者数；原所有者取消后需要替代加载，也不会把同一个调用重复计数 |
| `Loads` | 启动的共享加载次数，包括手动 `Refetch` 和失效后自动刷新；重试属于同一次加载 |
| `LoadSuccesses` | 最终无错误的已完成加载次数 |
| `LoadFailures` | 最终有错误的已完成加载次数，不包含取消和截止时间错误 |
| `LoadCancellations` | 以取消或截止时间错误结束的加载次数；只取消一个等待者不等于取消共享加载 |
| `Retries` | 实际执行的追加加载尝试次数；等待重试时取消不计入 |
| `TotalLoadDuration` | 已完成加载的总耗时，包括调度、重试等待和取消；使用独立于 `Options.Clock` 的耗时时钟 |
| `CacheEntries` | 当前保留的结果和错误条数，不含空类型元数据；尚未被缓存操作发现的超龄值仍计入 |
| `LRUEvictions` | 为满足 `MaxEntries` 而淘汰的结果或错误条数 |
| `GCCollections` | 被闲置缓存定时器清理的键数 |
| `AgeExpirations` | 缓存操作发现达到 `MaxAge` 后清理的值数，每个写入值只计一次 |

`Get`、查询对象的 `Snapshot()`、创建禁用查询，以及非法参数或开始时已取消的调用，不计入缓存命中和未命中。带错误的新鲜兜底值仍算命中。创建启用查询时发现旧数据算未命中，即使仍能展示旧值。手动 `Refetch` 和失效刷新计入加载次数，不增加命中或未命中次数。

运行中的请求已经计入 `Loads`，但结果分类和耗时在结束时才计入。被取消或脱离缓存的加载仍会统计，即使结果没有写入缓存；`Close` 后，之前启动的请求完成时仍可能更新计数。手动 `Remove` 和 `Clear` 不增加按清理原因分类的计数。

## 验证

```sh
go test -race ./... -run 'Test(Stats|HTTPStats)' -count=1
```

[统计测试](../stats_test.go) 覆盖计数边界、取消、重试、清理和客户端隔离；[HTTP e2e](../e2e/stats_test.go) 将共享请求、重试、缓存复用和刷新结果与实际 HTTP 请求次数核对。
