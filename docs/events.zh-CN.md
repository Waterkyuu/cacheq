# 诊断事件

[English](events.en-US.md)

`Client.SubscribeEvents` 提供按 key 过滤的诊断事件流，用于记录缓存命中、加载、请求合并、重试、失效和删除过程。该 API 可用于服务端、后台任务、CLI 及使用 `Query` 的应用。

`Stats()` 提供 Client 的累计统计；诊断事件提供单个 key 的操作来源、加载原因和执行结果。订阅不会改变现有的缓存、请求合并或取消行为。

## 快速开始

以下示例订阅 `product:42`，执行两次 `Fetch`，然后输出加载与缓存命中事件。将代码保存为已导入 cacheq 的 Go 模块中的 `main.go`，运行 `go run .`。

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main records loading and cache reuse for a single query key.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()

	events, err := client.SubscribeEvents(cacheq.EventOptions{
		Key:    "product:42",
		Buffer: 16,
	})
	if err != nil {
		panic(err)
	}
	defer events.Close()

	load := func(context.Context) (string, error) { return "product 42", nil }
	for range 2 {
		if _, err := cacheq.Fetch(context.Background(), client, "product:42", load); err != nil {
			panic(err)
		}
	}

	// The example buffers four events; services should consume the stream concurrently.
	client.Close()
	for event := range events.Events() {
		fmt.Printf("%v %s load=%d source=%s reason=%s joined=%d attempts=%d\n",
			event.Key, event.Kind, event.LoadID, event.Trigger, event.Reason, event.Joined, event.Attempts)
	}
	fmt.Printf("dropped=%d\n", events.Dropped())
}
```

输出：

```text
product:42 load_started load=1 source=fetch reason=missing joined=0 attempts=0
product:42 load_finished load=1 source=fetch reason=missing joined=0 attempts=1
product:42 cache_hit load=0 source=fetch reason=fresh joined=0 attempts=0
<nil> client_closed load=0 source=none reason=client_closed joined=0 attempts=0
dropped=0
```

第一次调用启动加载，开始与结束事件共享 `LoadID: 1`；第二次调用命中新鲜缓存，不执行加载函数。`Attempts` 在开始事件中为 0，在结束事件中为 1，表示实际执行了一次加载。

示例在关闭 Client 后读取缓冲事件。持续运行的应用应并发消费事件流，避免缓冲区长期占满。

## 订阅配置

```go
subscription, err := client.SubscribeEvents(cacheq.EventOptions{
	Key:    "product:42",
	Buffer: 256,
})
```

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `Key` | `nil` | `nil` 订阅全部 key；其他值按 key 精确过滤 |
| `Buffer` | `128` | 未读事件的最大数量；设置为 0 使用默认值 |

指定的过滤 key 必须可比较且与自身相等。切片、map 和包含 NaN 的 key 返回 `ErrInvalidKey`；负数缓冲区容量返回 `ErrInvalidEventOptions`；Client 已关闭时返回 `ErrClosed`。

订阅仅接收创建后的事件，不补发历史。加载中途订阅时，可能收到加入或结束事件，而没有对应的开始事件。

### 日志接入

以下片段使用标准库 `log` 包，在独立 goroutine 中消费已创建的订阅：

```go
go func() {
	for event := range subscription.Events() {
		log.Printf("key=%v event=%s load=%d trigger=%s reason=%s joined=%d attempts=%d duration=%s applied=%t err=%v",
			event.Key, event.Kind, event.LoadID, event.Trigger, event.Reason,
			event.Joined, event.Attempts, event.Duration, event.Applied, event.Err)
	}
}()
```

消费结束时调用 `subscription.Close()`。该操作只关闭诊断订阅，不取消查询或修改缓存。事件包含 key 和错误，不包含缓存值。

## 事件字段

| 字段 | 说明 |
| --- | --- |
| `Sequence` | Client 内发出的事件序号；过滤和丢弃可能造成序号不连续 |
| `Time` | 事件发生时间，使用 `Options.Clock` |
| `Key` | 查询 key；`EventClientClosed` 的 key 为 `nil` |
| `Kind` | 事件类型 |
| `LoadID` | Client 内共享加载的编号；未关联加载时为 0 |
| `Trigger` | 触发本次操作的 API 来源 |
| `Reason` | 缓存决策、删除或结果丢弃的原因 |
| `Joined` | 同一次加载累计的额外加入次数 |
| `Attempts` | 已执行的加载函数次数，包含首次调用和实际重试 |
| `Duration` | 加载结束时的总耗时，包含调度等待和重试间隔；计时独立于 `Options.Clock` |
| `Err` | 命中时保留的错误、重试前的错误或加载最终错误 |
| `Applied` | 加载结束时，结果是否写入共享缓存 |

## 事件类型

`Kind.String()` 返回下表中的日志名称。

| 类型 | 日志名称 | 触发条件 |
| --- | --- | --- |
| `EventCacheHit` | `cache_hit` | 查询决策复用了当前消费者认为新鲜的结果 |
| `EventLoadStarted` | `load_started` | 注册新的共享加载 |
| `EventLoadJoined` | `load_joined` | 另一个消费者加入正在执行的加载 |
| `EventRetryStarted` | `retry_started` | 开始额外的加载尝试；`Err` 为上次失败的错误 |
| `EventLoadFinished` | `load_finished` | 加载结束，包括失败和取消 |
| `EventResultDiscarded` | `result_discarded` | 加载结果未写入共享缓存 |
| `EventInvalidated` | `invalidated` | 已有 key 被主动标记为失效 |
| `EventCacheRemoved` | `cache_removed` | 缓存数据或错误被删除，或数据达到 `MaxAge` |
| `EventLocalWrite` | `local_write` | `Set` 成功写入本地值 |
| `EventClientClosed` | `client_closed` | Client 关闭；向所有订阅广播，包括指定 key 的订阅 |

`Get` 和 `Snapshot` 不产生缓存命中事件，但在发现数据超过 `MaxAge` 时会产生删除事件。被拒绝的 API 调用、删除不存在的 key 或空的类型绑定，不产生事件。存活的句柄可以在 `Remove` 或 `Clear` 后保留类型绑定；在重新写入数据或错误之前，重复执行这两种操作不会再次产生删除事件。

## 操作来源与原因

### 操作来源

| `Trigger` | 对应操作 |
| --- | --- |
| `TriggerFetch` | `Fetch`、`FetchWithOptions`、`FetchWithExpiry` |
| `TriggerQuery` | 创建启用的 `Query` 时自动加载或复用缓存 |
| `TriggerEnable` | `SetEnabled(true)` 从禁用切换为启用 |
| `TriggerRefetch` | `QueryHandle.Refetch` |
| `TriggerInvalidate` | 主动失效，以及失效引起的自动刷新 |
| `TriggerPrefetch` | `Prefetch` |
| `TriggerNone` | 本地写入、缓存删除或 Client 关闭等操作 |

重试和结束事件保留原加载发起者的来源；加入事件记录加入者的来源。失效事件使用 `TriggerInvalidate`，不表示一定启动了刷新。

### 原因

| `Reason` | 日志名称 | 说明 |
| --- | --- | --- |
| `ReasonFresh` | `fresh` | 当前消费者可以复用缓存 |
| `ReasonMissing` | `missing` | 没有可用缓存 |
| `ReasonExpired` | `expired` | 按当前消费者的新鲜度配置，缓存已过期 |
| `ReasonInvalidated` | `invalidated` | 缓存被主动标记为失效 |
| `ReasonPreviousFailure` | `previous_failure` | 上一次加载失败，结果仍需重新加载 |
| `ReasonRefetch` | `refetch` | 显式调用 `Refetch`，要求重新加载 |
| `ReasonCapacity` | `capacity` | 因 `MaxEntries` 限制被 LRU 淘汰 |
| `ReasonInactive` | `inactive` | 闲置达到 `GCTime`，被自动回收 |
| `ReasonMaxAge` | `max_age` | 数据达到最大可用年龄 |
| `ReasonRemove` | `remove` | 主动删除单个 key |
| `ReasonClear` | `clear` | 主动清空缓存 |
| `ReasonSet` | `set` | 本地写入取代旧加载，旧结果被丢弃 |
| `ReasonCancel` | `cancel` | 调用取消、超时或 `Client.Cancel` 导致结果丢弃 |
| `ReasonClientClosed` | `client_closed` | Client 生命周期结束 |
| `ReasonNone` | `none` | 无适用原因 |

普通缓存的新鲜度按消费者配置计算。同一个 key 可以对一个消费者产生 `fresh`，对另一个消费者产生 `expired`。`FetchWithExpiry` 失败时，如果返回未来的过期时间，备用数据与错误仍可复用；命中事件保留 `Err`，到期后原因是 `expired`。

删除历史只保留在事件流中。例如容量淘汰产生 `cache_removed reason=capacity`，之后重新加载的原因是 `missing`。`MaxAge` 清除数据后，若该 key 的类型记录仍保留，后续加载原因是 `max_age`。

## 加载关联与计数

开始、加入、重试、结束和丢弃事件使用同一个 `LoadID`。加载中的失效事件也携带该编号；其他缓存变化的编号为 0。无人订阅时仍会分配加载编号，因此中途订阅可关联后续事件。

`Joined` 记录额外加入次数，不包含发起者。结束事件的 `Joined: 3` 表示这次加载累计收到三次额外加入；它不表示独立用户数、当前等待人数或被动观察的句柄数，取消后的加入仍计入。

原发起者取消后，如果剩余调用重新发起加载，新加载使用独立编号和加入计数。`Stats().MergedRequests` 保持原有的按调用统计规则。

`Attempts` 统计每个 key 的加载函数调用，不推断数据库或 HTTP 请求次数。使用 `Batcher` 时，多个 key 的加载函数可以共用一次批量回调。

## 结果应用与丢弃

`EventLoadFinished.Applied` 表示结果是否安装到缓存。缓存中安装的错误，以及加载中被失效但仍安装的结果，也算已应用；该字段不表示执行成功或永久保留。

未应用的结果先产生 `EventLoadFinished`，其中 `Applied: false`，随后产生 `EventResultDiscarded`。丢弃原因可为取消、`Set`、`Remove` 或 `Clear`。迟到的旧结果不会覆盖替代状态。

## 投递与关闭规则

- 发送事件不等待消费者，也不执行应用的日志回调。
- 缓冲区满时丢弃新事件，`Dropped()` 返回累计丢弃数量；已有事件保持顺序。
- `Sequence` 在 Client 内递增。不同订阅收到同一条事件时，序号和时间戳相同；过滤或丢弃会造成跳号。
- 多 key 操作逐 key 发事件；按 map 选择 key 时，不保证 key 的顺序。
- `EventSubscription.Close()` 和 `Client.Close()` 会关闭相应的事件 channel；关闭后仍可读取已缓冲的事件。
- Client 关闭前尝试发送 `EventClientClosed`；缓冲区满时该事件也可能被丢弃，但 channel 仍会关闭。
- `Client.Close()` 不等待忽略取消的加载函数，迟到的完成事件不会发送到已关闭的订阅。

诊断事件采用有界投递，适用于日志和问题排查；需要完整记录时，应监控 `Dropped()`。该事件流不提供无损审计保证。
