# 按 key 查看诊断事件

[English](events.en-US.md)

`Client.SubscribeEvents` 用来回答：这个商品为什么又加载了？谁发起的？多少次调用加入了同一次加载？最后结果有没有写进缓存？`Stats()` 继续提供整体统计，事件则提供具体 key 的过程。服务端、后台任务、CLI 和查询句柄都能使用。

订阅只接收之后发生的事件，不保存历史，也不会改变现有的新鲜度、重试、取消和请求合并规则。

## 可以直接运行的例子

把程序保存成已导入 cacheq 的 Go 模块中的 `main.go`，运行 `go run .`。无需服务器或凭据。

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main observes a first load, cache reuse, and client shutdown for one key.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	events, err := client.SubscribeEvents(cacheq.EventOptions{Key: "product:42", Buffer: 16})
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

	// This short example fits in the buffer; a running service should consume concurrently.
	client.Close()
	for event := range events.Events() {
		fmt.Printf("%v %s load=%d source=%s reason=%s joined=%d attempts=%d\n",
			event.Key, event.Kind, event.LoadID, event.Trigger, event.Reason, event.Joined, event.Attempts)
	}
	fmt.Printf("dropped=%d\n", events.Dropped())
}
```

预期输出：

```text
product:42 load_started load=1 source=fetch reason=missing joined=0 attempts=0
product:42 load_finished load=1 source=fetch reason=missing joined=0 attempts=1
product:42 cache_hit load=0 source=fetch reason=fresh joined=0 attempts=0
<nil> client_closed load=0 source=none reason=client_closed joined=0 attempts=0
dropped=0
```

第一次 `Fetch` 调用加载函数，第二次直接复用缓存。前两行的 `load=1` 表示同一次加载的开始与结束；缓存命中没有关联的加载，所以 ID 为零。开始时 `attempts=0` 表示加载函数还没有执行，结束时为 1，表示实际调用了一次。

## 看一个 key，或者看全部

```go
events, err := client.SubscribeEvents(cacheq.EventOptions{
	Key:    "product:42",
	Buffer: 256,
})
```

- 不填 `Key` 就看所有 key。指定的 key 必须可以比较，而且与自身相等；切片、map、包含 NaN 的 key 返回 `ErrInvalidKey`。
- `Buffer: 0` 默认预留 128 条事件；负数返回 `ErrInvalidEventOptions`。
- Client 已关闭时返回 `ErrClosed`。
- 加载已经开始后才订阅，可以收到后续加入、结束事件，但不会补发开始事件。即使无人订阅，每次加载也有自己的 `LoadID`。

持续运行的服务应该边运行边读事件，可以接到已有的日志或监控代码：

```go
go func() {
	for event := range events.Events() {
		log.Printf("key=%v event=%s load=%d source=%s reason=%s joined=%d attempts=%d duration=%s applied=%t err=%v",
			event.Key, event.Kind, event.LoadID, event.Trigger, event.Reason,
			event.Joined, event.Attempts, event.Duration, event.Applied, event.Err)
	}
}()
```

这里的 `log` 是标准库包。退出日志消费时调用 `events.Close()`，只关闭诊断订阅；调用 `client.Close()` 也会关闭全部事件订阅。关闭后仍能读完缓冲区里已收到的事件。

## 发生了什么

`Kind` 表示发生的事情，`String()` 提供方便记录日志的名称。

| Kind / 日志名称 | 人话解释 |
| --- | --- |
| `EventCacheHit` / `cache_hit` | 这次调用认为缓存还新鲜，直接复用了结果 |
| `EventLoadStarted` / `load_started` | 登记了一次新的共享加载 |
| `EventLoadJoined` / `load_joined` | 另一次调用加入正在进行的加载 |
| `EventRetryStarted` / `retry_started` | 真正开始下一次重试，`Err` 是上次失败的错误 |
| `EventLoadFinished` / `load_finished` | 加载结束，包括失败、取消 |
| `EventResultDiscarded` / `result_discarded` | 加载结束了，但结果没有写入共享缓存 |
| `EventInvalidated` / `invalidated` | 已有 key 被主动标记为需要重新加载 |
| `EventCacheRemoved` / `cache_removed` | 缓存状态被删除，或数据达到 `MaxAge` |
| `EventLocalWrite` / `local_write` | `Set` 成功写入本地值 |
| `EventClientClosed` / `client_closed` | Client 关闭；key 为 nil，只订阅一个 key 也会收到 |

`Get` 和 `Snapshot` 只是查看当前状态，不记录缓存命中；但发现数据超过 `MaxAge` 时会记录删除事件。被拒绝的 API 调用、删除不存在的 key 不产生事件。内部释放空的类型记录也不会产生删除事件。

## 为什么加载，谁发起的

`Trigger` 表示来源：`fetch`、`query`、`enable`、`refetch`、`invalidate`、`prefetch`。重试和结束事件保留原发起者；加入事件记录加入者使用的 API。失效事件的来源也是 `invalidate`，即使没有立即刷新。写入、删除、关闭的来源为 `none`。

`Reason` 表示当时的原因：

| 日志名称 | 人话解释 |
| --- | --- |
| `fresh` | 这次调用认为缓存可以继续用 |
| `missing` | 没有可用缓存 |
| `expired` | 按这次调用的新鲜时间，缓存已经过期 |
| `invalidated` | 被主动标记过失效，需要新数据 |
| `previous_failure` | 上一次加载失败，结果仍需要重新加载 |
| `refetch` | 调用了句柄的 `Refetch`，要求重新加载 |
| `capacity` | 超过 `MaxEntries`，被 LRU 淘汰 |
| `inactive` | 闲置达到 `GCTime`，被自动回收 |
| `max_age` | 数据达到最大保存时间，不能再提供旧值 |
| `remove` / `clear` | 主动删除这个 key，或者清空缓存 |
| `set` | 本地写入覆盖了旧加载，旧结果被丢弃 |
| `cancel` | 调用取消、超时，或执行了 `Client.Cancel` |
| `client_closed` | Client 结束了生命周期 |

不同调用可以设置不同的 `StaleTime`，因此同一个 key 对一个调用是 `fresh`，对另一个调用是 `expired`，这符合现有规则。`FetchWithExpiry` 失败时如果返回未来的过期时间，可以缓存备用数据和错误：复用时记录带 `Err` 的 `cache_hit`，之后到期记录 `expired`。

删除原因保留在事件流里，不会无限保存已经删除的 key。例如先收到 `cache_removed reason=capacity`，下次加载的原因是 `missing`。`MaxAge` 清除数据后，如果还保留这个 key 的类型记录，下一次加载会带 `max_age` 原因。

## 怎么看合并次数和最终结果

- `LoadID`：同一个 Client 内的一次共享加载编号。开始、加入、重试、结束、丢弃都使用同一个编号。加载中主动失效也带这个编号；其他缓存变化为零。
- `Joined`：这次加载累计多了多少次加入。结束时为 3，表示除了发起者，又有三次加入。它不表示独立用户数、当前等待人数或旁观的句柄数；加入后取消仍计入。
- 原发起者取消后，如果剩下的调用重新发起加载，新加载有新的 ID，合并次数重新计算。`Stats().MergedRequests` 保留原来的按调用计数规则。
- `Attempts`：这个 key 的加载函数真正执行了几次，包括首次和实际重试。使用 `Batcher` 时，两个 key 的加载可能共用一次批量回调，因此不能拿它当 SQL 或 HTTP 请求次数。
- `Duration`：结束时的整次加载耗时，包含调度等待和重试间隔。`Time` 使用 `Options.Clock`；耗时使用独立的计时来源。
- `Applied`：结果有没有写入缓存。错误被写入、加载中被失效而仍写入旧状态，也算 true；它不表示加载成功，也不保证值一直保留。
- 结果不能写入时，先发 `load_finished`，其中 `Applied: false`，再发 `result_discarded`，说明是取消、`Set`、`Remove` 还是 `Clear` 导致。迟到的旧结果不会覆盖新状态。

事件包含 key 和错误，不包含缓存数据。记录日志时按业务需要处理 key 和错误信息。

## 读得慢会怎样

事件发送不会等日志消费，也不会执行应用的日志回调。缓冲区满了就丢弃新事件，并增加 `events.Dropped()`；已有事件保持顺序。这个计数不为零，表示诊断记录可能不完整，不能作为必须一条不漏的审计记录。

`Sequence` 是 Client 内发出的事件序号。只订阅一个 key、或者缓冲区满了，都可能看到跳号。不同订阅收到同一条事件时，序号和时间戳相同。一次多 key 操作会逐 key 发事件，但按 map 选择 key 时不保证 key 的顺序。

Client 关闭时会尝试发送 `client_closed`，然后关闭所有事件 channel；如果缓冲区满了，关闭事件也可能被丢弃，但 channel 一定关闭。`Client.Close` 不等待忽略取消信号的加载函数，这些函数迟到的结束结果不会再发到已经关闭的事件流。
