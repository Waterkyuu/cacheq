# 自动批量加载

[English](batching.en-US.md)

`Batcher[K, V]` 收集不同 key 的并发加载请求，把去重后的 key 列表交给业务提供的 `BatchFunc`。例如业务函数可以执行一次 SQL `WHERE id IN (...)`，或调用支持多个 ID 的 HTTP 接口。cacheq 不会探测后端能力，也不生成 SQL。如果业务函数内部仍然循环调用单对象接口，后端调用次数不会减少。

## 完整可运行示例

下面的程序不需要服务器或凭据。示例故意使用较长的收集窗口：两个并发 key 达到 `MaxBatchSize` 后立即执行，输出不依赖机器运行速度。在实际项目中，应按后端延迟预算选择较短的窗口。

```go
package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main demonstrates two concurrent callers sharing one bulk callback.
func main() {
	products, err := cacheq.NewBatcher(
		context.Background(),
		func(_ context.Context, ids []int) (map[int]cacheq.BatchResult[string], error) {
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

	ids := []int{42, 43}
	values := make([]string, len(ids))
	var callers sync.WaitGroup
	for i, id := range ids {
		callers.Add(1)
		go func() {
			defer callers.Done()
			value, err := products.Load(context.Background(), id)
			if err != nil {
				panic(err)
			}
			values[i] = value
		}()
	}
	callers.Wait()
	fmt.Println(values)
}
```

预期输出：

```text
bulk load: [42 43]
[product 42 product 43]
```

## 调度和生命周期

- `Wait` 从空窗口收到第一个唯一 key 时开始计时。零值不额外等待。串行调用并逐次等待结果，无法把这些调用合成同一批。
- `MaxBatchSize` 必须为正数，统计唯一 key 的数量。达到上限立即执行。不同批次可以并发运行，这个配置不是全局并发限制。
- 同 key 共享排队或执行中的加载。批量加载器不缓存已完成结果，下一次 `Load` 会重新加载。
- 构造函数的 context 决定批量函数的上下文值、截止时间和取消。单个调用者的 context 只控制自己的等待，不会把其上下文值或截止时间带进共享批次。
- 一个调用者取消，不影响其他调用者。排队中的 key 没有任何等待者时会被移除；执行中的批次在所有 key 都失去等待者后才取消。
- `Close` 可重复调用，让等待者收到 `ErrBatcherClosed`，并通知执行中的批量函数停止。它不等待忽略 context 的函数退出。父 context 取消时，等待者收到父 context 的错误。
- 不同数据源和权限范围使用独立的批量加载器。身份和查询参数应明确进入 key，不依赖某个调用者的 context。业务函数需要支持批次并发，发布后的返回数据应视为不可变。

## 返回结果和报错

通过 `map[K]BatchResult[V]` 按 key 对应结果，不要求后端按请求顺序返回。只要 map 中包含该 key，即使 `Data` 是零值也是有效结果。

| 情况 | 处理方式 |
| --- | --- |
| 把单对象函数传给批量入口 | Go 编译时报类型不匹配 |
| 批量函数为 nil | 构造函数返回 `ErrNoFetcher` |
| `Wait` 为负数或 `MaxBatchSize` 非正数 | 构造函数返回 `ErrInvalidBatchOptions` |
| 整个批量函数返回错误 | 所有请求 key 收到该错误，返回的 map 被忽略 |
| 某个 map 条目含有 `Err` | 只有该 key 收到对应的数据和错误 |
| 成功返回的 map 缺少请求 key | 该 key 收到包装后的 `ErrBatchResultMissing` |
| 返回额外的 key | 忽略该条目 |
| 调用者取消或截止时间到达 | `context.Canceled` 或 `context.DeadlineExceeded` |
| 批次失去等待者或关闭后才返回结果 | 丢弃该结果 |

通过 `errors.Is` 判断库错误和业务错误。商品不存在等业务结果应显式设置单个条目的业务错误；遗漏 map 条目表示批量函数响应不完整。nil、运行时不可比较的 key 和非自反 key（例如 NaN）返回 `ErrInvalidKey`。

## 验证

```sh
go test -race ./... -run TestBatcher -count=1
```

[行为测试](../batcher_test.go) 注入可控计时器，并用 channel 同步调用者，覆盖收集窗口、数量上限、共享加载、部分失败、遗漏结果、取消、上下文归属、关闭和迟到的批次结果。
