# 并发请求 benchmark

[English](benchmarks.en-US.md)

这些 benchmark 专门模拟并发应用请求访问 cacheq。每个场景明确启动 **4、16 或 64 个请求 goroutine**，CPU 并行度另行固定为 GOMAXPROCS=4，不用它决定请求数量。

## 主表：64 个请求并发访问

每批同时发起 64 个请求，分别测同一个商品 key 和 64 个不同商品 key。所有模式使用相同的加载函数：计算该 key 对应的固定 64 KiB 数据的 SHA-256 摘要。不同 key 的数据不同。

| 请求的数据 | 缓存状态 | 整批总耗时 | 实际加载次数 |
| --- | --- | ---: | ---: |
| 同一个 key | 无缓存 | 429.02 微秒 | 64 |
| 同一个 key | 从空缓存开始首次加载 | 71.17 微秒 | 1 |
| 同一个 key | 全部命中缓存 | 49.21 微秒 | 0 |
| 不同 key | 无缓存 | 439.12 微秒 | 64 |
| 不同 key | 从空缓存开始首次加载 | 474.60 微秒 | 64 |
| 不同 key | 全部命中缓存 | 51.29 微秒 | 0 |

同 key 的首次请求共享一次加载，或命中刚安装的结果。不同 key 的首次请求各自需要加载；没有数据可以复用时，缓存会增加管理开销，不会减少这些首次后端计算。

预热和每批之间清空冷缓存的准备成本不计时。预热阶段每个 key 加载一次，“全部命中”只统计正式测量阶段的加载。每个请求检查对应 key 的摘要，各模式都验证实际加载次数符合预期。

测量日期为 2026-10-08，环境是 Go 1.27.1、macOS / darwin arm64、Apple M3 Pro。耗时取三次样本的中位数。[原始输出](benchmarks-concurrent-darwin-arm64.txt) 包含所有请求数量、分配数据和平均加入进行中加载的请求数量。

## 并发如何产生

每批的流程是：

1. 创建指定数量的 goroutine，每个代表一个请求。
2. 每个 goroutine 报告已经就绪，然后等待同一个启动 channel。
3. 全部就绪后关闭 channel，一起放行这些请求。
4. 每个 goroutine 执行一次直接加载或 cacheq Fetch。
5. 等待所有请求结束，并检查所有结果。

这模拟同时到达的应用请求在处理函数中访问缓存。测试直接调用 Fetch，不包含 HTTP 传输、JSON 编码和真实数据库。整批计时包含创建 goroutine、启动屏障、调度、读取、等待结束和结果检查，表示一批请求的完成耗时，不是单请求尾延迟或稳定运行时的纯缓存读取吞吐。

加载函数执行确定性的 CPU 计算，没有 sleep 延迟。同 key 首次加载期间到达的请求计入 `merged/batch`，稍晚到达的请求可以命中新结果。合并数量是实际记录的，不假设必然等于请求数减一。本次 64 请求样本中，各轮平均合并数量的中位数为每批 59.37 个请求，但每批的实际加载次数都严格为一次。

## 复现

从仓库根目录运行所有请求数量，固定 CPU 并行度：

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchmem -benchtime=200ms -count=3 -cpu=4
```

只运行 64 个请求的场景：

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$/^Requests=64$' -benchmem -benchtime=200ms -count=3 -cpu=4
```

测试名称直接表达场景，例如：`BenchmarkConcurrentFetch/Requests=64/SameKey/FirstLoad`。

| 输出指标 | 含义 |
| --- | --- |
| `ns/op` | 一整批请求的总耗时，除以 1,000 得到微秒 |
| `requests/batch` | 明确创建的请求 goroutine 数量：4、16、64 |
| `loads/batch` | 测量阶段每批实际执行的 loader 次数 |
| `merged/batch` | 每批加入进行中加载的平均请求数量 |
| `B/op`、`allocs/op` | 整批内存分配，包含创建请求 goroutine 的成本 |

快速验证正确性和 race，不用于耗时结论：

```sh
go test -race -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchtime=1x -cpu=4
```

保持机器空闲，在相同条件下重复采样，并记录源码版本、Go 版本、CPU 和命令。这组 CPU 计算样本的收益取决于加载成本和复用程度；评估真实应用时应使用有代表性的加载负载。计时包含并发准备成本，不能直接和此前的单次读取数据比较。

分析锁竞争时，仍然明确指定请求数量：

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$/^Requests=64$/SameKey/CacheHit$' -benchtime=3s -cpu=4 -mutexprofile=/tmp/cacheq-mutex.pprof -o /tmp/cacheq-profile.test
go tool pprof /tmp/cacheq-profile.test /tmp/cacheq-mutex.pprof
```

普通耗时与 profile 分开采样。
