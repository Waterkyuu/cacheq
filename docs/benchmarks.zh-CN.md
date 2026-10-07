# 缓存收益 benchmark

[English](benchmarks.en-US.md)

主对照回答一个实际问题：应用重复读取同一份数据时，cacheq 能节省多少时间和实际加载？

## 主表：读取同一份数据 100 次

三个场景都顺序读取 100 次，返回同一份固定 64 KiB 数据的 SHA-256 摘要。使用相同的加载函数、context 和结果检查。表中的耗时是**完整读取 100 次的总耗时**。

| 场景 | 读取次数 | 总耗时 | 实际加载次数 |
| --- | ---: | ---: | ---: |
| 无缓存：每次都加载 | 100 | 2,184.51 微秒 | 100 |
| 首次加载：从空缓存开始，随后复用结果 | 100 | 46.56 微秒 | 1 |
| 缓存命中：开始时已有新鲜结果 | 100 | 21.46 微秒 | 0 |

“首次加载”包含第一次未命中和后续 99 次命中，衡量从空缓存开始服务重复请求的成本。“缓存命中”在单独预热后读取 100 次，预热的一次加载和耗时不计入这一行。每批之间清空冷缓存的准备成本也不计时。所有场景检查返回摘要，并验证实际加载次数必须符合预期。

测量日期为 2026-10-07，环境是 Go 1.27.1、macOS / darwin arm64、Apple M3 Pro。耗时取五次样本的中位数，从 ns/op 转换为每批微秒数。[原始输出](benchmarks-cache-benefit-darwin-arm64.txt) 包含全部样本和内存分配数据。

## 复现

从仓库根目录运行：

```sh
go test -run '^$' -bench '^BenchmarkCacheBenefit$' -benchmem -benchtime=300ms -count=5 -cpu=1
```

Go 输出中的一次操作表示一批读取：`ns/op` 是读取 100 次的总耗时，`reads/batch` 为 100，`loads/batch` 分别为 100、1、0。将 ns/op 除以 1,000 就得到主表的微秒数。`B/op` 和 `allocs/op` 同样表示整批的内存分配。

SHA-256 加载函数执行确定性的 CPU 计算，无需网络请求，也不通过 sleep 模拟慢加载。这是避免重复后端计算的可复现案例。收益取决于实际加载成本、新鲜时间和命中率；评估真实应用时应替换为自己的加载负载。这组样本没有测量请求合并、失败或重试。

快速检查负载正确性：

```sh
go test -race -run '^$' -bench '^BenchmarkCacheBenefit$' -benchtime=1x
```

耗时采样使用普通构建。记录源码版本、Go 版本、操作系统、架构、CPU、命令和全部样本，保持机器空闲，在相同条件下比较重复测量结果。

## 内部开销与并发诊断

其余 benchmark 用于维护者调查 API 开销和竞争。此前的[原始样本](benchmarks-darwin-arm64.txt) 单独保留，不混入缓存收益主表。

| Benchmark | 一次操作测量的内容 |
| --- | --- |
| `CacheHit/Get`、`Fetch`、`Snapshot` | 一次命中缓存的类型安全公开 API 读取 |
| `CacheHit/MapMutexReference` | 一次加锁的 map 读取，不包含查询生命周期管理 |
| `ParallelFetch/Keys=1,1024` | 多个工作线程中的一次命中缓存的 Fetch |
| `SharedLoad/Consumers=1,8,64` | 一批并发订阅，包含注册和清理 |
| `SubscriberUpdates/Subscribers=1,16,128` | 一次 Set 及所有通知；Consumed 还读取所有通知 |
| `CapacityEviction/Entries=64,1024` | 在已满的 LRU 缓存中写入并淘汰一条数据 |
| `GCRetentionRead/GCTime=0s,1h0m0s` | 关闭闲置回收或重新调度定时器时的一次读取 |

SharedLoad 使用 channel 屏障，所有消费者注册完成后才允许 loader 返回，并检查每批一次加载、其余消费者加入共享请求。LatestOnly 测量慢消费者的最新快照替换，容量测试检查每次写入一次淘汰。GCRetentionRead 测量定时器调度，不代表到期删除和回收吞吐。

map + mutex 参考实现提供的语义更少，不能展示缓存带来的收益。ParallelFetch 的 ns/op 表示整体吞吐折算成本，不是单请求尾延迟。增加 GOMAXPROCS 会影响并发工作线程，但不会把串行 benchmark 变成并行负载。

调查并发扩展性时单独运行：

```sh
go test -run '^$' -bench '^BenchmarkParallelFetch$' -benchmem -count=3 -cpu=1,4
```

采集锁竞争 profile：

```sh
go test -run '^$' -bench '^BenchmarkParallelFetch$' -benchtime=3s -cpu=4 -mutexprofile=/tmp/cacheq-mutex.pprof -o /tmp/cacheq-profile.test
go tool pprof /tmp/cacheq-profile.test /tmp/cacheq-mutex.pprof
```

性能分析本身有开销，普通耗时应单独采样。
