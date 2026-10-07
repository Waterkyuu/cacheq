# 可复现的 cacheq benchmark

[English](benchmarks.en-US.md)

[query_benchmark_test.go](../query_benchmark_test.go) 测量公开 API 的成本，不依赖网络服务，也不增加模块依赖。benchmark 检查返回值并报告分配次数；请求合并和淘汰负载还检查计数器，避免因为实际少做了工作而得到更快的结果。

## 从仓库根目录运行

重复采样并比较两种工作线程数量：

```sh
go test -run '^$' -bench . -benchmem -count=3 -cpu=1,4
```

快速验证负载是否正确可以使用 `-benchtime=1x`，但单次迭代不能作为性能数据。发布耗时数据时使用普通构建；race detector 会显著改变成本。

记录源码版本、Go 版本、操作系统、架构、CPU、命令和全部样本。测量时让机器保持空闲，比较时使用相同环境，不要只选择最快的一次。比较修改前后时分别保存输出，再使用统计工具分析重复样本。

## 负载与计量单位

| Benchmark | 一次操作测量的内容 |
| --- | --- |
| `CacheHit/Get`、`Fetch`、`Snapshot` | 一次已命中缓存的类型安全公开 API 读取；不计初始化和首次写入 |
| `CacheHit/MapMutexReference` | 一次加锁的 map 读取和结果检查，不包含 TTL、查询状态、统计和请求共享 |
| `ParallelFetch/Keys=1,1024` | 一次命中缓存的 Fetch；工作线程循环读取预加载的键，不执行 loader |
| `SharedLoad/Consumers=1,8,64` | 一批完整的并发订阅：清理键、注册消费者、加载一次、接收结果并关闭句柄 |
| `SubscriberUpdates/Subscribers=1,16,128` | 一次 Set 及所有通知；Consumed 还读取每个订阅者的通知 |
| `CapacityEviction/Entries=64,1024` | 在已满的 LRU 缓存中 Set，循环使用容量 + 1 个键，保证一次淘汰 |
| `GCRetentionRead/GCTime=0s,1h0m0s` | 读取一个没有订阅的键，比较关闭闲置回收与停止、重新调度定时器的成本 |

SharedLoad 的一次操作是**整批请求**，不是单个消费者调用。`consumers/burst` 和 `loads/burst` 表示实际工作量。channel 屏障让 loader 在所有并发消费者注册完成后才能返回，每批必须只执行一次 loader，其余消费者加入共享请求，不通过 sleep 制造重叠。

LatestOnly 在注册后不读取通知，测量慢消费者的最新快照替换；Consumed 读取每次发布。无论订阅者数量多少，一次 Set 都算一次操作。

ParallelFetch 使用 `testing.B.RunParallel`，`-cpu` 改变 GOMAXPROCS 和工作线程数量。ns/op 表示整体吞吐折算的成本，不是单请求尾延迟。不同键负载循环访问已预加载的工作集，不测无限插入；键提前转换为接口值，避免把键构造成本计入缓存访问。

GCRetentionRead 测量生产定时器的调度，**不代表到期删除或回收吞吐**。较长的到期时间避免测量过程中发生删除，实际 GC 清理行为仍由确定性的保留时间测试覆盖。

## 一次本地实测

测量日期为 2026-10-07，环境是 Go 1.27.1、macOS / darwin arm64、Apple M3 Pro。下表为三次样本的中位数，单位 ns/op，保留一位小数。[原始输出](benchmarks-darwin-arm64.txt) 包含全部负载和内存分配数据。

```sh
go test -run '^$' -bench . -benchmem -benchtime=200ms -count=3 -cpu=1,4
```

| 负载 | GOMAXPROCS=1 | GOMAXPROCS=4 |
| --- | ---: | ---: |
| CacheHit / Get | 208.1 | 207.4 |
| CacheHit / Fetch | 231.9 | 212.1 |
| ParallelFetch / 单键 | 189.1 | 338.3 |
| ParallelFetch / 1024 键 | 218.5 | 308.7 |
| SharedLoad / 64 消费者，每批 | 73235.0 | 77942.0 |
| SubscriberUpdates / 128 消费者读取，每次 Set | 12322.0 | 12301.0 |
| CapacityEviction / 1024 条 | 305.8 | 250.1 |
| GCRetentionRead / 启用定时器 | 423.3 | 391.5 |

所有共享加载配置都是每批一次 loader，所有容量配置都是每次一次淘汰。这台机器上的缓存并发读取没有随工作线程增加而线性扩展，可以在实际应用负载下分析共享锁，再判断分片是否值得引入。

这些数据是小整数值的短时间本地样本，不是跨平台性能承诺。订阅负载测量通知和读取，不包含界面渲染；没有外部 I/O、序列化、重试和大数据。分配次数包含公开 API 路径，批量负载还包含 goroutine 和订阅初始化。

## 正确解读对照

map + mutex 是最小同步成本参考，不是功能等价的缓存。它没有新鲜度、错误保留、失效、订阅、取消和请求合并，不能据此宣传某个完整方案快了多少倍。

与其他缓存比较时，需要对齐键构造、结果类型、过期语义、初始状态、并发、加载耗时和容量。基础读取与查询生命周期应分别比较，并说明实现同等语义需要额外编写哪些应用代码。

分析缓存读取的锁竞争：

```sh
go test -run '^$' -bench '^BenchmarkParallelFetch$' -benchtime=3s -cpu=4 -mutexprofile=/tmp/cacheq-mutex.pprof -o /tmp/cacheq-profile.test
go tool pprof /tmp/cacheq-profile.test /tmp/cacheq-mutex.pprof
```

性能分析本身有开销，普通耗时应单独采样。只有观察到的应用负载能够支持时，才引入优化带来的复杂度。
