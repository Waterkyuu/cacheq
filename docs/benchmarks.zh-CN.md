# 缓存收益 benchmark

[English](benchmarks.en-US.md)

主对照回答一个实际问题：应用重复读取同一份数据时，cacheq 能节省多少时间和实际加载？

## 主表：串行读取同一份数据 100 次

三个场景都由一个请求 goroutine **串行读取 100 次，每次等待上一次读取返回后再开始下一次**，返回同一份固定 64 KiB 数据的 SHA-256 摘要。使用相同的加载函数、context 和结果检查。表中的耗时是**完整串行读取 100 次的总耗时**。

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

## 独立并发请求测试

固定每批 **64 次请求**，比较 **1、4、16、64 个调用者 goroutine**，分别在 **GOMAXPROCS=1 和 4** 下测量。1 个 goroutine 串行完成 64 次；多个 goroutine 分担这 64 次请求。GOMAXPROCS 控制同时执行 Go 代码的 CPU 并行度，与调用者数量分别设置。

分别读取同一个 key 和 64 个不同 key，比较无缓存、首次加载和缓存命中。所有配置使用相同的加载函数：计算该 key 对应的固定 64 KiB 数据的 SHA-256 摘要。不同 key 的数据不同。下表单位均为**微秒 / 整批 64 次请求**，越小越快。

### GOMAXPROCS=1

| 请求的数据 | 缓存状态 | 1 个 goroutine | 4 个 | 16 个 | 64 个 | 实际加载次数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 同一个 key | 无缓存 | 1,398.58 | 1,430.35 | 1,417.42 | 1,456.69 | 64 |
| 同一个 key | 从空缓存开始首次加载 | 44.20 | 44.20 | 52.64 | 82.18 | 1 |
| 同一个 key | 全部命中缓存 | 15.02 | 16.45 | 23.81 | 51.90 | 0 |
| 不同 key | 无缓存 | 1,415.30 | 1,418.52 | 1,451.63 | 1,492.18 | 64 |
| 不同 key | 从空缓存开始首次加载 | 1,502.89 | 1,494.51 | 1,561.59 | 1,666.83 | 64 |
| 不同 key | 全部命中缓存 | 14.82 | 16.60 | 24.40 | 57.47 | 0 |

### GOMAXPROCS=4

| 请求的数据 | 缓存状态 | 1 个 goroutine | 4 个 | 16 个 | 64 个 | 实际加载次数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 同一个 key | 无缓存 | 1,398.65 | 421.73 | 428.44 | 495.77 | 64 |
| 同一个 key | 从空缓存开始首次加载 | 51.82 | 56.79 | 62.11 | 71.89 | 1 |
| 同一个 key | 全部命中缓存 | 17.43 | 23.13 | 34.19 | 49.60 | 0 |
| 不同 key | 无缓存 | 1,414.06 | 414.18 | 424.92 | 452.89 | 64 |
| 不同 key | 从空缓存开始首次加载 | 1,758.86 | 442.75 | 476.14 | 529.24 | 64 |
| 不同 key | 全部命中缓存 | 18.06 | 23.71 | 34.91 | 52.08 | 0 |

### 如何理解结果

CPU=1 时，无缓存计算没有因增加调用者而明显提速；CPU=4 时，4 个调用者把同 key 无缓存的整批耗时从 1,398.65 降至 421.73 微秒，约快 3.3 倍。不同 key 的首次加载也能利用 CPU 并行计算。

同 key 首次加载在所有配置下都只执行一次 loader，随后请求共享进行中的加载或命中结果。不同 key 首次加载仍需要 64 次，缓存不会省掉尚未加载过的独立数据。

命中缓存时，增加调用者没有提速。这些读取很短，计时还包含 goroutine 创建、调度和共享 Client 的同步成本。表格衡量整批请求完成耗时，不能把差异全部归因于锁竞争，也不表示每个请求都会变慢。

预热和每批之间清空冷缓存的准备成本不计时。每次读取检查对应 key 的摘要，并验证所有配置的实际加载次数。测量日期为 2026-10-08，环境是 Go 1.27.1、macOS / darwin arm64、Apple M3 Pro。耗时取三次样本的中位数。[原始输出](benchmarks-concurrent-darwin-arm64.txt) 包含所有 CPU 与调用者数量组合及分配数据。

### 首次加载的 profile 调查

针对上表的 **4 个调用者、同 key 首次加载**，分别采集 CPU、mutex、block profile 和执行 trace。诊断支持两个开销来源：等待加载的请求恢复执行，以及后续缓存读取争用 Client 的锁。

| Trace 中实际记录的情况 | GOMAXPROCS=1 | GOMAXPROCS=4 |
| --- | ---: | ---: |
| 每批等待加载的请求数，包含发起者 | 1.00 | 4.00 |
| 等待加载的请求被唤醒后，到恢复执行的中位耗时 | 0.19 微秒 | 4.93 微秒 |
| 每批因缓存锁而暂停的事件数 | 0 | 10.28 |

两次 trace 都记录了 1,001 批，包括 1 批预跑；每批仍然只执行一次 loader。单核下其他调用者通常直接命中新结果；四核下约 3 个调用者加入进行中的加载。`pending` 完成后直接返回数据的实现没有改变，但 goroutine 需要先被调度恢复执行。每个调用者还要完成后续读取，这些命中缓存的请求仍需取得 Client 的锁。

四核 mutex profile 将主要锁等待归到 `Client.fetch` 的解锁位置；这是释放锁、让等待者继续的调用栈，不表示 `Unlock` 自身计算很慢。block profile 也记录了 `Client.fetch` 中的锁等待。CPU profile 出现了调度、线程等待和唤醒相关的 runtime 函数，但它覆盖整个 benchmark，包括不计入 `ns/op` 的准备步骤，因此不使用总体占比推算缓存自身耗时。

profile 和 trace 会改变执行时序。这些数据可以确认等待和恢复执行确实发生，**不能把原表多出的 12.59 微秒精确分摊给锁或调度**。多个 goroutine 的等待时间也会重叠，不能相加当作整批耗时。完整输出和 trace 统计方法见 [调查原始记录](benchmarks-firstload-profile-darwin-arm64.txt)。串行主表与普通并发耗时样本保持原值。

复现时先采集单核，再采集四核，避免两个测试相互争用 CPU：

```sh
for cpu in 1 4; do
  go test -run '^$' -bench '^BenchmarkConcurrentFetch$/^Workers=4$/^SameKey$/^FirstLoad$' \
    -benchtime=2s -count=1 -cpu="$cpu" \
    -cpuprofile="/tmp/cacheq-firstload-cpu${cpu}.pprof" \
    -mutexprofile="/tmp/cacheq-firstload-mutex${cpu}.pprof" -mutexprofilefraction=1 \
    -blockprofile="/tmp/cacheq-firstload-block${cpu}.pprof" -blockprofilerate=1 \
    -o /tmp/cacheq-firstload-profile.test
  /tmp/cacheq-firstload-profile.test -test.run '^$' \
    -test.bench '^BenchmarkConcurrentFetch$/^Workers=4$/^SameKey$/^FirstLoad$' \
    -test.benchtime=1000x -test.count=1 -test.cpu="$cpu" \
    -test.trace="/tmp/cacheq-firstload-trace${cpu}.out"
done

go tool pprof -top /tmp/cacheq-firstload-profile.test /tmp/cacheq-firstload-mutex4.pprof
go tool pprof -top /tmp/cacheq-firstload-profile.test /tmp/cacheq-firstload-block4.pprof
go tool trace /tmp/cacheq-firstload-trace4.out
```

### 复现

从仓库根目录运行，比较两种 CPU 并行度：

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchmem -benchtime=200ms -count=3 -cpu=1,4
```

仅比较 1 和 4 个调用者：

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$/^Workers=(1|4)$' -benchmem -benchtime=200ms -count=3 -cpu=1,4
```

名称例如 `BenchmarkConcurrentFetch/Workers=4/SameKey/FirstLoad-4`，`Workers=4` 表示 4 个调用者，末尾 `-4` 表示 GOMAXPROCS=4；GOMAXPROCS=1 时没有数字后缀。

| 输出指标 | 含义 |
| --- | --- |
| `ns/op` | 一整批 64 次请求的总耗时，除以 1,000 得到微秒 |
| `requests/batch` | 每批请求数，固定为 64 |
| `goroutines/batch` | 每批调用者 goroutine 数：1、4、16、64，不含 cacheq 内部 goroutine |
| `loads/batch` | 测量阶段每批实际执行的 loader 次数 |
| `merged/batch` | 每批加入进行中加载的平均请求数量 |
| `B/op`、`allocs/op` | 整批内存分配，包含创建调用者 goroutine 的成本 |

快速验证正确性和 race，不用于耗时结论：

```sh
go test -race -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchtime=1x -cpu=1,4
```

保持机器空闲，在相同条件下重复采样，并记录源码版本、Go 版本、CPU 和命令。这组 CPU 计算样本的收益取决于加载成本和复用程度；评估真实应用时应使用有代表性的加载负载。

分析锁竞争时，可以单独采集 profile：

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$/^Workers=64$/SameKey/CacheHit$' -benchtime=3s -cpu=4 -mutexprofile=/tmp/cacheq-mutex.pprof -o /tmp/cacheq-profile.test
go tool pprof /tmp/cacheq-profile.test /tmp/cacheq-mutex.pprof
```

普通耗时与 profile 分开采样。
