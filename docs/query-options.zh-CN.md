# 每个消费者独立的查询配置

[English](query-options.en-US.md)

`QueryOptions` 为单个 `Query` 查询对象覆盖客户端默认配置，适用于共享同一个键、但需要不同新鲜期、重试或超时策略的消费者。同步读取可以通过 `FetchWithOptions` 的 `FetchOptions` 指定相同的覆盖项。

## 快速开始

在导入 cacheq 的 Go 模块中，将以下完整程序保存为 `main.go`，运行 `go run .`。示例不访问网络、不使用等待延时。两个消费者共享同一份数据，但分别判断新鲜度；首次加载遇到临时错误后重试一次。

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main demonstrates shared data with independent freshness and a filtered retry policy.
func main() {
	client := cacheq.NewClient(cacheq.Options{
		StaleTime: time.Minute,
		Retry:     2,
		Timeout:   10 * time.Second,
		Clock:     func() time.Time { return time.Unix(0, 0) },
	})
	defer client.Close()

	transient := errors.New("temporarily unavailable")
	var calls atomic.Int32
	load := func(context.Context) (string, error) {
		if calls.Add(1) == 1 {
			return "", transient
		}
		return "hello", nil
	}

	hour, immediate, timeout := time.Hour, time.Duration(0), 3*time.Second
	retries := 1
	steady := cacheq.Query(
		client,
		"greeting",
		load,
		cacheq.QueryOptions{
			StaleTime:  &hour,
			Retry:      &retries,
			RetryIf:    func(err error) bool { return errors.Is(err, transient) },
			RetryDelay: func(int) time.Duration { return 0 },
			Timeout:    &timeout,
		},
	)
	defer steady.Close()

	live := cacheq.Query(
		client,
		"greeting",
		load,
		cacheq.QueryOptions{
			Disable:   true,
			StaleTime: &immediate,
		},
	)
	defer live.Close()

	value, err := cacheq.FetchWithOptions(
		context.Background(),
		client,
		"greeting",
		load,
		cacheq.FetchOptions{StaleTime: &hour},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	fmt.Printf("steady stale=%t; live stale=%t\n", steady.Snapshot().Stale, live.Snapshot().Stale)
	stats := client.Stats()
	fmt.Printf("loads=%d; retries=%d\n", stats.Loads, stats.Retries)
}
```

预期输出：

```text
hello
steady stale=false; live stale=true
loads=1; retries=1
```

## 配置与显式零值

| 字段 | 未设置 / nil | 显式覆盖 |
| --- | --- | --- |
| `Disable` | 默认为 false，允许自动加载 | true 明确关闭自动加载 |
| `StaleTime` | 使用客户端新鲜时间 | 时长指针；零值让普通数据立即过期 |
| `Retry` | 使用客户端重试次数 | 整数指针；零值关闭额外重试 |
| `Timeout` | 使用客户端超时 | 时长指针；零值取消客户端超时限制，调用方 context 的取消和截止时间仍然有效 |
| `RetryIf` | 使用客户端错误判断函数 | 根据错误决定是否允许继续重试的函数 |
| `RetryDelay` | 使用客户端退避配置 | 接收额外尝试编号的函数，编号从 1 开始 |

`Disable` 仅存在于 `QueryOptions`；其余字段同时适用于 `QueryOptions` 和 `FetchOptions`。

调用或创建查询对象时会复制指针指向的值，之后修改原变量不会改变该查询对象。回调闭包仍然引用其捕获的状态，多次加载共享可变状态时需要自行同步。查询对象在启用状态切换和 `Refetch` 时继续使用创建时的策略。

`Disable` 默认为 false，因此空 `QueryOptions` 或只覆盖新鲜时间时都允许自动加载。设置 `Disable: true` 才会禁用自动加载。创建时会复制开关值，之后通过 `SetEnabled` 修改。传入多个 `QueryOptions` 时，仅最后一份配置覆盖客户端默认值。

客户端的 `Options` 和每个消费者的配置都可以设置 `RetryIf`。只有错误允许重试且剩余次数足够时才会调用判断函数；取消和截止时间错误永不重试。nil 继承客户端判断函数；如需覆盖客户端的限制，可提供始终返回 true 的函数。判断函数与延时函数在客户端锁外执行，可以调用客户端 API。

## 共享数据与独立新鲜期

普通成功加载或 `Set` 写入的数据，通过共享的 `UpdatedAt` 和消费者自己的 `StaleTime` 计算 `Stale` 与 `ExpiresAt`。`Get`、`Fetch` 和 `Prefetch` 使用客户端默认值。较长的新鲜窗口可以复用其他消费者认为已过期的数据，一个消费者的配置不会影响后续调用的默认值。

`FetchWithExpiry` 返回的是具有约束力的绝对截止时间，包含显式缓存的降级数据和错误。消费者的 `StaleTime` 不会缩短或延长这一截止时间。客户端 `MaxAge` 对两种数据来源都限制可用年龄和新鲜期限。容量、GC 和时钟继续由客户端管理。

显式失效和未安装新数据的刷新失败会让所有消费者将保留的数据视为过期。具有未来绝对截止时间的显式兜底结果仍遵循该截止时间。时间流逝本身不会发送更新或启动请求，`Snapshot` 会重新判断当前新鲜度；普通状态变化通过 `Updates` 向各消费者发布符合其策略的快照。

## 共享加载策略

启动请求的消费者提供加载函数、重试次数、错误判断、退避和整体超时。其他同键消费者加入现有请求时直接复用，不覆盖这些设置。加入者的 context 仍可结束自己的等待；其 `Timeout` 覆盖值只用于由自己启动的新请求。

失效操作触发自动刷新时，使用最早创建且仍启用的查询对象的加载函数和策略。禁用或关闭该查询对象后，由下一个启用的查询对象提供后续自动刷新配置。正在执行的请求继续使用发起者的设置。手动 `Refetch` 启动新请求时使用调用它的查询对象。

同键加载函数必须代表同一份数据。查询键应包含参数、用户身份和租户范围；配置覆盖不会隔离缓存结果。

## 开关配置迁移

v0.1.1 的 `QueryOptions.Enabled *bool` 替换为 `Disable bool`。原来的 `Enabled` 为 nil 或指向 true 时，直接省略 `Disable`；指向 false 时，改为 `Disable: true`。如果开关来自计算得到的布尔值，使用 `Disable: !enabled`。空 `QueryOptions{}` 和只覆盖新鲜时间仍默认自动加载。已有的 `SetEnabled(bool)` 调用不变。

## 验证

```sh
go test -race ./... -run 'Test(QueryOptions|FetchOptions)' -count=1
```

[行为测试](../query_options_test.go) 覆盖独立新鲜度、默认继承、零值覆盖、配置值复制、显式截止时间、可用年龄限制、共享请求、重试、回调取消和自动刷新策略的确定性选择。
