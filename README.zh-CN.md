## 安装

```sh
go get github.com/Waterkyuu/go-query@v0.1.0
```

# go-query

一个面向 Go 1.22 及以上版本、没有外部依赖的查询缓存库。

- 使用泛型提供类型安全的键和值。
- 查询结果保存在内存中，到期后再刷新。
- 同一个键的并发请求共用一次加载，不同键独立加载。
- 加载请求和等待者都支持 Context 取消。
- 可将失败结果缓存到指定的重试时间。

## 使用

```go
cache := query.New[string, string](time.Now)
value, err := cache.Get(ctx, "greeting", func(ctx context.Context) (string, time.Time, error) {
    return "hello", time.Now().Add(time.Hour), nil
})
```

导入路径：`github.com/Waterkyuu/go-query`，包名为 `query`。多个请求应复用同一个缓存实例。

加载函数返回绝对过期时间，从磁盘恢复的数据可以保留原有有效期。返回零值过期时间表示不缓存；失败时可以返回备用数据、将来的重试时间和错误，避免短时间内反复请求。

缓存值是共享的，调用者应将其视为只读。修改切片或映射前需复制。磁盘持久化、HTTP 请求、重试循环和后台刷新由使用方负责。缓存到期后，在下次查询时刷新。

第一次调用者的 Context 控制加载任务。取消等待者不会取消其他人的加载任务；如果发起加载的人取消请求，其他调用者可使用自己的 Context 重新加载。测试时可注入时钟。
