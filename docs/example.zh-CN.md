# 统一缓存失效示例

[English](example.en-US.md)

把程序复制到已引入 cacheq 的应用的 `main.go`，运行 `go run .`。同一个 `Invalidate` 方法支持单个键、`[]any` 键列表和 `func(any) bool` 条件函数。失效后仍能读取旧数据。

```go
package main

import (
	"fmt"
	"strings"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// main demonstrates the three selection forms of Invalidate with one shared client.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()

	if err := cacheq.Set(client, "user:42", "Alice"); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, "user:7", "Bob"); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, "users", []string{"Alice", "Bob"}); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, "settings", "dark"); err != nil {
		panic(err)
	}

	// An exact key selects only that result and retains its cached data.
	if err := client.Invalidate("user:42"); err != nil {
		panic(err)
	}
	detail := cacheq.Get[string](client, "user:42")
	fmt.Println("single:", detail.Data, detail.Stale)

	// A []any batch selects explicit keys; duplicates are invalidated once.
	if err := client.Invalidate([]any{"user:42", "users", "users"}); err != nil {
		panic(err)
	}
	fmt.Println("batch:", cacheq.Get[[]string](client, "users").Stale)

	// A predicate selects existing keys; RefetchNone suppresses new requests.
	if err := client.Invalidate(func(key any) bool {
		name, ok := key.(string)
		return ok && strings.HasPrefix(name, "user:")
	}, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
		panic(err)
	}
	fmt.Println("predicate:", cacheq.Get[string](client, "user:7").Stale)
	fmt.Println("unrelated:", cacheq.Get[string](client, "settings").Stale)
}
```

预期输出：

```text
single: Alice true
batch: true
predicate: true
unrelated: false
```

省略配置时，默认刷新有启用查询对象的键。本例没有查询对象，因此只标记过期。有启用查询对象时，也可以用 `RefetchNone` 阻止本次启动新请求。nil 参数或条件函数返回 `ErrInvalidKey`；空列表或 nil `[]any` 列表不做操作。`[]string` 等其他切片类型不被识别为批量列表。关闭的客户端返回 `ErrClosed`。
