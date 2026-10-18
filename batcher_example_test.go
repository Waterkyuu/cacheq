package soroi_test

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
)

// ExampleBatcher_Fetcher groups different query keys and reuses their individually cached results.
func ExampleBatcher_Fetcher() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	defer client.Close()
	var calls atomic.Int32
	products, err := soroi.NewBatcher(
		context.Background(),
		func(_ context.Context, ids []int) (map[int]soroi.BatchResult[string], error) {
			calls.Add(1)
			sort.Ints(ids)
			fmt.Printf("bulk load: %v\n", ids)
			results := make(map[int]soroi.BatchResult[string], len(ids))
			for _, id := range ids {
				results[id] = soroi.BatchResult[string]{Data: fmt.Sprintf("product %d", id)}
			}
			return results, nil
		},
		soroi.BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	if err != nil {
		panic(err)
	}
	defer products.Close()

	first := soroi.Query(client, "product:42", products.Fetcher(42))
	defer first.Close()
	second := soroi.Query(client, "product:43", products.Fetcher(43))
	defer second.Close()
	for _, handle := range []*soroi.QueryHandle[string]{first, second} {
		for state := range handle.Updates() {
			if state.Err != nil {
				panic(state.Err)
			}
			if state.HasData && !state.Fetching {
				fmt.Println(state.Data)
				break
			}
		}
	}
	value, err := soroi.Fetch(context.Background(), client, "product:42", products.Fetcher(42))
	if err != nil {
		panic(err)
	}
	fmt.Printf("cached: %s; bulk calls: %d\n", value, calls.Load())
	// Output:
	// bulk load: [42 43]
	// product 42
	// product 43
	// cached: product 42; bulk calls: 1
}
