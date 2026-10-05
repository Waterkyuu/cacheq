package query_test

import (
	"context"
	"fmt"
	"time"

	query "github.com/Waterkyuu/go-query"
)

// ExampleClient_Fetch shares one result across readers while its freshness window remains open.
func ExampleClient_Fetch() {
	client := query.NewClient[string, string](query.Options{StaleTime: time.Hour})
	defer client.Close()
	calls := 0
	fetch := func(context.Context) (string, error) {
		calls++
		return "shared", nil
	}
	first, _ := client.Fetch(context.Background(), "models", fetch)
	second, _ := client.Fetch(context.Background(), "models", fetch)
	fmt.Println(first, second, calls)
	// Output: shared shared 1
}

// ExampleClient_Snapshot checks expiration without starting a request.
func ExampleClient_Snapshot() {
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	client := query.NewClient[string, string](query.Options{
		StaleTime: time.Hour, Clock: func() time.Time { return now },
	})
	defer client.Close()
	client.Set("models", "available")
	fmt.Println(client.Snapshot("models").Stale)
	now = now.Add(time.Hour)
	fmt.Println(client.Snapshot("models").Stale)
	// Output:
	// false
	// true
}
