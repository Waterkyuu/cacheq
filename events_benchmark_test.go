package cacheq_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// BenchmarkEventFiltering measures cache hits with increasing numbers of unrelated exact-key subscriptions.
// Setup is excluded; each iteration reads the same value and must produce no matching diagnostic records.
func BenchmarkEventFiltering(b *testing.B) {
	for _, count := range []int{0, 1, 100, 1000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
			defer client.Close()
			if err := cacheq.Set(client, "hot", 1); err != nil {
				b.Fatal(err)
			}
			subscriptions := make([]*cacheq.EventSubscription, 0, count)
			for key := range count {
				subscription, err := client.SubscribeEvents(cacheq.EventOptions{Key: key, Buffer: 1})
				if err != nil {
					b.Fatal(err)
				}
				subscriptions = append(subscriptions, subscription)
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if value, err := cacheq.Fetch[int](ctx, client, "hot", nil); err != nil || value != 1 {
					b.Fatalf("cache hit = %d, %v", value, err)
				}
			}
			b.StopTimer()
			for _, subscription := range subscriptions {
				select {
				case event := <-subscription.Events():
					b.Fatalf("unrelated subscription received %+v", event)
				default:
				}
				if subscription.Dropped() != 0 {
					b.Fatal("unrelated events counted as dropped")
				}
			}
		})
	}
}
