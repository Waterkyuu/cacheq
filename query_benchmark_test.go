package cacheq_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

const (
	// benchmarkReadsPerBatch fixes the number of same-key reads in each benefit comparison.
	benchmarkReadsPerBatch = 100
	// benchmarkPayloadBytes fixes the amount of deterministic backend work at 64 KiB per load.
	benchmarkPayloadBytes = 64 * 1024
)

// BenchmarkCacheBenefit compares equal read batches without caching, from an empty cache, and from a warm cache.
// All modes use the same SHA-256 loader; cold-cache resets and warm-cache preparation are excluded from timing.
func BenchmarkCacheBenefit(b *testing.B) {
	payload := make([]byte, benchmarkPayloadBytes)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	want := sha256.Sum256(payload)
	for _, mode := range []string{"NoCache", "FirstLoad", "CacheHit"} {
		b.Run(mode, func(b *testing.B) {
			client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
			defer client.Close()
			var calls atomic.Uint64
			load := func(ctx context.Context) ([sha256.Size]byte, error) {
				if err := ctx.Err(); err != nil {
					return [sha256.Size]byte{}, err
				}
				calls.Add(1)
				return sha256.Sum256(payload), nil
			}
			ctx := context.Background()
			read := load
			if mode != "NoCache" {
				read = func(ctx context.Context) ([sha256.Size]byte, error) {
					return cacheq.Fetch(ctx, client, "benefit", load)
				}
			}
			if mode == "CacheHit" {
				if value, err := read(ctx); err != nil || value != want {
					b.Fatalf("warm cache preparation failed: %v", err)
				}
				calls.Store(0)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "FirstLoad" {
					// Each batch starts empty, so calibration cannot turn the cold case into a warm-cache test.
					b.StopTimer()
					client.Clear()
					b.StartTimer()
				}
				for range benchmarkReadsPerBatch {
					if value, err := read(ctx); err != nil || value != want {
						b.Fatalf("%s returned an incorrect result: %v", mode, err)
					}
				}
			}
			b.StopTimer()
			var expected uint64
			switch mode {
			case "NoCache":
				expected = uint64(b.N) * benchmarkReadsPerBatch
			case "FirstLoad":
				expected = uint64(b.N)
			}
			if calls.Load() != expected {
				b.Fatalf("%s executed %d loads, want %d", mode, calls.Load(), expected)
			}
			b.ReportMetric(float64(calls.Load())/float64(b.N), "loads/batch")
			b.ReportMetric(benchmarkReadsPerBatch, "reads/batch")
		})
	}
}

// BenchmarkCacheHit measures public cache reads and a minimal map-and-mutex reference.
func BenchmarkCacheHit(b *testing.B) {
	b.Run("MapMutexReference", func(b *testing.B) {
		var mu sync.Mutex
		values := map[string]int{"hit": 42}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mu.Lock()
			value := values["hit"]
			mu.Unlock()
			if value != 42 {
				b.Fatal("reference lost the cached value")
			}
		}
	})
	for _, operation := range []string{"Get", "Fetch", "Snapshot"} {
		b.Run(operation, func(b *testing.B) {
			client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
			defer client.Close()
			fetch := func(context.Context) (int, error) {
				b.Error("a warm cache hit called the loader")
				return 0, nil
			}
			if err := cacheq.Set(client, "hit", 42); err != nil {
				b.Fatal(err)
			}
			query := cacheq.Query(client, "hit", fetch, cacheq.QueryOptions{Disable: true})
			defer query.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch operation {
				case "Get":
					if state := cacheq.Get[int](client, "hit"); !state.HasData || state.Data != 42 {
						b.Fatal("Get lost the cached value")
					}
				case "Fetch":
					if value, err := cacheq.Fetch(
						context.Background(),
						client,
						"hit",
						fetch,
					); err != nil ||
						value != 42 {
						b.Fatalf("Fetch = %d, %v", value, err)
					}
				case "Snapshot":
					if state := query.Snapshot(); !state.HasData || state.Data != 42 {
						b.Fatal("Snapshot lost the cached value")
					}
				}
			}
		})
	}
}

// BenchmarkParallelFetch measures warm same-key and distributed-key access across benchmark workers.
func BenchmarkParallelFetch(b *testing.B) {
	for _, keyCount := range []int{1, 1024} {
		b.Run(fmt.Sprintf("Keys=%d", keyCount), func(b *testing.B) {
			client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
			defer client.Close()
			keys := make([]any, keyCount)
			for i := range keys {
				keys[i] = i
				if err := cacheq.Set(client, keys[i], 42); err != nil {
					b.Fatal(err)
				}
			}
			var workers atomic.Uint64
			fetch := func(context.Context) (int, error) {
				b.Error("a warm parallel fetch called the loader")
				return 0, nil
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				index := int(workers.Add(1)-1) % len(keys)
				for pb.Next() {
					value, err := cacheq.Fetch(context.Background(), client, keys[index], fetch)
					if err != nil || value != 42 {
						b.Errorf("Fetch = %d, %v", value, err)
						return
					}
					index = (index + 1) % len(keys)
				}
			})
		})
	}
}

// BenchmarkSharedLoad measures concurrent subscription bursts, including registration and cleanup.
// A channel barrier keeps the load active until all consumers have registered, independent of scheduling.
func BenchmarkSharedLoad(b *testing.B) {
	for _, consumers := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("Consumers=%d", consumers), func(b *testing.B) {
			client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
			defer client.Close()
			var calls atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := client.Remove("shared"); err != nil {
					b.Fatal(err)
				}
				release := make(chan struct{})
				fetch := func(ctx context.Context) (int, error) {
					calls.Add(1)
					select {
					case <-release:
						return 42, nil
					case <-ctx.Done():
						return 0, ctx.Err()
					}
				}
				queries := make([]*cacheq.QueryHandle[int], consumers)
				var registered sync.WaitGroup
				registered.Add(consumers)
				for consumer := range queries {
					go func() {
						defer registered.Done()
						queries[consumer] = cacheq.Query(client, "shared", fetch)
					}()
				}
				registered.Wait()
				close(release)
				for _, query := range queries {
					for state := range query.Updates() {
						if state.Err != nil {
							b.Fatal(state.Err)
						}
						if state.HasData && !state.Fetching {
							if state.Data != 42 {
								b.Fatal("shared load returned a different value")
							}
							break
						}
					}
					query.Close()
				}
			}
			b.StopTimer()
			stats := client.Stats()
			if calls.Load() != uint64(b.N) || stats.Loads != uint64(b.N) {
				b.Fatalf("expected one load per burst: calls=%d, loads=%d, bursts=%d", calls.Load(), stats.Loads, b.N)
			}
			if stats.MergedRequests != uint64(b.N)*uint64(consumers-1) {
				b.Fatalf("not every concurrent consumer joined the shared load: %+v", stats)
			}
			b.ReportMetric(float64(calls.Load())/float64(b.N), "loads/burst")
			b.ReportMetric(float64(consumers), "consumers/burst")
		})
	}
}

// BenchmarkSubscriberUpdates measures publishing to slow consumers and consumers that drain each update.
func BenchmarkSubscriberUpdates(b *testing.B) {
	for _, subscribers := range []int{1, 16, 128} {
		for _, mode := range []string{"LatestOnly", "Consumed"} {
			b.Run(fmt.Sprintf("Subscribers=%d/%s", subscribers, mode), func(b *testing.B) {
				client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
				defer client.Close()
				queries := make([]*cacheq.QueryHandle[int], subscribers)
				fetch := func(context.Context) (int, error) { return 42, nil }
				for i := range queries {
					queries[i] = cacheq.Query(client, "updates", fetch, cacheq.QueryOptions{Disable: true})
					<-queries[i].Updates()
					defer queries[i].Close()
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := cacheq.Set(client, "updates", 42); err != nil {
						b.Fatal(err)
					}
					if mode == "Consumed" {
						for _, query := range queries {
							if state := <-query.Updates(); !state.HasData || state.Data != 42 {
								b.Fatal("subscriber lost the published value")
							}
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(subscribers), "subscribers/update")
			})
		}
	}
}

// BenchmarkCapacityEviction measures steady writes that each evict one entry from a full LRU cache.
func BenchmarkCapacityEviction(b *testing.B) {
	for _, capacity := range []int{64, 1024} {
		b.Run(fmt.Sprintf("Entries=%d", capacity), func(b *testing.B) {
			client := cacheq.NewClient(cacheq.Options{MaxEntries: capacity})
			defer client.Close()
			keys := make([]any, capacity+1)
			for i := range keys {
				keys[i] = i
				if i < capacity {
					if err := cacheq.Set(client, keys[i], 42); err != nil {
						b.Fatal(err)
					}
				}
			}
			index := capacity
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := cacheq.Set(client, keys[index], 42); err != nil {
					b.Fatal(err)
				}
				index = (index + 1) % len(keys)
			}
			b.StopTimer()
			stats := client.Stats()
			if stats.CacheEntries != capacity || stats.LRUEvictions != uint64(b.N) {
				b.Fatalf("writes did not produce one eviction each: %+v", stats)
			}
			b.ReportMetric(float64(stats.LRUEvictions)/float64(b.N), "evictions/op")
		})
	}
}

// BenchmarkGCRetentionRead measures resetting the inactive-key timer compared with disabled retention.
func BenchmarkGCRetentionRead(b *testing.B) {
	for _, retention := range []time.Duration{0, time.Hour} {
		b.Run(fmt.Sprintf("GCTime=%s", retention), func(b *testing.B) {
			client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour, GCTime: retention})
			defer client.Close()
			if err := cacheq.Set(client, "inactive", 42); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if state := cacheq.Get[int](client, "inactive"); !state.HasData || state.Data != 42 {
					b.Fatal("retention read lost the cached value")
				}
			}
		})
	}
}
