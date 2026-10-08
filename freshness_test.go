package cacheq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestFetchAndQueryFreshness keeps imperative reads and automatic loading consistent at cache boundaries.
func TestFetchAndQueryFreshness(t *testing.T) {
	for _, tc := range []struct {
		// name identifies the deadline or invalidation rule exercised by both APIs.
		name string
		// policy overrides the consumer's freshness window without changing Client defaults.
		policy time.Duration
		// age caps availability independently of the consumer's freshness policy.
		age time.Duration
		// elapsed advances the injected clock after installation and observer registration.
		elapsed time.Duration
		// deadline supplies an authoritative loader expiry when nonzero.
		deadline time.Duration
		// invalidated makes ordinary data stale even under a longer consumer policy.
		invalidated bool
		// fallback caches data with an error until the explicit deadline.
		fallback bool
		// wantLoad indicates whether the read or enablement must refresh the installed value.
		wantLoad bool
	}{
		{name: "fresh", policy: time.Minute, elapsed: time.Minute - time.Nanosecond},
		{name: "deadline", policy: time.Minute, elapsed: time.Minute, wantLoad: true},
		{name: "longer policy", policy: time.Hour, elapsed: 2 * time.Minute},
		{name: "zero policy", wantLoad: true},
		{name: "negative policy", policy: -time.Second, wantLoad: true},
		{name: "age before deadline", policy: time.Hour, age: time.Minute, elapsed: time.Minute - time.Nanosecond},
		{name: "age deadline", policy: time.Hour, age: time.Minute, elapsed: time.Minute, wantLoad: true},
		{name: "explicit fresh", deadline: time.Minute, elapsed: time.Minute - time.Nanosecond},
		{name: "explicit deadline", policy: time.Hour, deadline: time.Minute, elapsed: time.Minute, wantLoad: true},
		{name: "invalidated", policy: time.Hour, invalidated: true, wantLoad: true},
		{name: "fresh fallback", deadline: time.Minute, fallback: true},
		{
			name: "expired fallback", policy: time.Hour, deadline: time.Minute,
			fallback: true, elapsed: time.Minute, wantLoad: true,
		},
	} {
		for _, api := range []string{"fetch", "query"} {
			t.Run(tc.name+"/"+api, func(t *testing.T) {
				client, now := newAgeClient(t, Options{StaleTime: time.Minute, MaxAge: tc.age})
				failure := errors.New("cached fallback")
				if tc.deadline != 0 {
					_, err := FetchWithExpiry(
						context.Background(),
						client,
						"a",
						func(context.Context) (int, time.Time, error) {
							var loadErr error
							if tc.fallback {
								loadErr = failure
							}
							return 7, time.Unix(0, int64(tc.deadline)), loadErr
						},
					)
					if err != nil && !errors.Is(err, failure) {
						t.Fatal(err)
					}
				} else if err := Set(client, "a", 7); err != nil {
					t.Fatal(err)
				}
				var calls atomic.Int32
				load := func(context.Context) (int, error) {
					calls.Add(1)
					return 9, nil
				}
				var handle *QueryHandle[int]
				if api == "query" {
					handle = Query(
						client,
						"a",
						load,
						QueryOptions{Disable: true, StaleTime: &tc.policy},
					)
					t.Cleanup(handle.Close)
					<-handle.Updates()
				}
				if tc.invalidated {
					if err := client.Invalidate("a", InvalidateOptions{Refetch: RefetchNone}); err != nil {
						t.Fatal(err)
					}
				}
				before := client.Stats()
				now.Store(int64(tc.elapsed))
				want := 7
				var wantErr error
				var wantCalls int32
				if tc.wantLoad {
					want, wantCalls = 9, 1
				} else if tc.fallback {
					wantErr = failure
				}
				var value int
				var err error
				if api == "fetch" {
					value, err = FetchWithOptions(
						context.Background(),
						client,
						"a",
						load,
						FetchOptions{StaleTime: &tc.policy},
					)
				} else {
					if err := handle.SetEnabled(true); err != nil {
						t.Fatal(err)
					}
					state := handle.Snapshot()
					if tc.wantLoad {
						state = awaitState(
							t,
							handle.Updates(),
							func(s Snapshot[int]) bool { return s.Data == want && !s.Fetching },
						)
					}
					value, err = state.Data, state.Err
				}
				wrongResult := value != want || !errors.Is(err, wantErr)
				if wrongResult || calls.Load() != wantCalls {
					t.Fatalf(
						"result = %d, %v; loads = %d; want %d, %v, %d",
						value,
						err,
						calls.Load(),
						want,
						wantErr,
						wantCalls,
					)
				}
				after := client.Stats()
				wantHits, wantMisses := uint64(1), uint64(0)
				if tc.wantLoad {
					wantHits, wantMisses = 0, 1
				}
				wrongDecisions := after.CacheHits-before.CacheHits != wantHits ||
					after.CacheMisses-before.CacheMisses != wantMisses
				if wrongDecisions || after.Loads-before.Loads != uint64(wantCalls) {
					t.Fatalf("cache decisions before = %+v, after = %+v", before, after)
				}
			})
		}
	}
}
