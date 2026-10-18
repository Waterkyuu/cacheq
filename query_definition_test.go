package cacheq

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// TestQueryDefinitionConstruction rejects extra configurations immediately without retaining a partial definition.
func TestQueryDefinitionConstruction(t *testing.T) {
	hour := time.Hour
	for count := range 4 {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			calls := 0
			options := make([]QueryOptions, count)
			for i := range options {
				options[i] = QueryOptions{Disable: true, StaleTime: &hour}
			}
			definition, err := QueryDefinition("answer", func(context.Context) (int, error) {
				calls++
				return 42, nil
			}, options...)
			if calls != 0 {
				t.Fatal("constructing a definition executed its loader")
			}
			if count > 1 {
				if !errors.Is(err, ErrTooManyOptions) {
					t.Fatalf("constructor error = %v", err)
				}
				if !reflect.DeepEqual(definition, Definition[int]{}) {
					t.Fatalf("rejected definition = %+v", definition)
				}
				return
			}
			if err != nil || definition.Key != "answer" || definition.Fetcher == nil {
				t.Fatalf("definition = %+v, error = %v", definition, err)
			}
			expected := QueryOptions{}
			if count == 1 {
				expected = options[0]
			}
			if !reflect.DeepEqual(definition.Options, expected) {
				t.Fatalf("options = %+v, want %+v", definition.Options, expected)
			}
		})
	}
}

// TestQueryDefinitionOptions verifies inheritance and explicit freshness in one configuration.
func TestQueryDefinitionOptions(t *testing.T) {
	zero := time.Duration(0)
	for _, tc := range []struct {
		// name identifies the policy selection case.
		name string
		// options supplies the constructor's single loading policy.
		options QueryOptions
		// calls is the number of source reads expected after two explicit fetches.
		calls int
	}{
		{name: "defaults", options: QueryOptions{}, calls: 1},
		{name: "override", options: QueryOptions{StaleTime: &zero}, calls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newAgeClient(t, Options{StaleTime: time.Hour})
			calls := 0
			definition, err := QueryDefinition("answer", func(context.Context) (int, error) {
				calls++
				return 42, nil
			}, tc.options)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				value, err := definition.Fetch(context.Background(), c)
				if err != nil || value != 42 {
					t.Fatalf("fetch = %d, %v", value, err)
				}
			}
			if calls != tc.calls {
				t.Fatalf("source calls = %d, want %d", calls, tc.calls)
			}
		})
	}
}

// TestQueryDefinitionNilFetcher infers a typed nil loader without validating or loading until use.
func TestQueryDefinitionNilFetcher(t *testing.T) {
	c, _ := newAgeClient(t, Options{StaleTime: time.Hour})
	var load Fetcher[int]
	definition, err := QueryDefinition("answer", load)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := definition.Fetch(context.Background(), c); !errors.Is(err, ErrNoFetcher) {
		t.Fatal(err)
	}
	if err := Set(c, "answer", 42); err != nil {
		t.Fatal(err)
	}
	if value, err := definition.Fetch(context.Background(), c); err != nil || value != 42 {
		t.Fatalf("cached fetch = %d, %v", value, err)
	}
	missing, err := QueryDefinition[int]("missing", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missing.Fetch(context.Background(), c); !errors.Is(err, ErrNoFetcher) {
		t.Fatal(err)
	}
}

// TestQueryDefinition reuses one definition across clients and loading entry points without owning a cache.
func TestQueryDefinition(t *testing.T) {
	hour, retries := time.Hour, 1
	calls := 0
	failure := errors.New("offline")
	definition, err := QueryDefinition("user", func(context.Context) (int, error) {
		calls++
		if calls == 1 {
			return 0, failure
		}
		return calls, nil
	}, QueryOptions{
		Disable: true, StaleTime: &hour, Retry: &retries,
		RetryDelay: func(int) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("constructing a definition loaded data")
	}
	c, _ := newAgeClient(t, Options{})
	h := definition.Query(c)
	t.Cleanup(h.Close)
	if h.Snapshot().Fetching || calls != 0 {
		t.Fatal("definition ignored automatic loading permission")
	}
	if err := definition.Prefetch(context.Background(), c); err != nil || calls != 2 {
		t.Fatalf("definition prefetch = %v, calls=%d", err, calls)
	}
	value, err := definition.Fetch(context.Background(), c)
	if err != nil || value != 2 || calls != 2 || h.Snapshot().Stale || h.Snapshot().Data != 2 {
		t.Fatalf("shared definition = %d, %v, state=%+v", value, err, h.Snapshot())
	}
	// Creating a handle copies policy; modifying the definition does not change its subscription.
	definition.Options.Disable = false
	if err := c.Invalidate(definition.Key); err != nil || calls != 2 || h.Snapshot().Fetching {
		t.Fatalf("existing definition handle changed permission: %v, calls=%d", err, calls)
	}
	other, _ := newAgeClient(t, Options{})
	value, err = definition.Fetch(context.Background(), other)
	if err != nil || value != 3 || calls != 3 {
		t.Fatalf("independent Client = %d, %v, calls=%d", value, err, calls)
	}
}

// TestQueryDefinitionErrors delegates validation and cancellation to the existing query entry points.
func TestQueryDefinitionErrors(t *testing.T) {
	for _, api := range []string{"fetch", "prefetch", "query"} {
		t.Run(api, func(t *testing.T) {
			c, _ := newAgeClient(t, Options{})
			failure := errors.New("read rejected")
			calls := 0
			definition, err := QueryDefinition("user", func(context.Context) (int, error) {
				calls++
				return 0, failure
			}, QueryOptions{Disable: true})
			if err != nil {
				t.Fatal(err)
			}
			switch api {
			case "fetch":
				_, err = definition.Fetch(context.Background(), c)
			case "prefetch":
				err = definition.Prefetch(context.Background(), c)
			case "query":
				h := definition.Query(c)
				t.Cleanup(h.Close)
				_, err = h.Refetch(context.Background())
			}
			if !errors.Is(err, failure) || calls != 1 {
				t.Fatalf("definition failure = %v, calls=%d", err, calls)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := definition.Fetch(ctx, c); !errors.Is(err, context.Canceled) || calls != 1 {
				t.Fatalf("canceled fetch = %v, calls=%d", err, calls)
			}
			if err := definition.Prefetch(ctx, c); !errors.Is(err, context.Canceled) || calls != 1 {
				t.Fatalf("canceled prefetch = %v, calls=%d", err, calls)
			}
			definition.Key = []int{1}
			if _, err := definition.Fetch(context.Background(), c); !errors.Is(err, ErrInvalidKey) {
				t.Fatal(err)
			}
			if err := definition.Prefetch(context.Background(), c); !errors.Is(err, ErrInvalidKey) {
				t.Fatal(err)
			}
			h := definition.Query(c)
			t.Cleanup(h.Close)
			if !errors.Is(h.Snapshot().Err, ErrInvalidKey) {
				t.Fatal(h.Snapshot().Err)
			}
		})
	}
}
