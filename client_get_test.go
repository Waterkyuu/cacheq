package soroi

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TestClientGetValues preserves dynamic values, including cached nils and zero values.
func TestClientGetValues(t *testing.T) {
	for _, tc := range []struct {
		// name identifies the result representation being read.
		name string
		// put installs data with the intended static result type.
		put func(*Client) error
		// want is the dynamic value expected from the untyped snapshot.
		want any
	}{
		{name: "string", put: func(c *Client) error { return Set(c, "key", "hello") }, want: "hello"},
		{name: "zero", put: func(c *Client) error { return Set(c, "key", 0) }, want: 0},
		{name: "slice", put: func(c *Client) error { return Set(c, "key", []int{1, 2}) }, want: []int{1, 2}},
		{name: "nil pointer", put: func(c *Client) error { return Set[*int](c, "key", nil) }, want: (*int)(nil)},
		{name: "nil interface", put: func(c *Client) error { return Set[any](c, "key", nil) }, want: nil},
		{name: "interface value", put: func(c *Client) error { return Set[any](c, "key", "hello") }, want: "hello"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newInvalidationClient(t)
			if err := tc.put(c); err != nil {
				t.Fatal(err)
			}
			state := c.Get("key")
			if !state.HasData || state.Status != Success || state.Err != nil || state.Stale || state.Fetching {
				t.Fatalf("cached state = %+v", state)
			}
			if !reflect.DeepEqual(state.Data, tc.want) {
				t.Fatalf("data = %#v (%T), want %#v (%T)", state.Data, state.Data, tc.want, tc.want)
			}
			if !state.UpdatedAt.Equal(time.Unix(0, 0)) || !state.ExpiresAt.Equal(time.Unix(0, 0).Add(time.Hour)) {
				t.Fatalf("cached timestamps = %+v", state)
			}
		})
	}
}

// TestClientGetTypeBindings reads without declaring a type while preserving typed API checks.
func TestClientGetTypeBindings(t *testing.T) {
	c := newInvalidationClient(t)
	if state := c.Get("missing"); state.Status != Idle || state.HasData || state.Data != nil || !state.Stale {
		t.Fatalf("missing state = %+v", state)
	}
	if err := Set(c, "missing", 42); err != nil {
		t.Fatalf("reading an unused key bound its type: %v", err)
	}
	if state := c.Get("missing"); state.Data != 42 || state.Err != nil {
		t.Fatalf("untyped read = %+v", state)
	}
	if state := Get[int](c, "missing"); state.Data != 42 || state.Err != nil {
		t.Fatalf("typed read = %+v", state)
	}
	if state := Get[any](c, "missing"); !errors.Is(state.Err, ErrTypeMismatch) || state.HasData {
		t.Fatalf("static any read = %+v", state)
	}
	if state := Get[string](c, "missing"); !errors.Is(state.Err, ErrTypeMismatch) || state.HasData {
		t.Fatalf("incompatible read = %+v", state)
	}
	if err := Set(c, "missing", "wrong"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("incompatible write = %v", err)
	}
	if state := c.Get("missing"); state.Data != 42 || state.Err != nil {
		t.Fatalf("rejected operations changed the value: %+v", state)
	}
}

// TestClientGetValidation rejects unsafe keys and closed clients before reading cached state.
func TestClientGetValidation(t *testing.T) {
	c := newInvalidationClient(t)
	for _, key := range []any{nil, []string{"users"}, map[string]int{}, struct {
		// Value exercises a comparable container holding a non-comparable dynamic value.
		Value any
	}{Value: []int{1}}} {
		state := c.Get(key)
		if !errors.Is(state.Err, ErrInvalidKey) || state.Status != Error || state.HasData || !state.Stale {
			t.Fatalf("invalid key state = %+v", state)
		}
	}
	c.Close()
	for _, key := range []any{"users", nil} {
		state := c.Get(key)
		if !errors.Is(state.Err, ErrClosed) || state.Status != Error || state.HasData || state.Data != nil {
			t.Fatalf("closed client state = %+v", state)
		}
	}
}

// TestClientGetLoadingAndCancellation observes shared work without starting or canceling a request.
func TestClientGetLoadingAndCancellation(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprint(retained), func(t *testing.T) {
			c := newInvalidationClient(t)
			if retained {
				if err := Set(c, "key", "old"); err != nil {
					t.Fatal(err)
				}
				if err := c.Invalidate("key", InvalidateOptions{Refetch: RefetchNone}); err != nil {
					t.Fatal(err)
				}
			}
			started := make(chan struct{})
			handle := Query(c, "key", func(ctx context.Context) (string, error) {
				close(started)
				<-ctx.Done()
				return "", ctx.Err()
			})
			t.Cleanup(handle.Close)
			<-started
			state := c.Get("key")
			if !state.Fetching || !state.Stale || state.HasData != retained || state.Err != nil {
				t.Fatalf("loading state = %+v", state)
			}
			if retained && state.Data != "old" {
				t.Fatalf("refresh lost retained data: %+v", state)
			}
			if !retained && (state.Status != Pending || state.Data != nil) {
				t.Fatalf("initial load state = %+v", state)
			}
			if err := c.Cancel("key"); err != nil {
				t.Fatal(err)
			}
			awaitState(t, handle.Updates(), func(s Snapshot[string]) bool { return !s.Fetching })
			state = c.Get("key")
			if state.Fetching || state.HasData != retained || state.Err != nil {
				t.Fatalf("canceled state = %+v", state)
			}
		})
	}
}

// TestClientGetExpiryAndErrors applies client freshness and age limits while retaining load errors.
func TestClientGetExpiryAndErrors(t *testing.T) {
	c, now := newAgeClient(t, Options{StaleTime: time.Hour, MaxAge: 10 * time.Minute})
	if err := Set(c, "key", "old"); err != nil {
		t.Fatal(err)
	}
	freshness := time.Minute
	handle := Query(
		c,
		"key",
		func(context.Context) (string, error) {
			t.Fatal("inspection started a request")
			return "", nil
		},
		QueryOptions{Disable: true, StaleTime: &freshness},
	)
	t.Cleanup(handle.Close)
	<-handle.Updates()
	now.Store(int64(2 * time.Minute))
	if state := c.Get("key"); state.Stale || !handle.Snapshot().Stale {
		t.Fatalf("client freshness = %+v", state)
	}
	if err := c.Invalidate("key", InvalidateOptions{Refetch: RefetchNone}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("offline")
	_, err := Fetch(
		context.Background(),
		c,
		"key",
		func(context.Context) (string, error) {
			return "", failure
		},
	)
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if state := c.Get("key"); !state.HasData || state.Data != "old" || !state.Stale || state.Status != Error {
		t.Fatalf("retained failure state = %+v", state)
	}
	now.Store(int64(10*time.Minute - time.Nanosecond))
	if state := c.Get("key"); !state.HasData || !errors.Is(state.Err, failure) {
		t.Fatalf("before age boundary = %+v", state)
	}
	now.Add(1)
	state := c.Get("key")
	if state.HasData || state.Data != nil || !state.Stale || !errors.Is(state.Err, failure) {
		t.Fatalf("at age boundary = %+v", state)
	}
	awaitState(t, handle.Updates(), func(s Snapshot[string]) bool { return !s.HasData })
	if err := Set(c, "key", 42); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("expiration released a live type binding: %v", err)
	}
}

// TestClientGetRetention restarts idle cleanup without extending the data's freshness deadline.
func TestClientGetRetention(t *testing.T) {
	c, scheduler := newGCClient(t, time.Minute)
	if err := Set(c, "key", "cached"); err != nil {
		t.Fatal(err)
	}
	first := scheduler.latest(t)
	scheduler.now.Add(int64(30 * time.Second))
	state := c.Get("key")
	if !state.HasData || !first.stopped.Load() {
		t.Fatalf("read did not restart retention: %+v", state)
	}
	current := scheduler.latest(t)
	scheduler.now.Add(int64(30 * time.Second))
	first.callback()
	if state := c.Get("key"); !state.HasData || !state.ExpiresAt.Equal(state.UpdatedAt.Add(time.Hour)) {
		t.Fatalf("old cleanup removed reused data or changed freshness: %+v", state)
	}
	if !current.stopped.Load() {
		t.Fatal("second read did not restart retention")
	}
	current = scheduler.latest(t)
	scheduler.now.Add(int64(time.Minute))
	current.callback()
	if state := c.Get("key"); state.HasData || state.Data != nil {
		t.Fatalf("idle data survived retention: %+v", state)
	}
}

// TestClientGetRecency keeps a recently inspected entry when another entry exceeds capacity.
func TestClientGetRecency(t *testing.T) {
	c, _ := newAgeClient(t, Options{MaxEntries: 2})
	for _, key := range []string{"a", "b"} {
		if err := Set(c, key, key); err != nil {
			t.Fatal(err)
		}
	}
	c.Get("a")
	if err := Set(c, "c", "c"); err != nil {
		t.Fatal(err)
	}
	if !c.Get("a").HasData || c.Get("b").HasData || !c.Get("c").HasData {
		t.Fatal("capacity eviction ignored untyped read recency")
	}
}

// ExampleClient_Get reads a cached value by key and retains the typed access alternative.
func ExampleClient_Get() {
	client := NewClient(Options{StaleTime: time.Minute})
	defer client.Close()
	if err := Set(client, "greeting", "hello"); err != nil {
		panic(err)
	}
	state := client.Get("greeting")
	if state.HasData {
		fmt.Println("cached:", state.Data)
	}
	typed := Get[string](client, "greeting")
	if typed.HasData {
		fmt.Println("typed:", typed.Data)
	}
	// Output:
	// cached: hello
	// typed: hello
}
