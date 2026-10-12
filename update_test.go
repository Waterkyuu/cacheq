package cacheq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestUpdateConcurrent commits one of two overlapping transformations without losing a write silently.
func TestUpdateConcurrent(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	Set(c, "counter", 0)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			results <- Update(c, "counter", func(value int) (int, error) {
				// Reentrant reads prove that application callbacks do not hold the Client lock.
				_ = Get[int](c, "counter")
				entered <- struct{}{}
				<-release
				return value + 1, nil
			})
		}()
	}
	<-entered
	<-entered
	close(release)
	first, second := <-results, <-results
	if first == nil {
		first, second = second, first
	}
	if !errors.Is(first, ErrUpdateConflict) || second != nil || Get[int](c, "counter").Data != 1 {
		t.Fatalf("overlapping updates = %v, %v, %+v", first, second, Get[int](c, "counter"))
	}
	if err := Update(c, "counter", func(value int) (int, error) { return value + 1, nil }); err != nil {
		t.Fatal(err)
	}
	if Get[int](c, "counter").Data != 2 {
		t.Fatal("explicit retry did not preserve both increments")
	}
}

// TestUpdateConflicts rejects changed state, including removal and recreation with the same value.
func TestUpdateConflicts(t *testing.T) {
	for _, action := range []string{"set", "invalidate", "remove", "clear", "recreate", "load", "age", "capacity"} {
		t.Run(action, func(t *testing.T) {
			c, now := newAgeClient(t, Options{StaleTime: time.Hour, MaxAge: time.Minute, MaxEntries: 1})
			Set(c, "key", 1)
			err := Update(c, "key", func(value int) (int, error) {
				switch action {
				case "set":
					Set(c, "key", 2)
				case "invalidate":
					c.Invalidate("key")
				case "remove":
					c.Remove("key")
				case "clear":
					c.Clear()
				case "recreate":
					c.Remove("key")
					Set(c, "key", 1)
				case "load":
					c.Invalidate("key")
					_, loadErr := Fetch(
						context.Background(),
						c,
						"key",
						func(context.Context) (int, error) { return 3, nil },
					)
					if loadErr != nil {
						t.Fatal(loadErr)
					}
				case "age":
					now.Add(int64(time.Minute))
				case "capacity":
					Set(c, "other", 4)
				}
				return value + 10, nil
			})
			if !errors.Is(err, ErrUpdateConflict) || Get[int](c, "key").Data == 11 {
				t.Fatalf("conflict = %v, state = %+v", err, Get[int](c, "key"))
			}
		})
	}
}

// TestUpdateValidation leaves cache data unchanged when validation or application logic fails.
func TestUpdateValidation(t *testing.T) {
	c := NewClient(Options{})
	t.Cleanup(c.Close)
	Set(c, "key", 7)
	if err := Update[int](c, "key", nil); !errors.Is(err, ErrNoUpdater) {
		t.Fatal(err)
	}
	never := func(int) (int, error) { t.Fatal("unexpected callback"); return 0, nil }
	if err := Update(c, "missing", never); !errors.Is(err, ErrNoData) {
		t.Fatal(err)
	}
	if err := Update(c, []int{1}, never); !errors.Is(err, ErrInvalidKey) {
		t.Fatal(err)
	}
	if err := Update(c, "key", func(string) (string, error) {
		t.Fatal("unexpected type-mismatched callback")
		return "", nil
	}); !errors.Is(err, ErrTypeMismatch) {
		t.Fatal(err)
	}
	failure := errors.New("rejected")
	if err := Update(c, "key", func(int) (int, error) { return 99, failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if Get[int](c, "key").Data != 7 {
		t.Fatal("failed callback changed cache")
	}
	if err := Update(
		c,
		"key",
		func(value int) (int, error) { c.Close(); return value + 1, nil },
	); !errors.Is(
		err,
		ErrClosed,
	) {
		t.Fatal(err)
	}
	if err := Update(c, "key", never); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// TestUpdateCancelsOldLoad publishes the new value and prevents a late fetch from replacing it.
func TestUpdateCancelsOldLoad(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	Set(c, "key", 1)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	h := Query(c, "key", func(context.Context) (int, error) { close(started); <-release; return 99, nil })
	t.Cleanup(h.Close)
	c.Invalidate("key")
	<-started
	if err := Update(c, "key", func(value int) (int, error) { return value + 1, nil }); err != nil {
		t.Fatal(err)
	}
	state := awaitState(t, h.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	if state.Stale || state.Err != nil {
		t.Fatalf("updated state = %+v", state)
	}
	once.Do(func() { close(release) })
	awaitStats(t, c, func(s Stats) bool { return s.LoadCancellations == 1 })
	if h.Snapshot().Data != 2 {
		t.Fatal("late loader replaced update")
	}
}
