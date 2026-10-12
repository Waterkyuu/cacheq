package cacheq

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

// receiveTestEvent bounds the harness wait without relying on elapsed time for event ordering.
func receiveTestEvent(t *testing.T, events <-chan Event) Event {
	t.Helper()
	select {
	case event, open := <-events:
		if !open {
			t.Fatal("event stream closed before the expected record")
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for diagnostic event")
		return Event{}
	}
}

// subscribeTestEvents constructs a diagnostic stream and releases it after the test.
func subscribeTestEvents(t *testing.T, c *Client, options EventOptions) *EventSubscription {
	t.Helper()
	s, err := c.SubscribeEvents(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// emitTestEvents supplies transition records to isolate stream delivery from query execution.
func emitTestEvents(c *Client, events ...Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, event := range events {
		c.emitEventLocked(event)
	}
}

// TestEventsSubscriptionValidation rejects unsafe filters and closed owners without registering a stream.
func TestEventsSubscriptionValidation(t *testing.T) {
	// unsafeKey exercises dynamic comparability inside a nominally comparable struct.
	type unsafeKey struct {
		// Value holds the invalid dynamic component.
		Value any
	}
	c := NewClient(Options{})
	t.Cleanup(c.Close)
	for _, tc := range []struct {
		// name identifies the rejected option.
		name string
		// options contains the invalid subscription settings.
		options EventOptions
		// err identifies the expected validation error.
		err error
	}{
		{name: "negative buffer", options: EventOptions{Buffer: -1}, err: ErrInvalidEventOptions},
		{name: "slice", options: EventOptions{Key: []int{1}}, err: ErrInvalidKey},
		{name: "nested interface", options: EventOptions{Key: unsafeKey{Value: []int{1}}}, err: ErrInvalidKey},
		{name: "non-reflexive", options: EventOptions{Key: math.NaN()}, err: ErrInvalidKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := c.SubscribeEvents(tc.options)
			if s != nil || !errors.Is(err, tc.err) {
				t.Fatalf("subscription = %v, %v; want nil, %v", s, err, tc.err)
			}
		})
	}
	subscribeTestEvents(t, c, EventOptions{Key: 0})
	c.Close()
	if s, err := c.SubscribeEvents(EventOptions{}); s != nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed owner = %v, %v", s, err)
	}
}

// TestEventsDelivery preserves order, exact-key filtering, and cumulative drops without replacing old events.
func TestEventsDelivery(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	c := NewClient(Options{Clock: func() time.Time { return now }})
	t.Cleanup(c.Close)
	all := subscribeTestEvents(t, c, EventOptions{Buffer: 8})
	selected := subscribeTestEvents(t, c, EventOptions{Key: "a", Buffer: 2})
	emitTestEvents(c,
		Event{Kind: EventLoadStarted, Key: "a", LoadID: 7},
		Event{Kind: EventLoadStarted, Key: "b", LoadID: 8},
		Event{Kind: EventLoadJoined, Key: "a", LoadID: 7},
		Event{Kind: EventLoadFinished, Key: "a", LoadID: 7},
	)
	for i, kind := range []EventKind{EventLoadStarted, EventLoadStarted, EventLoadJoined, EventLoadFinished} {
		event := receiveTestEvent(t, all.Events())
		if event.Sequence != uint64(i+1) || event.Kind != kind || !event.Time.Equal(now) {
			t.Fatalf("all-key event %d = %+v", i, event)
		}
	}
	for _, sequence := range []uint64{1, 3} {
		if event := receiveTestEvent(t, selected.Events()); event.Sequence != sequence || event.Key != "a" {
			t.Fatalf("filtered event = %+v", event)
		}
	}
	if selected.Dropped() != 1 || all.Dropped() != 0 {
		t.Fatalf("drops: filtered=%d all=%d", selected.Dropped(), all.Dropped())
	}
	emitTestEvents(c, Event{Kind: EventCacheHit, Key: "a"})
	if event := receiveTestEvent(t, selected.Events()); event.Sequence != 5 || event.Kind != EventCacheHit {
		t.Fatalf("delivery after drain = %+v", event)
	}
	selected.Close()
	selected.Close()
	if _, open := <-selected.Events(); open || selected.Dropped() != 1 {
		t.Fatal("closed subscription lost its drop count or remained open")
	}
}

// TestEventsClientClose broadcasts the terminal transition and preserves unread records until drained.
func TestEventsClientClose(t *testing.T) {
	c := NewClient(Options{})
	s := subscribeTestEvents(t, c, EventOptions{Key: "a", Buffer: 2})
	emitTestEvents(c, Event{Kind: EventCacheHit, Key: "a"})
	c.Close()
	c.Close()
	if event := receiveTestEvent(t, s.Events()); event.Kind != EventCacheHit {
		t.Fatalf("buffered event = %+v", event)
	}
	if event := receiveTestEvent(t, s.Events()); event.Kind != EventClientClosed || event.Key != nil {
		t.Fatalf("terminal event = %+v", event)
	}
	if _, open := <-s.Events(); open {
		t.Fatal("client closure left its event stream open")
	}
	s.Close()
}

// TestEventsDefaultBuffer bounds omitted capacities and counts a dropped terminal event.
func TestEventsDefaultBuffer(t *testing.T) {
	c := NewClient(Options{})
	s := subscribeTestEvents(t, c, EventOptions{})
	for range 129 {
		emitTestEvents(c, Event{Kind: EventCacheHit, Key: "a"})
	}
	c.Close()
	var received int
	for range s.Events() {
		received++
	}
	if received != 128 || s.Dropped() != 2 {
		t.Fatalf("default buffer delivered %d events and dropped %d", received, s.Dropped())
	}
}

// TestEventsConcurrentClosure safely detaches readers while another goroutine publishes transitions.
func TestEventsConcurrentClosure(t *testing.T) {
	c := NewClient(Options{})
	t.Cleanup(c.Close)
	s := subscribeTestEvents(t, c, EventOptions{Buffer: 1})
	var operations sync.WaitGroup
	operations.Add(2)
	go func() {
		defer operations.Done()
		for range 100 {
			emitTestEvents(c, Event{Kind: EventCacheHit, Key: "a"})
		}
	}()
	go func() {
		defer operations.Done()
		s.Close()
		_ = s.Dropped()
	}()
	operations.Wait()
	for range s.Events() {
	}
}

// TestEventsSameKeySubscriptions preserves independent ownership when exact-key subscribers share an index.
func TestEventsSameKeySubscriptions(t *testing.T) {
	c := NewClient(Options{})
	t.Cleanup(c.Close)
	all := subscribeTestEvents(t, c, EventOptions{})
	first := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	second := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	other := subscribeTestEvents(t, c, EventOptions{Key: "b"})
	Set(c, "a", 1)
	for _, subscription := range []*EventSubscription{all, first, second} {
		if event := receiveTestEvent(t, subscription.Events()); event.Sequence != 1 || event.Key != "a" {
			t.Fatalf("shared-key delivery = %+v", event)
		}
	}
	first.Close()
	Set(c, "a", 2)
	for _, subscription := range []*EventSubscription{all, second} {
		if event := receiveTestEvent(t, subscription.Events()); event.Sequence != 2 || event.Key != "a" {
			t.Fatalf("remaining subscriber = %+v", event)
		}
	}
	second.Close()
	replacement := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	Set(c, "a", 3)
	for _, subscription := range []*EventSubscription{all, replacement} {
		if event := receiveTestEvent(t, subscription.Events()); event.Sequence != 3 || event.Key != "a" {
			t.Fatalf("replacement subscriber = %+v", event)
		}
	}
	select {
	case event := <-other.Events():
		t.Fatalf("unrelated subscriber received %+v", event)
	default:
	}
	c.Close()
	for _, subscription := range []*EventSubscription{all, replacement, other} {
		event := receiveTestEvent(t, subscription.Events())
		if event.Kind != EventClientClosed || event.Sequence != 4 || subscription.Dropped() != 0 {
			t.Fatalf("indexed closure = %+v; dropped = %d", event, subscription.Dropped())
		}
		if _, open := <-subscription.Events(); open {
			t.Fatal("indexed subscription remained open")
		}
	}
}
