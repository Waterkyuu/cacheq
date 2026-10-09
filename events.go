package cacheq

import (
	"errors"
	"reflect"
	"sync/atomic"
	"time"
)

// ErrInvalidEventOptions indicates a negative event channel capacity.
var ErrInvalidEventOptions = errors.New("invalid event options")

// defaultEventBuffer bounds subscriptions that omit an explicit capacity.
const defaultEventBuffer = 128

// EventKind identifies an observed cache decision or lifecycle transition.
type EventKind uint8

const (
	// EventUnknown is the zero value used when no event kind has been assigned.
	EventUnknown EventKind = iota
	// EventCacheHit indicates fresh data was reused without invoking a loader.
	EventCacheHit
	// EventLoadStarted indicates a new shared operation was registered for a key.
	EventLoadStarted
	// EventLoadJoined indicates another consumer joined an existing shared operation.
	EventLoadJoined
	// EventRetryStarted indicates an additional loader attempt actually began.
	EventRetryStarted
	// EventLoadFinished indicates a shared operation completed, including cancellation.
	EventLoadFinished
	// EventResultDiscarded indicates a completed outcome was not applied to shared cache state.
	EventResultDiscarded
	// EventInvalidated indicates a key was explicitly marked stale.
	EventInvalidated
	// EventCacheRemoved indicates retained data or an error was removed, including data exceeding MaxAge.
	EventCacheRemoved
	// EventLocalWrite indicates Set installed a typed local value.
	EventLocalWrite
	// EventClientClosed is a terminal event delivered to every subscription regardless of key filter.
	EventClientClosed
)

// String returns a stable log label for the event kind.
func (k EventKind) String() string {
	switch k {
	case EventCacheHit:
		return "cache_hit"
	case EventLoadStarted:
		return "load_started"
	case EventLoadJoined:
		return "load_joined"
	case EventRetryStarted:
		return "retry_started"
	case EventLoadFinished:
		return "load_finished"
	case EventResultDiscarded:
		return "result_discarded"
	case EventInvalidated:
		return "invalidated"
	case EventCacheRemoved:
		return "cache_removed"
	case EventLocalWrite:
		return "local_write"
	case EventClientClosed:
		return "client_closed"
	default:
		return "unknown"
	}
}

// EventTrigger identifies the API decision that starts, joins, or reuses query work.
type EventTrigger uint8

const (
	// TriggerNone indicates the event is not associated with a query API decision.
	TriggerNone EventTrigger = iota
	// TriggerFetch identifies Fetch, FetchWithOptions, or FetchWithExpiry.
	TriggerFetch
	// TriggerQuery identifies automatic loading during Query construction.
	TriggerQuery
	// TriggerEnable identifies a handle transitioning from disabled to enabled.
	TriggerEnable
	// TriggerRefetch identifies an explicit QueryHandle.Refetch call.
	TriggerRefetch
	// TriggerInvalidate identifies automatic refreshing after invalidation.
	TriggerInvalidate
	// TriggerPrefetch identifies an imperative Prefetch call.
	TriggerPrefetch
)

// String returns a stable log label for the initiating API decision.
func (t EventTrigger) String() string {
	switch t {
	case TriggerFetch:
		return "fetch"
	case TriggerQuery:
		return "query"
	case TriggerEnable:
		return "enable"
	case TriggerRefetch:
		return "refetch"
	case TriggerInvalidate:
		return "invalidate"
	case TriggerPrefetch:
		return "prefetch"
	default:
		return "none"
	}
}

// EventReason describes a cache decision, removal, or discarded outcome without guessing backend activity.
type EventReason uint8

const (
	// ReasonNone indicates an event has no applicable cause.
	ReasonNone EventReason = iota
	// ReasonFresh indicates this consumer considered the retained outcome reusable.
	ReasonFresh
	// ReasonMissing indicates no usable cached result was available.
	ReasonMissing
	// ReasonExpired indicates this consumer's freshness deadline had been reached.
	ReasonExpired
	// ReasonInvalidated indicates an explicit invalidation required fresh data.
	ReasonInvalidated
	// ReasonPreviousFailure indicates an earlier failed load left the outcome stale.
	ReasonPreviousFailure
	// ReasonRefetch indicates a caller requested a load regardless of freshness.
	ReasonRefetch
	// ReasonCapacity indicates LRU removal to enforce MaxEntries.
	ReasonCapacity
	// ReasonInactive indicates idle-cache collection after GCTime.
	ReasonInactive
	// ReasonMaxAge indicates cached data reached its availability limit.
	ReasonMaxAge
	// ReasonRemove indicates explicit removal of one key.
	ReasonRemove
	// ReasonClear indicates explicit removal of all retained results.
	ReasonClear
	// ReasonSet indicates a local write replaced older shared work.
	ReasonSet
	// ReasonCancel indicates caller cancellation, a deadline, or explicit Client.Cancel.
	ReasonCancel
	// ReasonClientClosed indicates the Client ended its lifetime.
	ReasonClientClosed
)

// String returns a stable log label for the observed cause.
func (r EventReason) String() string {
	switch r {
	case ReasonFresh:
		return "fresh"
	case ReasonMissing:
		return "missing"
	case ReasonExpired:
		return "expired"
	case ReasonInvalidated:
		return "invalidated"
	case ReasonPreviousFailure:
		return "previous_failure"
	case ReasonRefetch:
		return "refetch"
	case ReasonCapacity:
		return "capacity"
	case ReasonInactive:
		return "inactive"
	case ReasonMaxAge:
		return "max_age"
	case ReasonRemove:
		return "remove"
	case ReasonClear:
		return "clear"
	case ReasonSet:
		return "set"
	case ReasonCancel:
		return "cancel"
	case ReasonClientClosed:
		return "client_closed"
	default:
		return "none"
	}
}

// Event is an immutable diagnostic record of a Client decision, without cached data.
// Load events describe per-key loader work, not SQL, HTTP, or bulk callback counts.
type Event struct {
	// Sequence increases across this Client's emitted events; filtering and drops can leave gaps.
	Sequence uint64
	// Time records observation time using Options.Clock, independently of elapsed load duration.
	Time time.Time
	// Key identifies the query; it is nil for the terminal Client event.
	Key any
	// Kind identifies the observed transition.
	Kind EventKind
	// LoadID correlates a shared operation within this Client; zero means no associated operation.
	LoadID uint64
	// Trigger identifies the current decision, or the operation initiator for retry and completion events.
	Trigger EventTrigger
	// Reason identifies the decision's cache condition or the removal and discard cause.
	Reason EventReason
	// Joined counts additional joins to this operation, not distinct users or currently active waiters.
	Joined uint64
	// Attempts counts actual loader invocations so far, including the initial attempt.
	Attempts int
	// Duration is elapsed operation time on completion, including scheduling and retry waits.
	Duration time.Duration
	// Err contains a retained cache error, the prior retry error, or the final load error as applicable.
	Err error
	// Applied is true on completion when its outcome was installed in shared cache state.
	// An installed error or a result left stale by in-flight invalidation still counts as applied.
	Applied bool
}

// EventOptions selects a query key and a bounded delivery channel.
type EventOptions struct {
	// Key limits delivery to one comparable key; nil observes every key.
	Key any
	// Buffer bounds unread events; zero uses 128, and negative values are rejected.
	Buffer int
}

// EventSubscription owns a filtered event stream until it or the Client closes.
// Full channels drop new events; receiving never changes cache data, recency, or retention.
type EventSubscription struct {
	// client serializes event publication and subscription closure.
	client *Client
	// key holds the optional exact-key filter.
	key any
	// events buffers ordered records and closes after detachment.
	events chan Event
	// dropped accumulates matching events rejected because the channel was full.
	dropped atomic.Uint64
}

// SubscribeEvents starts observing future transitions without retaining historical events.
// Invalid filters return ErrInvalidKey, negative buffers return ErrInvalidEventOptions,
// and a closed Client returns ErrClosed. Client closure is broadcast even to filtered subscriptions.
func (c *Client) SubscribeEvents(options EventOptions) (*EventSubscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if options.Buffer < 0 {
		return nil, ErrInvalidEventOptions
	}
	if options.Key != nil {
		value := reflect.ValueOf(options.Key)
		if !value.Comparable() ||
			!value.Equal(value) { //nolint:gocritic // Reject NaN-containing filters that cannot match themselves.
			return nil, ErrInvalidKey
		}
	}
	capacity := options.Buffer
	if capacity == 0 {
		capacity = defaultEventBuffer
	}
	s := &EventSubscription{client: c, key: options.Key, events: make(chan Event, capacity)}
	if c.eventSubscriptions == nil {
		c.eventSubscriptions = make(map[any]map[*EventSubscription]struct{})
	}
	if c.eventSubscriptions[s.key] == nil {
		c.eventSubscriptions[s.key] = make(map[*EventSubscription]struct{})
	}
	c.eventSubscriptions[s.key][s] = struct{}{}
	return s, nil
}

// Events returns an ordered stream of accepted future events; slow readers can lose new records.
func (s *EventSubscription) Events() <-chan Event { return s.events }

// Dropped returns the cumulative number of matching events lost to a full buffer, even after Close.
func (s *EventSubscription) Dropped() uint64 { return s.dropped.Load() }

// Close detaches this subscription and closes its channel without affecting query work.
// Buffered events remain readable, and repeated closure is harmless.
func (s *EventSubscription) Close() {
	c := s.client
	c.mu.Lock()
	defer c.mu.Unlock()
	subscriptions := c.eventSubscriptions[s.key]
	if _, exists := subscriptions[s]; !exists {
		return
	}
	delete(subscriptions, s)
	if len(subscriptions) == 0 {
		delete(c.eventSubscriptions, s.key)
	}
	close(s.events)
}

// emitEventLocked stamps and publishes without running application callbacks or blocking queries.
func (c *Client) emitEventLocked(event Event) {
	if len(c.eventSubscriptions) == 0 {
		return
	}
	c.eventSequence++
	event.Sequence, event.Time = c.eventSequence, c.options.Clock()
	if event.Kind == EventClientClosed {
		for _, subscriptions := range c.eventSubscriptions {
			for subscription := range subscriptions {
				subscription.publishLocked(event)
			}
		}
		return
	}
	// Exact-key observers must not extend the global critical section for unrelated queries.
	for subscription := range c.eventSubscriptions[nil] {
		subscription.publishLocked(event)
	}
	if event.Key != nil {
		for subscription := range c.eventSubscriptions[event.Key] {
			subscription.publishLocked(event)
		}
	}
}

// publishLocked delivers one matching event while the Client lock excludes subscription closure.
func (s *EventSubscription) publishLocked(event Event) {
	select {
	case s.events <- event:
	default:
		s.dropped.Add(1)
	}
}
