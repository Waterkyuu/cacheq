package redistore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Waterkyuu/cacheq"
	"github.com/redis/go-redis/v9"
)

// redisSubscriber is the optional Pub/Sub capability consumed without expanding RedisClient's P0 contract.
type redisSubscriber interface {
	// Subscribe creates an independently closable Pub/Sub connection on the writable server.
	Subscribe(context.Context, ...string) *redis.PubSub
}

// invalidationWire preserves arbitrary Go string bytes without JSON replacing invalid UTF-8 identities.
type invalidationWire struct {
	// ID identifies the committed operation independently of delivery order.
	ID string
	// Key represents a logical shared key as base64-encoded JSON bytes.
	Key []byte
	// Tags represents exact labels as base64-encoded JSON byte slices.
	Tags [][]byte
	// RegisteredTags transfers newly registered relationships without declaring a data change.
	RegisteredTags [][]byte
}

// redisInvalidationSubscription owns one Pub/Sub reader and its cancellation-bound connection.
type redisInvalidationSubscription struct {
	// pubsub is dedicated to this subscription; the application-owned command client remains open.
	pubsub *redis.PubSub
	// events buffers a bounded number of notifications without silently dropping new messages.
	events chan cacheq.SharedInvalidation
	// cancel ends the reader's lifetime and its blocking channel sends.
	cancel context.CancelFunc
	// done closes after the reader and Pub/Sub connection have been released.
	done chan struct{}
	// once makes explicit resource release safe under concurrent Close calls.
	once sync.Once
}

// marshalInvalidation encodes a committed operation without losing binary key or label identities.
func marshalInvalidation(event cacheq.SharedInvalidation) ([]byte, error) {
	labels := make([][]byte, 0, len(event.Tags))
	for _, tag := range event.Tags {
		labels = append(labels, []byte(tag))
	}
	registered := make([][]byte, 0, len(event.RegisteredTags))
	for _, tag := range event.RegisteredTags {
		registered = append(registered, []byte(tag))
	}
	return json.Marshal(
		invalidationWire{ID: event.ID, Key: []byte(event.Key), Tags: labels, RegisteredTags: registered},
	)
}

// SubscribeInvalidations confirms subscription admission before returning a cancellation-bound reader.
func (s *Store) SubscribeInvalidations(ctx context.Context) (cacheq.SharedInvalidationSubscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.enableNotifications {
		return nil, cacheq.ErrSharedCapability
	}
	client, ok := s.client.(redisSubscriber)
	if !ok {
		return nil, cacheq.ErrSharedCapability
	}
	lifetime, cancel := context.WithCancel(ctx)
	pubsub := client.Subscribe(lifetime, s.channel())
	// Closing the dedicated connection interrupts Receive even when its driver ignores context deadlines.
	stop := context.AfterFunc(lifetime, func() { _ = pubsub.Close() })
	message, err := pubsub.ReceiveTimeout(lifetime, time.Second)
	if err != nil {
		stop()
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf("redis subscribe: %w", err)
	}
	if _, ok := message.(*redis.Subscription); !ok {
		stop()
		cancel()
		_ = pubsub.Close()
		return nil, errors.New("redis subscribe did not confirm admission")
	}
	stream := &redisInvalidationSubscription{
		pubsub: pubsub,
		events: make(chan cacheq.SharedInvalidation, 32),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go stream.run(lifetime, stop)
	return stream, nil
}

// Events exposes the ordered event stream, including resets after transport or decoding gaps.
func (s *redisInvalidationSubscription) Events() <-chan cacheq.SharedInvalidation { return s.events }

// Close cancels pending reads and waits for dedicated connection release; it never closes the command client.
func (s *redisInvalidationSubscription) Close() error {
	s.once.Do(func() { s.cancel(); _ = s.pubsub.Close() })
	<-s.done
	return nil
}

// run preserves subscription acknowledgements so reconnects cannot silently lose invalidation history.
func (s *redisInvalidationSubscription) run(ctx context.Context, stop func() bool) {
	defer close(s.done)
	defer close(s.events)
	defer stop()
	defer s.cancel()
	defer func() { _ = s.pubsub.Close() }()
	var disconnected bool
	for {
		message, err := s.pubsub.Receive(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if !disconnected && !s.send(ctx, cacheq.SharedInvalidation{Reset: true, Err: err}) {
				return
			}
			disconnected = true
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		switch message := message.(type) {
		case *redis.Subscription:
			disconnected = false
			if !s.send(ctx, cacheq.SharedInvalidation{Reset: true}) {
				return
			}
		case *redis.Message:
			var wire invalidationWire
			err := json.Unmarshal([]byte(message.Payload), &wire)
			keyEvent, tagEvent := len(wire.Key) != 0, len(wire.Tags) != 0
			validIdentity := wire.ID != "" && keyEvent != tagEvent
			validRegistration := len(wire.RegisteredTags) == 0 || keyEvent
			valid := err == nil && validIdentity && validRegistration
			event := cacheq.SharedInvalidation{ID: wire.ID, Key: string(wire.Key), Tags: []string{}}
			for _, tag := range wire.Tags {
				event.Tags = append(event.Tags, string(tag))
			}
			for _, tag := range wire.RegisteredTags {
				event.RegisteredTags = append(event.RegisteredTags, string(tag))
			}
			if !valid {
				event = cacheq.SharedInvalidation{Reset: true, Err: errors.New("invalid redis invalidation payload")}
			}
			if !s.send(ctx, event) {
				return
			}
		}
	}
}

// send applies backpressure instead of dropping invalidations, and cancellation always releases the producer.
func (s *redisInvalidationSubscription) send(ctx context.Context, event cacheq.SharedInvalidation) bool {
	select {
	case s.events <- event:
		return true
	case <-ctx.Done():
		return false
	}
}
