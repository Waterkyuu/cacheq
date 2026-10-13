package redistore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Waterkyuu/cacheq"
	"github.com/redis/go-redis/v9"
)

// TestRedisNotificationReceipt observes the exact committed key receipt without relying on sleeps.
func TestRedisNotificationReceipt(t *testing.T) {
	remote := newTestRedis(t)
	store, err := New(remote, Options{Prefix: "notify", EnableNotifications: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := store.SubscribeInvalidations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	receipt, err := store.InvalidateKey(ctx, "tenant:42:user:7")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream.Events():
		if event.ID != receipt.ID || event.Key != receipt.Key || event.Reset {
			t.Fatalf("receipt = %+v, event = %+v", receipt, event)
		}
	case <-ctx.Done():
		t.Fatal("committed receipt was not delivered")
	}
}

// TestRedisTagGenerations covers unseen tags, unions, OR invalidation, and obsolete in-flight writes.
func TestRedisTagGenerations(t *testing.T) {
	remote := newTestRedis(t)
	store, err := New(remote, Options{Prefix: "tags", EnableNotifications: true, EnableTags: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := store.ReadTagged(ctx, "detail", []string{"tenant:42:users", ""})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Write(
		ctx,
		"detail",
		cacheq.SharedWrite{Version: first.Version, Data: []byte("old"), TTL: time.Minute},
	); err != nil ||
		!ok {
		t.Fatalf("tagged write = %v, %v", ok, err)
	}
	added, err := store.ReadTagged(ctx, "detail", []string{"tenant:42:totals", "tenant:42:users"})
	if err != nil || len(added.Tags) != 3 || string(added.Data) != "old" {
		t.Fatalf("union = %+v, %v", added, err)
	}
	// Registering another tag changes the key snapshot, fencing writers that did not capture the added relationship.
	if ok, err := store.Write(
		ctx,
		"detail",
		cacheq.SharedWrite{Version: first.Version, Data: []byte("late"), TTL: time.Minute},
	); err != nil ||
		ok {
		t.Fatalf("membership fence = %v, %v", ok, err)
	}
	other, err := store.ReadTagged(ctx, "other-tenant", []string{"tenant:99:users"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Write(
		ctx,
		"other-tenant",
		cacheq.SharedWrite{Version: other.Version, Data: []byte("untouched"), TTL: time.Minute},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InvalidateTags(
		ctx,
		[]string{"tenant:42:users", "not-registered", "tenant:42:users"},
	); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Write(
		ctx,
		"detail",
		cacheq.SharedWrite{Version: added.Version, Data: []byte("resurrected"), TTL: time.Minute},
	); err != nil ||
		ok {
		t.Fatalf("tag fence = %v, %v", ok, err)
	}
	if miss, err := store.Read(ctx, "detail"); err != nil || len(miss.Data) != 0 || len(miss.Tags) != 3 {
		t.Fatalf("plain read ignored tags = %+v, %v", miss, err)
	}
	if hit, err := store.Read(ctx, "other-tenant"); err != nil || string(hit.Data) != "untouched" {
		t.Fatalf("tenant isolation = %+v, %v", hit, err)
	}
	if empty, err := store.InvalidateTags(ctx, []string{}); err != nil || empty.ID != "" {
		t.Fatalf("empty tags = %+v, %v", empty, err)
	}
	if _, err := store.InvalidateTags(ctx, []string{""}); err != nil {
		t.Fatal(err)
	}
}

// TestRedisTaggedQueryPropagation groups differently typed queries while preserving another tenant's data.
func TestRedisTaggedQueryPropagation(t *testing.T) {
	remote := newTestRedis(t)
	store, err := New(remote, Options{Prefix: "tagged-queries", EnableNotifications: true, EnableTags: true})
	if err != nil {
		t.Fatal(err)
	}
	intShared, err := cacheq.NewSharedCache[int](
		store,
		cacheq.SharedCacheOptions[int]{Key: "detail", Format: "int.v1", TTL: time.Hour, ShareTags: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	listShared, err := cacheq.NewSharedCache[[]int](
		store,
		cacheq.SharedCacheOptions[[]int]{Key: "list", Format: "list.v1", TTL: time.Hour, ShareTags: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	a, b := cacheq.NewClient(
		cacheq.Options{StaleTime: time.Hour},
	), cacheq.NewClient(
		cacheq.Options{StaleTime: time.Hour},
	)
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watch, err := b.WatchShared(ctx, store, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watch.Close)
	detail := cacheq.QueryDefinition[int]{
		Key:     "detail",
		Shared:  intShared,
		Tags:    []string{"tenant:42:users"},
		Fetcher: func(context.Context) (int, error) { return 7, nil },
		Options: cacheq.QueryOptions{Disable: true},
	}
	list := cacheq.QueryDefinition[[]int]{
		Key:     "list",
		Shared:  listShared,
		Tags:    []string{"tenant:42:users"},
		Fetcher: func(context.Context) ([]int, error) { return []int{7}, nil },
		Options: cacheq.QueryOptions{Disable: true},
	}
	if _, err := detail.Fetch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := list.Fetch(ctx, b); err != nil {
		t.Fatal(err)
	}
	d, l := detail.Query(b), list.Query(b)
	t.Cleanup(d.Close)
	t.Cleanup(l.Close)
	if err := a.InvalidateSharedTags(ctx, store, []string{"tenant:42:users"}); err != nil {
		t.Fatal(err)
	}
	for _, updates := range []<-chan bool{staleSignal(ctx, d.Updates()), staleSignal(ctx, l.Updates())} {
		select {
		case stale := <-updates:
			if !stale {
				t.Fatal("tag invalidation did not publish stale data")
			}
		case <-ctx.Done():
			t.Fatal("tag notification timed out")
		}
	}
	if !d.Snapshot().HasData || !l.Snapshot().HasData {
		t.Fatal("tag invalidation discarded usable stale data")
	}
}

// staleSignal adapts differently typed subscription streams without polling query state.
func staleSignal[V any](ctx context.Context, updates <-chan cacheq.Snapshot[V]) <-chan bool {
	result := make(chan bool, 1)
	go func() {
		for {
			select {
			case state, ok := <-updates:
				if !ok {
					result <- false
					return
				}
				if state.Stale {
					result <- true
					return
				}
			case <-ctx.Done():
				result <- false
				return
			}
		}
	}()
	return result
}

// TestRedisTagCapabilityAndClusterSlot rejects implicit layout changes and verifies atomic tag key placement.
func TestRedisTagCapabilityAndClusterSlot(t *testing.T) {
	remote := newTestRedis(t)
	plain, err := New(remote, Options{Prefix: "same", EnableNotifications: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.ReadTagged(
		context.Background(),
		"key",
		[]string{"tag"},
	); !errors.Is(
		err,
		cacheq.ErrSharedCapability,
	) {
		t.Fatal(err)
	}
	if _, err := cacheq.NewSharedCache[int](
		plain,
		cacheq.SharedCacheOptions[int]{Key: "key", Format: "int.v1", TTL: time.Minute, ShareTags: true},
	); !errors.Is(
		err,
		cacheq.ErrSharedCapability,
	) {
		t.Fatal(err)
	}
	tagged, err := New(remote, Options{Prefix: "same", EnableNotifications: true, EnableTags: true})
	if err != nil {
		t.Fatal(err)
	}
	keys := tagged.tagKeys("key")
	for _, key := range keys {
		if hashTag(key) != hashTag(keys[0]) {
			t.Fatalf("tag script crosses cluster slots: %v", keys)
		}
	}
	if tagged.keys("key")[0] == plain.keys("key")[0] {
		t.Fatal("tag mode reused the P0 key layout")
	}
}

// hashTag extracts the adapter's deliberately bounded Redis Cluster tag.
func hashTag(key string) string {
	start, end := -1, -1
	for i, ch := range key {
		if ch == '{' {
			start = i
		}
		if ch == '}' {
			end = i
			break
		}
	}
	if start < 0 || end < start {
		return ""
	}
	return key[start : end+1]
}

// TestRedisNotificationReconnect observes a resubscription barrier before checking further delivery.
func TestRedisNotificationReconnect(t *testing.T) {
	remote := newTestRedis(t)
	store, err := New(remote, Options{Prefix: "reconnect", EnableNotifications: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := store.SubscribeInvalidations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err := remote.ClientKillByFilter(ctx, "TYPE", "pubsub").Err(); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case event := <-stream.Events():
			if event.Reset && event.Err == nil {
				goto subscribed
			}
		case <-ctx.Done():
			t.Fatal("Redis subscription did not recover")
		}
	}
subscribed:
	receipt, err := store.InvalidateKey(ctx, "after-reconnect")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream.Events():
		if event.ID != receipt.ID {
			t.Fatalf("recovered event = %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("notification lost after resubscription")
	}
}

// TestRedisTagMetadataLoss prevents a removed relationship index from making old writes or data reusable.
func TestRedisTagMetadataLoss(t *testing.T) {
	remote := newTestRedis(t)
	store, err := New(remote, Options{Prefix: "metadata", EnableNotifications: true, EnableTags: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	old, err := store.ReadTagged(ctx, "key", []string{"tag"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Write(
		ctx,
		"key",
		cacheq.SharedWrite{Version: old.Version, Data: []byte("old"), TTL: time.Hour},
	); err != nil {
		t.Fatal(err)
	}
	if err := remote.Del(ctx, store.tagKeys("key")[2]).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Write(
		ctx,
		"key",
		cacheq.SharedWrite{Version: old.Version, Data: []byte("late"), TTL: time.Hour},
	); err != nil ||
		ok {
		t.Fatalf("missing membership = %v, %v", ok, err)
	}
	fresh, err := store.ReadTagged(ctx, "key", []string{"tag"})
	if err != nil || fresh.Version == old.Version || len(fresh.Data) != 0 {
		t.Fatalf("metadata reset = %+v, %v", fresh, err)
	}
	if err := remote.Del(ctx, store.tagGenerationKey()).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Write(
		ctx,
		"key",
		cacheq.SharedWrite{Version: fresh.Version, Data: []byte("lost-tag-version"), TTL: time.Hour},
	); err != nil ||
		ok {
		t.Fatalf("missing tag generations = %v, %v", ok, err)
	}
}

// TestRedisBinaryIdentityAndMalformedEvents preserves exact bytes and resets on unreadable notifications.
func TestRedisBinaryIdentityAndMalformedEvents(t *testing.T) {
	remote := newTestRedis(t)
	store, err := New(remote, Options{Prefix: "binary", EnableNotifications: true, EnableTags: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := store.SubscribeInvalidations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	key, tag := "key\xff", "label\xfe"
	record, err := store.ReadTagged(ctx, key, []string{tag})
	if err != nil || len(record.Tags) != 1 || record.Tags[0] != tag {
		t.Fatalf("binary membership = %+v, %v", record, err)
	}
	select {
	case event := <-stream.Events():
		if len(event.RegisteredTags) != 1 || event.RegisteredTags[0] != tag {
			t.Fatalf("binary registration = %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("membership registration missing")
	}
	if _, err := store.InvalidateKey(ctx, key); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream.Events():
		if event.Key != key {
			t.Fatalf("binary key = %q", event.Key)
		}
	case <-ctx.Done():
		t.Fatal("binary notification missing")
	}
	if _, err := store.InvalidateTags(ctx, []string{tag}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream.Events():
		if len(event.Tags) != 1 || event.Tags[0] != tag {
			t.Fatalf("binary tags = %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("binary tag notification missing")
	}
	if err := remote.Publish(ctx, store.channel(), "malformed").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream.Events():
		if !event.Reset || event.Err == nil {
			t.Fatalf("malformed payload = %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("malformed event was silently discarded")
	}
}

// TestRedisRemoteMembershipPropagation makes late labels visible to clients that originally declared another tag.
func TestRedisRemoteMembershipPropagation(t *testing.T) {
	remote := newTestRedis(t)
	owner, err := New(remote, Options{Prefix: "membership", EnableNotifications: true, EnableTags: true})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := New(remote, Options{Prefix: "membership", EnableNotifications: true, EnableTags: true})
	if err != nil {
		t.Fatal(err)
	}
	shared, err := cacheq.NewSharedCache[int](
		owner,
		cacheq.SharedCacheOptions[int]{Key: "shared-key", Format: "int.v1", TTL: time.Hour, ShareTags: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watch, err := client.WatchShared(ctx, owner, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watch.Close)
	d := cacheq.QueryDefinition[int]{
		Key:     "local",
		Tags:    []string{"original"},
		Shared:  shared,
		Fetcher: func(context.Context) (int, error) { return 7, nil },
		Options: cacheq.QueryOptions{Disable: true},
	}
	if _, err := d.Fetch(ctx, client); err != nil {
		t.Fatal(err)
	}
	if cacheq.Get[int](client, d.Key).Stale {
		t.Fatal("membership registration invalidated its own fresh load")
	}
	h := d.Query(client)
	t.Cleanup(h.Close)
	if _, err := peer.ReadTagged(ctx, "shared-key", []string{"added-by-peer"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.InvalidateTags(ctx, []string{"added-by-peer"}); err != nil {
		t.Fatal(err)
	}
	select {
	case stale := <-staleSignal(ctx, h.Updates()):
		if !stale {
			t.Fatal("peer tag did not invalidate local data")
		}
	case <-ctx.Done():
		t.Fatal("remote membership was not propagated")
	}
}

// TestRedisDefaultInvalidationNeedsNoPubSubPermissions preserves the original ACL contract until notifications are enabled.
func TestRedisDefaultInvalidationNeedsNoPubSubPermissions(t *testing.T) {
	admin := newTestRedis(t)
	ctx := context.Background()
	if err := admin.Do(ctx, "ACL", "SETUSER", "cacheq-p0", "on", ">test-only", "~*", "+@read", "+@write", "+eval", "+hello", "+auth", "-publish", "-subscribe").
		Err(); err != nil {
		t.Fatal(err)
	}
	connection := redis.NewClient(
		&redis.Options{
			Network:               "unix",
			Addr:                  admin.Options().Addr,
			Username:              "cacheq-p0",
			Password:              "test-only",
			Protocol:              2,
			DisableIdentity:       true,
			ContextTimeoutEnabled: true,
		},
	)
	defer connection.Close()
	store, err := New(connection, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	if err := store.Invalidate(ctx, "key"); err != nil {
		t.Fatalf("default invalidation requires Pub/Sub permissions: %v", err)
	}
	if _, err := store.SubscribeInvalidations(ctx); !errors.Is(err, cacheq.ErrSharedCapability) {
		t.Fatalf("notifications were not opt-in: %v", err)
	}
}
