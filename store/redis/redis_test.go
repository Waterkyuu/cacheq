package redistore

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Waterkyuu/cacheq"
	"github.com/redis/go-redis/v9"
)

// newTestRedis starts a disposable local server with persistence and TCP disabled.
// CI requires the binary; developer machines without Redis explicitly skip integration tests.
func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		if os.Getenv("CACHEQ_REQUIRE_REDIS") == "1" {
			t.Fatal("redis-server is required")
		}
		t.Skip("redis-server is required for adapter integration tests")
	}
	// Unix socket paths have a small platform limit; avoid the long test-name paths from t.TempDir.
	directory, err := os.MkdirTemp("", "cq-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "redis.sock")
	cmd := exec.Command(binary, "--port", "0", "--unixsocket", socket,
		"--unixsocketperm", "700", "--save", "", "--appendonly", "no", "--loglevel", "warning")
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	client := redis.NewClient(&redis.Options{
		Network: "unix", Addr: socket, ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if err := client.Ping(ctx).Err(); err == nil {
			return client
		}
		if ctx.Err() != nil {
			t.Fatal("local Redis did not become ready")
		}
		// Only process readiness uses a bounded poll; correctness orderings use channels or scripts.
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRedisVersionContract tests atomic misses, conditional writes, expiration, and unseen-key invalidation.
func TestRedisVersionContract(t *testing.T) {
	client := newTestRedis(t)
	store, err := New(client, Options{Prefix: "contract"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := store.Read(ctx, "tenant:42:user:7")
	if err != nil || first.Version == "" || len(first.Data) != 0 {
		t.Fatalf("miss = %+v, %v", first, err)
	}
	written, err := store.Write(ctx, "tenant:42:user:7", cacheq.SharedWrite{
		Version: first.Version, Data: []byte("old"), TTL: time.Minute,
	})
	if err != nil || !written {
		t.Fatalf("write = %v, %v", written, err)
	}
	hit, err := store.Read(ctx, "tenant:42:user:7")
	if err != nil || string(hit.Data) != "old" || hit.Version != first.Version {
		t.Fatalf("hit = %+v, %v", hit, err)
	}
	if err := store.Invalidate(ctx, "tenant:42:user:7"); err != nil {
		t.Fatal(err)
	}
	written, err = store.Write(ctx, "tenant:42:user:7", cacheq.SharedWrite{
		Version: first.Version, Data: []byte("resurrected"), TTL: time.Minute,
	})
	if err != nil || written {
		t.Fatalf("obsolete write = %v, %v", written, err)
	}
	miss, err := store.Read(ctx, "tenant:42:user:7")
	if err != nil || len(miss.Data) != 0 || miss.Version == first.Version {
		t.Fatalf("invalidated = %+v, %v", miss, err)
	}
	if err := store.Invalidate(ctx, "unseen"); err != nil {
		t.Fatal(err)
	}
	if unseen, err := store.Read(ctx, "unseen"); err != nil || unseen.Version == "" {
		t.Fatalf("unseen = %+v, %v", unseen, err)
	}
	keys := store.keys("tenant:42:user:7")
	if ttl := client.PTTL(ctx, keys[0]).Val(); ttl != -1 {
		t.Fatalf("version lifetime = %v", ttl)
	}
	if ok, err := store.Write(ctx, "tenant:42:user:7", cacheq.SharedWrite{
		Version: miss.Version, Data: []byte("new"), TTL: time.Minute,
	}); err != nil || !ok {
		t.Fatalf("new write = %v, %v", ok, err)
	}
	if ttl := client.PTTL(ctx, keys[1]).Val(); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("record TTL = %v", ttl)
	}
	// Force expiry directly so the boundary does not depend on test scheduling.
	if err := client.PExpire(ctx, keys[1], 0).Err(); err != nil {
		t.Fatal(err)
	}
	if expired, err := store.Read(
		ctx,
		"tenant:42:user:7",
	); err != nil || len(expired.Data) != 0 ||
		expired.Version != miss.Version {
		t.Fatalf("expired = %+v, %v", expired, err)
	}
}

// TestRedisMetadataLoss refuses old versions and discards orphaned payloads when metadata is recreated.
func TestRedisMetadataLoss(t *testing.T) {
	client := newTestRedis(t)
	store, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	old, err := store.Read(ctx, "key")
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
	if err := client.Del(ctx, store.keys("key")[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Write(
		ctx,
		"key",
		cacheq.SharedWrite{Version: old.Version, Data: []byte("late"), TTL: time.Hour},
	); err != nil ||
		ok {
		t.Fatalf("missing version write = %v, %v", ok, err)
	}
	fresh, err := store.Read(ctx, "key")
	if err != nil || fresh.Version == old.Version || len(fresh.Data) != 0 {
		t.Fatalf("recreated = %+v, %v", fresh, err)
	}
}

// TestRedisQueryIntegration verifies actual serialized result reuse and fenced source completion.
func TestRedisQueryIntegration(t *testing.T) {
	client := newTestRedis(t)
	store, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	shared, err := cacheq.NewSharedCache[int](
		store,
		cacheq.SharedCacheOptions[int]{Key: "answer", Format: "int.v1", TTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	d := cacheq.QueryDefinition("answer", func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	})
	d.Shared = shared
	c1 := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	c2 := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	t.Cleanup(c1.Close)
	t.Cleanup(c2.Close)
	done := make(chan error, 1)
	go func() { _, err := d.Fetch(context.Background(), c1); done <- err }()
	<-started
	if err := d.InvalidateShared(context.Background(), c1); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if shared.Stats().WriteConflicts != 1 {
		t.Fatalf("fence = %+v", shared.Stats())
	}
	d.Fetcher = func(context.Context) (int, error) { return 2, nil }
	if value, err := d.Fetch(context.Background(), c2); err != nil || value != 2 {
		t.Fatalf("replacement = %d, %v", value, err)
	}
	c3 := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	t.Cleanup(c3.Close)
	d.Fetcher = func(context.Context) (int, error) { t.Fatal("shared hit reached source"); return 0, nil }
	if value, err := d.Fetch(context.Background(), c3); err != nil || value != 2 {
		t.Fatalf("shared hit = %d, %v", value, err)
	}
}

// TestRedisValidationAndCancellation rejects malformed input without issuing commands.
func TestRedisValidationAndCancellation(t *testing.T) {
	if _, err := New(nil, Options{}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatal(err)
	}
	client := newTestRedis(t)
	if _, err := New(client, Options{Prefix: "{unsafe}"}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatal(err)
	}
	store, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(context.Background(), ""); !errors.Is(err, ErrInvalidOptions) {
		t.Fatal(err)
	}
	if _, err := store.Write(
		context.Background(),
		"key",
		cacheq.SharedWrite{Version: "v", Data: []byte("x")},
	); !errors.Is(
		err,
		ErrInvalidOptions,
	) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Read(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := store.Invalidate(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestRedisClusterKeys keeps a key's generation and payload in one hash slot and separates namespaces.
func TestRedisClusterKeys(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("127.0.0.1", "1")})
	defer client.Close()
	a, err := New(client, Options{Prefix: "a"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(client, Options{Prefix: "b"})
	if err != nil {
		t.Fatal(err)
	}
	keys := a.keys("tenant:{42}:user:7")
	if keys[0][:len(keys[0])-len(":version")] != keys[1][:len(keys[1])-len(":data")] {
		t.Fatal("hash tags differ")
	}
	if keys[0] == b.keys("tenant:{42}:user:7")[0] {
		t.Fatal("prefixes collide")
	}
}
