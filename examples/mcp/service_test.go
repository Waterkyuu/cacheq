package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// fixture owns a real HTTP MCP endpoint and a deadline used only to detect deadlocks.
type fixture struct {
	// ctx bounds communication if a protocol operation unexpectedly stops progressing.
	ctx context.Context
	// app exposes shared cache behavior while retaining the real SDK transport.
	app *service
	// endpoint is the loopback URL for authenticated MCP clients.
	endpoint string
}

// newFixture creates an isolated backend, server, and cache and closes them after clients exit.
func newFixture(t *testing.T, options cacheq.Options) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	app := newService(newDocumentStore(), options)
	t.Cleanup(app.cache.Close)
	server := httptest.NewServer(app)
	t.Cleanup(server.Close)
	return &fixture{ctx: ctx, app: app, endpoint: server.URL}
}

// reader connects a new authorization-scoped session with an initially empty SDK cache.
func (f *fixture) reader(t *testing.T, credential string) *demoClient {
	t.Helper()
	reader, err := connect(f.ctx, f.endpoint, credential)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	if version := reader.session.InitializeResult().ProtocolVersion; version != protocolVersion {
		t.Fatalf("expected modern caching and subscription semantics, got %s", version)
	}
	return reader
}

// read verifies that a complete private resource is actually delivered over the SDK API.
func (f *fixture) read(t *testing.T, reader *demoClient) *mcp.ReadResourceResult {
	t.Helper()
	result, err := reader.read(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contents) != 1 || result.CacheScope != "private" {
		t.Fatalf("invalid private resource response: %+v", result)
	}
	if result.Contents[0].URI != resourceURI || result.TTLMs < 0 {
		t.Fatalf("invalid resource identity or TTL: %+v", result)
	}
	return result
}

// update invokes the actual mutation tool and checks its application result.
func (f *fixture) update(t *testing.T, reader *demoClient, text string) {
	t.Helper()
	result, err := reader.session.CallTool(f.ctx, &mcp.CallToolParams{
		Name: "update_settings", Arguments: map[string]any{"text": text},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("mutation failed: %+v", result)
	}
}

// receive waits for a protocol or loader event without sleep-based ordering.
func receive[V any](t *testing.T, ctx context.Context, events <-chan V) V {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatal("timed out waiting for MCP activity")
		var zero V
		return zero
	}
}

// awaitStats observes request admission or completion using the public client snapshot.
func (f *fixture) awaitStats(t *testing.T, matches func(cacheq.Stats) bool) cacheq.Stats {
	t.Helper()
	for {
		state := f.app.cache.Stats()
		if matches(state) {
			return state
		}
		select {
		case <-f.ctx.Done():
			t.Fatalf("timed out waiting for statistics: %+v", state)
		default:
			runtime.Gosched()
		}
	}
}

// TestMCPReuseAndPrivateScopes distinguishes cross-session cacheq reuse from the SDK's local TTL cache.
func TestMCPReuseAndPrivateScopes(t *testing.T) {
	f := newFixture(t, cacheq.Options{})
	alice := f.reader(t, "alice")
	aliceAgain := f.reader(t, "alice")
	bob := f.reader(t, "bob")
	for _, reader := range []*demoClient{alice, aliceAgain} {
		if result := f.read(t, reader); result.Contents[0].Text != "theme=light" {
			t.Fatalf("alice received another scope's document: %+v", result)
		}
	}
	if result := f.read(t, bob); result.Contents[0].Text != "theme=blue" {
		t.Fatalf("bob received another scope's document: %+v", result)
	}
	state := f.app.cache.Stats()
	decisions := state.CacheHits == 1 && state.CacheMisses == 2
	if !decisions || state.Loads != 2 || f.app.store.readCount() != 2 {
		t.Fatalf("three MCP connections did not reuse scoped data: %+v", state)
	}
	f.read(t, alice)
	if f.app.cache.Stats() != state {
		t.Fatal("SDK-local reuse was counted as another server-side lookup")
	}
	if _, err := alice.session.ReadResource(f.ctx, &mcp.ReadResourceParams{URI: "config://app/unknown"}); err == nil {
		t.Fatal("an unknown URI reused another resource's cached content")
	}
	if _, err := connect(f.ctx, f.endpoint, "unknown"); err == nil {
		t.Fatal("unrecognized credential could access a warm private cache")
	}
	request, err := http.NewRequestWithContext(
		f.ctx,
		http.MethodGet,
		f.endpoint,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || f.app.cache.Stats() != state {
		t.Fatal("authentication did not run before cache lookup")
	}
}

// TestMCPNotificationInvalidation proves a mutation invalidates both server and subscribed SDK caches.
func TestMCPNotificationInvalidation(t *testing.T) {
	f := newFixture(t, cacheq.Options{})
	alice := f.reader(t, "alice")
	aliceAgain := f.reader(t, "alice")
	bob := f.reader(t, "bob")
	for _, reader := range []*demoClient{alice, aliceAgain, bob} {
		f.read(t, reader)
		if err := reader.subscribe(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	f.update(t, alice, "theme=dark")
	for _, reader := range []*demoClient{alice, aliceAgain} {
		if uri := receive(t, f.ctx, reader.updated); uri != resourceURI {
			t.Fatalf("unexpected update URI: %s", uri)
		}
		if result := f.read(t, reader); result.Contents[0].Text != "theme=dark" {
			t.Fatalf("notification did not invalidate old content: %+v", result)
		}
	}
	state := f.app.cache.Stats()
	if state.Loads != 3 || f.app.store.readCount() != 3 {
		t.Fatalf("subscribed clients duplicated the post-mutation backend read: %+v", state)
	}
	if result := f.read(t, bob); result.Contents[0].Text != "theme=blue" {
		t.Fatalf("alice's mutation changed bob's document: %+v", result)
	}
	if f.app.cache.Stats() != state {
		t.Fatal("alice's change invalidated bob's SDK cache")
	}
	select {
	case <-bob.updated:
		t.Fatal("private change notifications crossed authorization scopes")
	default:
	}
	result, err := alice.session.CallTool(f.ctx, &mcp.CallToolParams{
		Name: "update_settings", Arguments: map[string]any{"text": " "},
	})
	if err != nil || !result.IsError {
		t.Fatalf("invalid mutation was not rejected: %+v, %v", result, err)
	}
	if f.app.cache.Stats() != state {
		t.Fatal("rejected mutation invalidated cached data")
	}
}

// TestMCPConcurrentReads merges real HTTP resource requests from separate clients into one backend operation.
func TestMCPConcurrentReads(t *testing.T) {
	f := newFixture(t, cacheq.Options{})
	started, release := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	f.app.load = func(ctx context.Context, principal string) (string, error) {
		if attempts.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return f.app.store.read(ctx, principal)
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	alice, another := f.reader(t, "alice"), f.reader(t, "alice")
	finished := make(chan error, 2)
	go func() { _, err := alice.read(f.ctx); finished <- err }()
	receive(t, f.ctx, started)
	go func() { _, err := another.read(f.ctx); finished <- err }()
	f.awaitStats(t, func(s cacheq.Stats) bool { return s.MergedRequests == 1 })
	close(release)
	for range 2 {
		if err := receive(t, f.ctx, finished); err != nil {
			t.Fatal(err)
		}
	}
	state := f.app.cache.Stats()
	if state.Loads != 1 || attempts.Load() != 1 || f.app.store.readCount() != 1 {
		t.Fatalf("concurrent HTTP reads duplicated backend work: %+v", state)
	}
}

// TestMCPRemainingTTL preserves the installation deadline and honors MaxAge at its exact boundary.
func TestMCPRemainingTTL(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Unix(1000, 0).UnixNano())
	f := newFixture(t, cacheq.Options{
		MaxAge: 20 * time.Second,
		Clock:  func() time.Time { return time.Unix(0, now.Load()) },
	})
	for _, expected := range []int{20000, 10000, 20000} {
		reader := f.reader(t, "alice")
		if result := f.read(t, reader); result.TTLMs != expected {
			t.Fatalf("remaining TTL = %d, want %d", result.TTLMs, expected)
		}
		now.Add(int64(10 * time.Second))
	}
	state := f.app.cache.Stats()
	if state.CacheHits != 1 || state.AgeExpirations != 1 || f.app.store.readCount() != 2 {
		t.Fatalf("expiry was extended or its boundary was missed: %+v", state)
	}
}

// TestMCPImmediateStaleness prevents non-positive freshness from producing a cacheable wire TTL.
func TestMCPImmediateStaleness(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Minute} {
		t.Run(ttl.String(), func(t *testing.T) {
			f := newFixture(t, cacheq.Options{})
			f.app.ttl = ttl
			reader := f.reader(t, "alice")
			for range 2 {
				if result := f.read(t, reader); result.TTLMs != 0 {
					t.Fatalf("non-positive freshness advertised TTL %d", result.TTLMs)
				}
			}
			if count := f.app.store.readCount(); count != 2 {
				t.Fatalf("stale resource was reused %d times", count)
			}
		})
	}
}

// TestMCPReadFailure retries on the next request without caching an error as a successful resource.
func TestMCPReadFailure(t *testing.T) {
	f := newFixture(t, cacheq.Options{})
	var attempts atomic.Int32
	f.app.load = func(ctx context.Context, principal string) (string, error) {
		if attempts.Add(1) == 1 {
			return "", errors.New("backend unavailable")
		}
		return f.app.store.read(ctx, principal)
	}
	reader := f.reader(t, "alice")
	if _, err := reader.read(f.ctx); err == nil {
		t.Fatal("failed backend read returned a successful MCP resource")
	}
	if result := f.read(t, reader); result.Contents[0].Text != "theme=light" {
		t.Fatalf("subsequent read did not recover: %+v", result)
	}
	state := f.app.cache.Stats()
	if state.LoadFailures != 1 || state.LoadSuccesses != 1 || attempts.Load() != 2 {
		t.Fatalf("failed MCP read was silently reused: %+v", state)
	}
}

// TestMCPCancellation propagates a real client's cancellation into backend work and permits later recovery.
func TestMCPCancellation(t *testing.T) {
	f := newFixture(t, cacheq.Options{})
	started := make(chan struct{})
	var attempts atomic.Int32
	f.app.load = func(ctx context.Context, principal string) (string, error) {
		if attempts.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		}
		return f.app.store.read(ctx, principal)
	}
	reader := f.reader(t, "alice")
	ctx, cancel := context.WithCancel(f.ctx)
	t.Cleanup(cancel)
	finished := make(chan error, 1)
	go func() { _, err := reader.read(ctx); finished <- err }()
	receive(t, f.ctx, started)
	cancel()
	if err := receive(t, f.ctx, finished); !errors.Is(err, context.Canceled) {
		t.Fatalf("MCP cancellation = %v", err)
	}
	f.awaitStats(t, func(s cacheq.Stats) bool { return s.LoadCancellations == 1 })
	f.read(t, reader)
	if state := f.app.cache.Stats(); state.LoadSuccesses != 1 || state.Loads != 2 {
		t.Fatalf("canceled read prevented recovery: %+v", state)
	}
}

// TestMCPMutationDuringRead never advertises a fresh TTL for a response captured before a mutation.
func TestMCPMutationDuringRead(t *testing.T) {
	f := newFixture(t, cacheq.Options{})
	started, release := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	f.app.load = func(ctx context.Context, principal string) (string, error) {
		text, err := f.app.store.read(ctx, principal)
		if attempts.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return text, err
	}
	reader, writer := f.reader(t, "alice"), f.reader(t, "alice")
	result := make(chan *mcp.ReadResourceResult, 1)
	finished := make(chan error, 1)
	go func() {
		resource, err := reader.read(f.ctx)
		result <- resource
		finished <- err
	}()
	receive(t, f.ctx, started)
	f.update(t, writer, "theme=dark")
	close(release)
	if err := receive(t, f.ctx, finished); err != nil {
		t.Fatal(err)
	}
	if old := receive(t, f.ctx, result); old.TTLMs != 0 {
		t.Fatalf("pre-mutation content was advertised as fresh: %+v", old)
	}
	if fresh := f.read(t, reader); fresh.Contents[0].Text != "theme=dark" {
		t.Fatalf("late response prevented a fresh read: %+v", fresh)
	}
}
