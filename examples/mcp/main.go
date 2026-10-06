package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// main runs a finite MCP server/client demonstration with no external services or credentials.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run demonstrates cross-connection reuse, private scopes, and notification-driven re-reading.
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := newDocumentStore()
	app := newService(store, cacheq.Options{MaxEntries: 16, GCTime: time.Minute})
	defer app.cache.Close()
	server := httptest.NewServer(app)
	defer server.Close()
	readers := make([]*demoClient, 0, 3)
	defer func() {
		for _, reader := range readers {
			if err := reader.Close(); err != nil {
				fmt.Fprintln(os.Stderr, "close MCP client:", err)
			}
		}
	}()
	for _, credential := range []string{"alice", "alice", "bob"} {
		reader, err := connect(ctx, server.URL, credential)
		if err != nil {
			return err
		}
		readers = append(readers, reader)
		resource, err := reader.read(ctx)
		if err != nil {
			return fmt.Errorf("read settings: %w", err)
		}
		fmt.Printf("%s: %s\n", credential, resource.Contents[0].Text)
	}
	fmt.Printf("backend reads after three connections: %d\n", store.readCount())
	alice := readers[0]
	if err := alice.subscribe(ctx); err != nil {
		return err
	}
	result, err := alice.session.CallTool(ctx, &mcp.CallToolParams{
		Name: "update_settings", Arguments: map[string]any{"text": "theme=dark"},
	})
	if err != nil {
		return fmt.Errorf("update settings: %w", err)
	}
	if result.IsError {
		return fmt.Errorf("update settings tool failed: %v", result.Content)
	}
	select {
	case uri := <-alice.updated:
		fmt.Println("resource changed:", uri)
	case <-ctx.Done():
		return fmt.Errorf("wait for resource change: %w", ctx.Err())
	}
	resource, err := alice.read(ctx)
	if err != nil {
		return fmt.Errorf("re-read settings: %w", err)
	}
	fmt.Println("alice after update:", resource.Contents[0].Text)
	state := app.cache.Stats()
	fmt.Printf(
		"backend reads=%d cache hits=%d cache misses=%d loads=%d\n",
		store.readCount(),
		state.CacheHits,
		state.CacheMisses,
		state.Loads,
	)
	return nil
}
