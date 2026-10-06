package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// resourceURI identifies the same logical private resource for both demo users.
	resourceURI = "config://app/settings"
	// protocolVersion selects the cache-hint and subscription-stream contract demonstrated here.
	protocolVersion = "2026-07-28"
)

// resourceKey keeps private cached results within one credential context.
type resourceKey struct {
	// scope identifies a credential without retaining it as a cache key.
	scope [32]byte
	// uri identifies the resource within the authenticated scope.
	uri string
}

// resourceValue retains the original deadline so cache hits never renew the advertised TTL.
type resourceValue struct {
	// text contains immutable resource content returned by the backend.
	text string
	// expiresAt is the original freshness deadline of this installed value.
	expiresAt time.Time
}

// updateInput describes the new settings supplied to the demo mutation tool.
type updateInput struct {
	// Text replaces the authenticated user's settings.
	Text string `json:"text" jsonschema:"new settings text"`
}

// updateOutput confirms the document written by the mutation tool.
type updateOutput struct {
	// Text contains the accepted settings text.
	Text string `json:"text"`
}

// service owns a shared backend cache and separates MCP notification streams by credential.
type service struct {
	// cache deduplicates backend reads across authenticated MCP connections.
	cache *cacheq.Client
	// store owns the authoritative documents modified by the tool.
	store *documentStore
	// load is the backend read dependency, replaceable before serving requests in tests.
	load func(context.Context, string) (string, error)
	// now supplies the same freshness clock as the cache.
	now func() time.Time
	// ttl bounds freshness from the moment a backend value is installed.
	ttl time.Duration
	// servers separates subscriptions and resource results for each accepted demo credential.
	servers map[string]*mcp.Server
	// handler authenticates every HTTP request before allowing any cache lookup.
	handler http.Handler
}

// newService builds an authenticated local MCP endpoint with one shared cache.
func newService(store *documentStore, options cacheq.Options) *service {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	s := &service{
		cache: cacheq.NewClient(options), store: store, load: store.read,
		now: options.Clock, ttl: time.Minute, servers: make(map[string]*mcp.Server),
	}
	// These literal credentials exist only for this self-contained example.
	for _, principal := range []string{"alice", "bob"} {
		s.servers[principal] = s.scopedServer(principal)
	}
	transport := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return s.servers[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
	}, &mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true})
	verify := func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		if s.servers[token] == nil {
			return nil, auth.ErrInvalidToken
		}
		scope := sha256.Sum256([]byte(token))
		return &auth.TokenInfo{UserID: fmt.Sprintf("%x", scope)}, nil
	}
	s.handler = auth.RequireBearerToken(
		verify,
		&auth.RequireBearerTokenOptions{AllowMissingExpiration: true},
	)(
		transport,
	)
	return s
}

// ServeHTTP delegates only authenticated requests to the SDK's Streamable HTTP transport.
func (s *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// scopedServer exposes one principal's resource and sends changes only within that scope.
func (s *service) scopedServer(principal string) *mcp.Server {
	key := resourceKey{scope: sha256.Sum256([]byte(principal)), uri: resourceURI}
	server := mcp.NewServer(&mcp.Implementation{Name: "cacheq-resources", Version: "1.0.0"}, &mcp.ServerOptions{
		SupportedProtocolVersions: []string{protocolVersion},
		SubscribeHandler: func(_ context.Context, req *mcp.SubscribeRequest) error {
			if req.Params.URI != resourceURI {
				return fmt.Errorf("unknown resource: %s", req.Params.URI)
			}
			return nil
		},
		UnsubscribeHandler: func(context.Context, *mcp.UnsubscribeRequest) error { return nil },
		SetCacheable:       func(_ context.Context, _ mcp.Request, hints *mcp.Cacheable) { hints.CacheScope = "private" },
	})
	server.AddResource(&mcp.Resource{URI: resourceURI, Name: "settings", MIMEType: "text/plain"},
		func(ctx context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			value, err := cacheq.FetchWithExpiry(
				ctx,
				s.cache,
				key,
				func(ctx context.Context) (resourceValue, time.Time, error) {
					text, err := s.load(ctx, principal)
					if err != nil {
						return resourceValue{}, time.Time{}, fmt.Errorf("load settings: %w", err)
					}
					expiresAt := s.now().Add(s.ttl)
					return resourceValue{text: text, expiresAt: expiresAt}, expiresAt, nil
				},
			)
			if err != nil {
				return nil, err
			}
			// Use the effective cache deadline, including MaxAge, instead of issuing a new TTL on a hit.
			deadline := cacheq.Get[resourceValue](s.cache, key).ExpiresAt
			if value.expiresAt.Before(deadline) {
				deadline = value.expiresAt
			}
			remaining := 0
			if duration := deadline.Sub(s.now()); duration > 0 {
				remaining = int(duration / time.Millisecond)
			}
			return &mcp.ReadResourceResult{
				Cacheable: mcp.Cacheable{TTLMs: remaining, CacheScope: "private"},
				Contents:  []*mcp.ResourceContents{{URI: resourceURI, MIMEType: "text/plain", Text: value.text}},
			}, nil
		})
	mcp.AddTool(
		server,
		&mcp.Tool{Name: "update_settings", Description: "replace your settings"},
		func(ctx context.Context, _ *mcp.CallToolRequest, input updateInput) (*mcp.CallToolResult, updateOutput, error) {
			if strings.TrimSpace(input.Text) == "" {
				return nil, updateOutput{}, fmt.Errorf("settings text must not be empty")
			}
			s.store.write(principal, input.Text)
			if err := s.cache.Invalidate(key, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
				return nil, updateOutput{}, fmt.Errorf("invalidate updated settings: %w", err)
			}
			if err := server.ResourceUpdated(
				ctx,
				&mcp.ResourceUpdatedNotificationParams{URI: resourceURI},
			); err != nil {
				return nil, updateOutput{}, fmt.Errorf("notify updated settings: %w", err)
			}
			return nil, updateOutput(input), nil
		},
	)
	return server
}
