package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// bearerTransport supplies one immutable authorization context for every request of a session.
type bearerTransport struct {
	// token is a demonstration credential and must never change while this transport is in use.
	token string
	// base performs the actual HTTP exchange after the request has been cloned.
	base http.RoundTripper
}

// RoundTrip adds authentication without mutating the SDK-owned request or its shared headers.
func (t *bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(clone)
}

// demoClient owns an MCP session and bounded signals for one resource subscription.
type demoClient struct {
	// session uses the SDK's native client-side TTL cache.
	session *mcp.ClientSession
	// updated reports coalesced resource notifications after the SDK invalidates its cache.
	updated chan string
	// acknowledged signals that the server has installed a resource subscription.
	acknowledged chan struct{}
}

// connect creates a separate session for one endpoint and authorization context.
func connect(ctx context.Context, endpoint, credential string) (*demoClient, error) {
	c := &demoClient{
		updated: make(chan string, 1), acknowledged: make(chan struct{}, 1),
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "cacheq-reader", Version: "1.0.0"}, &mcp.ClientOptions{
		ResourceUpdatedHandler: func(_ context.Context, req *mcp.ResourceUpdatedNotificationRequest) {
			select {
			case c.updated <- req.Params.URI:
			default:
				// This example watches one URI; repeated notifications coalesce into one pending re-read.
			}
		},
	})
	client.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
			result, err := next(ctx, method, request)
			if method == "notifications/subscriptions/acknowledged" && err == nil {
				select {
				case c.acknowledged <- struct{}{}:
				default:
				}
			}
			return result, err
		}
	})
	httpClient := &http.Client{Transport: &bearerTransport{token: credential, base: http.DefaultTransport}}
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: httpClient}, nil)
	if err != nil {
		return nil, fmt.Errorf("connect MCP client: %w", err)
	}
	c.session = session
	return c, nil
}

// subscribe waits for the protocol acknowledgment while the SDK keeps the listen request open.
// The demo invokes this once per client; Close terminates the long-lived request.
func (c *demoClient) subscribe(ctx context.Context) error {
	if err := c.session.Subscribe(ctx, &mcp.SubscribeParams{URI: resourceURI}); err != nil {
		return fmt.Errorf("subscribe to settings: %w", err)
	}
	select {
	case <-c.acknowledged:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// read retrieves the demo resource through a real MCP resources/read call or the SDK's TTL cache.
func (c *demoClient) read(ctx context.Context) (*mcp.ReadResourceResult, error) {
	return c.session.ReadResource(ctx, &mcp.ReadResourceParams{URI: resourceURI})
}

// Close releases the transport and cancels all SDK-owned subscription streams.
func (c *demoClient) Close() error {
	return c.session.Close()
}
