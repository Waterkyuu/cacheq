package main

import (
	"context"
	"sync"
)

// documentStore represents a backend whose per-user documents outlive the cache.
type documentStore struct {
	// mu protects backend data and the number of actual reads.
	mu sync.Mutex
	// documents contains private settings indexed by the authenticated principal.
	documents map[string]string
	// reads counts successful backend accesses independently of MCP and cache statistics.
	reads int
}

// newDocumentStore supplies two distinct documents for the local demonstration.
func newDocumentStore() *documentStore {
	return &documentStore{documents: map[string]string{"alice": "theme=light", "bob": "theme=blue"}}
}

// read returns one principal's settings and counts actual backend work.
func (s *documentStore) read(ctx context.Context, principal string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	return s.documents[principal], nil
}

// write replaces a document after its caller has been authenticated.
func (s *documentStore) write(principal, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.documents[principal] = text
}

// readCount reports backend work without racing concurrent resource reads.
func (s *documentStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}
