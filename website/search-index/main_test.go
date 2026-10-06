package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBuiltRoutesAndUnicode verifies nested locale URLs, real anchors, highlighted code, and sidebar exclusion.
func TestBuiltRoutesAndUnicode(t *testing.T) {
	directory := t.TempDir()
	page := `<html lang="zh-CN"><head><title data-rh="true">缓存 | cacheq</title></head><body><nav>not searchable</nav><article><h1>缓存</h1><p>共享数据</p><h2 id="set">Set<span>` + "\u200b" + `</span></h2><pre><code><span>cacheq</span><span>.</span><span>Set</span>(client, &quot;用户&quot;, 1)</code></pre><h2 id="expiry">过期时间</h2><p>GCTime 自动清理</p><script>private script</script></article></body></html>`
	target := filepath.Join(directory, "cache", "index.html")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(page), 0o600); err != nil {
		t.Fatal(err)
	}
	records, err := buildIndex(directory, "/cacheq/")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("records = %d", len(records))
	}
	var found bool
	for _, record := range records {
		if !strings.HasPrefix(record.URL, "/cacheq/cache/") || record.Language != "zh-CN" {
			t.Fatalf("wrong route or language: %+v", record)
		}
		if strings.Contains(record.Content, "not searchable") || strings.Contains(record.Content, "private script") {
			t.Fatalf("indexed non-document text: %s", record.Content)
		}
		if record.URL == "/cacheq/cache/#set" {
			found = true
			if !strings.Contains(record.Content, `cacheq.Set(client, "用户", 1)`) {
				t.Fatalf("lost highlighted code or Unicode: %s", record.Content)
			}
			if strings.Contains(record.Content, "GCTime") {
				t.Fatal("section swallowed the next heading")
			}
		}
	}
	if !found {
		t.Fatal("missing real heading anchor")
	}
}

// TestAPISourceIsolation ensures exported declarations remain discoverable without indexing internal or test code.
func TestAPISourceIsolation(t *testing.T) {
	directory := t.TempDir()
	source := `package cacheq
// Client owns shared queries.
type Client struct{}
// Query loads typed data.
func Query[V any]() V {var value V;return value}
// Close releases shared work.
func (c *Client) Close(){}
func internal(){}
// ErrClosed signals shutdown.
var ErrClosed = "closed"
`
	if err := os.WriteFile(filepath.Join(directory, "client.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(directory, "client_test.go"),
		[]byte("invalid Go deliberately excluded"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	records, err := apiEntries(directory, "main")
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool)
	for _, record := range records {
		names[record.Title] = true
		if !strings.Contains(record.URL, "/blob/main/client.go#L") {
			t.Fatalf("wrong source link: %s", record.URL)
		}
	}
	for _, name := range []string{"Client", "Query", "Client.Close", "ErrClosed"} {
		if !names[name] {
			t.Fatalf("missing exported API %s", name)
		}
	}
	if names["internal"] {
		t.Fatal("private implementation was indexed")
	}
	if len(records) != 8 {
		t.Fatalf("expected both locales for four symbols, got %d", len(records))
	}
}

// TestIndexFailures prevents empty or misconfigured indexes from being published successfully.
func TestIndexFailures(t *testing.T) {
	if _, err := buildIndex(t.TempDir(), "/cacheq"); err == nil {
		t.Fatal("empty build accepted")
	}
	if _, err := buildIndex(t.TempDir(), "/../escape"); err == nil {
		t.Fatal("invalid base accepted")
	}
	if _, err := apiEntries(t.TempDir(), "main"); err == nil {
		t.Fatal("missing source accepted")
	}
}
