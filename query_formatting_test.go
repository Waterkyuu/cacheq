package cacheq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// queryStringKey lets a comparable key reenter its Client through Stringer diagnostics.
type queryStringKey struct {
	// format points to the application callback without making the key non-comparable.
	format *func()
}

// String runs application diagnostics before returning a stable key label.
func (key queryStringKey) String() string {
	(*key.format)()
	return "diagnostic-key"
}

// queryFormatKey exercises fmt.Formatter with the same comparable callback storage.
type queryFormatKey queryStringKey

// Format runs application diagnostics when fmt attempts to render this key's value.
func (key queryFormatKey) Format(fmt.State, rune) {
	(*key.format)()
}

// TestQueryTypeMismatchFormatting keeps application formatters outside locked validation paths.
func TestQueryTypeMismatchFormatting(t *testing.T) {
	for _, formatter := range []string{"stringer", "formatter"} {
		for _, operation := range []string{"get", "set", "update", "fetch", "query"} {
			t.Run(formatter+"/"+operation, func(t *testing.T) {
				c := NewClient(Options{})
				formatted := make(chan struct{}, 1)
				format := func() {
					formatted <- struct{}{}
					_ = c.Stats()
				}
				var key any = queryStringKey{format: &format}
				if formatter == "formatter" {
					key = queryFormatKey{format: &format}
				}
				if err := Set(c, key, 42); err != nil {
					c.Close()
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() {
					switch operation {
					case "get":
						done <- Get[string](c, key).Err
					case "set":
						done <- Set(c, key, "wrong")
					case "update":
						done <- Update(c, key, func(string) (string, error) {
							t.Error("type mismatch invoked updater")
							return "wrong", nil
						})
					case "fetch":
						_, err := Fetch(context.Background(), c, key, func(context.Context) (string, error) {
							t.Error("type mismatch invoked loader")
							return "wrong", nil
						})
						done <- err
					case "query":
						h := Query(c, key, func(context.Context) (string, error) {
							t.Error("type mismatch invoked loader")
							return "wrong", nil
						})
						defer h.Close()
						done <- h.Snapshot().Err
					}
				}()
				select {
				case err := <-done:
					defer c.Close()
					if !errors.Is(err, ErrTypeMismatch) {
						t.Fatalf("mismatch error = %v", err)
					}
					if state := Get[int](c, key); state.Data != 42 || state.Err != nil {
						t.Fatalf("mismatch changed cached data: %+v", state)
					}
				case <-time.After(3 * time.Second):
					select {
					case <-formatted:
						t.Fatal("key formatter reentered Client.Stats while validation held the client lock")
					default:
						t.Fatal("type mismatch did not return")
					}
				}
			})
		}
	}
}
