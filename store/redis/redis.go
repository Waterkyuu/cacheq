// Package redistore implements cacheq.SharedStore using atomic Redis scripts.
// The application supplies and closes its go-redis client.
package redistore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Waterkyuu/cacheq"
	"github.com/redis/go-redis/v9"
)

// ErrInvalidOptions indicates missing dependencies, unsafe namespaces, or malformed store operations.
var ErrInvalidOptions = errors.New("invalid Redis shared store options")

const (
	// readScript snapshots generation and data atomically, discarding orphaned data after metadata loss.
	readScript = `
local version = redis.call('GET', KEYS[1])
if not version then
    version = ARGV[1]
    redis.call('SET', KEYS[1], version)
    redis.call('DEL', KEYS[2])
end
return {version, redis.call('GET', KEYS[2]) or ''}
`
	// writeScript rejects obsolete or missing generations before installing a record with its remaining TTL.
	writeScript = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
return 1
`
	// invalidateScript changes the generation and deletes data as one atomic server operation.
	invalidateScript = `
redis.call('SET', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
if ARGV[4] == '1' then redis.call('PUBLISH', ARGV[2], ARGV[3]) end
return 1
`
)

// RedisClient is the command capability consumed by Store.
// go-redis Client, ClusterClient, and Ring implement it; configure context and socket timeouts at creation.
type RedisClient interface {
	// Eval executes a script atomically on the primary owning the supplied keys.
	Eval(context.Context, string, []string, ...any) *redis.Cmd
}

// Options isolates an application's shared records from other Redis keys.
type Options struct {
	// Prefix namespaces generated Redis keys; empty selects "cacheq" and braces are rejected for cluster safety.
	Prefix string
	// EnableTags places this namespace in one cluster slot for atomic key and tag generation checks.
	// Tagged namespaces use a separate physical key layout; false preserves the per-key P0 layout.
	EnableTags bool
	// EnableNotifications publishes invalidations and allows watchers; false preserves P0 command permissions.
	EnableNotifications bool
}

// Store owns an immutable Redis namespace and a borrowed command client.
// Each logical key uses a persistent generation key and an expiring data key in the same cluster slot.
// Generation metadata intentionally has no TTL; plan capacity for one small key per touched identity.
type Store struct {
	// client executes atomic scripts and remains owned by the application.
	client RedisClient
	// prefix separates this store from other applications and excludes Redis hash-tag delimiters.
	prefix string
	// enableTags selects the explicit namespace-wide tag generation layout.
	enableTags bool
	// enableNotifications keeps Pub/Sub commands opt-in for existing deployments and ACLs.
	enableNotifications bool
	// scope uniquely identifies this borrowed store instance for local watcher routing.
	scope string
}

// New validates a Redis store configuration without opening a connection or accessing the server.
func New(client RedisClient, options Options) (*Store, error) {
	if client == nil || strings.ContainsAny(options.Prefix, "{}") {
		return nil, ErrInvalidOptions
	}
	if options.Prefix == "" {
		options.Prefix = "cacheq"
	}
	if options.EnableNotifications {
		// Ring shards are independent servers: subscribing on one shard cannot observe other shards' Lua publications.
		if _, ring := client.(*redis.Ring); ring {
			return nil, cacheq.ErrSharedCapability
		}
		if _, ok := client.(redisSubscriber); !ok {
			return nil, cacheq.ErrSharedCapability
		}
	}
	return &Store{
		client:              client,
		prefix:              options.Prefix,
		enableTags:          options.EnableTags,
		enableNotifications: options.EnableNotifications,
		scope:               rand.Text(),
	}, nil
}

// Read obtains an atomic snapshot, initializing a random generation even on a cache miss.
// Missing metadata receives a new token and discards orphaned data; old tokens cannot regain write permission.
func (s *Store) Read(ctx context.Context, key string) (cacheq.SharedRecord, error) {
	if s.enableTags {
		return s.ReadTagged(ctx, key, []string{})
	}
	if err := ctx.Err(); err != nil {
		return cacheq.SharedRecord{}, err
	}
	if key == "" {
		return cacheq.SharedRecord{}, ErrInvalidOptions
	}
	result, err := s.client.Eval(ctx, readScript, s.keys(key), rand.Text()).Slice()
	if err != nil {
		return cacheq.SharedRecord{}, fmt.Errorf("redis read: %w", err)
	}
	if len(result) != 2 {
		return cacheq.SharedRecord{}, errors.New("redis read returned an invalid snapshot")
	}
	version, versionOK := result[0].(string)
	data, dataOK := result[1].(string)
	if !versionOK || !dataOK || version == "" {
		return cacheq.SharedRecord{}, errors.New("redis read returned invalid fields")
	}
	return cacheq.SharedRecord{Version: version, Data: []byte(data)}, nil
}

// Write atomically installs data only if its generation still matches the pre-load read.
// Sub-millisecond TTLs round up to one millisecond; the record's absolute deadline remains authoritative.
func (s *Store) Write(ctx context.Context, key string, write cacheq.SharedWrite) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	invalidRecord := write.Version == "" || len(write.Data) == 0 || write.TTL <= 0
	if key == "" || invalidRecord {
		return false, ErrInvalidOptions
	}
	milliseconds := write.TTL.Milliseconds()
	if write.TTL%time.Millisecond != 0 {
		milliseconds++
	}
	if s.enableTags {
		return s.writeTagged(ctx, key, write, milliseconds)
	}
	result, err := s.client.Eval(ctx, writeScript, s.keys(key), write.Version, write.Data, milliseconds).Int64()
	if err != nil {
		return false, fmt.Errorf("redis conditional write: %w", err)
	}
	return result == 1, nil
}

// Invalidate fences all earlier readers by replacing the random generation before removing the payload.
func (s *Store) Invalidate(ctx context.Context, key string) error {
	_, err := s.InvalidateKey(ctx, key)
	return err
}

// InvalidateKey replaces the generation and removes data, publishing its receipt atomically when notifications are enabled.
func (s *Store) InvalidateKey(ctx context.Context, key string) (cacheq.SharedInvalidation, error) {
	if err := ctx.Err(); err != nil {
		return cacheq.SharedInvalidation{}, err
	}
	if key == "" {
		return cacheq.SharedInvalidation{}, ErrInvalidOptions
	}
	event := cacheq.SharedInvalidation{ID: rand.Text(), Key: key}
	data, err := marshalInvalidation(event)
	if err != nil {
		return cacheq.SharedInvalidation{}, err
	}
	if err := s.client.Eval(ctx, invalidateScript, s.keys(key), event.ID, s.channel(), data, s.notificationFlag()).
		Err(); err != nil {
		return cacheq.SharedInvalidation{}, fmt.Errorf("redis invalidate: %w", err)
	}
	return event, nil
}

// keys uses a stable digest hash tag so cluster scripts operate on a single slot for every logical key.
func (s *Store) keys(key string) []string {
	base := fmt.Sprintf("%s:{%x}", s.prefix, sha256.Sum256([]byte(key)))
	if s.enableTags {
		base = fmt.Sprintf("%s:tagged:{%x}:%x", s.prefix, sha256.Sum256([]byte(s.prefix)), sha256.Sum256([]byte(key)))
	}
	return []string{base + ":version", base + ":data"}
}

// InvalidationScope returns an immutable process-local identity for this store instance.
func (s *Store) InvalidationScope() string { return s.scope }

// channel isolates notifications by namespace and key layout, including a wire protocol version.
func (s *Store) channel() string {
	return fmt.Sprintf("%s:invalidations:v1:tags=%t", s.prefix, s.enableTags)
}

// notificationFlag supplies the script's explicit Pub/Sub permission boundary.
func (s *Store) notificationFlag() string {
	if s.enableNotifications {
		return "1"
	}
	return "0"
}
