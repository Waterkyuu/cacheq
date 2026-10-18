package redistore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/Waterkyuu/cacheq"
)

const (
	// taggedReadScript snapshots key and tag versions while registering additive tag membership.
	taggedReadScript = `
local version = redis.call('GET', KEYS[1])
local initialized = redis.call('HEXISTS', KEYS[3], '!initialized')
if not version or initialized == 0 then
    version = ARGV[1]
    redis.call('SET', KEYS[1], version)
    redis.call('DEL', KEYS[2])
    redis.call('HSET', KEYS[3], '!initialized', '1')
end
local added = false
for _, tag in ipairs(cjson.decode(ARGV[2])) do
    if redis.call('HEXISTS', KEYS[3], tag) == 0 then
        local current = redis.call('HGET', KEYS[4], tag)
        if not current then
            current = ARGV[1]
            redis.call('HSET', KEYS[4], tag, current)
        end
        redis.call('HSET', KEYS[3], tag, current)
        added = true
    end
end
if added then
    version = ARGV[1]
    redis.call('SET', KEYS[1], version)
    if ARGV[5] == '1' then redis.call('PUBLISH', ARGV[3], ARGV[4]) end
end
local fields = redis.call('HGETALL', KEYS[3])
local snapshot = {}
local stale = false
for i = 1, #fields, 2 do
    local tag = fields[i]
    if tag ~= '!initialized' then
    local current = redis.call('HGET', KEYS[4], tag)
    if not current then
        current = ARGV[1]
        redis.call('HSET', KEYS[4], tag, current)
    end
    snapshot[tag] = current
    if current ~= fields[i + 1] then stale = true end
    end
end
if stale then redis.call('DEL', KEYS[2]) end
return {cjson.encode({key = version, tags = snapshot}), redis.call('GET', KEYS[2]) or ''}
`
	// taggedWriteScript rejects changes to the key, any tag, or membership before publishing a payload.
	taggedWriteScript = `
local snapshot = cjson.decode(ARGV[1])
if redis.call('GET', KEYS[1]) ~= snapshot.key then return 0 end
if redis.call('HEXISTS', KEYS[3], '!initialized') == 0 then return 0 end
for tag, version in pairs(snapshot.tags) do
    if redis.call('HGET', KEYS[4], tag) ~= version then return 0 end
end
for tag, version in pairs(snapshot.tags) do
    redis.call('HSET', KEYS[3], tag, version)
end
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
return 1
`
	// invalidateTagsScript advances every selected tag and publishes one receipt without scanning data keys.
	invalidateTagsScript = `
for _, tag in ipairs(cjson.decode(ARGV[2])) do
    redis.call('HSET', KEYS[1], tag, ARGV[1])
end
if ARGV[5] == '1' then redis.call('PUBLISH', ARGV[3], ARGV[4]) end
return 1
`
)

// tagSnapshot encodes the opaque generation passed from ReadTagged to conditional Write.
type tagSnapshot struct {
	// Key holds the per-record generation, also changed when membership grows.
	Key string `json:"key"`
	// Tags maps each registered label to its current remote invalidation generation.
	Tags map[string]string `json:"tags"`
}

// TagsEnabled reports the explicit atomic-tag layout selected at construction.
func (s *Store) TagsEnabled() bool { return s.enableTags }

// ReadTagged registers exact labels and returns an atomic key/tag snapshot, preserving remote tag unions.
func (s *Store) ReadTagged(ctx context.Context, key string, tags []string) (cacheq.SharedRecord, error) {
	if err := ctx.Err(); err != nil {
		return cacheq.SharedRecord{}, err
	}
	if !s.enableTags {
		return cacheq.SharedRecord{}, cacheq.ErrSharedCapability
	}
	if key == "" {
		return cacheq.SharedRecord{}, ErrInvalidOptions
	}
	encodedLabels := make([]string, 0, len(tags))
	for _, tag := range tags {
		encodedLabels = append(encodedLabels, base64.StdEncoding.EncodeToString([]byte(tag)))
	}
	labels, err := json.Marshal(encodedLabels)
	if err != nil {
		return cacheq.SharedRecord{}, err
	}
	token := rand.Text()
	registration, err := marshalInvalidation(
		cacheq.SharedInvalidation{ID: token, Key: key, RegisteredTags: append([]string{}, tags...)},
	)
	if err != nil {
		return cacheq.SharedRecord{}, err
	}
	result, err := s.client.Eval(ctx, taggedReadScript, s.tagKeys(key), token, labels, s.channel(), registration, s.notificationFlag()).
		Slice()
	if err != nil {
		return cacheq.SharedRecord{}, fmt.Errorf("redis tagged read: %w", err)
	}
	if len(result) != 2 {
		return cacheq.SharedRecord{}, ErrInvalidOptions
	}
	version, versionOK := result[0].(string)
	data, dataOK := result[1].(string)
	if !versionOK || !dataOK {
		return cacheq.SharedRecord{}, ErrInvalidOptions
	}
	var snapshot tagSnapshot
	if err := json.Unmarshal([]byte(version), &snapshot); err != nil {
		return cacheq.SharedRecord{}, err
	}
	canonical, err := json.Marshal(snapshot)
	if err != nil {
		return cacheq.SharedRecord{}, err
	}
	membership := make([]string, 0, len(snapshot.Tags))
	for tag := range snapshot.Tags {
		decoded, err := base64.StdEncoding.DecodeString(tag)
		if err != nil {
			return cacheq.SharedRecord{}, err
		}
		membership = append(membership, string(decoded))
	}
	slices.Sort(membership)
	return cacheq.SharedRecord{Version: string(canonical), Data: []byte(data), Tags: membership}, nil
}

// writeTagged validates a decoded snapshot before the server performs atomic key and tag comparisons.
func (s *Store) writeTagged(
	ctx context.Context,
	key string,
	write cacheq.SharedWrite,
	milliseconds int64,
) (bool, error) {
	var snapshot tagSnapshot
	if err := json.Unmarshal([]byte(write.Version), &snapshot); err != nil {
		return false, ErrInvalidOptions
	}
	if snapshot.Key == "" || snapshot.Tags == nil {
		return false, ErrInvalidOptions
	}
	result, err := s.client.Eval(ctx, taggedWriteScript, s.tagKeys(key), write.Version, write.Data, milliseconds).
		Int64()
	if err != nil {
		return false, fmt.Errorf("redis tagged write: %w", err)
	}
	return result == 1, nil
}

// InvalidateTags advances exact tag generations with OR semantics and broadcasts one committed receipt.
func (s *Store) InvalidateTags(ctx context.Context, tags []string) (cacheq.SharedInvalidation, error) {
	if err := ctx.Err(); err != nil {
		return cacheq.SharedInvalidation{}, err
	}
	if !s.enableTags {
		return cacheq.SharedInvalidation{}, cacheq.ErrSharedCapability
	}
	if len(tags) == 0 {
		return cacheq.SharedInvalidation{}, nil
	}
	labels := slices.Clone(tags)
	slices.Sort(labels)
	labels = slices.Compact(labels)
	event := cacheq.SharedInvalidation{ID: rand.Text(), Tags: labels}
	data, err := marshalInvalidation(event)
	if err != nil {
		return cacheq.SharedInvalidation{}, err
	}
	encodedLabels := make([]string, 0, len(labels))
	for _, tag := range labels {
		encodedLabels = append(encodedLabels, base64.StdEncoding.EncodeToString([]byte(tag)))
	}
	encodedTags, err := json.Marshal(encodedLabels)
	if err != nil {
		return cacheq.SharedInvalidation{}, err
	}
	if err := s.client.Eval(ctx, invalidateTagsScript, []string{s.tagGenerationKey()}, event.ID, encodedTags, s.channel(), data, s.notificationFlag()).
		Err(); err != nil {
		return cacheq.SharedInvalidation{}, fmt.Errorf("redis tag invalidation: %w", err)
	}
	return event, nil
}

// tagGenerationKey places the namespace's tag generations in the same slot as every tagged record.
func (s *Store) tagGenerationKey() string {
	return fmt.Sprintf("%s:tagged:{%x}:tag-generations", s.prefix, sha256.Sum256([]byte(s.prefix)))
}

// tagKeys supplies the key generation, payload, membership, and namespace tag generations in script order.
func (s *Store) tagKeys(key string) []string {
	keys := s.keys(key)
	return append(keys, keys[0]+":tags", s.tagGenerationKey())
}
