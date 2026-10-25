package bridge

import (
	"encoding/json"
	"sync"
	"time"
)

const settingsFile = "settings.json"

// Log entry kinds. A "request" entry is one upstream call and carries the
// latency and token columns the log table renders; an "event" entry is a
// diagnostic line (key added, settings saved) with no upstream behind it.
const (
	KindRequest = "request"
	KindEvent   = "event"
)

// logEntry is one line shown in the plugin console.
//
// The request columns mirror what the host and upstream actually reported:
// FirstByteMs is measured here because neither River nor CPA exposes a
// time-to-first-token for a call this plugin proxied, and TokensPerSecond is
// derived from it so the table can show generation speed rather than total
// throughput.
type logEntry struct {
	Time     string `json:"time"`
	Level    string `json:"level"`
	Kind     string `json:"kind,omitempty"`
	Model    string `json:"model,omitempty"`
	Upstream string `json:"upstream,omitempty"`
	AuthID   string `json:"auth_id,omitempty"`
	Method   string `json:"method,omitempty"`
	Status   int    `json:"status,omitempty"`
	Millis   int64  `json:"ms,omitempty"`
	Detail   string `json:"detail,omitempty"`

	FirstByteMs      int64   `json:"first_byte_ms,omitempty"`
	PromptTokens     int     `json:"prompt_tokens,omitempty"`
	CachedTokens     int     `json:"cached_tokens,omitempty"`
	CompletionTokens int     `json:"completion_tokens,omitempty"`
	TotalTokens      int     `json:"total_tokens,omitempty"`
	TokensPerSecond  float64 `json:"tokens_per_second,omitempty"`
	CostUSD          float64 `json:"cost_usd,omitempty"`
	Stream           bool    `json:"stream,omitempty"`
}

// logRing is a fixed-size ring buffer of recent diagnostics.
type logRing struct {
	mu    sync.RWMutex
	limit int
	next  int
	full  bool
	items []logEntry
}

func newLogRing(limit int) *logRing {
	if limit <= 0 {
		limit = 300
	}
	return &logRing{limit: limit, items: make([]logEntry, limit)}
}

func (l *logRing) setLimit(limit int) {
	if limit <= 0 || limit == l.limit {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entries := l.snapshotLocked()
	l.limit = limit
	l.items = make([]logEntry, limit)
	l.next, l.full = 0, false
	for _, e := range entries {
		l.addLocked(e)
	}
}

func (l *logRing) add(e logEntry) {
	if e.Time == "" {
		e.Time = time.Now().Format(time.RFC3339)
	}
	l.mu.Lock()
	l.addLocked(e)
	l.mu.Unlock()
}

func (l *logRing) addLocked(e logEntry) {
	l.items[l.next] = e
	l.next = (l.next + 1) % l.limit
	if l.next == 0 {
		l.full = true
	}
}

// snapshot returns entries oldest first.
func (l *logRing) snapshot() []logEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.snapshotLocked()
}

func (l *logRing) snapshotLocked() []logEntry {
	size := l.next
	if l.full {
		size = l.limit
	}
	out := make([]logEntry, 0, size)
	start := 0
	if l.full {
		start = l.next
	}
	for i := 0; i < size; i++ {
		out = append(out, l.items[(start+i)%l.limit])
	}
	return out
}

func (l *logRing) clear() {
	l.mu.Lock()
	l.items = make([]logEntry, l.limit)
	l.next, l.full = 0, false
	l.mu.Unlock()
}

// usageStat accumulates per-credential token and cost accounting.
type usageStat struct {
	Requests        int64   `json:"requests"`
	Failures        int64   `json:"failures"`
	InputTokens     int64   `json:"input_tokens"`
	CachedTokens    int64   `json:"cached_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	CostUSD         float64 `json:"cost_usd"`
	LastUsedAt      string  `json:"last_used_at,omitempty"`
	LastModel       string  `json:"last_model,omitempty"`
	LastError       string  `json:"last_error,omitempty"`
	LastErrorAt     string  `json:"last_error_at,omitempty"`
}

// usageFile is the persisted shape of the usage store.
type usageFile struct {
	SchemaVersion int                   `json:"schema_version"`
	Stats         map[string]*usageStat `json:"stats"`
}

// usageStore keeps counters for every credential and model. River reports usage
// per response, so the counters come straight from the upstream `usage` object.
type usageStore struct {
	mu    sync.RWMutex
	stats map[string]*usageStat
	dirty bool
}

func newUsageStore() *usageStore {
	return &usageStore{stats: map[string]*usageStat{}}
}

func (u *usageStore) entry(credID string) *usageStat {
	stat, ok := u.stats[credID]
	if !ok {
		stat = &usageStat{}
		u.stats[credID] = stat
	}
	return stat
}

// RecordSuccess adds one successful request's accounting.
func (u *usageStore) RecordSuccess(credID, model string, usage *Usage) {
	u.mu.Lock()
	defer u.mu.Unlock()
	stat := u.entry(credID)
	stat.Requests++
	stat.LastUsedAt = time.Now().Format(time.RFC3339)
	stat.LastModel = model
	stat.LastError = ""
	if usage == nil {
		u.dirty = true
		return
	}
	stat.InputTokens += int64(usage.PromptTokens)
	stat.CachedTokens += int64(usage.CachedTokens)
	stat.OutputTokens += int64(usage.CompletionTokens)
	stat.ReasoningTokens += int64(usage.ReasoningTokens)
	stat.CostUSD += usage.CostUSD
	u.dirty = true
}

// RecordFailure records a failed request against the credential.
func (u *usageStore) RecordFailure(credID, model, message string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	stat := u.entry(credID)
	stat.Requests++
	stat.Failures++
	stat.LastUsedAt = time.Now().Format(time.RFC3339)
	stat.LastModel = model
	stat.LastError = message
	stat.LastErrorAt = time.Now().Format(time.RFC3339)
	u.dirty = true
}

// Snapshot copies the counters for the management API.
func (u *usageStore) Snapshot() map[string]usageStat {
	u.mu.RLock()
	defer u.mu.RUnlock()
	out := make(map[string]usageStat, len(u.stats))
	for k, v := range u.stats {
		out[k] = *v
	}
	return out
}

// Reset clears counters for one credential, or for all when credID is empty.
func (u *usageStore) Reset(credID string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if credID == "" {
		u.stats = map[string]*usageStat{}
	} else {
		delete(u.stats, credID)
	}
	u.dirty = true
}

func (u *usageStore) save(dir string) error {
	u.mu.RLock()
	if !u.dirty {
		u.mu.RUnlock()
		return nil
	}
	body, err := json.MarshalIndent(usageFile{SchemaVersion: 1, Stats: u.stats}, "", "  ")
	u.mu.RUnlock()
	if err != nil {
		return err
	}
	if err := writeFileAtomic(dir, "usage.json", body); err != nil {
		return err
	}
	u.mu.Lock()
	u.dirty = false
	u.mu.Unlock()
	return nil
}

func (u *usageStore) load(dir string) {
	body, err := readFile(dir, "usage.json")
	if err != nil || len(body) == 0 {
		return
	}
	var file usageFile
	if err := json.Unmarshal(body, &file); err != nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if file.Stats != nil {
		u.stats = file.Stats
	}
}

// Usage mirrors the parts of River's `usage` object this plugin accounts for.
type Usage struct {
	PromptTokens     int     `json:"prompt_tokens"`
	CompletionTokens int     `json:"completion_tokens"`
	TotalTokens      int     `json:"total_tokens"`
	CachedTokens     int     `json:"cached_tokens"`
	ReasoningTokens  int     `json:"reasoning_tokens"`
	CostUSD          float64 `json:"cost_usd"`
	// Raw keeps the upstream object for debugging when usage parsing surprises.
	Raw json.RawMessage `json:"-"`
}

// usageEnvelope matches the OpenAI chat-completions usage shape plus River's
// nested detail objects.
type usageEnvelope struct {
	PromptTokens        int `json:"prompt_tokens"`
	CompletionTokens    int `json:"completion_tokens"`
	TotalTokens         int `json:"total_tokens"`
	PromptTokensDetails *struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionTokensDetails *struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

func (u usageEnvelope) toUsage() *Usage {
	out := &Usage{
		PromptTokens:     u.PromptTokens,
		CompletionTokens: u.CompletionTokens,
		TotalTokens:      u.TotalTokens,
	}
	if u.PromptTokensDetails != nil {
		out.CachedTokens = u.PromptTokensDetails.CachedTokens
	}
	if u.CompletionTokensDetails != nil {
		out.ReasoningTokens = u.CompletionTokensDetails.ReasoningTokens
	}
	if out.TotalTokens == 0 {
		out.TotalTokens = out.PromptTokens + out.CompletionTokens
	}
	return out
}
