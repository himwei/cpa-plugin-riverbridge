package bridge

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const settingsFile = "settings.json"

const (
	requestJournalName  = "requests.journal.jsonl"
	requestSnapshotName = "requests.snapshot.json"
	statisticsMetaName  = "statistics.json"
	statisticsDirName   = "statistics"
	usageFileName       = "usage.json"
)

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

// Journal and snapshot shapes. The snapshot is the compacted state; the journal
// is an append-only log of everything that happened since, so a crash costs at
// most the lines that never reached disk.
type requestLogEvent struct {
	Sequence  uint64    `json:"sequence"`
	Entry     *logEntry `json:"entry,omitempty"`
	Retention int       `json:"retention"`
	Clear     bool      `json:"clear,omitempty"`
}

type requestLogSnapshot struct {
	Version  int        `json:"version"`
	Sequence uint64     `json:"sequence"`
	Logs     []logEntry `json:"logs"`
}

// logRing is the retained view the console reads, backed by the journal.
//
// Entries live in memory for speed but every add is also appended to the
// journal on disk: the host restarts an ordinary amount, and a log that only
// exists in RAM would mean the request history vanishes exactly when someone
// is trying to work out what happened before the restart.
type logRing struct {
	mu    sync.Mutex
	dir   string
	limit int
	items []logEntry

	sequence      uint64
	journalBytes  int64 // Last complete, successfully written line; trim torn tails here.
	journalEvents int
	snapshotBytes int64
	pending       []requestLogEvent
	needsSnapshot bool // After a write failure, items is the bounded recovery state.
	retryAt       time.Time

	writeError string
}

func newLogRing(dir string, limit int) *logRing {
	if limit <= 0 {
		limit = 500
	}
	l := &logRing{dir: dir, limit: limit}
	l.load()
	return l
}

// load restores the snapshot and replays the journal on top of it.
func (l *logRing) load() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items = nil
	l.sequence, l.journalBytes, l.journalEvents, l.snapshotBytes = 0, 0, 0, 0
	if l.dir == "" {
		return
	}

	if body, err := os.ReadFile(filepath.Join(l.dir, requestSnapshotName)); err == nil {
		var snapshot requestLogSnapshot
		if err := json.Unmarshal(body, &snapshot); err != nil || snapshot.Version != 1 {
			l.writeError = "request log snapshot unreadable: " + err.Error()
		} else {
			l.items, l.sequence = snapshot.Logs, snapshot.Sequence
			l.snapshotBytes = int64(len(body))
		}
	}

	f, err := os.Open(filepath.Join(l.dir, requestJournalName))
	if err != nil {
		if !os.IsNotExist(err) {
			l.writeError = "open request journal: " + err.Error()
		}
		return
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		line, readErr := reader.ReadBytes('\n')
		if errors.Is(readErr, io.EOF) {
			// A killed writer can leave a partial last line. Never append behind
			// it: the byte count below is what the next write truncates to.
			return
		}
		if readErr != nil {
			l.writeError = "read request journal: " + readErr.Error()
			return
		}
		var event requestLogEvent
		if err := json.Unmarshal(line, &event); err != nil {
			// One corrupt line must not take the whole history with it; stop at
			// the last good line and let the next compaction rewrite the file.
			l.writeError = fmt.Sprintf("request journal at byte %d: %v", l.journalBytes, err)
			l.needsSnapshot = l.sequence > 0
			return
		}
		if event.Sequence <= l.sequence {
			l.journalBytes += int64(len(line))
			continue
		}
		l.sequence = event.Sequence
		l.applyLocked(event)
		l.journalEvents++
		l.journalBytes += int64(len(line))
	}
}

// applyLocked replays one journal event onto the retained view.
func (l *logRing) applyLocked(event requestLogEvent) {
	if event.Clear {
		l.items = nil
	}
	if event.Entry != nil {
		l.items = append(l.items, *event.Entry)
	}
	// Honour whichever is smaller: the retention recorded when the line was
	// written, and the retention this run was configured with. Lowering
	// log_retention must not resurrect lines the old setting kept.
	retention := event.Retention
	if retention <= 0 || retention > l.limit {
		retention = l.limit
	}
	if len(l.items) > retention {
		l.items = l.items[len(l.items)-retention:]
	}
}

// setLimit changes how many entries are retained. The on-disk history is not
// rewritten here; the next journal write carries the new retention so a
// restart replays to the same size.
func (l *logRing) setLimit(limit int) {
	if limit <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limit = limit
	if len(l.items) > limit {
		l.items = l.items[len(l.items)-limit:]
	}
}

func (l *logRing) add(e logEntry) {
	if e.Time == "" {
		e.Time = time.Now().UTC().Format(time.RFC3339Nano)
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.items = append(l.items, e)
	if len(l.items) > l.limit {
		l.items = l.items[len(l.items)-l.limit:]
	}
	l.sequence++
	event := requestLogEvent{Sequence: l.sequence, Retention: l.limit, Entry: &e}
	l.pending = append(l.pending, event)
	l.flushLocked()
}

// snapshot returns entries oldest first.
func (l *logRing) snapshot() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]logEntry, len(l.items))
	copy(out, l.items)
	return out
}

func (l *logRing) clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items = nil
	l.sequence++
	l.pending = append(l.pending, requestLogEvent{Sequence: l.sequence, Retention: l.limit, Clear: true})
	l.flushLocked()
}

func (l *logRing) lastError() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writeError
}

// flushLocked appends pending events, compacting when the journal outgrows the
// snapshot or when a previous write failed.
func (l *logRing) flushLocked() {
	if l.dir == "" {
		l.pending = nil
		return
	}
	if l.needsSnapshot && time.Now().Before(l.retryAt) {
		return
	}
	var err error
	if l.needsSnapshot {
		err = l.compactLocked()
	} else {
		err = l.writeJournalLocked()
	}
	if err == nil && (l.journalBytes >= 8<<20 ||
		(l.journalEvents >= 512 && l.journalBytes >= l.snapshotBytes && l.snapshotBytes > 0)) {
		err = l.compactLocked()
	}
	if err != nil {
		l.writeError = err.Error()
		// items already holds every pending absolute update, so a checkpoint can
		// safely replace the failed appends once IO recovers.
		l.pending = nil
		l.needsSnapshot = true
		l.retryAt = time.Now().Add(2 * time.Second)
		return
	}
	l.writeError = ""
	l.retryAt = time.Time{}
}

func (l *logRing) writeJournalLocked() error {
	if len(l.pending) == 0 {
		return nil
	}
	var data bytes.Buffer
	for _, event := range l.pending {
		line, err := json.Marshal(event)
		if err != nil {
			return err
		}
		data.Write(line)
		data.WriteByte('\n')
	}
	f, err := os.OpenFile(filepath.Join(l.dir, requestJournalName), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err == nil && info.Size() < l.journalBytes {
		err = errors.New("request journal unexpectedly shortened")
	}
	if err == nil && info.Size() > l.journalBytes {
		err = f.Truncate(l.journalBytes)
	}
	if err == nil {
		_, err = f.Seek(l.journalBytes, io.SeekStart)
	}
	if err == nil {
		_, err = f.Write(data.Bytes())
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	l.journalBytes += int64(data.Len())
	l.journalEvents += len(l.pending)
	l.pending = nil
	return nil
}

// compactLocked writes a fresh snapshot and empties the journal.
func (l *logRing) compactLocked() error {
	data, err := json.MarshalIndent(requestLogSnapshot{Version: 1, Sequence: l.sequence, Logs: l.items}, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(l.dir, requestSnapshotName, data); err != nil {
		return err
	}
	// Only after the snapshot is safely in place may the journal be emptied:
	// the reverse order loses whatever was in the journal if the write fails.
	journal, err := os.OpenFile(filepath.Join(l.dir, requestJournalName), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := journal.Close(); err != nil {
		return err
	}
	l.journalBytes, l.journalEvents = 0, 0
	l.snapshotBytes = int64(len(data))
	l.pending, l.needsSnapshot = nil, false
	return nil
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

// statisticsTotals is one minute bucket of request accounting.
//
// These counters live outside the log retention on purpose: the log is a
// rolling window that forgets old lines, while "how much did I use this month"
// has to keep counting after those lines are gone.
type statisticsTotals struct {
	Requests         int64 `json:"requests"`
	Failures         int64 `json:"failures"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CachedTokens     int64 `json:"cached_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	RateTokens       int64 `json:"rate_tokens"`
	RateDurationMS   int64 `json:"rate_duration_ms"`
	FirstByteMS      int64 `json:"first_byte_ms"`
	FirstByteSamples int64 `json:"first_byte_samples"`
	DurationMS       int64 `json:"duration_ms"`
	CostMicroUSD     int64 `json:"cost_micro_usd"`
}

func (t *statisticsTotals) add(v statisticsTotals) {
	t.Requests += v.Requests
	t.Failures += v.Failures
	t.PromptTokens += v.PromptTokens
	t.CachedTokens += v.CachedTokens
	t.CompletionTokens += v.CompletionTokens
	t.RateTokens += v.RateTokens
	t.RateDurationMS += v.RateDurationMS
	t.FirstByteMS += v.FirstByteMS
	t.FirstByteSamples += v.FirstByteSamples
	t.DurationMS += v.DurationMS
	t.CostMicroUSD += v.CostMicroUSD
}

type statisticsMeta struct {
	Version    int       `json:"version"`
	Generation string    `json:"generation"`
	Since      time.Time `json:"since"`
}

type statisticsState struct {
	meta  statisticsMeta
	days  map[string]map[string]statisticsTotals
	dirty map[string]bool
}

// statisticsStore persists per-day, per-minute buckets so the totals survive
// both restarts and log truncation.
type statisticsStore struct {
	mu        sync.Mutex
	dir       string
	meta      statisticsMeta
	days      map[string]map[string]statisticsTotals
	dirty     map[string]bool
	loaded    bool
	writeErr  string
	loadError string
}

func newStatisticsStore(dir string) *statisticsStore {
	st := &statisticsStore{dir: dir, days: map[string]map[string]statisticsTotals{}, dirty: map[string]bool{}}
	st.load()
	return st
}

func (s *statisticsStore) metaPath() string {
	return filepath.Join(s.dir, statisticsMetaName)
}

func (s *statisticsStore) dayPath(day string) string {
	return filepath.Join(s.dir, statisticsDirName, s.meta.Generation, day+".json")
}

func (s *statisticsStore) load() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.days = map[string]map[string]statisticsTotals{}
	s.dirty = map[string]bool{}
	if s.dir == "" {
		return
	}
	body, err := os.ReadFile(s.metaPath())
	if err != nil {
		if !os.IsNotExist(err) {
			s.loadError = "read statistics metadata: " + err.Error()
		}
		// First run (or a wiped data dir): mint the generation now so the first
		// flush writes day files under a directory that will be read back.
		s.meta = statisticsMeta{Version: 1, Generation: newGeneration(), Since: time.Now().UTC()}
		s.loaded = true
		return
	}
	if err := json.Unmarshal(body, &s.meta); err != nil || s.meta.Version != 1 || s.meta.Generation == "" || s.meta.Since.IsZero() {
		// An unreadable pointer is not worth failing the plugin over: start a
		// fresh generation and keep the old directory on disk as a rollback.
		s.loadError = "invalid statistics metadata; starting a new generation"
		s.meta = statisticsMeta{Version: 1, Generation: newGeneration(), Since: time.Now().UTC()}
		s.loaded = true
		return
	}
	entries, readErr := os.ReadDir(filepath.Join(s.dir, statisticsDirName, s.meta.Generation))
	if readErr != nil && !os.IsNotExist(readErr) {
		s.loadError = "read statistics directory: " + readErr.Error()
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		day := strings.TrimSuffix(entry.Name(), ".json")
		if _, err := time.Parse("2006-01-02", day); err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, statisticsDirName, s.meta.Generation, entry.Name()))
		if err != nil {
			continue
		}
		var buckets map[string]statisticsTotals
		if err := json.Unmarshal(data, &buckets); err != nil {
			continue
		}
		s.days[day] = buckets
	}
	s.loaded = true
}

// record folds one finished request into the minute bucket it belongs to.
func (s *statisticsStore) record(e logEntry) {
	if e.Kind != KindRequest {
		return
	}
	at, err := time.Parse(time.RFC3339Nano, e.Time)
	if err != nil {
		at = time.Now().UTC()
	}
	at = at.UTC()

	s.mu.Lock()
	defer s.mu.Unlock()
	day := at.Format("2006-01-02")
	minute := at.Truncate(time.Minute).Format(time.RFC3339)
	if s.days[day] == nil {
		s.days[day] = map[string]statisticsTotals{}
	}
	v := statisticsTotals{
		Requests:         1,
		PromptTokens:     int64(e.PromptTokens),
		CachedTokens:     int64(e.CachedTokens),
		CompletionTokens: int64(e.CompletionTokens),
		DurationMS:       e.Millis,
		CostMicroUSD:     int64(e.CostUSD*1e6 + 0.5),
	}
	if e.Status >= 400 || e.Level == "error" {
		v.Failures = 1
	}
	if e.TokensPerSecond > 0 {
		// Store the sample as tokens and milliseconds so the average is a plain
		// ratio over the totals and stays exact when summed across days.
		generationMS := e.Millis - e.FirstByteMs
		if generationMS > 0 {
			v.RateTokens = int64(e.CompletionTokens)
			v.RateDurationMS = generationMS
		}
	}
	if e.FirstByteMs > 0 {
		v.FirstByteMS = e.FirstByteMs
		v.FirstByteSamples = 1
	}
	total := s.days[day][minute]
	total.add(v)
	s.days[day][minute] = total
	s.dirty[day] = true
}

// flush writes the day files that changed.
func (s *statisticsStore) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushLocked()
}

func (s *statisticsStore) flushLocked() {
	if s.dir == "" || len(s.dirty) == 0 {
		return
	}
	if _, err := os.Stat(s.metaPath()); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Join(s.dir, statisticsDirName), 0o700); err != nil {
			s.writeErr = err.Error()
			return
		}
		if err := writeFileAtomic(s.dir, statisticsMetaName, jsonBytesIndent(s.meta)); err != nil {
			s.writeErr = err.Error()
			return
		}
	}
	days := make([]string, 0, len(s.dirty))
	for day := range s.dirty {
		days = append(days, day)
	}
	sort.Strings(days)
	for _, day := range days {
		path := filepath.Join(s.dir, statisticsDirName, s.meta.Generation)
		if err := os.MkdirAll(path, 0o700); err != nil {
			s.writeErr = err.Error()
			return
		}
		body := jsonBytesIndent(s.days[day])
		if err := writeFileAtomic(path, day+".json", body); err != nil {
			s.writeErr = err.Error()
			return
		}
		delete(s.dirty, day)
	}
	s.writeErr = ""
}

// rangeTotals sums the buckets inside [start, end). A zero bound is open.
func (s *statisticsStore) rangeTotals(start, end time.Time) (statisticsTotals, time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total statisticsTotals
	for _, buckets := range s.days {
		for minute, value := range buckets {
			ts, err := time.Parse(time.RFC3339, minute)
			if err != nil {
				continue
			}
			if !start.IsZero() && ts.Before(start) {
				continue
			}
			if !end.IsZero() && !ts.Before(end) {
				continue
			}
			total.add(value)
		}
	}
	return total, s.meta.Since, s.meta.Generation != ""
}

func (s *statisticsStore) lastError() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr != "" {
		return s.writeErr
	}
	return s.loadError
}

// reset starts a new generation so old files stay on disk as a rollback copy.
func (s *statisticsStore) reset() (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushLocked()
	if s.writeErr != "" {
		return time.Time{}, errors.New("统计写入失败，请稍后重试")
	}
	s.meta = statisticsMeta{Version: 1, Generation: newGeneration(), Since: time.Now().UTC()}
	s.days = map[string]map[string]statisticsTotals{}
	s.dirty = map[string]bool{}
	if err := writeFileAtomic(s.dir, statisticsMetaName, jsonBytesIndent(s.meta)); err != nil {
		return time.Time{}, err
	}
	return s.meta.Since, nil
}

func newGeneration() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%024x", time.Now().UnixNano()&0xffffffffffff)
	}
	return hex.EncodeToString(b[:])
}

// ---------------------------------------------------------------------------
// Usage (per-credential counters)
// ---------------------------------------------------------------------------

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
	stat.LastUsedAt = time.Now().UTC().Format(time.RFC3339)
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
	stat.LastUsedAt = time.Now().UTC().Format(time.RFC3339)
	stat.LastModel = model
	stat.LastError = message
	stat.LastErrorAt = time.Now().UTC().Format(time.RFC3339)
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
	body := jsonBytesIndent(usageFile{SchemaVersion: 1, Stats: u.stats})
	u.mu.RUnlock()
	if err := writeFileAtomic(dir, usageFileName, body); err != nil {
		return err
	}
	u.mu.Lock()
	u.dirty = false
	u.mu.Unlock()
	return nil
}

func (u *usageStore) load(dir string) {
	body, err := readFile(dir, usageFileName)
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

// dayIndex lists the days the store holds counters for, so the console can tell
// "no traffic then" apart from "not collected yet".
func (s *statisticsStore) dayIndex() ([]string, time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	days := make([]string, 0, len(s.days))
	for day, buckets := range s.days {
		if len(buckets) == 0 {
			continue
		}
		days = append(days, day)
	}
	sort.Strings(days)
	return days, s.meta.Since, s.meta.Generation != ""
}
