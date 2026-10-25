package bridge

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// settingsPatch is the editable part of Config exposed by the console.
//
// Only operator-facing knobs are here. data_dir is deliberately excluded: it
// decides where state is read from at startup, so changing it from a running
// process would strand every existing credential and usage file.
type settingsPatch struct {
	Strategy              *string   `json:"strategy"`
	RetryOnFailure        *bool     `json:"retry_on_failure"`
	DisabledModels        *[]string `json:"disabled_models"`
	LogRetention          *int      `json:"log_retention"`
	RequestTimeoutSeconds *int      `json:"request_timeout_seconds"`
	IncludeStreamUsage    *bool     `json:"include_stream_usage"`
	BaseURL               *string   `json:"base_url"`
}

// managementSettings reports the current configuration plus the runtime state
// an operator needs to tell whether it is actually in effect.
func (s *Service) managementSettings() (any, error) {
	cfg := s.config()
	s.mu.RLock()
	creds := make([]Credential, 0, len(s.creds))
	for _, c := range s.creds {
		creds = append(creds, c)
	}
	s.mu.RUnlock()

	enabled, disabled := 0, 0
	for _, c := range creds {
		if c.Disabled {
			disabled++
		} else {
			enabled++
		}
	}

	models := make([]string, 0, 8)
	seen := map[string]struct{}{}
	for _, c := range creds {
		// The global block list edits this set, so it has to contain every
		// model any key knows about - including the ones already blocked -
		// or a blocked model would vanish and could never be re-enabled.
		for _, m := range c.Models {
			if _, ok := seen[m.ID]; ok {
				continue
			}
			seen[m.ID] = struct{}{}
			models = append(models, m.ID)
		}
	}
	if len(models) == 0 {
		for _, m := range catalogueModels() {
			models = append(models, m.ID)
		}
	}
	sort.Strings(models)

	return managementJSON(http.StatusOK, map[string]any{
		"settings": map[string]any{
			"strategy":                cfg.Strategy,
			"retry_on_failure":        cfg.RetryOnFailure,
			"disabled_models":         nonNilStrings(cfg.DisabledModels),
			"log_retention":           cfg.LogRetention,
			"request_timeout_seconds": cfg.RequestTimeoutSeconds,
			"include_stream_usage":    cfg.IncludeStreamUsage,
			"base_url":                cfg.BaseURL,
			"data_dir":                cfg.DataDir,
		},
		"strategies": []map[string]string{
			{"value": StrategyFillFirst, "label": "固定优先（fill-first）",
				"detail": "始终使用优先级最高的启用账号，命中率高、利于上游缓存；仅在该账号不可用时切换。"},
			{"value": StrategyRoundRobin, "label": "轮询（round-robin）",
				"detail": "每个请求在启用账号间依次轮转，压力与额度消耗更平均。"},
		},
		"scheduler":   s.schedulerState(),
		"credentials": map[string]any{"enabled": enabled, "disabled": disabled, "total": len(creds)},
		"models":      models,
		"version":     Version,
		"uptime_s":    int64(s.uptimeSeconds()),
	})
}

func (s *Service) uptimeSeconds() int64 {
	s.mu.RLock()
	started := s.started
	s.mu.RUnlock()
	return int64(time.Since(started).Seconds())
}

// managementSaveSettings persists a settings patch to settings.json and
// applies it without restarting.
func (s *Service) managementSaveSettings(body []byte) (any, error) {
	var req settingsPatch
	if err := json.Unmarshal(body, &req); err != nil {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
	}

	s.configMu.Lock()
	cfg := s.cfg
	if req.Strategy != nil {
		cfg.Strategy = *req.Strategy
	}
	if req.RetryOnFailure != nil {
		cfg.RetryOnFailure = *req.RetryOnFailure
	}
	if req.DisabledModels != nil {
		cfg.DisabledModels = normalizeModelList(*req.DisabledModels)
	}
	if req.LogRetention != nil {
		cfg.LogRetention = *req.LogRetention
	}
	if req.RequestTimeoutSeconds != nil {
		cfg.RequestTimeoutSeconds = *req.RequestTimeoutSeconds
	}
	if req.IncludeStreamUsage != nil {
		cfg.IncludeStreamUsage = *req.IncludeStreamUsage
	}
	if req.BaseURL != nil {
		cfg.BaseURL = *req.BaseURL
	}
	cfg.normalize()
	if err := writeSettingsLocked(cfg); err != nil {
		s.configMu.Unlock()
		return managementJSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	s.cfg = cfg
	s.configMu.Unlock()

	s.mu.RLock()
	logs := s.logs
	s.mu.RUnlock()
	if logs != nil {
		logs.setLimit(cfg.LogRetention)
	}

	// The host caches each auth's model list under its model_revision, so a
	// global model block only reaches /v1/models once every credential whose
	// visible set moved gets a new revision and the host re-reads it.
	if req.DisabledModels != nil {
		s.republishAllAuths("global model list changed")
	}

	s.log("info", "", "", http.StatusOK, 0, "settings saved (strategy="+cfg.Strategy+")")
	return s.managementSettings()
}

// republishAllAuths bumps every credential's model revision and rewrites its
// auth file, which is how the host learns that a cached model list is stale.
// A global model toggle changes every auth's visible set without editing any
// credential, so nothing else would invalidate those caches.
func (s *Service) republishAllAuths(reason string) {
	s.mu.Lock()
	updates := make([]Credential, 0, len(s.creds))
	for id, c := range s.creds {
		c.ModelRevision = revisionOf(c.AdvertisedModels(s.config()))
		s.creds[id] = c
		updates = append(updates, c)
	}
	files := make(map[string]string, len(s.files))
	for id, name := range s.files {
		files[id] = name
	}
	authDir := s.authDir
	s.mu.Unlock()

	for _, c := range updates {
		fileName := files[c.ID]
		if fileName == "" {
			fileName = s.authFileName(c.ID)
		}
		if err := s.persistCredential(c, authDir, fileName); err != nil {
			s.log("warn", "", c.ID, 0, 0, "republish auth ("+reason+"): "+err.Error())
		}
	}
	s.refreshHostModels()
}

// writeSettingsLocked persists the whole config so a restart reproduces what
// the operator sees now. The caller holds configMu.
func writeSettingsLocked(cfg Config) error {
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return describeErr("create data dir %s: %w", cfg.DataDir, err)
	}
	return writeFileAtomic(cfg.DataDir, settingsFile, jsonBytesIndent(cfg))
}

// managementLogsQuery serves the log page: newest first, filtered and paged in
// the plugin so the browser never has to hold the whole ring.
func (s *Service) managementLogsQuery(method string, query map[string][]string) (any, error) {
	s.mu.RLock()
	logs := s.logs
	s.mu.RUnlock()
	entries := []logEntry{}
	if logs != nil {
		entries = logs.snapshot()
	}

	get := func(key string) string {
		if v, ok := query[key]; ok && len(v) > 0 {
			return strings.TrimSpace(v[0])
		}
		return ""
	}
	level := strings.ToLower(get("level"))
	model := strings.ToLower(get("model"))
	authID := strings.ToLower(get("auth_id"))
	search := strings.ToLower(get("q"))
	kind := strings.ToLower(get("kind"))
	// since is an RFC3339 cutoff. Relative windows are resolved in the browser
	// so the browser's clock decides what "today" means, not the server's zone.
	since := get("since")
	var sinceTime time.Time
	if since != "" && since != "all" {
		if parsed, err := time.Parse(time.RFC3339, since); err == nil {
			sinceTime = parsed
		}
	}

	filtered := make([]logEntry, 0, len(entries))
	for _, e := range entries {
		if level != "" && level != "all" && strings.ToLower(e.Level) != level {
			continue
		}
		if kind != "" && kind != "all" && strings.ToLower(e.Kind) != kind {
			continue
		}
		if !sinceTime.IsZero() {
			ts, err := time.Parse(time.RFC3339, e.Time)
			if err != nil || ts.Before(sinceTime) {
				continue
			}
		}
		if model != "" && model != "all" && strings.ToLower(e.Model) != model {
			continue
		}
		if authID != "" && authID != "all" && strings.ToLower(e.AuthID) != authID {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(e.Detail), search) &&
			!strings.Contains(strings.ToLower(e.Model), search) {
			continue
		}
		filtered = append(filtered, e)
	}

	// Newest first: the page opens on what just happened.
	for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
		filtered[i], filtered[j] = filtered[j], filtered[i]
	}

	limit := atoiDefault(get("limit"), 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	offset := atoiDefault(get("offset"), 0)
	if offset < 0 {
		offset = 0
	}
	total := len(filtered)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	// Facets drive the filter dropdowns, so they must reflect every entry in
	// the selected time window and not just the current page.
	levels := map[string]int{}
	models := map[string]int{}
	auths := map[string]int{}
	for _, e := range entries {
		if !sinceTime.IsZero() {
			ts, err := time.Parse(time.RFC3339, e.Time)
			if err != nil || ts.Before(sinceTime) {
				continue
			}
		}
		if e.Level != "" {
			levels[e.Level]++
		}
		if e.Model != "" {
			models[e.Model]++
		}
		if e.AuthID != "" {
			auths[e.AuthID]++
		}
	}

	return managementJSON(http.StatusOK, map[string]any{
		"logs":   filtered[offset:end],
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"facets": map[string]any{
			"levels": levels, "models": models, "auth_ids": auths,
		},
		"summary": summariseLogs(filtered),
		// durable covers the same window from the persisted minute buckets, so a
		// window longer than the retention still reports real totals instead of
		// whatever happens to still be in memory.
		"durable":   s.durableTotals(sinceTime, time.Time{}),
		"retention": s.config().LogRetention,
		"storage":   s.storageState(),
	})
}

// durableTotals renders the persisted statistics for [start, end).
func (s *Service) durableTotals(start, end time.Time) map[string]any {
	s.mu.RLock()
	stats := s.stats
	s.mu.RUnlock()
	if stats == nil {
		return map[string]any{"available": false}
	}
	total, since, ok := stats.rangeTotals(start, end)
	out := map[string]any{
		"available":         ok,
		"requests":          total.Requests,
		"failures":          total.Failures,
		"prompt_tokens":     total.PromptTokens,
		"cached_tokens":     total.CachedTokens,
		"completion_tokens": total.CompletionTokens,
		"total_tokens":      total.PromptTokens + total.CompletionTokens,
		"cost_usd":          float64(total.CostMicroUSD) / 1e6,
		"resolution":        "minute",
		"collecting_since":  since,
		"persistence_error": stats.lastError(),
	}
	if total.PromptTokens > 0 {
		out["cache_hit_rate"] = math.Round(float64(total.CachedTokens)/float64(total.PromptTokens)*1000) / 10
	}
	if total.RateDurationMS > 0 {
		out["avg_tokens_per_second"] = math.Round(float64(total.RateTokens)*1000/float64(total.RateDurationMS)*10) / 10
	}
	if total.FirstByteSamples > 0 {
		out["avg_first_byte_ms"] = total.FirstByteMS / total.FirstByteSamples
	}
	if total.Requests > 0 {
		out["avg_ms"] = total.DurationMS / total.Requests
	}
	return out
}

// storageState reports whether the log and the counters are reaching disk, so
// a read-only data directory shows up in the console instead of being silent.
func (s *Service) storageState() map[string]any {
	cfg := s.config()
	s.mu.RLock()
	logs := s.logs
	stats := s.stats
	s.mu.RUnlock()
	out := map[string]any{"dir": cfg.DataDir}
	if logs != nil {
		out["log_error"] = logs.lastError()
	}
	if stats != nil {
		out["stats_error"] = stats.lastError()
	}
	return out
}

// logSummary is the header row of numbers above the log table. It describes
// the whole filtered set, not the visible page, so it stays stable while
// paging and answers "how did this slice behave".
type logSummary struct {
	Requests         int     `json:"requests"`
	Failures         int     `json:"failures"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CachedTokens     int64   `json:"cached_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	TotalTokens      int64   `json:"total_tokens"`
	CacheHitRate     float64 `json:"cache_hit_rate"`
	AvgTokensPerSec  float64 `json:"avg_tokens_per_second"`
	AvgFirstByteMs   int64   `json:"avg_first_byte_ms"`
	AvgMillis        int64   `json:"avg_ms"`
	CostUSD          float64 `json:"cost_usd"`
}

// summariseLogs aggregates the entries the request columns apply to. Only
// request rows take part: a settings change has no tokens and no latency, and
// folding it in would drag the averages toward zero.
func summariseLogs(entries []logEntry) logSummary {
	var out logSummary
	var withSpeed, withFirstByte, timed int
	for _, e := range entries {
		if e.Kind != KindRequest {
			continue
		}
		out.Requests++
		if e.Status >= 400 || e.Level == "error" {
			out.Failures++
		}
		out.PromptTokens += int64(e.PromptTokens)
		out.CachedTokens += int64(e.CachedTokens)
		out.CompletionTokens += int64(e.CompletionTokens)
		out.TotalTokens += int64(e.TotalTokens)
		out.CostUSD += e.CostUSD
		if e.Millis > 0 {
			out.AvgMillis += e.Millis
			timed++
		}
		if e.FirstByteMs > 0 {
			out.AvgFirstByteMs += e.FirstByteMs
			withFirstByte++
		}
		if e.TokensPerSecond > 0 {
			out.AvgTokensPerSec += e.TokensPerSecond
			withSpeed++
		}
	}
	if out.PromptTokens > 0 {
		out.CacheHitRate = math.Round(float64(out.CachedTokens)/float64(out.PromptTokens)*1000) / 10
	}
	if timed > 0 {
		out.AvgMillis /= int64(timed)
	}
	if withFirstByte > 0 {
		out.AvgFirstByteMs /= int64(withFirstByte)
	}
	if withSpeed > 0 {
		out.AvgTokensPerSec = math.Round(out.AvgTokensPerSec/float64(withSpeed)*10) / 10
	}
	out.CostUSD = math.Round(out.CostUSD*1e6) / 1e6
	return out
}

// atoiDefault parses a query value, falling back when it is absent or junk.
func atoiDefault(raw string, fallback int) int {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return v
}

// nonNilStrings keeps []string from serializing as null in the console, which
// would render as "null" instead of an empty list.
func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

// managementExport returns the whole log ring as newline-delimited JSON, which
// is what an operator pastes into a bug report.
func (s *Service) managementExportLogs() (any, error) {
	s.mu.RLock()
	logs := s.logs
	s.mu.RUnlock()
	entries := []logEntry{}
	if logs != nil {
		entries = logs.snapshot()
	}
	var b strings.Builder
	for _, e := range entries {
		line, err := json.Marshal(e)
		if err != nil {
			continue
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	return map[string]any{
		"StatusCode": http.StatusOK,
		"Headers": http.Header{
			"Content-Type":        []string{"application/x-ndjson; charset=utf-8"},
			"Content-Disposition": []string{`attachment; filename="riverbridge-logs.ndjson"`},
			"Cache-Control":       []string{"no-store"},
		},
		"Body": []byte(b.String()),
	}, nil
}

// settingsPath is where writeSettingsLocked puts settings.json.
func settingsPath(cfg Config) string { return filepath.Join(cfg.DataDir, settingsFile) }

// managementStatistics serves the durable counters. Unlike /logs this reads the
// persisted minute buckets, so a window longer than the log retention still
// reports real totals: the log is a rolling window, the counters are not.
func (s *Service) managementStatistics(query url.Values) (any, error) {
	get := func(key string) string {
		if v, ok := query[key]; ok && len(v) > 0 {
			return strings.TrimSpace(v[0])
		}
		return ""
	}
	parse := func(key string) (time.Time, error) {
		raw := get(key)
		if raw == "" {
			return time.Time{}, nil
		}
		ts, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s must be an RFC3339 timestamp", key)
		}
		return ts.UTC(), nil
	}
	start, err := parse("start")
	if err != nil {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	end, err := parse("end")
	if err != nil {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	if !start.IsZero() && !end.IsZero() && !end.After(start) {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "end must be after start"})
	}

	body := s.durableTotals(start, end)
	// A zero window means "everything ever counted", which is what the console
	// shows on the 全部 tab.
	s.mu.RLock()
	stats := s.stats
	s.mu.RUnlock()
	if stats != nil {
		days, since, ok := stats.dayIndex()
		body["days"] = days
		body["available"] = ok
		if start.IsZero() && end.IsZero() {
			body["collecting_since"] = since
		}
	}
	return managementJSON(http.StatusOK, body)
}

// managementResetStatistics starts a new counting generation. The old files
// stay on disk as a rollback copy; no logs or credentials are touched.
func (s *Service) managementResetStatistics(body []byte) (any, error) {
	var req struct {
		Confirm bool `json:"confirm"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}
	if !req.Confirm {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "请确认清空统计数据"})
	}
	s.mu.RLock()
	stats := s.stats
	s.mu.RUnlock()
	if stats == nil {
		return managementJSON(http.StatusInternalServerError, map[string]string{"error": "statistics unavailable"})
	}
	since, err := stats.reset()
	if err != nil {
		return managementJSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	s.log("info", "", "", http.StatusOK, 0, "statistics reset")
	return managementJSON(http.StatusOK, map[string]any{"ok": true, "since": since})
}
