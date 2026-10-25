package bridge

import (
	"encoding/json"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// apiBase is the plugin's Management API prefix. CPA only allows plugins to
// register routes under /v0/management/.
const apiBase = "/v0/management/" + PluginID

// registerManagement declares the console route plus the JSON API.
func (s *Service) registerManagement(raw json.RawMessage) (any, error) {
	routes := []map[string]string{}
	for _, p := range []string{"status", "credentials", "models", "logs", "logs/export", "usage", "settings"} {
		routes = append(routes, map[string]string{"Method": "GET", "Path": apiBase + "/" + p})
	}
	for _, p := range []string{"credentials", "credentials/test", "credentials/model",
		"models/refresh", "usage/reset", "logs/clear", "settings"} {
		routes = append(routes, map[string]string{"Method": "POST", "Path": apiBase + "/" + p})
	}
	for _, p := range []string{"credentials"} {
		routes = append(routes, map[string]string{"Method": "PATCH", "Path": apiBase + "/" + p})
		routes = append(routes, map[string]string{"Method": "PUT", "Path": apiBase + "/" + p})
	}
	routes = append(routes, map[string]string{"Method": "DELETE", "Path": apiBase + "/credentials"})
	return map[string]any{
		"routes": routes,
		"resources": []map[string]string{{
			"Path":        "/console",
			"Menu":        "RiverBridge",
			"Description": "River API keys, river- models, routing strategy and token usage",
		}},
	}, nil
}

// handleManagement serves both the HTML console and the JSON API.
func (s *Service) handleManagement(raw json.RawMessage) (any, error) {
	var r struct {
		Method  string
		Path    string
		Headers http.Header
		Query   url.Values
		Body    []byte
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	if r.Method == http.MethodGet && r.Path == "/v0/resource/plugins/"+PluginID+"/console" {
		// The console is served under /v0/resource/plugins/<id>/ where the
		// management guard does not apply, so its XHRs must learn the API base
		// from the page itself instead of hardcoding it.
		return managementHTML(strings.ReplaceAll(consoleHTML, "__RIVER_API_BASE__", apiBase))
	}
	if !strings.HasPrefix(r.Path, apiBase) {
		return managementJSON(http.StatusNotFound, map[string]string{"error": "not found"})
	}
	p := strings.TrimPrefix(r.Path, apiBase)
	if p == "" {
		p = "/"
	}

	switch r.Method + " " + p {
	case "GET /status":
		return s.managementStatus()
	case "GET /credentials":
		return s.managementCredentials()
	case "POST /credentials", "PUT /credentials":
		return s.managementAddCredential(r.Body)
	case "PATCH /credentials":
		return s.managementPatchCredential(r.Body)
	case "POST /credentials/model":
		return s.managementSetModelState(r.Body)
	case "DELETE /credentials":
		return s.managementDeleteCredential(r.Query)
	case "POST /credentials/test":
		return s.managementTestCredential(r.Body)
	case "GET /models":
		return s.managementModels()
	case "POST /models/refresh":
		return s.managementRefreshModels(r.Body)
	case "GET /logs":
		return s.managementLogsQuery(r.Method, r.Query)
	case "GET /logs/export":
		return s.managementExportLogs()
	case "POST /logs/clear":
		s.mu.RLock()
		logs := s.logs
		s.mu.RUnlock()
		if logs != nil {
			logs.clear()
		}
		return managementJSON(http.StatusOK, map[string]any{"ok": true})
	case "GET /usage":
		return s.managementUsage()
	case "POST /usage/reset":
		return s.managementResetUsage(r.Body)
	case "GET /settings":
		return s.managementSettings()
	case "POST /settings":
		return s.managementSaveSettings(r.Body)
	}
	return managementJSON(http.StatusNotFound, map[string]string{"error": "unknown route: " + r.Method + " " + p})
}

func managementJSON(status int, v any) (any, error) {
	return map[string]any{
		"StatusCode": status,
		"Headers": http.Header{
			"Content-Type":  []string{"application/json; charset=utf-8"},
			"Cache-Control": []string{"no-store"},
		},
		"Body": jsonBytes(v),
	}, nil
}

func managementHTML(body string) (any, error) {
	return map[string]any{
		"StatusCode": http.StatusOK,
		"Headers": http.Header{
			"Content-Type":           []string{"text/html; charset=utf-8"},
			"Cache-Control":          []string{"no-store"},
			"X-Content-Type-Options": []string{"nosniff"},
		},
		"Body": []byte(body),
	}, nil
}

// managementStatus describes the plugin to the console.
func (s *Service) managementStatus() (any, error) {
	cfg := s.config()
	s.mu.RLock()
	count := len(s.creds)
	authDir := s.authDir
	s.mu.RUnlock()
	return managementJSON(http.StatusOK, map[string]any{
		"name":         Name,
		"version":      Version,
		"provider":     Provider,
		"model_prefix": ModelPrefix,
		"base_url":     cfg.BaseURL,
		"auth_dir":     authDir,
		"data_dir":     cfg.DataDir,
		"credentials":  count,
		"models":       len(catalogueModels()),
		"uptime_s":     int64(time.Since(s.started).Seconds()),
		"strategy":     cfg.Strategy,
		"scheduler":    s.schedulerState(),
	})
}

// credentialView is one card in the console.
type credentialView struct {
	ID             string            `json:"id"`
	Remark         string            `json:"remark"`
	Label          string            `json:"label"`
	FileName       string            `json:"file_name"`
	APIKeyMasked   string            `json:"api_key_masked"`
	BaseURL        string            `json:"base_url"`
	Disabled       bool              `json:"disabled"`
	Priority       int               `json:"priority"`
	DisabledModels []string          `json:"disabled_models"`
	ModelCount     int               `json:"model_count"`
	Models         []DiscoveredModel `json:"models"`
	CreatedAt      string            `json:"created_at"`
	UpdatedAt      string            `json:"updated_at"`
	Usage          usageStat         `json:"usage"`
	LastError      string            `json:"last_error,omitempty"`
}

func (s *Service) managementCredentials() (any, error) {
	s.mu.RLock()
	creds := make([]Credential, 0, len(s.creds))
	files := make(map[string]string, len(s.files))
	for id, c := range s.creds {
		creds = append(creds, c)
		files[id] = s.files[id]
	}
	s.mu.RUnlock()

	stats := map[string]usageStat{}
	s.mu.RLock()
	store := s.usage
	s.mu.RUnlock()
	if store != nil {
		stats = store.Snapshot()
	}

	cfg := s.config()
	views := make([]credentialView, 0, len(creds))
	for _, c := range creds {
		views = append(views, credentialView{
			ID:             c.ID,
			Remark:         c.Remark,
			Label:          labelOf(c),
			FileName:       files[c.ID],
			APIKeyMasked:   maskKey(c.APIKey),
			BaseURL:        c.BaseURLOrDefault(),
			Disabled:       c.Disabled,
			Priority:       c.Priority,
			DisabledModels: nonNilStrings(c.DisabledModels),
			ModelCount:     len(c.AdvertisedModels(cfg)),
			Models:         c.AdvertisedModels(cfg),
			CreatedAt:      c.CreatedAt,
			UpdatedAt:      c.UpdatedAt,
			Usage:          stats[c.ID],
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
	return managementJSON(http.StatusOK, map[string]any{"credentials": views, "count": len(views)})
}

// managementAddCredential validates a key upstream, then persists it as an auth
// file so the host's watcher registers it like any other credential.
func (s *Service) managementAddCredential(body []byte) (any, error) {
	var req struct {
		APIKey  string `json:"api_key"`
		Label   string `json:"label"`
		Remark  string `json:"remark"`
		BaseURL string `json:"base_url"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
	}
	key := strings.TrimSpace(req.APIKey)
	if key == "" {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "api_key is required"})
	}
	if !strings.HasPrefix(key, "rv_") {
		return managementJSON(http.StatusBadRequest,
			map[string]string{"error": "River API keys start with rv_"})
	}

	// Probe before persisting so an unusable key never reaches the auth dir.
	models, err := s.ValidateKey(key, req.BaseURL)
	if err != nil {
		_, status := HostErrorDetails(err)
		if status == 0 {
			status = http.StatusBadGateway
		}
		s.log("warn", "", "", status, 0, "key rejected: "+err.Error())
		return managementJSON(status, map[string]string{"error": err.Error()})
	}

	cfg := s.config()
	id := CredentialID(key)
	label := strings.TrimSpace(req.Label)
	if label == "" {
		label = id
	}
	remark := strings.TrimSpace(req.Remark)
	stamp := nowStamp()

	s.mu.RLock()
	existing, ok := s.creds[id]
	authDir := s.authDir
	s.mu.RUnlock()

	c := Credential{
		Type:          Provider,
		ID:            id,
		Label:         label,
		Remark:        remark,
		APIKey:        key,
		BaseURL:       strings.TrimSpace(req.BaseURL),
		Models:        models,
		ModelRevision: revisionOf(models),
		UpdatedAt:     stamp,
		CreatedAt:     stamp,
	}
	if ok && existing.CreatedAt != "" {
		c.CreatedAt = existing.CreatedAt
	}
	if c.BaseURL == "" && ok {
		c.BaseURL = existing.BaseURL
	}

	fileName := s.authFileName(id)

	// Persist through the host rather than writing the auth directory directly:
	// the host resolves the directory itself and reports where the file landed,
	// so adding a key works even on a cold start before any auth file has been
	// parsed (auth.parse is the only callback that carries the directory).
	var saved struct {
		Path string `json:"path"`
	}
	err = s.call("host.auth.save", map[string]any{
		"name": fileName,
		// The host decodes this as a JSON object, so it must be sent as raw
		// JSON rather than a base64-encoded byte slice.
		"json": json.RawMessage(jsonBytes(c)),
	}, &saved)
	if err != nil {
		// Fall back to the directory learned from a previous parse, when there
		// is one; otherwise the credential would silently not exist on disk.
		if authDir == "" {
			s.log("error", "", id, 0, 0, "persist auth file: "+err.Error())
			return managementJSON(http.StatusInternalServerError,
				map[string]string{"error": "CPA could not store the auth file: " + err.Error()})
		}
		if writeErr := writeFileAtomic(authDir, fileName, jsonBytesIndent(c)); writeErr != nil {
			s.log("error", "", id, 0, 0, "write auth file: "+writeErr.Error())
			return managementJSON(http.StatusInternalServerError,
				map[string]string{"error": "writing the auth file failed: " + writeErr.Error()})
		}
	} else if saved.Path != "" {
		authDir = filepath.Dir(saved.Path)
		fileName = filepath.Base(saved.Path)
	}

	// Update the in-memory view immediately; the watcher callback confirms it.
	s.mu.Lock()
	s.creds[id] = c
	s.files[id] = fileName
	if authDir != "" {
		s.authDir = authDir
	}
	s.mu.Unlock()

	// Re-register models with the host so the new auth's models appear at once.
	s.refreshHostModels()

	s.log("info", "", id, http.StatusOK, 0, "credential saved with "+itoa(len(models))+" model(s)")
	_ = cfg
	return managementJSON(http.StatusOK, map[string]any{
		"ok":        true,
		"id":        id,
		"label":     label,
		"file_name": fileName,
		"models":    models,
		"count":     len(models),
	})
}

func (s *Service) managementDeleteCredential(query url.Values) (any, error) {
	id := strings.TrimSpace(query.Get("id"))
	if id == "" {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "id is required"})
	}
	s.mu.RLock()
	_, ok := s.creds[id]
	fileName := s.files[id]
	authDir := s.authDir
	s.mu.RUnlock()
	if !ok {
		return managementJSON(http.StatusNotFound, map[string]string{"error": "no such credential"})
	}
	if fileName == "" {
		fileName = s.authFileName(id)
	}
	if authDir != "" {
		if err := removeFile(authDir, fileName); err != nil {
			return managementJSON(http.StatusInternalServerError,
				map[string]string{"error": "removing the auth file failed: " + err.Error()})
		}
	}
	s.mu.Lock()
	delete(s.creds, id)
	delete(s.files, id)
	s.revoked[id] = true
	s.mu.Unlock()
	if store := s.usage; store != nil {
		store.Reset(id)
	}
	s.refreshHostModels()
	s.log("info", "", id, http.StatusOK, 0, "credential removed")
	return managementJSON(http.StatusOK, map[string]any{"ok": true, "id": id})
}

// managementTestCredential re-validates a key without changing anything.
func (s *Service) managementTestCredential(body []byte) (any, error) {
	var req struct {
		ID      string `json:"id"`
		APIKey  string `json:"api_key"`
		BaseURL string `json:"base_url"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
	}
	key := strings.TrimSpace(req.APIKey)
	if key == "" && req.ID != "" {
		s.mu.RLock()
		if c, ok := s.creds[req.ID]; ok {
			key = c.APIKey
			if req.BaseURL == "" {
				req.BaseURL = c.BaseURL
			}
		}
		s.mu.RUnlock()
	}
	models, err := s.ValidateKey(key, req.BaseURL)
	if err != nil {
		_, status := HostErrorDetails(err)
		if status == 0 {
			status = http.StatusBadGateway
		}
		return managementJSON(status, map[string]any{"ok": false, "error": err.Error()})
	}
	return managementJSON(http.StatusOK, map[string]any{
		"ok": true, "models": models, "count": len(models),
	})
}

// managementModels lists every model, annotated with which keys can reach it.
func (s *Service) managementModels() (any, error) {
	s.mu.RLock()
	creds := make([]Credential, 0, len(s.creds))
	for _, c := range s.creds {
		if !c.Disabled {
			creds = append(creds, c)
		}
	}
	s.mu.RUnlock()

	type modelView struct {
		ID              string   `json:"id"`
		Upstream        string   `json:"upstream"`
		ContextLength   int      `json:"context_length"`
		MaxOutputTokens int      `json:"max_output_tokens"`
		Credentials     []string `json:"credentials"`
	}
	index := map[string]*modelView{}
	for _, c := range creds {
		source := c.Models
		if len(source) == 0 {
			source = catalogueModels()
		}
		for _, m := range source {
			view, ok := index[m.ID]
			if !ok {
				view = &modelView{
					ID: m.ID, Upstream: m.Upstream,
					ContextLength: m.ContextLength, MaxOutputTokens: m.MaxOutputTokens,
				}
				index[m.ID] = view
			}
			if m.ContextLength > view.ContextLength {
				view.ContextLength = m.ContextLength
			}
			if m.MaxOutputTokens > view.MaxOutputTokens {
				view.MaxOutputTokens = m.MaxOutputTokens
			}
			view.Credentials = append(view.Credentials, c.ID)
		}
	}
	out := make([]modelView, 0, len(index))
	for _, v := range index {
		sort.Strings(v.Credentials)
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return managementJSON(http.StatusOK, map[string]any{"models": out, "count": len(out)})
}

// managementRefreshModels re-queries River for one key, or for all when no id
// is given.
func (s *Service) managementRefreshModels(body []byte) (any, error) {
	var req struct {
		ID string `json:"id"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}
	s.mu.RLock()
	targets := make([]Credential, 0, len(s.creds))
	for id, c := range s.creds {
		if req.ID == "" || id == req.ID {
			targets = append(targets, c)
		}
	}
	s.mu.RUnlock()
	if len(targets) == 0 {
		return managementJSON(http.StatusNotFound, map[string]string{"error": "no matching credential"})
	}

	type result struct {
		ID    string `json:"id"`
		OK    bool   `json:"ok"`
		Count int    `json:"count"`
		Error string `json:"error,omitempty"`
	}
	results := make([]result, 0, len(targets))
	for _, c := range targets {
		models, err := s.discoverModels(c)
		if err != nil {
			results = append(results, result{ID: c.ID, Error: err.Error()})
			continue
		}
		c.Models = models
		c.ModelRevision = revisionOf(models)
		c.UpdatedAt = nowStamp()
		s.mu.Lock()
		s.creds[c.ID] = c
		fileName := s.files[c.ID]
		authDir := s.authDir
		s.mu.Unlock()
		if authDir != "" && fileName != "" {
			_ = writeFileAtomic(authDir, fileName, jsonBytesIndent(c))
		}
		results = append(results, result{ID: c.ID, OK: true, Count: len(models)})
	}
	s.refreshHostModels()
	return managementJSON(http.StatusOK, map[string]any{"results": results})
}

func (s *Service) managementUsage() (any, error) {
	s.mu.RLock()
	store := s.usage
	s.mu.RUnlock()
	stats := map[string]usageStat{}
	if store != nil {
		stats = store.Snapshot()
	}
	var totals usageStat
	for _, v := range stats {
		totals.Requests += v.Requests
		totals.Failures += v.Failures
		totals.InputTokens += v.InputTokens
		totals.CachedTokens += v.CachedTokens
		totals.OutputTokens += v.OutputTokens
		totals.ReasoningTokens += v.ReasoningTokens
		totals.CostUSD += v.CostUSD
	}
	return managementJSON(http.StatusOK, map[string]any{
		"stats": stats, "totals": totals, "count": len(stats),
	})
}

func (s *Service) managementResetUsage(body []byte) (any, error) {
	var req struct {
		ID string `json:"id"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}
	s.mu.RLock()
	store := s.usage
	s.mu.RUnlock()
	if store != nil {
		store.Reset(strings.TrimSpace(req.ID))
	}
	return managementJSON(http.StatusOK, map[string]any{"ok": true})
}

// refreshHostModels asks the host to re-read this provider's auth files so the
// advertised model list tracks the credentials.
func (s *Service) refreshHostModels() {
	var out struct{}
	if err := s.call("host.log", map[string]any{
		"level":   "debug",
		"message": "riverbridge credential set changed; re-registering models",
	}, &out); err != nil {
		// Logging is best effort; a failure here must not break the caller.
		_ = err
	}
}

// maskKey renders a key for display without leaking it.
func maskKey(key string) string {
	if len(key) <= 12 {
		return "rv_****"
	}
	return key[:5] + strings.Repeat("*", 8) + key[len(key)-4:]
}
