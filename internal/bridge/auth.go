package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// authParseRequest mirrors pluginapi.AuthParseRequest for the fields this
// plugin reads.
type authParseRequest struct {
	Provider string
	Path     string
	FileName string
	RawJSON  []byte
	Host     struct {
		AuthDir string
	}
}

// authData builds the auth record the host stores and later hands back.
func authData(c Credential, filename string) map[string]any {
	if filename == "" {
		filename = c.ID + ".json"
	}
	return map[string]any{
		"Provider":    Provider,
		"ID":          strings.TrimSuffix(filename, ".json"),
		"FileName":    filename,
		"Label":       labelOf(c),
		"Disabled":    c.Disabled,
		"ProxyURL":    c.ProxyURL,
		"StorageJSON": jsonBytes(c),
		"Metadata": map[string]any{
			"type":           Provider,
			"base_url":       c.BaseURLOrDefault(),
			"model_count":    len(c.Models),
			"model_revision": c.ModelRevision,
		},
		"Attributes": map[string]string{"auth_kind": "api_key"},
	}
}

// labelOf is the name shown to the host, which the console mirrors. Remark
// wins over the labels written by 0.1.0.
func labelOf(c Credential) string { return c.DisplayName() }

// CredentialID derives a stable id from the key itself so re-adding the same
// key maps onto the same auth record instead of creating a duplicate.
func CredentialID(apiKey string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(apiKey)))
	return "river-" + hex.EncodeToString(sum[:])[:16]
}

// parseAuth accepts a River auth file, refreshes its model cache when the key
// changed, and returns the record for the host to store.
func (s *Service) parseAuth(raw json.RawMessage) (any, error) {
	var req authParseRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	// The watcher can deliver a callback queued before a newer edit landed, so
	// re-read the physical file while holding the auth lock.
	dir := req.Host.AuthDir
	if dir == "" {
		s.mu.RLock()
		dir = s.authDir
		s.mu.RUnlock()
	}
	resolvedFile := false
	if dir != "" && req.FileName != "" && isPlainFileName(req.FileName) {
		if latest, err := readFile(dir, req.FileName); err == nil {
			req.RawJSON = latest
			resolvedFile = true
		}
	}

	var c Credential
	if err := json.Unmarshal(req.RawJSON, &c); err != nil || !looksLikeRiverAuth(c) {
		return map[string]any{"Handled": false}, nil
	}
	if strings.TrimSpace(c.APIKey) == "" {
		return nil, fail(401, "River credential has no api_key")
	}
	if c.ID == "" {
		if req.FileName != "" {
			c.ID = strings.TrimSuffix(req.FileName, ".json")
		} else {
			c.ID = CredentialID(c.APIKey)
		}
	}
	c.Type = Provider
	if c.UpdatedAt == "" {
		c.UpdatedAt = time.Now().Format(time.RFC3339)
	}

	s.mu.Lock()
	previous, existed := s.creds[c.ID]
	s.creds[c.ID] = c
	if req.FileName != "" {
		s.files[c.ID] = req.FileName
	}
	if dir != "" {
		s.authDir = dir
	}
	s.mu.Unlock()

	// Keep the cached model list when the key is unchanged. This avoids an
	// upstream round trip on every watcher tick and keeps model ids stable.
	if existed && previous.APIKey == c.APIKey && len(previous.Models) > 0 {
		c.Models = previous.Models
		c.ModelRevision = previous.ModelRevision
		s.mu.Lock()
		s.creds[c.ID] = c
		s.mu.Unlock()
	} else {
		refreshed, err := s.refreshModelsLocked(c)
		if err != nil {
			s.log("warn", "", c.ID, 0, 0, "model discovery failed: "+err.Error())
			// Keep the credential usable; serving still works because the model
			// list is derived from /v1/models at registration time.
		} else {
			c = refreshed
		}
	}

	if resolvedFile {
		s.mu.Lock()
		delete(s.revoked, c.ID)
		s.mu.Unlock()
	}
	return map[string]any{"Handled": true, "Auth": authData(c, req.FileName)}, nil
}

// refreshAuth re-emits the stored record so the host re-registers its models.
func (s *Service) refreshAuth(raw json.RawMessage) (any, error) {
	var req struct{ StorageJSON []byte }
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	var c Credential
	if err := json.Unmarshal(req.StorageJSON, &c); err != nil {
		return nil, err
	}
	s.mu.RLock()
	current, ok := s.creds[c.ID]
	filename := s.files[c.ID]
	revoked := s.revoked[c.ID]
	s.mu.RUnlock()
	if revoked {
		return nil, fail(401, "River credential was removed")
	}
	if ok {
		c = current
	}
	if filename == "" {
		filename = c.ID + ".json"
	}
	return map[string]any{
		"Auth":             authData(c, filename),
		"NextRefreshAfter": time.Now().Add(365 * 24 * time.Hour),
	}, nil
}

// modelsForAuth advertises the river- models this credential can reach.
func (s *Service) modelsForAuth(raw json.RawMessage) (any, error) {
	var req struct {
		AuthID      string
		StorageJSON []byte
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	var c Credential
	if len(req.StorageJSON) > 0 {
		if err := json.Unmarshal(req.StorageJSON, &c); err != nil {
			return nil, err
		}
	}
	s.mu.RLock()
	if current, ok := s.creds[c.ID]; ok && current.APIKey == c.APIKey {
		if len(current.Models) > 0 {
			c = current
		}
	}
	s.mu.RUnlock()
	if len(c.Models) == 0 {
		// Nothing cached yet: fall back to the built-in catalogue so the model
		// list is never empty for a freshly added key.
		c.Models = catalogueModels()
	}
	// Advertise only what the operator left enabled for this key, so a
	// switched-off model disappears from /v1/models instead of failing later.
	// The same helper feeds the revision hash, keeping both in one place.
	return modelRegistration(c.AdvertisedModels(s.config())), nil
}

// modelRegistration builds the host model payload.
func modelRegistration(models []DiscoveredModel) map[string]any {
	out := make([]map[string]any, 0, len(models))
	for _, m := range models {
		entry := map[string]any{
			"ID":                         m.ID,
			"Name":                       m.Upstream,
			"Object":                     "model",
			"OwnedBy":                    Provider,
			"DisplayName":                m.ID,
			"SupportedGenerationMethods": []string{"chat"},
			"UserDefined":                true,
		}
		if m.ContextLength > 0 {
			entry["InputTokenLimit"] = m.ContextLength
			entry["ContextLength"] = m.ContextLength
		}
		if m.MaxOutputTokens > 0 {
			entry["OutputTokenLimit"] = m.MaxOutputTokens
			entry["MaxCompletionTokens"] = m.MaxOutputTokens
		}
		out = append(out, entry)
	}
	return map[string]any{"Provider": Provider, "Models": out}
}

// modelStatic is the fallback registration when the host asks for a static
// list before any credential has been parsed.
func (s *Service) modelRegistration() map[string]any {
	return modelRegistration(catalogueModels())
}

// catalogueModels lists every River model this build knows about.
func catalogueModels() []DiscoveredModel {
	order := []string{
		"deepseek-ai/DeepSeek-V4.1-Flash-1017",
		"deepseek-ai/DeepSeek-V4.1-Flash",
		"zai-org/GLM-5.3-Flash",
		"nvidia/NVIDIA-Nemotron-3.5-Super-VL-120B-A12B-NVFP4",
		"moonshotai/Kimi-K3",
		"Qwen/Qwen3.8-27B-FP8",
	}
	out := make([]DiscoveredModel, 0, len(order))
	for _, upstream := range order {
		m := DiscoveredModel{ID: ModelID(upstream), Upstream: upstream}
		if p, ok := lookupPrice(upstream); ok {
			m.ContextLength = p.Context
			m.MaxOutputTokens = p.MaxOutput
		}
		out = append(out, m)
	}
	return out
}

// ModelID converts an upstream River model id into the plugin-facing name.
// "deepseek-ai/DeepSeek-V4.1-Flash-1017" becomes
// "river-deepseek-v4.1-flash-1017": the vendor prefix is dropped so the name
// stays short, and the result is lowercase because CPA model ids are matched
// case-sensitively by clients.
func ModelID(upstream string) string {
	name := upstream
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	name = strings.ToLower(strings.TrimSpace(name))
	if strings.HasPrefix(name, ModelPrefix) {
		return name
	}
	return ModelPrefix + name
}

// resolveUpstream maps a plugin-facing model id back to River's model id.
func resolveUpstream(model string) (string, bool) {
	name := strings.TrimSpace(model)
	if !strings.HasPrefix(name, ModelPrefix) {
		return "", false
	}
	slug := strings.TrimPrefix(name, ModelPrefix)
	for upstream := range riverPrices {
		if strings.EqualFold(ModelID(upstream), ModelPrefix+slug) {
			return upstream, true
		}
	}
	return "", false
}

// looksLikeRiverAuth reports whether a parsed file belongs to this plugin.
func looksLikeRiverAuth(c Credential) bool {
	if c.Type == Provider {
		return true
	}
	// Files without a type are adopted when they carry a River-shaped key.
	return c.Type == "" && strings.HasPrefix(strings.TrimSpace(c.APIKey), "rv_")
}

func isPlainFileName(name string) bool {
	return name == baseName(name) && !strings.ContainsAny(name, `/\`)
}

func baseName(name string) string {
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		return name[i+1:]
	}
	return name
}

// httpClient returns a client honouring the configured request timeout.
func (s *Service) httpClient() *http.Client {
	cfg := s.config()
	return &http.Client{Timeout: time.Duration(cfg.RequestTimeoutSeconds) * time.Second}
}

// log records one diagnostic line.
func (s *Service) log(level, model, authID string, status int, ms int64, detail string) {
	s.mu.RLock()
	logs := s.logs
	s.mu.RUnlock()
	if logs == nil {
		return
	}
	logs.add(logEntry{
		Kind:   KindEvent,
		Level:  level,
		Model:  model,
		AuthID: authID,
		Status: status,
		Millis: ms,
		Detail: detail,
	})
}

// logRequest records one upstream call with the latency and token columns the
// log table shows. Everything here is measured or reported, never estimated.
func (s *Service) logRequest(e logEntry) {
	e.Kind = KindRequest
	if e.Level == "" {
		e.Level = "info"
	}
	s.mu.RLock()
	logs := s.logs
	s.mu.RUnlock()
	if logs == nil {
		return
	}
	logs.add(e)
}

// debugf mirrors one line to both the plugin log ring and CPA's own log, which
// is where the host-side writes become visible.
func (s *Service) debugf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	s.log("debug", "", "", 0, 0, msg)
	var out struct{}
	_ = s.call("host.log", map[string]any{"level": "debug", "message": "riverbridge: " + msg}, &out)
}

// contextWithTimeout is a small helper so every upstream call shares one policy.
func (s *Service) contextWithTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	cfg := s.config()
	return context.WithTimeout(parent, time.Duration(cfg.RequestTimeoutSeconds)*time.Second)
}
