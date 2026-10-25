package bridge

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Service is the plugin instance. The host creates exactly one per loaded
// shared library and drives it entirely through Handle.
type Service struct {
	configMu sync.RWMutex
	cfg      Config

	mu      sync.RWMutex
	host    func(method string, payload any, out any) error
	creds   map[string]Credential
	files   map[string]string
	revoked map[string]bool
	authDir string
	loaded  bool
	stopped bool
	started time.Time

	logs  *logRing
	usage *usageStore
}

// NewService builds an unconfigured service. The host calls plugin.register
// before anything else.
func NewService() *Service {
	return &Service{
		cfg:     defaultConfig(),
		creds:   map[string]Credential{},
		files:   map[string]string{},
		revoked: map[string]bool{},
		started: time.Now(),
		logs:    newLogRing(defaultConfig().LogRetention),
		usage:   newUsageStore(),
	}
}

// SetHost installs the host callback used for logging, auth persistence and
// model refreshes.
func (s *Service) SetHost(h func(string, any, any) error) {
	s.mu.Lock()
	s.host = h
	s.mu.Unlock()
}

func (s *Service) call(method string, in, out any) error {
	s.mu.RLock()
	host := s.host
	s.mu.RUnlock()
	if host == nil {
		return fail(500, "plugin host bridge is not available")
	}
	return host(method, in, out)
}

func (s *Service) config() Config {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.cfg
}

// Handle dispatches one RPC method from the host.
func (s *Service) Handle(method string, raw json.RawMessage) (any, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		if err := s.configure(raw); err != nil {
			return nil, err
		}
		return registration(), nil

	case "auth.identifier", "executor.identifier", "quota.identifier":
		return map[string]string{"identifier": Provider}, nil

	case "model.static":
		return s.modelRegistration(), nil
	case "model.for_auth":
		return s.modelsForAuth(raw)

	case "auth.parse":
		return s.parseAuth(raw)
	case "auth.refresh":
		return s.refreshAuth(raw)
	case "auth.login.start":
		return nil, failCode(501, "unsupported_operation",
			"RiverBridge uses API keys; add a key in the plugin console instead of logging in")
	case "auth.login.poll":
		return nil, failCode(501, "unsupported_operation",
			"RiverBridge uses API keys; add a key in the plugin console instead of logging in")

	case "executor.execute":
		return s.execute(raw)
	case "executor.execute_stream":
		return s.executeStream(raw)
	case "executor.count_tokens":
		// A missing count endpoint is availability-neutral for the host; 404
		// keeps a healthy credential from being cooled down.
		return nil, failCode(404, "count_tokens_unavailable",
			"RiverBridge does not implement token counting")
	case "executor.http_request":
		return nil, failCode(501, "unsupported_operation",
			"RiverBridge does not proxy raw upstream HTTP")

	case "quota.describe", "quota.fetch", "quota.reset":
		return nil, failCode(404, "quota_unavailable", "RiverBridge has no quota endpoint")

	case "management.register":
		return s.registerManagement(raw)
	case "management.handle":
		return s.handleManagement(raw)

	case "plugin.quiesce", "plugin.shutdown":
		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()
		s.flush()
		return map[string]any{}, nil
	}
	return nil, failCode(404, "unknown_method", "unsupported plugin method: "+method)
}

// configure applies plugins.configs.riverbridge from config.yaml, then layers
// settings.json so console edits win over the YAML baseline.
func (s *Service) configure(raw json.RawMessage) error {
	s.configMu.Lock()
	defer s.configMu.Unlock()

	var req struct {
		ConfigYAML []byte `json:"config_yaml"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return err
		}
	}
	cfg := defaultConfig()
	if len(req.ConfigYAML) > 0 {
		if err := yaml.Unmarshal(req.ConfigYAML, &cfg); err != nil {
			return err
		}
	}
	cfg.normalize()

	if body, err := os.ReadFile(filepath.Join(cfg.DataDir, settingsFile)); err == nil {
		if err := json.Unmarshal(body, &cfg); err != nil {
			return describeErr("decode %s: %w", settingsFile, err)
		}
	}
	cfg.normalize()

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return describeErr("create data dir %s: %w", cfg.DataDir, err)
	}

	s.cfg = cfg
	s.mu.Lock()
	s.logs.setLimit(cfg.LogRetention)
	s.mu.Unlock()

	s.mu.RLock()
	store := s.usage
	s.mu.RUnlock()
	if store != nil {
		store.load(cfg.DataDir)
	}
	return nil
}

// registration declares what the plugin can do. Model registration is dynamic
// (executor_model_scope "oauth") so models disappear when every key is gone.
func registration() any {
	return map[string]any{
		"schema_version": 6,
		"metadata": map[string]any{
			"Name":             Name,
			"Version":          Version,
			"Author":           "himwei",
			"GitHubRepository": Repository,
			"Description":      "River inference adapter: River API keys as CPA credential groups with river- prefixed models",
			"Logo":             "river",
			"ConfigFields": []map[string]any{
				{"Name": "data_dir", "Type": "string",
					"Description": "Persistent plugin state directory"},
				{"Name": "base_url", "Type": "string",
					"Description": "River OpenAI-compatible base URL (default https://api.river.ai/v1)"},
				{"Name": "log_retention", "Type": "number",
					"Description": "Number of diagnostic log entries kept in memory"},
				{"Name": "request_timeout_seconds", "Type": "number",
					"Description": "Timeout for one non-streaming upstream call"},
			},
		},
		"capabilities": map[string]any{
			"auth_provider":           true,
			"model_provider":          true,
			"executor":                true,
			"executor_model_scope":    "oauth",
			"executor_input_formats":  []string{"chat-completions"},
			"executor_output_formats": []string{"chat-completions"},
			"management_api":          true,
		},
	}
}

// flush persists accumulated usage counters.
func (s *Service) flush() {
	cfg := s.config()
	s.mu.RLock()
	usage := s.usage
	s.mu.RUnlock()
	if usage != nil {
		_ = usage.save(cfg.DataDir)
	}
}

func (s *Service) authFileName(credID string) string {
	if !strings.HasSuffix(credID, ".json") {
		return credID + ".json"
	}
	return credID
}
