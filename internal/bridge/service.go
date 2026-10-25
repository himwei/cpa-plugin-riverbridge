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
	// stats counts every finished request into per-day buckets that outlive
	// the log retention window (see statisticsStore).
	stats *statisticsStore

	// rr and cooldown drive auth selection in pickAuth.
	rr       *roundRobin
	cooldown *cooldowns

	// schedPicks counts scheduler.pick calls; see markSchedulerActive.
	schedPicks     int64
	schedFirstPick string

	// stopFlush closes the background persistence loop on shutdown.
	stopFlush chan struct{}
	flushOnce sync.Once
	// spent holds budgets and running spend in the plugin's own data dir. It
	// must not live in the auth file: the host re-parses that file on every
	// request and rebuilds the credential from it, which would reset a counter
	// written there.
	spent *spendStore
}

// NewService builds an unconfigured service. The host calls plugin.register
// before anything else.
func NewService() *Service {
	return &Service{
		cfg:       defaultConfig(),
		creds:     map[string]Credential{},
		files:     map[string]string{},
		revoked:   map[string]bool{},
		started:   time.Now(),
		logs:      newLogRing("", defaultConfig().LogRetention),
		usage:     newUsageStore(),
		stats:     newStatisticsStore(""),
		rr:        newRoundRobin(),
		cooldown:  newCooldowns(90 * time.Second),
		stopFlush: make(chan struct{}),
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

// startFlushLoop persists counters on a timer. Waiting for shutdown alone is
// not enough: a process that is killed or wedged loses everything since the
// last clean exit, which is exactly the history someone wants after a crash.
func (s *Service) startFlushLoop() {
	s.flushOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-s.stopFlush:
					return
				case <-ticker.C:
					s.flushCounters()
				}
			}
		}()
	})
}

// flushCounters writes the statistics buckets and usage counters. The journal
// is written inline by logRing.add, so it needs no timer.
func (s *Service) flushCounters() {
	cfg := s.config()
	s.mu.RLock()
	usage := s.usage
	stats := s.stats
	s.mu.RUnlock()
	if usage != nil {
		_ = usage.save(cfg.DataDir)
	}
	if stats != nil {
		stats.flush()
	}
	s.flushSpent()
}

// flushSpent persists the budget counters. Failures are logged and retried on
// the next tick rather than surfaced to a request.
func (s *Service) flushSpent() {
	s.mu.RLock()
	spent := s.spent
	s.mu.RUnlock()
	if spent == nil {
		return
	}
	if err := spent.save(); err != nil {
		s.log("warn", "", "", 0, 0, "saving spend counters failed: "+err.Error())
	}
}

// mergeSpend layers the plugin's budget numbers onto a credential decoded from
// the auth file. The file never carries them, so they always come from here.
func (s *Service) mergeSpend(c Credential) Credential {
	s.mu.RLock()
	spent := s.spent
	s.mu.RUnlock()
	if spent == nil {
		return c
	}
	r := spent.get(c.ID)
	c.BalanceUSD = r.BudgetUSD
	c.SpentUSD = r.SpentUSD
	return c
}

// setSpend records a new budget for one key and keeps the cached credential in
// step so the console reflects it without waiting for a re-parse.
func (s *Service) setSpend(id string, budget float64) {
	s.mu.RLock()
	spent := s.spent
	s.mu.RUnlock()
	var r spendRecord
	if spent != nil {
		r = spent.get(id)
		r.BudgetUSD = budget
		spent.set(id, r)
		_ = spent.save()
	}
	s.mu.Lock()
	if c, ok := s.creds[id]; ok {
		c.BalanceUSD = budget
		s.creds[id] = c
	}
	s.mu.Unlock()

	// Lowering the ceiling below what is already spent has to take effect
	// immediately. Waiting for the next request would leave the key serving
	// traffic that the operator has just said it may not spend.
	if spent != nil && budget > 0 && r.SpentUSD >= budget {
		r.AutoDisabled = true
		spent.set(id, r)
		_ = spent.save()
		s.setDisabled(id, true)
		s.log("warn", "", id, 0, 0, "budget already reached; key taken out of rotation")
	}
}

// addSpend charges a successful call against a key's running total and takes
// the key out of service once the ceiling is reached.
func (s *Service) addSpend(id string, cost float64) {
	if id == "" || cost <= 0 {
		return
	}
	s.mu.RLock()
	spent := s.spent
	s.mu.RUnlock()
	if spent == nil {
		return
	}
	r := spent.get(id)
	r.SpentUSD += cost
	spent.set(id, r)
	// Written through immediately. Unlike the diagnostic log, this number is
	// money: dropping the last few seconds of it on a restart is not an
	// acceptable trade for saving one small write per request.
	if err := spent.save(); err != nil {
		s.log("warn", "", id, 0, 0, "saving spend failed: "+err.Error())
	}

	s.mu.Lock()
	c, ok := s.creds[id]
	if ok {
		c.SpentUSD = r.SpentUSD
		s.creds[id] = c
	}
	disableNow := ok && r.BudgetUSD > 0 && r.SpentUSD >= r.BudgetUSD && !c.Disabled
	s.mu.Unlock()

	// Removing the key from the pool is the only lever that actually stops
	// traffic. Answering 403 at execution time also works, but CPA remembers
	// the failure against that auth and keeps skipping it afterwards -- the key
	// stays unusable even once the budget is topped up again.
	if disableNow {
		r.AutoDisabled = true
		spent.set(id, r)
		_ = spent.save()
		s.setDisabled(id, true)
		s.log("warn", "", id, 0, 0, "budget reached; key taken out of rotation")
	}
}

// setDisabled flips a credential's disabled flag, persists it and re-registers
// the model list so the host stops advertising it.
func (s *Service) setDisabled(id string, disabled bool) {
	s.mu.Lock()
	c, ok := s.creds[id]
	fileName := s.files[id]
	dir := s.authDir
	if !ok || c.Disabled == disabled {
		s.mu.Unlock()
		return
	}
	c.Disabled = disabled
	c.ModelRevision = revisionOf(c.AdvertisedModels(s.cfg))
	c.UpdatedAt = nowStamp()
	s.creds[id] = c
	s.mu.Unlock()

	if err := s.persistCredential(c, dir, fileName); err != nil {
		s.log("error", "", id, 0, 0, "persist budget disable: "+err.Error())
		return
	}
	s.refreshHostModels()
}

// clearAutoDisable re-enables a key this plugin switched off, and only such a
// key: a deliberate operator disable must survive a budget change.
func (s *Service) clearAutoDisable(id string) {
	s.mu.RLock()
	spent := s.spent
	s.mu.RUnlock()
	if spent == nil {
		return
	}
	r := spent.get(id)
	if !r.AutoDisabled || r.SpentUSD >= r.BudgetUSD && r.BudgetUSD > 0 {
		return
	}
	r.AutoDisabled = false
	spent.set(id, r)
	_ = spent.save()
	s.setDisabled(id, false)
	s.log("info", "", id, 0, 0, "budget available again; key returned to rotation")
}

// resetSpend clears the running total after a top-up, keeping the ceiling.
func (s *Service) resetSpend(id string) {
	s.mu.RLock()
	spent := s.spent
	s.mu.RUnlock()
	if spent == nil {
		return
	}
	r := spent.get(id)
	r.SpentUSD = 0
	r.AutoDisabled = false
	spent.set(id, r)
	_ = spent.save()
	s.mu.Lock()
	if c, ok := s.creds[id]; ok {
		c.SpentUSD = 0
		s.creds[id] = c
	}
	s.mu.Unlock()
}

func (s *Service) stopFlushLoop() {
	select {
	case <-s.stopFlush:
	default:
		close(s.stopFlush)
	}
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
	case "scheduler.pick":
		return s.pickAuth(raw)
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
		s.stopFlushLoop()
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
	// Restart the on-disk log from the configured directory so a changed
	// data_dir is honoured; the ring replays the journal into memory here.
	s.mu.Lock()
	s.logs = newLogRing(cfg.DataDir, cfg.LogRetention)
	s.spent = newSpendStore(cfg.DataDir)
	s.mu.Unlock()

	s.mu.RLock()
	store := s.usage
	s.mu.RUnlock()
	if store != nil {
		store.load(cfg.DataDir)
	}

	s.mu.RLock()
	stats := s.stats
	s.mu.RUnlock()
	if stats != nil {
		stats.dir = cfg.DataDir
		stats.load()
	}
	s.startFlushLoop()
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
			"auth_provider":  true,
			"model_provider": true,
			"executor":       true,
			// scheduler makes the plugin's pickAuth the auth chooser for routes
			// that involve River. The host uses the first registered scheduler,
			// so this only takes effect while no other plugin claims it.
			"scheduler":               true,
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
	s.mu.RLock()
	logs := s.logs
	stats := s.stats
	s.mu.RUnlock()
	if stats != nil {
		stats.flush()
	}
	// Give the ring one last chance to reach disk; a shutdown that silently
	// drops the newest lines is the one restart case people notice.
	if logs != nil {
		logs.mu.Lock()
		logs.flushLocked()
		logs.mu.Unlock()
	}
}

func (s *Service) authFileName(credID string) string {
	if !strings.HasSuffix(credID, ".json") {
		return credID + ".json"
	}
	return credID
}
