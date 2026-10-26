// Package bridge implements the CLIProxyAPI River plugin.
//
// It presents River's OpenAI-compatible inference API as a CPA provider: API
// keys become auth records, River models are advertised with a "river-" prefix,
// and every chat completion is proxied upstream with per-key usage accounting.
package bridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Plugin identity. PluginID is the key used under plugins.configs in CPA's
// config.yaml and in the management API path.
const (
	PluginID   = "riverbridge"
	Provider   = "river"
	Name       = "RiverBridge"
	Version    = "0.3.1"
	Repository = "https://github.com/himwei/cpa-plugin-riverbridge"

	// ModelPrefix separates River models from every other provider's models in
	// CPA's shared /v1/models namespace.
	ModelPrefix = "river-"

	// DefaultBaseURL is River's OpenAI-compatible inference endpoint.
	DefaultBaseURL = "https://api.river.ai/v1"
)

// pluginError carries an HTTP status and optional plugin error code back to the
// host. The host reads them through the small interfaces asserted in
// cmd/riverbridge/main.go.
type pluginError struct {
	message string
	code    string
	status  int
	retry   bool
}

func (e *pluginError) Error() string { return e.message }

func (e *pluginError) StatusCode() int { return e.status }

func (e *pluginError) Code() string { return e.code }

func (e *pluginError) Retryable() bool { return e.retry }

func fail(status int, message string) error {
	return &pluginError{message: message, status: status}
}

func failCode(status int, code, message string) error {
	return &pluginError{message: message, code: code, status: status}
}

func failRetry(status int, code, message string) error {
	return &pluginError{message: message, code: code, status: status, retry: true}
}

// HostErrorDetails unwraps an error into the message and HTTP status the host
// should surface. Exported so cmd/riverbridge can build its RPC error payload.
func HostErrorDetails(err error) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	var statused interface{ StatusCode() int }
	if errors.As(err, &statused) && statused.StatusCode() > 0 {
		return err.Error(), statused.StatusCode()
	}
	return err.Error(), http.StatusInternalServerError
}

// Credential is the plugin-owned auth payload. It is both the on-disk auth file
// body and the StorageJSON the host hands back on each execution.
type Credential struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	// Remark is the operator-facing name shown in the console and as the auth
	// label. Label is kept for records written by 0.1.0 and is migrated into
	// Remark on load.
	Remark   string `json:"remark,omitempty"`
	Label    string `json:"label,omitempty"`
	APIKey   string `json:"api_key"`
	BaseURL  string `json:"base_url,omitempty"`
	ProxyURL string `json:"proxy_url,omitempty"`
	// Disabled takes the credential out of rotation entirely: the scheduler
	// stops offering it and its models stop being advertised.
	//
	// No omitempty on purpose. The host rebuilds the auth file by layering the
	// metadata it was handed over the previously stored JSON, so a key left
	// absent keeps its old value: clearing "disabled" would silently revert to
	// the record being disabled. Always emitting the field makes false and []
	// expressible instead of merely absent.
	Disabled bool `json:"disabled"`
	// DisabledModels lists plugin-facing model ids this key must not serve.
	// An empty list means every discovered model is available.
	DisabledModels []string `json:"disabled_models"`
	// Priority biases both fill-first and round-robin ordering: higher wins.
	Priority int `json:"priority,omitempty"`
	// BalanceUSD is what this key may still spend, in US dollars. Zero means
	// "no ceiling" rather than "nothing left": a key with no budget set must
	// keep working, and a spent-out key is expressed by SpentUSD reaching a
	// non-zero limit instead.
	//
	// River publishes no balance endpoint (every candidate path answers 200
	// with an empty body, including ones that cannot exist), so spending has
	// to be accumulated locally from the prices the plugin already bills with.
	BalanceUSD float64 `json:"-"`
	// SpentUSD is the running total this plugin has charged against the key.
	// It lives in the credential because the auth file is the only state the
	// host persists for us; usage.json would be lost on re-import.
	SpentUSD float64 `json:"-"`

	// Models caches what this key can actually reach upstream. It is refreshed
	// when the key is added and by the management API on demand.
	Models []DiscoveredModel `json:"models,omitempty"`

	// ModelRevision changes whenever Models changes so CPA's file watcher
	// re-registers the model list for this auth.
	ModelRevision string `json:"model_revision,omitempty"`

	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// Exhausted reports whether the key has spent its budget. A zero BalanceUSD
// means no ceiling was set, not "nothing left", so an unbudgeted key is never
// exhausted and keeps serving.
func (c Credential) Exhausted() bool {
	return c.BalanceUSD > 0 && c.SpentUSD >= c.BalanceUSD
}

// RemainingUSD is what is left before the ceiling, or -1 when none is set.
func (c Credential) RemainingUSD() float64 {
	if c.BalanceUSD <= 0 {
		return -1
	}
	if left := c.BalanceUSD - c.SpentUSD; left > 0 {
		return left
	}
	return 0
}

// DisplayName is the operator-facing name for a credential.
func (c Credential) DisplayName() string {
	if v := strings.TrimSpace(c.Remark); v != "" {
		return v
	}
	if v := strings.TrimSpace(c.Label); v != "" {
		return v
	}
	return c.ID
}

// ModelDisabled reports whether the operator switched this model off for the
// credential.
func (c Credential) ModelDisabled(model string) bool {
	for _, m := range c.DisabledModels {
		if strings.EqualFold(strings.TrimSpace(m), strings.TrimSpace(model)) {
			return true
		}
	}
	return false
}

// EnabledModels is the credential's model list minus the disabled entries. It
// is what the host advertises for this auth.
func (c Credential) EnabledModels() []DiscoveredModel {
	if len(c.DisabledModels) == 0 {
		return c.Models
	}
	out := make([]DiscoveredModel, 0, len(c.Models))
	for _, m := range c.Models {
		if !c.ModelDisabled(m.ID) {
			out = append(out, m)
		}
	}
	return out
}

// AdvertisedModels is the list this credential actually publishes: its own
// enabled models minus everything blocked globally. Both the registration
// payload and the revision hash come from here, so flipping a global model
// changes the hash and makes the host re-read the auth.
func (c Credential) AdvertisedModels(cfg Config) []DiscoveredModel {
	blocked := cfg.DisabledModels
	if len(blocked) == 0 && len(c.DisabledModels) == 0 {
		return c.Models
	}
	out := make([]DiscoveredModel, 0, len(c.Models))
	for _, m := range c.EnabledModels() {
		skip := false
		for _, b := range blocked {
			if strings.EqualFold(strings.TrimSpace(b), m.ID) {
				skip = true
				break
			}
		}
		if !skip {
			out = append(out, m)
		}
	}
	return out
}

// DiscoveredModel is one upstream River model reachable by a credential.
type DiscoveredModel struct {
	// ID is the plugin-facing name, always ModelPrefix + slug.
	ID string `json:"id"`
	// Upstream is River's own model id, for example
	// "deepseek-ai/DeepSeek-V4.1-Flash-1017".
	Upstream string `json:"upstream"`
	// ContextLength and MaxOutputTokens mirror River's advertised limits when
	// its /v1/models payload includes them.
	ContextLength   int `json:"context_length,omitempty"`
	MaxOutputTokens int `json:"max_output_tokens,omitempty"`
}

// BaseURLOrDefault returns the effective upstream base URL for a credential.
func (c Credential) BaseURLOrDefault() string {
	if b := strings.TrimSpace(c.BaseURL); b != "" {
		return strings.TrimRight(b, "/")
	}
	return DefaultBaseURL
}

// Log retention bounds. The default and the accepted range match
// ClinePassBridge's, so the same number means the same thing in both consoles
// instead of looking like one plugin lost rows the other kept.
const (
	DefaultLogRetention = 1000
	MinLogRetention     = 50
	MaxLogRetention     = 10000
)

// Routing strategies. "fill-first" keeps using the highest-priority enabled
// key until it fails, which preserves prompt-cache affinity. "round-robin"
// spreads requests across every enabled key.
const (
	StrategyFillFirst  = "fill-first"
	StrategyRoundRobin = "round-robin"
)

// Config is the plugin configuration decoded from plugins.configs.riverbridge.
// It is layered: the YAML in config.yaml first, then settings.json written by
// the management UI.
type Config struct {
	DataDir string `json:"data_dir" yaml:"data_dir"`
	// BaseURL overrides the default upstream for every credential.
	BaseURL string `json:"base_url" yaml:"base_url"`
	// LogRetention bounds the in-memory diagnostic log.
	LogRetention int `json:"log_retention" yaml:"log_retention"`
	// RequestTimeoutSeconds bounds one non-streaming upstream call.
	RequestTimeoutSeconds int `json:"request_timeout_seconds" yaml:"request_timeout_seconds"`
	// IncludeStreamUsage asks River to report token usage on streaming calls.
	IncludeStreamUsage bool `json:"include_stream_usage" yaml:"include_stream_usage"`
	// Strategy selects how an auth is picked for the next request.
	Strategy string `json:"strategy" yaml:"strategy"`
	// RetryOnFailure lets the scheduler skip a key that just failed instead of
	// handing it the next request.
	RetryOnFailure bool `json:"retry_on_failure" yaml:"retry_on_failure"`
	// DisabledModels applies to every credential, in addition to the per-key
	// list. Useful for retiring a model account-wide.
	DisabledModels []string `json:"disabled_models" yaml:"disabled_models"`
}

func defaultConfig() Config {
	return Config{
		DataDir:               "plugins/riverbridge-data",
		BaseURL:               DefaultBaseURL,
		LogRetention:          DefaultLogRetention,
		RequestTimeoutSeconds: 600,
		IncludeStreamUsage:    true,
		Strategy:              StrategyFillFirst,
		RetryOnFailure:        true,
	}
}

func (c *Config) normalize() {
	if strings.TrimSpace(c.DataDir) == "" {
		c.DataDir = "plugins/riverbridge-data"
	}
	if strings.TrimSpace(c.BaseURL) == "" {
		c.BaseURL = DefaultBaseURL
	}
	c.BaseURL = strings.TrimRight(c.BaseURL, "/")
	if c.LogRetention <= 0 {
		c.LogRetention = DefaultLogRetention
	}
	if c.RequestTimeoutSeconds <= 0 {
		c.RequestTimeoutSeconds = 600
	}
	// Accept the same aliases CPA's own routing endpoint takes, so a value
	// pasted from there behaves identically here instead of silently reverting
	// to fill-first.
	switch strings.ToLower(strings.TrimSpace(c.Strategy)) {
	case StrategyRoundRobin, "rr", "roundrobin", "round_robin":
		c.Strategy = StrategyRoundRobin
	default:
		c.Strategy = StrategyFillFirst
	}
	c.DisabledModels = normalizeModelList(c.DisabledModels)
}

// normalizeModelList trims, lowercases and dedupes model ids.
func normalizeModelList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.ToLower(strings.TrimSpace(raw))
		if v == "" {
			continue
		}
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func jsonBytes(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return raw
}

func jsonBytesIndent(v any) []byte {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return []byte("null")
	}
	return raw
}

func tryJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return raw
}

func shortID(prefix, hash string) string {
	tail := hash
	if len(tail) > 12 {
		tail = tail[:12]
	}
	return prefix + "-" + tail
}

func describeErr(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}
