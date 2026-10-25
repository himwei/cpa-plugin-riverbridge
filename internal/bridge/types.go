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
	Version    = "0.1.0"
	Repository = "https://github.com/himwei/riverbridge"

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
	Type     string `json:"type"`
	ID       string `json:"id"`
	Label    string `json:"label,omitempty"`
	APIKey   string `json:"api_key"`
	BaseURL  string `json:"base_url,omitempty"`
	ProxyURL string `json:"proxy_url,omitempty"`
	Disabled bool   `json:"disabled,omitempty"`

	// Models caches what this key can actually reach upstream. It is refreshed
	// when the key is added and by the management API on demand.
	Models []DiscoveredModel `json:"models,omitempty"`

	// ModelRevision changes whenever Models changes so CPA's file watcher
	// re-registers the model list for this auth.
	ModelRevision string `json:"model_revision,omitempty"`

	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
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
}

func defaultConfig() Config {
	return Config{
		DataDir:               "plugins/riverbridge-data",
		BaseURL:               DefaultBaseURL,
		LogRetention:          300,
		RequestTimeoutSeconds: 600,
		IncludeStreamUsage:    true,
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
		c.LogRetention = 300
	}
	if c.RequestTimeoutSeconds <= 0 {
		c.RequestTimeoutSeconds = 600
	}
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
