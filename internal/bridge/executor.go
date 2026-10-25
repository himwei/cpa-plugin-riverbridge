package bridge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// executorRequest mirrors pluginapi.ExecutorRequest for the fields this plugin
// reads.
type executorRequest struct {
	AuthID          string
	AuthProvider    string
	Model           string
	Format          string
	Stream          bool
	Alt             string
	Headers         http.Header
	OriginalRequest []byte
	SourceFormat    string
	Payload         []byte
	StorageJSON     []byte
	AuthMetadata    map[string]any
	AuthAttributes  map[string]string
}

// executorResponse is the non-streaming result handed back to the host.
type executorResponse struct {
	Payload  []byte
	Headers  http.Header
	Metadata map[string]any
}

// executorStreamChunk is one streamed payload or error.
type executorStreamChunk struct {
	Payload []byte
	Err     error
}

// executorStreamResponse is the streaming result handed back to the host.
type executorStreamResponse struct {
	Headers http.Header
	Chunks  <-chan executorStreamChunk
}

// credentialFrom resolves the credential a request was routed to. The host
// passes the stored payload back, which is authoritative; the local cache only
// fills in fields the host does not echo.
func (s *Service) credentialFrom(r executorRequest) (Credential, error) {
	var c Credential
	if len(r.StorageJSON) > 0 {
		if err := json.Unmarshal(r.StorageJSON, &c); err != nil {
			return c, fail(500, "stored River credential is unreadable")
		}
	}
	if c.ID == "" {
		c.ID = r.AuthID
	}
	if c.APIKey == "" {
		return c, fail(401, "River credential has no api_key")
	}
	s.mu.RLock()
	if current, ok := s.creds[c.ID]; ok && current.APIKey == c.APIKey {
		// Local edits (label, models) win over a stale host snapshot.
		if current.BaseURL != "" {
			c.BaseURL = current.BaseURL
		}
		if c.ProxyURL == "" {
			c.ProxyURL = current.ProxyURL
		}
	}
	s.mu.RUnlock()
	return c, nil
}

// execute performs one non-streaming chat completion.
func (s *Service) execute(raw json.RawMessage) (any, error) {
	var r executorRequest
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	c, err := s.credentialFrom(r)
	if err != nil {
		return nil, err
	}
	upstream, body, err := s.prepareUpstream(c, r)
	if err != nil {
		return nil, err
	}

	started := time.Now()
	resp, cancel, err := s.postUpstream(context.Background(), c, upstream, body, false)
	if err != nil {
		elapsed := time.Since(started).Milliseconds()
		s.recordFailure(c.ID, r.Model, err.Error())
		s.log("error", r.Model, c.ID, statusOf(err), elapsed, err.Error())
		return nil, err
	}
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	payload, readErr := io.ReadAll(resp.Body)
	elapsed := time.Since(started).Milliseconds()
	if readErr != nil {
		s.recordFailure(c.ID, r.Model, readErr.Error())
		s.log("error", r.Model, c.ID, 0, elapsed, "read upstream response: "+readErr.Error())
		return nil, fail(502, "reading River response failed: "+readErr.Error())
	}
	if resp.StatusCode >= 400 {
		mapped := mapUpstreamStatus(resp.StatusCode, payload)
		s.recordFailure(c.ID, r.Model, mapped.Error())
		s.log("error", r.Model, c.ID, resp.StatusCode, elapsed, mapped.Error())
		return nil, mapped
	}

	usage := usageFromBody(payload)
	s.recordSuccess(c.ID, r.Model, upstream, usage)
	s.log("info", r.Model, c.ID, resp.StatusCode, elapsed, summariseUsage(usage))

	return executorResponse{
		Payload: payload,
		Headers: responseHeaders(resp.Header),
		Metadata: map[string]any{
			"provider": Provider,
			"upstream": upstream,
			"usage":    usage,
		},
	}, nil
}

// executeStream performs one streaming chat completion.
func (s *Service) executeStream(raw json.RawMessage) (any, error) {
	var r executorRequest
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	c, err := s.credentialFrom(r)
	if err != nil {
		return nil, err
	}
	upstream, body, err := s.prepareUpstream(c, r)
	if err != nil {
		return nil, err
	}

	// Streaming bypasses the per-request timeout: River can legitimately take
	// many minutes, and the host enforces its own stream deadlines.
	resp, cancel, err := s.postUpstream(context.Background(), c, upstream, body, true)
	if err != nil {
		s.recordFailure(c.ID, r.Model, err.Error())
		s.log("error", r.Model, c.ID, statusOf(err), 0, err.Error())
		return nil, err
	}
	if resp.StatusCode >= 400 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		cancel()
		mapped := mapUpstreamStatus(resp.StatusCode, payload)
		s.recordFailure(c.ID, r.Model, mapped.Error())
		s.log("error", r.Model, c.ID, resp.StatusCode, 0, mapped.Error())
		return nil, mapped
	}

	chunks := make(chan executorStreamChunk, 16)
	started := time.Now()
	go s.pumpStream(c, r.Model, upstream, resp.Body, chunks, started, cancel)

	return executorStreamResponse{
		Headers: responseHeaders(resp.Header),
		Chunks:  chunks,
	}, nil
}

// pumpStream copies SSE bytes downstream while watching for the final usage
// frame so accounting still works on streamed requests.
func (s *Service) pumpStream(c Credential, model, upstream string, body io.ReadCloser, out chan<- executorStreamChunk, started time.Time, cancel context.CancelFunc) {
	defer close(out)
	defer cancel()
	defer func() { _ = body.Close() }()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64<<10), 8<<20)

	var usage *Usage
	var sawDone bool
	for scanner.Scan() {
		line := scanner.Bytes()
		if bytes.Contains(line, []byte(`"usage"`)) {
			if u := usageFromSSELine(line); u != nil {
				usage = u
			}
		}
		if bytes.Equal(bytes.TrimSpace(line), []byte("data: [DONE]")) {
			sawDone = true
		}
		// Re-add the newline the scanner stripped so the SSE framing survives.
		payload := append(append([]byte{}, line...), '\n')
		select {
		case out <- executorStreamChunk{Payload: payload}:
		case <-time.After(5 * time.Minute):
			s.recordFailure(c.ID, model, "downstream stopped reading the stream")
			s.log("error", model, c.ID, 0, time.Since(started).Milliseconds(),
				"downstream stalled for 5m")
			return
		}
	}
	if err := scanner.Err(); err != nil {
		s.recordFailure(c.ID, model, err.Error())
		s.log("error", model, c.ID, 0, time.Since(started).Milliseconds(),
			"stream read: "+err.Error())
		select {
		case out <- executorStreamChunk{Err: err}:
		default:
		}
		return
	}
	if !sawDone {
		// A truncated stream must not look like a clean finish.
		s.log("warn", model, c.ID, 0, time.Since(started).Milliseconds(),
			"stream ended without [DONE]")
	}
	s.recordSuccess(c.ID, model, upstream, usage)
	s.log("info", model, c.ID, 200, time.Since(started).Milliseconds(),
		"stream "+summariseUsage(usage))
}

// prepareUpstream builds the River request body from the host payload.
func (s *Service) prepareUpstream(c Credential, r executorRequest) (string, []byte, error) {
	upstream, ok := resolveUpstream(r.Model)
	if !ok {
		return "", nil, fail(400, "unknown River model: "+r.Model)
	}
	body := r.Payload
	if len(body) == 0 {
		body = r.OriginalRequest
	}
	if len(body) == 0 {
		return "", nil, fail(400, "request body is empty")
	}

	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil {
		return "", nil, fail(400, "request body is not valid JSON: "+err.Error())
	}
	doc["model"] = jsonBytes(upstream)
	cfg := s.config()
	if r.Stream || cfg.IncludeStreamUsage {
		// Ask River to embed usage in the final stream frame. Harmless on
		// non-streaming calls, where usage is always present.
		if _, has := doc["stream_options"]; !has && r.Stream {
			doc["stream_options"] = jsonBytes(map[string]any{"include_usage": true})
		}
	}
	patched, err := json.Marshal(doc)
	if err != nil {
		return "", nil, fail(400, "cannot rebuild the upstream request: "+err.Error())
	}
	return upstream, patched, nil
}

// postUpstream sends one chat completion to River. The returned cancel func
// releases the request context and must be called once the body is closed.
func (s *Service) postUpstream(ctx context.Context, c Credential, upstream string, body []byte, stream bool) (*http.Response, context.CancelFunc, error) {
	cfg := s.config()

	// Streaming gets a cancelable context without a deadline: River can take
	// many minutes and the host enforces its own stream deadlines.
	reqCtx, cancel := context.WithCancel(ctx)
	if !stream {
		cancel()
		reqCtx, cancel = context.WithTimeout(ctx, time.Duration(cfg.RequestTimeoutSeconds)*time.Second)
	}

	url := c.BaseURLOrDefault() + "/chat/completions"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		cancel()
		return nil, func() {}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}

	client := s.httpClient()
	if stream {
		client.Timeout = 0
	}
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		return nil, func() {}, fail(502, "River request failed: "+err.Error())
	}
	return resp, cancel, nil
}

// mapUpstreamStatus converts a River error into a CPA-facing plugin error,
// preserving the upstream status so the host applies its own cooldown policy.
func mapUpstreamStatus(status int, body []byte) error {
	msg := riverErrorMessage(body)
	if msg == "" {
		msg = "River request failed"
	}
	code := "river_upstream_error"
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return failCode(status, "river_auth", msg)
	case http.StatusPaymentRequired:
		return failCode(status, "river_balance", msg)
	case http.StatusTooManyRequests:
		return failRetry(status, "river_rate_limit", msg)
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return failCode(status, "river_bad_request", msg)
	case http.StatusNotFound:
		return failCode(status, "river_not_found", msg)
	case http.StatusRequestEntityTooLarge:
		return failCode(status, "river_too_large", msg)
	case http.StatusServiceUnavailable, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusGatewayTimeout:
		return failRetry(status, code, msg)
	}
	return &pluginError{message: msg, code: code, status: status, retry: status >= 500}
}

// responseHeaders keeps only the headers worth forwarding downstream.
func responseHeaders(in http.Header) http.Header {
	out := http.Header{}
	for _, key := range []string{"Content-Type", "X-Request-Id", "Retry-After"} {
		if v := in.Get(key); v != "" {
			out.Set(key, v)
		}
	}
	return out
}

// usageFromBody pulls the `usage` object out of a complete chat completion.
func usageFromBody(body []byte) *Usage {
	var doc struct {
		Usage json.RawMessage `json:"usage"`
	}
	if err := json.Unmarshal(body, &doc); err != nil || len(doc.Usage) == 0 {
		return nil
	}
	return parseUsage(doc.Usage)
}

// usageFromSSELine extracts usage from one `data: {...}` SSE line.
func usageFromSSELine(line []byte) *Usage {
	text := strings.TrimSpace(string(line))
	text = strings.TrimPrefix(text, "data:")
	text = strings.TrimSpace(text)
	if text == "" || text == "[DONE]" {
		return nil
	}
	var doc struct {
		Usage json.RawMessage `json:"usage"`
	}
	if err := json.Unmarshal([]byte(text), &doc); err != nil || len(doc.Usage) == 0 {
		return nil
	}
	return parseUsage(doc.Usage)
}

func parseUsage(raw json.RawMessage) *Usage {
	var env usageEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil
	}
	u := env.toUsage()
	if u.TotalTokens == 0 {
		return nil
	}
	u.Raw = raw
	return u
}

func summariseUsage(u *Usage) string {
	if u == nil {
		return "no usage reported"
	}
	return "tokens in=" + itoa(u.PromptTokens) +
		" cached=" + itoa(u.CachedTokens) +
		" out=" + itoa(u.CompletionTokens)
}

func itoa(v int) string {
	return strings.TrimSpace(jsonNumberString(v))
}

func jsonNumberString(v int) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

func statusOf(err error) int {
	var statused interface{ StatusCode() int }
	if asStatus(err, &statused) {
		return statused.StatusCode()
	}
	return 0
}

func asStatus(err error, out *interface{ StatusCode() int }) bool {
	if v, ok := err.(interface{ StatusCode() int }); ok {
		*out = v
		return true
	}
	return false
}

// recordSuccess books a response against the credential.
func (s *Service) recordSuccess(credID, model, upstream string, usage *Usage) {
	if usage != nil {
		usage.CostUSD = priceUsage(upstream, usage)
	}
	s.mu.RLock()
	store := s.usage
	s.mu.RUnlock()
	if store != nil {
		store.RecordSuccess(credID, model, usage)
	}
}

// recordFailure books a failed request against the credential.
func (s *Service) recordFailure(credID, model, message string) {
	s.mu.RLock()
	store := s.usage
	s.mu.RUnlock()
	if store != nil && credID != "" {
		store.RecordFailure(credID, model, message)
	}
}
