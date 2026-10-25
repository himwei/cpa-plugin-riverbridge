package bridge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
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
	Metadata        map[string]any
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

// executorStreamRequest is the streaming subset of executorRequest: the host
// also names the output stream the plugin must emit frames onto.
//
// The SDK embeds ExecutorRequest (whose fields carry no json tags and so travel
// under their Go names) and adds StreamID with an explicit `stream_id` tag.
// Matching that tag matters: without it the identifier decodes to "" and every
// stream fails as if the host had sent nothing.
type executorStreamRequest struct {
	executorRequest
	StreamID string `json:"stream_id"`
}

// requestPath reports which downstream protocol the request belongs to, which
// decides who owns the SSE "data: " envelope.
func (r executorStreamRequest) requestPath() string {
	if path, ok := r.Metadata["request_path"].(string); ok {
		return path
	}
	return ""
}

// ssePayload extracts the bare JSON body of one upstream SSE line. Comment and
// blank lines carry nothing to forward. A line without the "data:" prefix is
// passed through unchanged so a non-conforming upstream still works.
func ssePayload(line []byte) ([]byte, bool) {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 || trimmed[0] == ':' {
		return nil, false
	}
	if !bytes.HasPrefix(trimmed, []byte("data:")) {
		return trimmed, true
	}
	body := bytes.TrimSpace(trimmed[len("data:"):])
	if len(body) == 0 {
		return nil, false
	}
	return body, true
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
	// The host hands us its own snapshot of the auth file, which never carries
	// the budget counters. Without this the exhausted check below would always
	// see zero and a spent-out key would keep serving.
	c = s.mergeSpend(c)
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
		s.logRequest(logEntry{
			Level: "error", Model: r.Model, Upstream: upstream, AuthID: c.ID,
			Status: statusOf(err), Millis: elapsed,
			Detail: err.Error(),
		})
		return nil, err
	}
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	payload, readErr := io.ReadAll(resp.Body)
	elapsed := time.Since(started).Milliseconds()
	if readErr != nil {
		s.recordFailure(c.ID, r.Model, readErr.Error())
		s.logRequest(logEntry{
			Level: "error", Model: r.Model, Upstream: upstream, AuthID: c.ID,
			Status: resp.StatusCode, Millis: elapsed,
			Detail: "read upstream response: " + readErr.Error(),
		})
		return nil, fail(502, "reading River response failed: "+readErr.Error())
	}
	if resp.StatusCode >= 400 {
		mapped := mapUpstreamStatus(resp.StatusCode, payload)
		s.recordFailure(c.ID, r.Model, mapped.Error())
		s.logRequest(logEntry{
			Level: "error", Model: r.Model, Upstream: upstream, AuthID: c.ID,
			Status: resp.StatusCode, Millis: elapsed,
			Detail: mapped.Error(),
		})
		return nil, mapped
	}

	usage := usageFromBody(payload)
	s.recordSuccess(c.ID, r.Model, upstream, usage)
	// FirstByteMs stays unset: a non-streaming call hands the whole body over in
	// one read, so there is no time-to-first-token to report and pretending the
	// total was one would inflate every average that includes these rows.
	entry := logEntry{
		Model: r.Model, Upstream: upstream, AuthID: c.ID,
		Status: resp.StatusCode, Millis: elapsed,
		Detail: summariseUsage(usage),
	}
	entry.fillUsage(usage)
	s.logRequest(entry)

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
//
// Exactly one module boundary is crossed here: the plugin runs inside a C
// shared object, so a Go channel cannot be returned to the host. Frames are
// therefore pushed with host.stream.emit and the call returns only a plain,
// encodable descriptor. Returning the frames instead makes CPA reject the
// response with "plugin response cannot be encoded".
func (s *Service) executeStream(raw json.RawMessage) (any, error) {
	var r executorStreamRequest
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	if r.StreamID == "" {
		return nil, fail(500, "host provided no output stream identifier")
	}
	c, err := s.credentialFrom(r.executorRequest)
	if err != nil {
		return nil, err
	}
	upstream, body, err := s.prepareUpstream(c, r.executorRequest)
	if err != nil {
		return nil, err
	}

	// Streaming bypasses the per-request timeout: River can legitimately take
	// many minutes, and the host enforces its own stream deadlines.
	started := time.Now()
	resp, cancel, err := s.postUpstream(context.Background(), c, upstream, body, true)
	if err != nil {
		elapsed := time.Since(started).Milliseconds()
		s.recordFailure(c.ID, r.Model, err.Error())
		// No frame ever arrived, so there is no first byte to report; leaving it
		// unset keeps a connect failure out of the first-byte average.
		s.logRequest(logEntry{
			Level: "error", Model: r.Model, Upstream: upstream, AuthID: c.ID,
			Status: statusOf(err), Millis: elapsed,
			Stream: true, Detail: err.Error(),
		})
		return nil, err
	}
	if resp.StatusCode >= 400 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		cancel()
		mapped := mapUpstreamStatus(resp.StatusCode, payload)
		s.recordFailure(c.ID, r.Model, mapped.Error())
		s.logRequest(logEntry{
			Level: "error", Model: r.Model, Upstream: upstream, AuthID: c.ID,
			Status: resp.StatusCode, Millis: time.Since(started).Milliseconds(),
			Stream: true, Detail: mapped.Error(),
		})
		return nil, mapped
	}

	// Hand the first frame back before returning, so a failure to reach model
	// output is still reported through the RPC envelope instead of surfacing as
	// a truncated stream. Emits are acknowledged by the host one at a time.
	ready := make(chan error, 1)
	go s.pumpStream(c, r, upstream, resp.Body, ready, started, cancel)
	if err := <-ready; err != nil {
		return nil, err
	}
	return map[string]any{
		"headers": http.Header{
			"Content-Type":  []string{"text/event-stream"},
			"Cache-Control": []string{"no-cache"},
		},
	}, nil
}

// pumpStream forwards SSE bytes to the host while watching for the final usage
// frame so accounting still works on streamed requests.
func (s *Service) pumpStream(c Credential, r executorStreamRequest, upstream string, body io.ReadCloser, ready chan<- error, started time.Time, cancel context.CancelFunc) {
	defer cancel()
	defer func() { _ = body.Close() }()

	model := r.Model
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64<<10), 8<<20)

	var usage *Usage
	var sawDone bool
	announced := false
	// firstByteMs is time-to-first-token as observed by this plugin: the
	// moment the first content frame is forwarded. River does not report it
	// and CPA cannot see inside the stream, so it has to be measured here.
	var firstByteMs int64

	// emit pushes one frame; the first successful one releases the caller.
	emit := func(payload []byte) error {
		if err := s.call("host.stream.emit", map[string]any{
			"stream_id": r.StreamID,
			"payload":   payload,
		}, nil); err != nil {
			return err
		}
		if !announced {
			announced = true
			firstByteMs = time.Since(started).Milliseconds()
			ready <- nil
		}
		return nil
	}

	closeStream := func(streamErr error) {
		payload := map[string]any{"stream_id": r.StreamID}
		if streamErr != nil {
			payload["error"] = streamErr.Error()
		}
		_ = s.call("host.stream.close", payload, nil)
	}

	defer func() {
		if rec := recover(); rec != nil {
			msg := "stream handler panicked"
			s.recordFailure(c.ID, model, msg)
			s.logRequest(logEntry{
				Level: "error", Model: model, Upstream: upstream, AuthID: c.ID,
				Millis: time.Since(started).Milliseconds(), FirstByteMs: firstByteMs,
				Stream: true, Detail: msg,
			})
			closeStream(fail(502, msg))
			if !announced {
				ready <- fail(502, msg)
			}
			return
		}
		if !announced {
			// Nothing reached the client: report through the RPC envelope.
			err := fail(502, "River stream ended before any completion frame")
			s.recordFailure(c.ID, model, err.Error())
			closeStream(err)
			ready <- err
			return
		}
		total := time.Since(started).Milliseconds()
		if !sawDone {
			// A truncated stream must not look like a clean finish.
			s.log("warn", model, c.ID, 0, total, "stream ended without [DONE]")
		}
		closeStream(nil)
		s.recordSuccess(c.ID, model, upstream, usage)
		entry := logEntry{
			Model: model, Upstream: upstream, AuthID: c.ID,
			Status: 200, Millis: total, FirstByteMs: firstByteMs,
			Stream: true, Detail: summariseUsage(usage),
		}
		if !sawDone {
			entry.Level = "warn"
			entry.Detail += " · 未收到 [DONE]"
		}
		entry.fillUsage(usage)
		s.logRequest(entry)
	}()

	for scanner.Scan() {
		line := scanner.Bytes()
		if bytes.Contains(line, []byte(`"usage"`)) {
			if u := usageFromSSELine(line); u != nil {
				usage = u
			}
		}
		trimmed := bytes.TrimSpace(line)
		if bytes.Equal(trimmed, []byte("data: [DONE]")) {
			sawDone = true
			// CPA owns the terminal marker; emitting it here would duplicate
			// it as "data: data: [DONE]".
			continue
		}
		// Either the host re-adds the "data: " envelope itself (chat and
		// responses) or, for /v1/messages, the plugin supplies it. Strip the
		// upstream envelope either way so frames carry bare JSON.
		frameBytes, ok := ssePayload(line)
		if !ok {
			continue
		}
		if r.requestPath() == "/v1/messages" {
			frameBytes = append(append([]byte("data: "), frameBytes...), []byte("\n\n")...)
		}
		if err := emit(frameBytes); err != nil {
			s.recordFailure(c.ID, model, "downstream stopped reading the stream")
			s.logRequest(logEntry{
				Level: "error", Model: model, Upstream: upstream, AuthID: c.ID,
				Millis: time.Since(started).Milliseconds(), FirstByteMs: firstByteMs,
				Stream: true, Detail: "stream emit: " + err.Error(),
			})
			return
		}
	}
	if err := scanner.Err(); err != nil {
		s.recordFailure(c.ID, model, err.Error())
		s.logRequest(logEntry{
			Level: "error", Model: model, Upstream: upstream, AuthID: c.ID,
			Millis: time.Since(started).Milliseconds(), FirstByteMs: firstByteMs,
			Stream: true, Detail: "stream read: " + err.Error(),
		})
		closeStream(fail(502, "River stream read failed: "+err.Error()))
		if !announced {
			ready <- fail(502, "River stream read failed: "+err.Error())
		}
		return
	}
}

// prepareUpstream builds the River request body from the host payload.
func (s *Service) prepareUpstream(c Credential, r executorRequest) (string, []byte, error) {
	upstream, ok := resolveUpstream(r.Model)
	if !ok {
		return "", nil, fail(400, "unknown River model: "+r.Model)
	}
	// A model switched off for this key must not be served by it. The host
	// already filters the advertised list, but a client can still name the
	// model directly, and the scheduler may have routed here before the
	// per-key list was refreshed.
	if c.Disabled {
		return "", nil, failCode(403, "river_key_disabled",
			"this River key is disabled in RiverBridge")
	}
	// No 403 for an exhausted key here. Refusing at dispatch works, but CPA
	// remembers the failure against that auth and keeps skipping it afterwards,
	// so the key stays dark even once the budget is topped up again. A spent-out
	// key is instead taken out of rotation by addSpend/setSpend, which is
	// reversible; this check only catches a request already in flight.
	if c.Exhausted() {
		return "", nil, fail(429, "this River key has spent its budget in RiverBridge")
	}
	if isModelBlocked(s.config(), c, r.Model) {
		return "", nil, failCode(403, "river_model_disabled",
			"River model "+r.Model+" is disabled in RiverBridge")
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

// fillUsage copies the token counters and derived speed onto a log entry.
//
// TokensPerSecond describes generation, not total throughput: it divides the
// output tokens by the time spent after the first frame arrived, so a long
// prompt wait does not make a fast model look slow. With no first-byte stamp
// or no output tokens there is nothing honest to report and the field stays 0.
func (e *logEntry) fillUsage(u *Usage) {
	if u == nil {
		return
	}
	e.PromptTokens = u.PromptTokens
	e.CachedTokens = u.CachedTokens
	e.CompletionTokens = u.CompletionTokens
	e.TotalTokens = u.TotalTokens
	e.CostUSD = u.CostUSD
	if e.FirstByteMs <= 0 || e.Millis <= e.FirstByteMs || u.CompletionTokens <= 0 {
		return
	}
	seconds := float64(e.Millis-e.FirstByteMs) / 1000
	if seconds <= 0 {
		return
	}
	e.TokensPerSecond = math.Round(float64(u.CompletionTokens)/seconds*10) / 10
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
	// Charge the spend against the key's budget. Kept in the plugin's own store
	// and saved on the flush timer: a write per request would put a disk sync
	// on the hot path, and the auth file is unreadable for this purpose since
	// the host re-parses it and resets whatever we put there.
	if usage != nil && credID != "" {
		s.addSpend(credID, usage.CostUSD)
	}
	// A success clears any cooldown: the key is demonstrably good again.
	if s.cooldown != nil && credID != "" {
		s.cooldown.clear(credID)
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
	// Park the key briefly so the next request prefers another one. This only
	// matters while retry_on_failure is on; the scheduler ignores cooldowns
	// otherwise.
	if s.cooldown != nil && credID != "" && s.config().RetryOnFailure {
		s.cooldown.mark(credID)
	}
}
