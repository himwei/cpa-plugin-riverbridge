package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// rvModelsResponse is River's /v1/models payload.
type rvModelsResponse struct {
	Data []struct {
		ID            string `json:"id"`
		Object        string `json:"object"`
		OwnedBy       string `json:"owned_by"`
		Name          string `json:"name"`
		ContextLength int    `json:"context_length"`
		MaxTokens     int    `json:"max_output_tokens"`
		MaxCompletion int    `json:"max_completion_tokens"`
	} `json:"data"`
}

// refreshModelsLocked queries River for this key's model list and stores it on
// the credential. It never fails the auth parse: an unreachable upstream keeps
// the previous list so the credential stays usable.
func (s *Service) refreshModelsLocked(c Credential) (Credential, error) {
	models, err := s.discoverModels(c)
	if err != nil {
		return c, err
	}
	c.Models = models
	c.ModelRevision = revisionOf(models)
	return c, nil
}

// discoverModels lists models for one key.
func (s *Service) discoverModels(c Credential) ([]DiscoveredModel, error) {
	body, err := s.riverGET(c, "/models")
	if err != nil {
		return nil, err
	}
	var payload rvModelsResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode /models: %w", err)
	}
	if len(payload.Data) == 0 {
		return nil, fmt.Errorf("/models returned no models for this key")
	}
	seen := map[string]bool{}
	out := make([]DiscoveredModel, 0, len(payload.Data))
	for _, m := range payload.Data {
		upstream := strings.TrimSpace(m.ID)
		if upstream == "" {
			continue
		}
		id := ModelID(upstream)
		if seen[id] {
			continue
		}
		seen[id] = true
		entry := DiscoveredModel{ID: id, Upstream: upstream}
		// Prefer what the API reported; fall back to the built-in table.
		if p, ok := lookupPrice(upstream); ok {
			entry.ContextLength, entry.MaxOutputTokens = p.Context, p.MaxOutput
		}
		if m.ContextLength > 0 {
			entry.ContextLength = m.ContextLength
		}
		if m.MaxTokens > 0 {
			entry.MaxOutputTokens = m.MaxTokens
		} else if m.MaxCompletion > 0 {
			entry.MaxOutputTokens = m.MaxCompletion
		}
		out = append(out, entry)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("/models returned no usable ids")
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// riverGET performs an authenticated GET against River and returns the body.
func (s *Service) riverGET(c Credential, path string) ([]byte, error) {
	ctx, cancel := s.contextWithTimeout(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURLOrDefault()+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &upstreamError{status: resp.StatusCode, body: body, path: path}
	}
	return body, nil
}

// upstreamError is a non-2xx River response, kept intact so callers can map it
// onto CPA's error policy without inventing a message.
type upstreamError struct {
	status int
	body   []byte
	path   string
}

func (e *upstreamError) Error() string {
	msg := riverErrorMessage(e.body)
	if msg == "" {
		msg = fmt.Sprintf("River returned HTTP %d for %s", e.status, e.path)
	}
	return msg
}

func (e *upstreamError) StatusCode() int { return e.status }

func (e *upstreamError) Code() string { return "river_upstream_error" }

// riverErrorMessage extracts the useful part of a River error body.
func riverErrorMessage(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var envelope struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    any    `json:"code"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil {
		if envelope.Error.Message != "" {
			return envelope.Error.Message
		}
		if envelope.Message != "" {
			return envelope.Message
		}
	}
	text := strings.TrimSpace(string(body))
	if len(text) > 400 {
		text = text[:400] + "…"
	}
	return text
}

// revisionOf hashes a model list so the host re-registers exactly when it
// changes.
func revisionOf(models []DiscoveredModel) string {
	sum := sha256.Sum256(jsonBytes(models))
	return hex.EncodeToString(sum[:])
}

// ValidateKey probes a key without touching any stored credential. It reports
// the reachable models so the console can show them straight after adding a
// key, and returns a precise message when the key is wrong.
func (s *Service) ValidateKey(apiKey, baseURL string) ([]DiscoveredModel, error) {
	key := strings.TrimSpace(apiKey)
	if key == "" {
		return nil, fail(400, "api key is required")
	}
	c := Credential{APIKey: key, BaseURL: baseURL}
	models, err := s.discoverModels(c)
	if err != nil {
		if up, ok := err.(*upstreamError); ok {
			switch up.status {
			case http.StatusUnauthorized, http.StatusForbidden:
				return nil, fail(up.status, "River rejected this API key: "+up.Error())
			case http.StatusPaymentRequired:
				return nil, fail(up.status, "River account has no balance: "+up.Error())
			}
		}
		return nil, err
	}
	return models, nil
}

// nowStamp is the shared timestamp format for credential bookkeeping.
func nowStamp() string { return time.Now().Format(time.RFC3339) }
