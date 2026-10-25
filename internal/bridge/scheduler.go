package bridge

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"
)

// schedulerPickRequest mirrors pluginapi.SchedulerPickRequest for the fields
// this plugin reads. The host sends the full struct; unknown fields are ignored.
type schedulerPickRequest struct {
	Provider string `json:"Provider"`
	Model    string `json:"Model"`
	Stream   bool   `json:"Stream"`
	// Providers carries every provider key the route will accept. A River
	// request can appear here even when "" or another provider is primary, so
	// this is what decides whether the pick is ours to make.
	Providers  []string `json:"Providers"`
	Candidates []struct {
		ID         string            `json:"ID"`
		Provider   string            `json:"Provider"`
		Priority   int               `json:"Priority"`
		Status     string            `json:"Status"`
		Attributes map[string]string `json:"Attributes"`
		Metadata   map[string]any    `json:"Metadata"`
	} `json:"Candidates"`
}

// roundRobin tracks the last key served per model so consecutive requests
// rotate instead of piling on one key.
type roundRobin struct {
	mu   sync.Mutex
	next map[string]int
}

func newRoundRobin() *roundRobin {
	return &roundRobin{next: map[string]int{}}
}

func (r *roundRobin) advance(model string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.next[model]
	r.next[model] = n + 1
	return n
}

// cooldowns parks a credential for a short window after an upstream failure so
// the next request tries a different key.
type cooldowns struct {
	mu     sync.Mutex
	until  map[string]time.Time
	window time.Duration
}

func newCooldowns(window time.Duration) *cooldowns {
	return &cooldowns{until: map[string]time.Time{}, window: window}
}

func (c *cooldowns) mark(authID string) {
	c.mu.Lock()
	c.until[authID] = time.Now().Add(c.window)
	c.mu.Unlock()
}

func (c *cooldowns) active(authID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	deadline, ok := c.until[authID]
	if !ok {
		return false
	}
	if time.Now().After(deadline) {
		delete(c.until, authID)
		return false
	}
	return true
}

// clear drops a cooldown after a success, so a recovered key returns to
// rotation immediately.
func (c *cooldowns) clear(authID string) {
	c.mu.Lock()
	delete(c.until, authID)
	c.mu.Unlock()
}

// pickAuth chooses an auth for one request.
//
// The host validates the answer: the auth id must appear in Candidates, and an
// empty answer with Handled=true is a protocol error. When every candidate is
// unusable we therefore hand the decision back to the host's built-in
// scheduler rather than returning nothing, so a request never fails purely
// because of this plugin's bookkeeping.
func (s *Service) pickAuth(raw json.RawMessage) (any, error) {
	var req schedulerPickRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	if !s.ownsProvider(req.Providers) {
		return notHandled(), nil
	}
	// The host calls exactly one scheduler plugin, the first one to register
	// the capability. Recording the first call is how the console learns that
	// this plugin is the one in control rather than another plugin or the
	// host's built-in scheduler.
	s.markSchedulerActive()

	cfg := s.config()
	s.mu.RLock()
	creds := make(map[string]Credential, len(s.creds))
	for id, c := range s.creds {
		creds[id] = c
	}
	s.mu.RUnlock()

	model := strings.ToLower(strings.TrimSpace(req.Model))
	out := make([]schedulerCandidate, 0, len(req.Candidates))
	for _, cand := range req.Candidates {
		if !strings.EqualFold(strings.TrimSpace(cand.Provider), Provider) {
			continue
		}
		c, known := creds[cand.ID]
		if known && c.Disabled {
			continue
		}
		if known && c.Exhausted() {
			continue
		}
		if model != "" && isModelBlocked(cfg, c, model) {
			continue
		}
		out = append(out, schedulerCandidate{
			ID:       cand.ID,
			Priority: effectivePriority(c, cand.Priority),
			Status:   cand.Status,
		})
	}
	if len(out) == 0 {
		// Nothing of ours is eligible. Let the host fall back; it knows about
		// auths this plugin has not parsed yet.
		return delegateToBuiltin(cfg.Strategy), nil
	}

	if cfg.RetryOnFailure {
		filtered := out[:0]
		for _, cand := range out {
			if !s.cooldown.active(cand.ID) {
				filtered = append(filtered, cand)
			}
		}
		// If every key is cooling down, ignore the cooldowns rather than
		// stalling the request on a guess.
		if len(filtered) > 0 {
			out = filtered
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		// A degraded status ("error"/"disabled") is a hint, not a veto: keep it
		// last but still usable when it is all we have.
		if (out[i].Status == "") != (out[j].Status == "") {
			return out[i].Status == ""
		}
		return out[i].ID < out[j].ID
	})

	switch cfg.Strategy {
	case StrategyRoundRobin:
		idx := s.rr.advance(model) % len(out)
		return picked(out[idx].ID), nil
	default:
		return picked(out[0].ID), nil
	}
}

// schedulerCandidate is the reduced view the picker sorts on.
type schedulerCandidate struct {
	ID       string
	Priority int
	Status   string
}

// ownsProvider reports whether this route involves the River provider at all.
func (s *Service) ownsProvider(providers []string) bool {
	if len(providers) == 0 {
		return true
	}
	for _, p := range providers {
		if strings.EqualFold(strings.TrimSpace(p), Provider) {
			return true
		}
	}
	return false
}

// effectivePriority prefers the plugin's own priority, falling back to the
// host's when the operator has not set one.
func effectivePriority(c Credential, hostPriority int) int {
	if c.Priority != 0 {
		return c.Priority
	}
	return hostPriority
}

// isModelBlocked reports whether a model is switched off for a credential,
// either per key or account-wide.
func isModelBlocked(cfg Config, c Credential, model string) bool {
	for _, m := range cfg.DisabledModels {
		if strings.EqualFold(m, model) {
			return true
		}
	}
	return c.ModelDisabled(model)
}

func picked(authID string) map[string]any {
	return map[string]any{"Handled": true, "AuthID": authID}
}

// markSchedulerActive remembers that the host routed a pick to this plugin.
// When it never fires, another plugin owns the scheduler slot and the operator
// is steering auth selection with a strategy that is not in effect.
func (s *Service) markSchedulerActive() {
	s.mu.Lock()
	if s.schedFirstPick == "" {
		s.schedFirstPick = nowStamp()
	}
	s.schedPicks++
	s.mu.Unlock()
}

// schedulerState reports whether this plugin is actually choosing auths.
func (s *Service) schedulerState() map[string]any {
	s.mu.RLock()
	picks, firstPick := s.schedPicks, s.schedFirstPick
	s.mu.RUnlock()
	return map[string]any{
		"active":     picks > 0,
		"picks":      picks,
		"first_pick": firstPick,
	}
}

func notHandled() map[string]any {
	return map[string]any{"Handled": false}
}

// delegateToBuiltin asks the host to run its own scheduler, named after the
// strategy the operator chose.
func delegateToBuiltin(strategy string) map[string]any {
	builtin := "fill-first"
	if strategy == StrategyRoundRobin {
		builtin = "round-robin"
	}
	return map[string]any{"Handled": true, "DelegateBuiltin": builtin}
}
