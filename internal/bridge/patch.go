package bridge

import (
	"encoding/json"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// patchRequest carries the operator-editable fields of one credential. A nil
// field means "leave unchanged", which is what makes this a patch rather than
// a replace: renaming must not silently clear the disabled model list.
type patchRequest struct {
	ID         string    `json:"id"`
	Remark     *string   `json:"remark"`
	Disabled   *bool     `json:"disabled"`
	Priority   *int      `json:"priority"`
	BalanceUSD *float64  `json:"balance_usd"`
	Models     *[]string `json:"disabled_models"`
	BaseURL    *string   `json:"base_url"`
	RefreshIDs *bool     `json:"refresh_models"`
}

// managementPatchCredential applies operator edits to one credential and
// re-registers its models with the host.
func (s *Service) managementPatchCredential(body []byte) (any, error) {
	var req patchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "id is required"})
	}

	s.mu.RLock()
	c, ok := s.creds[id]
	fileName := s.files[id]
	authDir := s.authDir
	s.mu.RUnlock()
	if !ok {
		return managementJSON(http.StatusNotFound, map[string]string{"error": "no such credential"})
	}

	if req.Remark != nil {
		c.Remark = strings.TrimSpace(*req.Remark)
		// Drop the legacy field so DisplayName cannot fall back to a stale
		// label after the operator renamed the account.
		if c.Remark != "" {
			c.Label = ""
		}
	}
	if req.Disabled != nil {
		c.Disabled = *req.Disabled
	}
	if req.Priority != nil {
		c.Priority = *req.Priority
	}
	if req.BalanceUSD != nil {
		// Negative is meaningless, and zero explicitly means "no ceiling".
		budget := math.Max(0, math.Round(*req.BalanceUSD*1e6)/1e6)
		c.BalanceUSD = budget
		s.setSpend(id, budget)
		// Raising the ceiling has to put an auto-disabled key back, otherwise
		// the operator tops up and the account stays dark until a restart.
		s.clearAutoDisable(id)
	}
	if req.BaseURL != nil {
		c.BaseURL = strings.TrimRight(strings.TrimSpace(*req.BaseURL), "/")
	}
	if req.Models != nil {
		if err := s.validateModelSelection(c, *req.Models); err != nil {
			return managementJSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		c.DisabledModels = normalizeModelList(*req.Models)
	}

	if req.RefreshIDs != nil && *req.RefreshIDs {
		if models, err := s.discoverModels(c); err == nil {
			c.Models = models
		}
	}
	// Disabling a key or a model changes the advertised list, and the host only
	// re-reads an auth record when its revision changes. Bump it whenever the
	// visible set could have moved, so the change lands on the next watcher tick
	// instead of at restart.
	if req.Disabled != nil || req.Models != nil || req.RefreshIDs != nil {
		c.ModelRevision = revisionOf(c.AdvertisedModels(s.config()))
	}
	c.UpdatedAt = nowStamp()

	// Publish the new state before touching the auth file. Writing first opens a
	// window in which the host's watcher can ask this plugin about the record
	// and be answered from the pre-edit copy, which puts the old value straight
	// back (that is exactly how a "disabled" toggle used to bounce back).
	s.mu.Lock()
	previous := s.creds[id]
	s.creds[id] = c
	s.mu.Unlock()

	if err := s.persistCredential(c, authDir, fileName); err != nil {
		s.mu.Lock()
		s.creds[id] = previous
		s.mu.Unlock()
		s.log("error", "", id, 0, 0, "persist credential edit: "+err.Error())
		return managementJSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	s.mu.Lock()
	if fileName != "" {
		s.files[id] = fileName
	}
	if authDir != "" {
		s.authDir = authDir
	}
	s.mu.Unlock()

	// Re-register models so the host sees the new set immediately.
	s.refreshHostModels()
	s.log("info", "", id, http.StatusOK, 0, describePatch(req))

	return managementJSON(http.StatusOK, map[string]any{
		"ok":              true,
		"id":              id,
		"remark":          c.Remark,
		"disabled":        c.Disabled,
		"priority":        c.Priority,
		"balance_usd":     c.BalanceUSD,
		"spent_usd":       c.SpentUSD,
		"remaining_usd":   c.RemainingUSD(),
		"disabled_models": c.DisabledModels,
		"model_count":     len(c.AdvertisedModels(s.config())),
	})
}

// describePatch renders a patch as one log line, so the log explains what
// changed rather than just that something did.
func describePatch(req patchRequest) string {
	parts := make([]string, 0, 4)
	if req.Remark != nil {
		if v := strings.TrimSpace(*req.Remark); v == "" {
			parts = append(parts, "remark cleared")
		} else {
			parts = append(parts, "renamed to "+v)
		}
	}
	if req.Disabled != nil {
		if *req.Disabled {
			parts = append(parts, "disabled")
		} else {
			parts = append(parts, "enabled")
		}
	}
	if req.Priority != nil {
		parts = append(parts, "priority="+itoa(*req.Priority))
	}
	if req.BalanceUSD != nil {
		parts = append(parts, "budget=$"+strconv.FormatFloat(*req.BalanceUSD, 'f', 2, 64))
	}
	if req.Models != nil {
		parts = append(parts, "disabled models="+itoa(len(*req.Models)))
	}
	if len(parts) == 0 {
		return "credential unchanged"
	}
	return "credential updated: " + strings.Join(parts, ", ")
}

// validateModelSelection rejects ids that this credential cannot serve, so a
// typo surfaces now instead of silently blocking nothing.
func (s *Service) validateModelSelection(c Credential, wanted []string) error {
	known := make(map[string]struct{}, len(c.Models))
	for _, m := range c.Models {
		known[strings.ToLower(m.ID)] = struct{}{}
	}
	for _, m := range catalogueModels() {
		known[strings.ToLower(m.ID)] = struct{}{}
	}
	for _, raw := range wanted {
		v := strings.ToLower(strings.TrimSpace(raw))
		if v == "" {
			continue
		}
		if _, ok := known[v]; !ok {
			return describeErr("unknown model %q; refresh the key's model list first", raw)
		}
	}
	return nil
}

// persistCredential writes the credential straight to its auth file.
//
// host.auth.save cannot be used here. The host rebuilds the record from the
// file it just wrote (saveAuthFile -> buildAuthFromFileData -> upsertAuthRecord)
// and that rebuild resets Disabled from the core auth default, so every write
// lands with the disabled flag turned back on no matter what this plugin sends.
// Writing the file directly leaves the content authoritative and lets CPA's own
// file watcher re-parse it exactly as it does for an operator edit.
func (s *Service) persistCredential(c Credential, authDir, fileName string) error {
	if fileName == "" {
		fileName = s.authFileName(c.ID)
	}
	if authDir == "" {
		return describeErr("CPA auth directory is unknown; re-add the key")
	}
	if err := writeFileAtomic(authDir, fileName, jsonBytes(c)); err != nil {
		return err
	}
	s.mu.Lock()
	s.files[c.ID] = filepath.Base(fileName)
	s.mu.Unlock()
	s.debugf("auth file written %s disabled=%t readback=%s",
		fileName, c.Disabled, readBackFile(filepath.Join(authDir, filepath.Base(fileName))))
	return nil
}

// managementResetSpent clears a key's accumulated spend, which is what a
// top-up means: the ceiling stays, the running total starts over.
func (s *Service) managementResetSpent(body []byte) (any, error) {
	var req struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "id is required"})
	}

	s.mu.RLock()
	_, ok := s.creds[id]
	s.mu.RUnlock()
	if !ok {
		return managementJSON(http.StatusNotFound, map[string]string{"error": "no such credential"})
	}
	s.resetSpend(id)
	s.clearAutoDisable(id)
	s.log("info", "", id, http.StatusOK, 0, "spend counter reset")
	return managementJSON(http.StatusOK, map[string]any{"ok": true, "id": id, "spent_usd": 0})
}

// managementSetModelState is the convenience endpoint behind the per-model
// switches: it reads the current list, flips one entry, and patches.
func (s *Service) managementSetModelState(body []byte) (any, error) {
	var req struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Enabled bool   `json:"enabled"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return managementJSON(http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
	}
	id := strings.TrimSpace(req.ID)
	model := strings.ToLower(strings.TrimSpace(req.Model))
	if id == "" || model == "" {
		return managementJSON(http.StatusBadRequest,
			map[string]string{"error": "id and model are required"})
	}
	s.mu.RLock()
	c, ok := s.creds[id]
	s.mu.RUnlock()
	if !ok {
		return managementJSON(http.StatusNotFound, map[string]string{"error": "no such credential"})
	}

	next := make([]string, 0, len(c.DisabledModels)+1)
	for _, m := range c.DisabledModels {
		if !strings.EqualFold(m, model) {
			next = append(next, m)
		}
	}
	if !req.Enabled {
		next = append(next, model)
	}
	patched, err := json.Marshal(patchRequest{ID: id, Models: &next})
	if err != nil {
		return managementJSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return s.managementPatchCredential(patched)
}

// readBackFile reports the "disabled" value the host actually persisted, which
// is the only way to tell an accepted write from one the host rewrote.
func readBackFile(path string) string {
	if strings.TrimSpace(path) == "" {
		return "no path"
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "read error: " + err.Error()
	}
	var probe struct {
		Disabled *bool `json:"disabled"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return "decode error: " + err.Error()
	}
	if probe.Disabled == nil {
		return "absent"
	}
	if *probe.Disabled {
		return "true"
	}
	return "false"
}
