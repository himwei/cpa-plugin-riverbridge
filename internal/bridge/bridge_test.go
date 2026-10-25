package bridge

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestModelIDPrefixesAndStripsVendor(t *testing.T) {
	cases := []struct {
		upstream string
		want     string
	}{
		// The vendor segment is dropped and the slug is lowercased.
		{"deepseek-ai/DeepSeek-V4.1-Flash-1017", "river-deepseek-v4.1-flash-1017"},
		{"zai-org/GLM-5.3-Flash", "river-glm-5.3-flash"},
		{"moonshotai/Kimi-K3", "river-kimi-k3"},
		// A slug that already carries the prefix is not prefixed twice.
		{"river-kimi-k3", "river-kimi-k3"},
		// A model with no vendor segment keeps its name.
		{"gpt-6-luna", "river-gpt-6-luna"},
	}
	for _, tc := range cases {
		if got := ModelID(tc.upstream); got != tc.want {
			t.Errorf("ModelID(%q) = %q, want %q", tc.upstream, got, tc.want)
		}
	}
}

// Every model the plugin advertises must map back to the exact upstream id, or
// a request would silently go to the wrong River model.
func TestResolveUpstreamRoundTrips(t *testing.T) {
	for upstream := range riverPrices {
		id := ModelID(upstream)
		got, ok := resolveUpstream(id)
		if !ok {
			t.Errorf("resolveUpstream(%q) failed for upstream %q", id, upstream)
			continue
		}
		if got != upstream {
			t.Errorf("resolveUpstream(%q) = %q, want %q", id, got, upstream)
		}
	}
}

func TestResolveUpstreamRejectsForeignModels(t *testing.T) {
	for _, model := range []string{"deepseek-v4.1-flash", "gpt-6-luna", "", "river-does-not-exist"} {
		if _, ok := resolveUpstream(model); ok {
			t.Errorf("resolveUpstream(%q) unexpectedly succeeded", model)
		}
	}
}

func TestLookupPriceIsCaseInsensitive(t *testing.T) {
	want, ok := riverPrices["deepseek-ai/DeepSeek-V4.1-Flash-1017"]
	if !ok {
		t.Fatal("expected the 1017 model in the price table")
	}
	got, ok := lookupPrice("deepseek-ai/deepseek-v4.1-flash-1017")
	if !ok {
		t.Fatal("lookupPrice did not tolerate a case difference")
	}
	if got != want {
		t.Errorf("lookupPrice returned %+v, want %+v", got, want)
	}
	if _, ok := lookupPrice("nope/unknown"); ok {
		t.Error("lookupPrice invented a price for an unknown model")
	}
}

func TestPriceUsageBillsCachedTokensSeparately(t *testing.T) {
	// 1M prompt tokens of which 400k were cached, plus 1M output tokens.
	p := riverPrices["deepseek-ai/DeepSeek-V4.1-Flash-1017"]
	u := &Usage{PromptTokens: 1_000_000, CachedTokens: 400_000, CompletionTokens: 1_000_000}
	got := priceUsage("deepseek-ai/DeepSeek-V4.1-Flash-1017", u)

	want := 600_000/1e6*p.Input + 400_000/1e6*p.Cached + 1_000_000/1e6*p.Output
	if diff := got - want; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("priceUsage = %v, want %v", got, want)
	}
}

func TestPriceUsageIgnoresUnknownModelsAndNilUsage(t *testing.T) {
	if got := priceUsage("nope/unknown", &Usage{PromptTokens: 1000}); got != 0 {
		t.Errorf("priceUsage for an unknown model = %v, want 0", got)
	}
	if got := priceUsage("deepseek-ai/DeepSeek-V4.1-Flash-1017", nil); got != 0 {
		t.Errorf("priceUsage with nil usage = %v, want 0", got)
	}
}

func TestCredentialIDIsStableAndKeyed(t *testing.T) {
	const key = "rv_abcdef"
	a, b := CredentialID(key), CredentialID("  "+key+"  ")
	if a != b {
		t.Errorf("CredentialID is not whitespace stable: %q vs %q", a, b)
	}
	if CredentialID("rv_other") == a {
		t.Error("CredentialID collided for two different keys")
	}
	if a == CredentialID(key)[:len("river-")] {
		t.Error("CredentialID lost its prefix")
	}
}

func TestRegistrationAdvertisesModelScope(t *testing.T) {
	reg, ok := registration().(map[string]any)
	if !ok {
		t.Fatal("registration() did not return a map")
	}
	caps, ok := reg["capabilities"].(map[string]any)
	if !ok {
		t.Fatal("registration() has no capabilities")
	}
	// "oauth" scope makes models disappear when the last key is removed, which
	// static registration would not do.
	if caps["executor_model_scope"] != "oauth" {
		t.Errorf("executor_model_scope = %v, want oauth", caps["executor_model_scope"])
	}
	for _, key := range []string{"auth_provider", "model_provider", "executor", "management_api"} {
		if caps[key] != true {
			t.Errorf("capability %s is not advertised", key)
		}
	}
}

func TestConfigNormalizeFillsDefaults(t *testing.T) {
	var c Config
	c.normalize()
	if c.DataDir == "" || c.BaseURL == "" {
		t.Errorf("normalize left empty fields: %+v", c)
	}
	if c.LogRetention <= 0 || c.RequestTimeoutSeconds <= 0 {
		t.Errorf("normalize left non-positive bounds: %+v", c)
	}
	if c.BaseURL[len(c.BaseURL)-1] == '/' {
		t.Errorf("normalize kept a trailing slash: %q", c.BaseURL)
	}
}

func TestLooksLikeRiverAuth(t *testing.T) {
	cases := []struct {
		c    Credential
		want bool
	}{
		{Credential{Type: Provider}, true},
		// An untyped file carrying a River-shaped key is adopted.
		{Credential{APIKey: "rv_live"}, true},
		{Credential{APIKey: "sk-nope"}, false},
		// Another provider's credential is never adopted, even when its key
		// happens to look like ours: the type is authoritative.
		{Credential{Type: "workbuddy", APIKey: "rv_x"}, false},
	}
	for _, tc := range cases {
		if got := looksLikeRiverAuth(tc.c); got != tc.want {
			t.Errorf("looksLikeRiverAuth(%+v) = %v, want %v", tc.c, got, tc.want)
		}
	}
}

// CPA owns the downstream SSE envelope, so the plugin must forward bare JSON.
// Forwarding the upstream line verbatim produced "data: data: [...]", and
// forwarding [DONE] produced a duplicate terminal marker.
func TestSSEPayloadStripsEnvelope(t *testing.T) {
	cases := []struct {
		name  string
		line  string
		want  string
		okGot bool
	}{
		{"data frame", `data: {"id":"x"}`, `{"id":"x"}`, true},
		{"no space after prefix", `data:{"id":"x"}`, `{"id":"x"}`, true},
		{"done marker is dropped", `data: [DONE]`, `[DONE]`, true},
		{"blank line", ``, ``, false},
		{"comment line", `: keep-alive`, ``, false},
		{"empty data", `data: `, ``, false},
		{"bare json passes through", `{"id":"x"}`, `{"id":"x"}`, true},
	}
	for _, tc := range cases {
		got, ok := ssePayload([]byte(tc.line))
		if ok != tc.okGot {
			t.Errorf("%s: ok = %v, want %v", tc.name, ok, tc.okGot)
			continue
		}
		if ok && string(got) != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, string(got), tc.want)
		}
	}
}

// The host names the output stream on the RPC envelope under an explicit
// stream_id tag. Decoding it as "" makes every stream fail at startup, so the
// wire shape is worth pinning down.
func TestStreamRequestDecodesStreamID(t *testing.T) {
	raw := []byte(`{"Model":"river-x","stream_id":"abc123","Metadata":{"request_path":"/v1/messages"}}`)
	var r executorStreamRequest
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.StreamID != "abc123" {
		t.Errorf("StreamID = %q, want abc123", r.StreamID)
	}
	if r.Model != "river-x" {
		t.Errorf("Model = %q, want river-x", r.Model)
	}
	if r.requestPath() != "/v1/messages" {
		t.Errorf("requestPath = %q, want /v1/messages", r.requestPath())
	}
}

func TestDisplayNamePrefersRemark(t *testing.T) {
	c := Credential{ID: "abc123", Label: "legacy"}
	if got := c.DisplayName(); got != "legacy" {
		t.Fatalf("DisplayName = %q, want the legacy label", got)
	}
	c.Remark = "主号"
	if got := c.DisplayName(); got != "主号" {
		t.Fatalf("DisplayName = %q, want the remark", got)
	}
	c.Remark = ""
	c.Label = ""
	if got := c.DisplayName(); got != "abc123" {
		t.Fatalf("DisplayName = %q, want the id as last resort", got)
	}
}

func TestModelDisabledMatchesCaseInsensitively(t *testing.T) {
	c := Credential{DisabledModels: []string{"DeepSeek-V4.1-Flash"}}
	if !c.ModelDisabled("river-deepseek-v4.1-flash") && c.ModelDisabled("deepseek-v4.1-flash") != true {
		t.Fatal("expected the disabled model to match regardless of case")
	}
	if c.ModelDisabled("glm-5.3-flash") {
		t.Fatal("an unrelated model must stay enabled")
	}
}

func TestEnabledModelsDropsDisabledAndKeepsOrder(t *testing.T) {
	c := Credential{
		Models: []DiscoveredModel{
			{ID: "river-a"}, {ID: "river-b"}, {ID: "river-c"},
		},
		DisabledModels: []string{"river-b"},
	}
	got := c.EnabledModels()
	if len(got) != 2 || got[0].ID != "river-a" || got[1].ID != "river-c" {
		t.Fatalf("EnabledModels = %+v, want a and c", got)
	}
}

func TestNormalizeModelListTrimsAndDedupes(t *testing.T) {
	got := normalizeModelList([]string{" River-A ", "river-a", "", "GLM"})
	if len(got) != 2 || got[0] != "river-a" || got[1] != "glm" {
		t.Fatalf("normalizeModelList = %v, want [river-a glm]", got)
	}
}

func TestStrategyNormalizeAcceptsAliases(t *testing.T) {
	cases := map[string]string{
		"":            StrategyFillFirst,
		"rr":          StrategyRoundRobin,
		"round-robin": StrategyRoundRobin,
		"ROUNDROBIN":  StrategyRoundRobin,
		"ff":          StrategyFillFirst,
		"fillfirst":   StrategyFillFirst,
		"nonsense":    StrategyFillFirst,
	}
	for in, want := range cases {
		c := Config{Strategy: in}
		c.normalize()
		if c.Strategy != want {
			t.Errorf("normalize(%q).Strategy = %q, want %q", in, c.Strategy, want)
		}
	}
}

func TestRoundRobinAdvanceIsPerModel(t *testing.T) {
	rr := newRoundRobin()
	if a, b, c := rr.advance("m1"), rr.advance("m1"), rr.advance("m2"); a != 0 || b != 1 || c != 0 {
		t.Fatalf("advance = %d,%d,%d; want 0,1,0", a, b, c)
	}
}

func TestCooldownWindowExpires(t *testing.T) {
	cd := newCooldowns(10 * time.Millisecond)
	cd.mark("a")
	if !cd.active("a") {
		t.Fatal("a freshly marked auth should be cooling down")
	}
	cd.clear("a")
	if cd.active("a") {
		t.Fatal("clear should end the cooldown")
	}
	cd.mark("b")
	time.Sleep(20 * time.Millisecond)
	if cd.active("b") {
		t.Fatal("the cooldown should have expired")
	}
}

func TestPickAuthFiltersDisabledAndBlocked(t *testing.T) {
	svc := &Service{
		cfg:      Config{Strategy: StrategyFillFirst},
		creds:    map[string]Credential{},
		rr:       newRoundRobin(),
		cooldown: newCooldowns(time.Second),
	}
	svc.creds["on"] = Credential{ID: "on", Type: Provider}
	svc.creds["off"] = Credential{ID: "off", Type: Provider, Disabled: true}
	svc.creds["blocked"] = Credential{
		ID: "blocked", Type: Provider, DisabledModels: []string{"river-x"},
	}

	raw, _ := json.Marshal(map[string]any{
		"Model":     "river-x",
		"Providers": []string{Provider},
		"Candidates": []map[string]any{
			{"ID": "off", "Provider": Provider},
			{"ID": "blocked", "Provider": Provider},
			{"ID": "on", "Provider": Provider},
		},
	})
	got, err := svc.pickAuth(raw)
	if err != nil {
		t.Fatalf("pickAuth: %v", err)
	}
	m := got.(map[string]any)
	if m["AuthID"] != "on" {
		t.Fatalf("pickAuth chose %v, want the only enabled candidate", m["AuthID"])
	}
}

func TestPickAuthDelegatesWhenNothingIsEligible(t *testing.T) {
	svc := &Service{
		cfg:      Config{Strategy: StrategyRoundRobin},
		creds:    map[string]Credential{"off": {ID: "off", Disabled: true}},
		rr:       newRoundRobin(),
		cooldown: newCooldowns(time.Second),
	}
	raw, _ := json.Marshal(map[string]any{
		"Providers":  []string{Provider},
		"Candidates": []map[string]any{{"ID": "off", "Provider": Provider}},
	})
	got, err := svc.pickAuth(raw)
	if err != nil {
		t.Fatalf("pickAuth: %v", err)
	}
	m := got.(map[string]any)
	if m["DelegateBuiltin"] != "round-robin" {
		t.Fatalf("pickAuth = %+v, want a delegation to the builtin", m)
	}
}

func TestPickAuthIgnoresForeignProviders(t *testing.T) {
	svc := &Service{cfg: Config{}, creds: map[string]Credential{}, rr: newRoundRobin()}
	raw, _ := json.Marshal(map[string]any{
		"Providers":  []string{"deepseek"},
		"Candidates": []map[string]any{{"ID": "x", "Provider": "deepseek"}},
	})
	got, err := svc.pickAuth(raw)
	if err != nil {
		t.Fatalf("pickAuth: %v", err)
	}
	if m := got.(map[string]any); m["Handled"] != false {
		t.Fatalf("pickAuth = %+v, want Handled=false for another provider", m)
	}
}

func TestDescribePatchNamesEveryChange(t *testing.T) {
	remark := "主号"
	disabled := true
	models := []string{"river-a"}
	got := describePatch(patchRequest{Remark: &remark, Disabled: &disabled, Models: &models})
	for _, want := range []string{"renamed to 主号", "disabled", "disabled models=1"} {
		if !strings.Contains(got, want) {
			t.Errorf("describePatch = %q, want it to mention %q", got, want)
		}
	}
}

func TestAtoiDefaultFallsBackOnJunk(t *testing.T) {
	if got := atoiDefault("", 7); got != 7 {
		t.Errorf("empty = %d, want the fallback", got)
	}
	if got := atoiDefault("x", 7); got != 7 {
		t.Errorf("junk = %d, want the fallback", got)
	}
	if got := atoiDefault("42", 7); got != 42 {
		t.Errorf("42 = %d, want 42", got)
	}
}

func TestAdvertisedModelsHonoursGlobalBlocks(t *testing.T) {
	c := Credential{
		Models: []DiscoveredModel{
			{ID: "river-a"}, {ID: "river-b"}, {ID: "river-c"},
		},
	}
	cfg := Config{DisabledModels: []string{"river-b"}}
	got := c.AdvertisedModels(cfg)
	if len(got) != 2 || got[0].ID != "river-a" || got[1].ID != "river-c" {
		t.Fatalf("global block not applied: %#v", got)
	}
	if len(c.AdvertisedModels(Config{})) != 3 {
		t.Fatal("an empty block list must keep every model")
	}
}

func TestAdvertisedModelsCombinesPerCredentialAndGlobalBlocks(t *testing.T) {
	c := Credential{
		Models:         []DiscoveredModel{{ID: "river-a"}, {ID: "river-b"}, {ID: "river-c"}},
		DisabledModels: []string{"river-a"},
	}
	got := c.AdvertisedModels(Config{DisabledModels: []string{"river-c"}})
	if len(got) != 1 || got[0].ID != "river-b" {
		t.Fatalf("expected only river-b, got %#v", got)
	}
}

func TestRevisionTracksGlobalBlocks(t *testing.T) {
	c := Credential{Models: []DiscoveredModel{{ID: "river-a"}, {ID: "river-b"}}}
	before := revisionOf(c.AdvertisedModels(Config{}))
	after := revisionOf(c.AdvertisedModels(Config{DisabledModels: []string{"river-b"}}))
	if before == after {
		t.Fatal("blocking a model globally must change the revision")
	}
}
