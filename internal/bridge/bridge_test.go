package bridge

import "testing"

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
