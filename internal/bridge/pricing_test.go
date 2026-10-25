package bridge

import (
	"testing"
)

func TestFillUsageComputesGenerationSpeed(t *testing.T) {
	e := logEntry{FirstByteMs: 500, Millis: 1500}
	e.fillUsage(&Usage{PromptTokens: 1000, CachedTokens: 900, CompletionTokens: 200, TotalTokens: 1200})
	if e.TokensPerSecond != 200 {
		t.Fatalf("tokens per second = %v, want 200", e.TokensPerSecond)
	}
	if e.CachedTokens != 900 || e.PromptTokens != 1000 {
		t.Fatalf("token counters not copied: %+v", e)
	}
}

func TestFillUsageSkipsSpeedWithoutWindow(t *testing.T) {
	// One round trip with no gap between first byte and completion has no
	// meaningful generation window, so speed must stay unset rather than
	// divide by zero or report an invented number.
	e := logEntry{FirstByteMs: 800, Millis: 800}
	e.fillUsage(&Usage{CompletionTokens: 50})
	if e.TokensPerSecond != 0 {
		t.Fatalf("tokens per second = %v, want 0", e.TokensPerSecond)
	}
}

func TestSummariseLogsIgnoresEvents(t *testing.T) {
	got := summariseLogs([]logEntry{
		{Kind: KindRequest, Status: 200, Millis: 1000, FirstByteMs: 400, TokensPerSecond: 100,
			PromptTokens: 1000, CachedTokens: 500, CompletionTokens: 200, TotalTokens: 1200, CostUSD: 0.001},
		{Kind: KindRequest, Status: 500, Level: "error", Millis: 200, FirstByteMs: 200, PromptTokens: 0},
		{Kind: KindEvent, Level: "info", Detail: "settings saved"},
	})
	if got.Requests != 2 || got.Failures != 1 {
		t.Fatalf("requests/failures = %d/%d, want 2/1", got.Requests, got.Failures)
	}
	if got.CacheHitRate != 50 {
		t.Fatalf("cache hit rate = %v, want 50", got.CacheHitRate)
	}
	if got.AvgFirstByteMs != 300 {
		t.Fatalf("avg first byte = %v, want 300", got.AvgFirstByteMs)
	}
	if got.TotalTokens != 1200 {
		t.Fatalf("total tokens = %v, want 1200 (event rows must not add any)", got.TotalTokens)
	}
}

func TestPricingViewCarriesPublishedRates(t *testing.T) {
	p := viewOf("deepseek-ai/DeepSeek-V4.1-Flash-1017")
	if p.InputUSDPerMillion != 0.12 || p.CachedUSDPerMillion != 0.0024 || p.OutputUSDPerMillion != 0.48 {
		t.Fatalf("unexpected pricing view: %+v", p)
	}
	if unknown := viewOf("nobody/unknown-model"); unknown.InputUSDPerMillion != 0 {
		t.Fatalf("unknown model must not invent a rate: %+v", unknown)
	}
}
