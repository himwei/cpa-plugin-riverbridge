package bridge

import "strings"

// price is River's published rate for one model, in USD per million tokens.
// Source: https://console.river.ai/api/public/pricing
type price struct {
	Input  float64
	Cached float64
	Output float64
	// Context and MaxOutput mirror the published capability table when known.
	Context   int
	MaxOutput int
}

// riverPrices is the built-in price and capability table. Unknown models cost
// nothing here; the plugin never invents a rate it cannot justify.
var riverPrices = map[string]price{
	"deepseek-ai/DeepSeek-V4.1-Flash-1017": {
		Input: 0.12, Cached: 0.0024, Output: 0.48,
		Context: 1048576, MaxOutput: 384000,
	},
	"deepseek-ai/DeepSeek-V4.1-Flash": {
		Input: 0.12, Cached: 0.0024, Output: 0.48,
		Context: 1048576, MaxOutput: 384000,
	},
	"zai-org/GLM-5.3-Flash": {
		Input: 0.12, Cached: 0.024, Output: 0.4,
		Context: 1048576, MaxOutput: 131072,
	},
	"nvidia/NVIDIA-Nemotron-3.5-Super-VL-120B-A12B-NVFP4": {
		Input: 0.07, Cached: 0.024, Output: 0.37,
		Context: 1048576, MaxOutput: 131072,
	},
	"moonshotai/Kimi-K3": {
		Input: 3.0, Cached: 0.3, Output: 15.0,
		Context: 262144, MaxOutput: 262144,
	},
	"Qwen/Qwen3.8-27B-FP8": {
		Input: 0.1, Cached: 0.02, Output: 2.0,
		Context: 262144, MaxOutput: 262143,
	},
}

// lookupPrice resolves an upstream model id, tolerating case differences.
func lookupPrice(upstream string) (price, bool) {
	if p, ok := riverPrices[upstream]; ok {
		return p, true
	}
	lower := strings.ToLower(upstream)
	for k, v := range riverPrices {
		if strings.ToLower(k) == lower {
			return v, true
		}
	}
	return price{}, false
}

// priceUsage prices one response for a known upstream model.
func priceUsage(upstream string, u *Usage) float64 {
	if u == nil {
		return 0
	}
	p, ok := lookupPrice(upstream)
	if !ok {
		return 0
	}
	billableInput := u.PromptTokens - u.CachedTokens
	if billableInput < 0 {
		billableInput = 0
	}
	cost := float64(billableInput) / 1e6 * p.Input
	cost += float64(u.CachedTokens) / 1e6 * p.Cached
	cost += float64(u.CompletionTokens) / 1e6 * p.Output
	return cost
}
