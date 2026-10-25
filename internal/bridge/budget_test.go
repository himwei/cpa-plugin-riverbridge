package bridge

import (
	"encoding/json"
	"math"
	"testing"
)

// A key with no ceiling must never read as exhausted: zero balance means "no
// budget set", and treating that as "nothing left" would take every key that
// nobody configured out of service.
func TestExhaustedIgnoresUnbudgetedKeys(t *testing.T) {
	c := Credential{}
	if c.Exhausted() {
		t.Fatal("an unbudgeted key must not be exhausted")
	}
	c.SpentUSD = 99
	if c.Exhausted() {
		t.Fatal("spending without a ceiling must not exhaust a key")
	}
	if got := c.RemainingUSD(); got != -1 {
		t.Fatalf("RemainingUSD for no ceiling = %v, want -1", got)
	}
}

func TestExhaustedAtCeiling(t *testing.T) {
	c := Credential{BalanceUSD: 25, SpentUSD: 24.999}
	if c.Exhausted() {
		t.Fatal("under the ceiling is not exhausted")
	}
	if got := c.RemainingUSD(); math.Abs(got-0.001) > 1e-9 {
		t.Fatalf("RemainingUSD = %v, want 0.001", got)
	}
	c.SpentUSD = 25
	if !c.Exhausted() {
		t.Fatal("reaching the ceiling must exhaust the key")
	}
	c.SpentUSD = 40
	if got := c.RemainingUSD(); got != 0 {
		t.Fatalf("overspent RemainingUSD = %v, want 0 (not negative)", got)
	}
}

func TestPatchCarriesBudget(t *testing.T) {
	var req patchRequest
	if err := json.Unmarshal([]byte(`{"id":"a","priority":9,"balance_usd":25}`), &req); err != nil {
		t.Fatal(err)
	}
	if req.Priority == nil || *req.Priority != 9 {
		t.Fatal("priority not decoded")
	}
	if req.BalanceUSD == nil || *req.BalanceUSD != 25 {
		t.Fatal("balance not decoded")
	}
	// Omitting the field must leave it alone rather than zero it.
	var empty patchRequest
	if err := json.Unmarshal([]byte(`{"id":"a"}`), &empty); err != nil {
		t.Fatal(err)
	}
	if empty.BalanceUSD != nil {
		t.Fatal("absent balance must stay nil so it is not written")
	}
}

// An exhausted key has to drop out of the picker the same way a disabled one
// does, or fill-first keeps choosing a key that cannot pay.
func TestPickAuthSkipsExhaustedKeys(t *testing.T) {
	s := NewService()
	s.creds = map[string]Credential{
		"rich": {ID: "rich", Priority: 100, BalanceUSD: 25, SpentUSD: 1},
		"poor": {ID: "poor", Priority: 100, BalanceUSD: 25, SpentUSD: 25},
	}
	raw, _ := json.Marshal(map[string]any{
		"Model": "river-deepseek-v4.1-flash",
		"Candidates": []map[string]any{
			{"ID": "rich", "Provider": Provider, "Priority": 100},
			{"ID": "poor", "Provider": Provider, "Priority": 100},
		},
	})
	out, err := s.pickAuth(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := out.(map[string]any)
	if got["Handled"] != true || got["AuthID"] != "rich" {
		t.Fatalf("picked %+v, want rich", got)
	}
}

// The counter must survive the host re-parsing the auth file, which is what
// resets anything stored there. This pins the store as the source of truth.
func TestSpendSurvivesAuthReparse(t *testing.T) {
	dir := t.TempDir()
	s := NewService()
	s.spent = newSpendStore(dir)
	s.setSpend("river-x", 25)
	s.addSpend("river-x", 0.5)

	// Simulate what the host does: hand back the file contents, which never
	// carry the counters.
	fromFile := Credential{ID: "river-x", APIKey: "rv_test"}
	merged := s.mergeSpend(fromFile)
	if merged.BalanceUSD != 25 {
		t.Fatalf("budget after reparse = %v, want 25", merged.BalanceUSD)
	}
	if merged.SpentUSD != 0.5 {
		t.Fatalf("spent after reparse = %v, want 0.5", merged.SpentUSD)
	}
}

// The auth file never carries the counters, so a reparse must not resurrect a
// stale ceiling: whatever the store says wins.
func TestSpendIgnoresStaleAuthFileBudget(t *testing.T) {
	s := NewService()
	s.spent = newSpendStore(t.TempDir())
	s.setSpend("river-y", 5)
	merged := s.mergeSpend(Credential{ID: "river-y", BalanceUSD: 999, SpentUSD: 999})
	if merged.BalanceUSD != 5 {
		t.Fatalf("stale auth-file budget won: %+v", merged)
	}
}

func TestSpendResetKeepsBudget(t *testing.T) {
	s := NewService()
	s.spent = newSpendStore(t.TempDir())
	s.setSpend("river-z", 25)
	s.addSpend("river-z", 9)
	s.resetSpend("river-z")
	got := s.mergeSpend(Credential{ID: "river-z"})
	if got.SpentUSD != 0 {
		t.Fatalf("spent after reset = %v, want 0", got.SpentUSD)
	}
	if got.BalanceUSD != 25 {
		t.Fatalf("reset must keep the ceiling, got %v", got.BalanceUSD)
	}
}
