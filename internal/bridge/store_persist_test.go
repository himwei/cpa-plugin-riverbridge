package bridge

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func sampleEntry(i int) logEntry {
	return logEntry{
		Time:             time.Now().UTC().Format(time.RFC3339Nano),
		Level:            "info",
		Kind:             KindRequest,
		Model:            "river-demo",
		Status:           200,
		Millis:           1000,
		FirstByteMs:      400,
		PromptTokens:     100,
		CachedTokens:     50,
		CompletionTokens: 20,
		TotalTokens:      120,
		CostUSD:          0.001,
	}
}

func TestLogRingSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ring := newLogRing(dir, 100)
	for i := 0; i < 3; i++ {
		ring.add(sampleEntry(i))
	}
	if got := len(ring.snapshot()); got != 3 {
		t.Fatalf("entries in memory = %d, want 3", got)
	}

	// A fresh ring over the same directory is what a host restart produces.
	reopened := newLogRing(dir, 100)
	entries := reopened.snapshot()
	if len(entries) != 3 {
		t.Fatalf("entries after reload = %d, want 3", len(entries))
	}
	if entries[2].Model != "river-demo" || entries[2].PromptTokens != 100 {
		t.Fatalf("reloaded entry lost its columns: %+v", entries[2])
	}
}

func TestLogRingHonoursRetentionOnReload(t *testing.T) {
	dir := t.TempDir()
	ring := newLogRing(dir, 100)
	for i := 0; i < 5; i++ {
		ring.add(sampleEntry(i))
	}
	// Reopening with a smaller retention must not resurrect the trimmed lines.
	reopened := newLogRing(dir, 2)
	if got := len(reopened.snapshot()); got != 2 {
		t.Fatalf("entries after reload = %d, want 2", got)
	}
}

func TestLogRingTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	ring := newLogRing(dir, 10)
	ring.add(sampleEntry(0))

	// Simulate a process killed mid-write.
	path := filepath.Join(dir, requestJournalName)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(body, []byte(`{"sequence":99,"entr`)...), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened := newLogRing(dir, 10)
	if got := len(reopened.snapshot()); got != 1 {
		t.Fatalf("entries after reload = %d, want 1 (torn line dropped)", got)
	}
	reopened.add(sampleEntry(1))
	reloaded := newLogRing(dir, 10)
	if got := len(reloaded.snapshot()); got != 2 {
		t.Fatalf("entries after appending behind a torn tail = %d, want 2", got)
	}
}

func TestLogRingClearIsDurable(t *testing.T) {
	dir := t.TempDir()
	ring := newLogRing(dir, 10)
	ring.add(sampleEntry(0))
	ring.clear()
	if got := len(newLogRing(dir, 10).snapshot()); got != 0 {
		t.Fatalf("entries after replayed clear = %d, want 0", got)
	}
}

func TestStatisticsOutliveLogRetention(t *testing.T) {
	dir := t.TempDir()
	stats := newStatisticsStore(dir)
	stats.dir = dir
	for i := 0; i < 4; i++ {
		stats.record(sampleEntry(i))
	}
	stats.flush()

	// A new store over the same directory sees the totals; nothing about them
	// depends on the log ring still holding the lines.
	reopened := newStatisticsStore(dir)
	total, _, ok := reopened.rangeTotals(time.Time{}, time.Time{})
	if !ok {
		t.Fatal("statistics reported unavailable")
	}
	if total.Requests != 4 {
		t.Fatalf("requests = %d, want 4", total.Requests)
	}
	if total.PromptTokens != 400 || total.CachedTokens != 200 || total.CompletionTokens != 80 {
		t.Fatalf("token totals lost: %+v", total)
	}
	if total.CostMicroUSD != 4000 {
		t.Fatalf("cost micro usd = %d, want 4000", total.CostMicroUSD)
	}
}

func TestStatisticsRangeFiltersByMinute(t *testing.T) {
	dir := t.TempDir()
	stats := newStatisticsStore(dir)
	stats.dir = dir

	old := sampleEntry(0)
	old.Time = time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	stats.record(old)
	stats.record(sampleEntry(1))
	stats.flush()

	recent, _, _ := stats.rangeTotals(time.Now().UTC().Add(-time.Hour), time.Time{})
	if recent.Requests != 1 {
		t.Fatalf("recent requests = %d, want 1", recent.Requests)
	}
	all, _, _ := stats.rangeTotals(time.Time{}, time.Time{})
	if all.Requests != 2 {
		t.Fatalf("all requests = %d, want 2", all.Requests)
	}
}

func TestStatisticsIgnoreEventRows(t *testing.T) {
	stats := newStatisticsStore(t.TempDir())
	stats.record(logEntry{Kind: KindEvent, Level: "info", Detail: "settings saved"})
	total, _, _ := stats.rangeTotals(time.Time{}, time.Time{})
	if total.Requests != 0 {
		t.Fatalf("event rows must not count as requests: %+v", total)
	}
}

func TestStatisticsResetStartsNewGeneration(t *testing.T) {
	dir := t.TempDir()
	stats := newStatisticsStore(dir)
	stats.dir = dir
	stats.record(sampleEntry(0))
	stats.flush()
	firstGen := stats.meta.Generation

	since, err := stats.reset()
	if err != nil {
		t.Fatal(err)
	}
	if since.IsZero() {
		t.Fatal("reset returned a zero timestamp")
	}
	if stats.meta.Generation == firstGen {
		t.Fatal("reset must start a new generation")
	}
	if total, _, _ := stats.rangeTotals(time.Time{}, time.Time{}); total.Requests != 0 {
		t.Fatalf("requests after reset = %d, want 0", total.Requests)
	}
	// The previous generation stays on disk as a rollback copy.
	if _, err := os.Stat(filepath.Join(dir, statisticsDirName, firstGen)); err != nil {
		t.Fatalf("previous generation was deleted: %v", err)
	}
}
