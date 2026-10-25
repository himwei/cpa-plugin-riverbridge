package bridge

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// spendStore keeps each key's budget and running spend in the plugin's own
// data directory.
//
// It cannot live in the auth file: CPA re-runs auth.parse for a credential on
// every request, and parseAuth rebuilds the record from that file. A total
// written at flush time would be overwritten by the next parse reading the
// older file, so the counter silently fell back to zero within a second of
// being charged. A file we own is never re-read by the host.
type spendStore struct {
	mu   sync.Mutex
	dir  string
	data map[string]spendRecord
}

// spendRecord is one key's budget state. Budget of zero means no ceiling.
type spendRecord struct {
	BudgetUSD float64 `json:"budget_usd"`
	SpentUSD  float64 `json:"spent_usd"`
	// AutoDisabled records that this plugin switched the key off because its
	// budget ran out, as opposed to the operator doing it. Only the former is
	// undone automatically on a top-up.
	AutoDisabled bool `json:"auto_disabled,omitempty"`
}

const spendFileName = "spend.json"

func newSpendStore(dir string) *spendStore {
	st := &spendStore{dir: dir, data: map[string]spendRecord{}}
	st.load()
	return st
}

func (st *spendStore) path() string {
	if st.dir == "" {
		return ""
	}
	return filepath.Join(st.dir, spendFileName)
}

func (st *spendStore) load() {
	p := st.path()
	if p == "" {
		return
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		return
	}
	var data map[string]spendRecord
	if json.Unmarshal(raw, &data) == nil && data != nil {
		st.data = data
	}
}

// save writes the whole map. It is called on the flush timer, not per request:
// a charged call only marks memory, and losing a few seconds of accounting to
// a crash is preferable to a disk sync on the hot path.
func (st *spendStore) save() error {
	p := st.path()
	if p == "" {
		return nil
	}
	st.mu.Lock()
	raw, err := json.Marshal(st.data)
	st.mu.Unlock()
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Dir(p), filepath.Base(p), raw)
}

func (st *spendStore) get(id string) spendRecord {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.data[id]
}

func (st *spendStore) set(id string, r spendRecord) {
	st.mu.Lock()
	if r.BudgetUSD == 0 && r.SpentUSD == 0 {
		delete(st.data, id)
	} else {
		st.data[id] = r
	}
	st.mu.Unlock()
}
