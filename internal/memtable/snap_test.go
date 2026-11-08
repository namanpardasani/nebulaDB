package memtable

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/naman/nebuladb/internal/iterator"
)

// Snapshot reads against one memtable while a writer inserts 3-key batches and
// publishes a "visible" sequence number after each batch (what the engine does).
func TestSnapshotBatchesNeverTorn(t *testing.T) {
	m := New()
	var visible atomic.Uint64
	var stop atomic.Bool
	var wg sync.WaitGroup
	keys := []string{"a", "b", "c"}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				snap := visible.Load()
				var vals [3]string
				var seqs [3]uint64
				for i, k := range keys {
					if e, ok := m.Get([]byte(k), snap); ok {
						vals[i], seqs[i] = string(e.Value), e.Seq
					}
				}
				if vals[0] != vals[1] || vals[1] != vals[2] {
					t.Errorf("snap=%d torn: %v seqs=%v", snap, vals, seqs)
					return
				}
			}
		}()
	}
	seq := uint64(0)
	for i := 0; i < 20000; i++ {
		s := strconv.Itoa(i)
		for _, k := range keys {
			seq++
			m.Add(seq, iterator.KindPut, []byte(k), []byte(s), 0)
		}
		visible.Store(seq)
	}
	stop.Store(true)
	wg.Wait()
}
