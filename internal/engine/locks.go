package engine

import (
	"sort"
	"sync"
)

const numStripes = 256

// keyLocks is a fixed array of mutexes indexed by hash(key). Holding a key's
// stripe across read -> decide -> WAL commit -> apply makes read-modify-write
// commands linearizable per key, while writers on different keys proceed in
// parallel and so still reach the WAL together and share one fsync.
type keyLocks struct {
	s [numStripes]sync.Mutex
}

func stripeOf(key []byte) int {
	h := uint32(2166136261) // FNV-1a
	for _, c := range key {
		h = (h ^ uint32(c)) * 16777619
	}
	return int(h % numStripes)
}

// lock acquires the stripes covering keys in ascending order (so concurrent
// multi-key operations cannot deadlock) and returns the release function.
func (k *keyLocks) lock(keys ...[]byte) func() {
	idx := make([]int, 0, len(keys))
	seen := map[int]bool{}
	for _, key := range keys {
		s := stripeOf(key)
		if !seen[s] {
			seen[s] = true
			idx = append(idx, s)
		}
	}
	sort.Ints(idx)
	for _, s := range idx {
		k.s[s].Lock()
	}
	return func() {
		for i := len(idx) - 1; i >= 0; i-- {
			k.s[idx[i]].Unlock()
		}
	}
}

func (k *keyLocks) lockAll() func() {
	for i := range k.s {
		k.s[i].Lock()
	}
	return func() {
		for i := len(k.s) - 1; i >= 0; i-- {
			k.s[i].Unlock()
		}
	}
}
