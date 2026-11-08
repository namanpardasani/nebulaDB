// Package cache implements the SSTable block cache: a sharded, byte-bounded
// LRU keyed by (file number, block offset).
//
// # Why cache blocks
//
// A point lookup that survives the Bloom filter must read one 4 KiB data block.
// Hot keys cluster in a small set of blocks, so keeping recently used blocks in
// RAM turns most reads into a hash lookup instead of a pread (and a page-cache
// copy). The OS page cache helps too, but our cache holds blocks already
// checksum-verified and in the form the reader wants, and its size is under our
// control.
//
// # Design
//
//   - LRU: a doubly linked list per shard; the most recently used entry is at
//     the front, eviction takes from the back until the shard fits its budget.
//   - Sharding: the key is hashed to one of 16 shards, each with its own mutex,
//     so concurrent readers rarely contend (a single global LRU lock would
//     serialise every read, since even a Get mutates the recency list).
//   - Immutability: cached slices are never modified. An iterator that still
//     holds a block keeps it alive through the garbage collector even after
//     eviction, so no pinning protocol is needed.
//   - Keys include the file number; numbers are never reused, so a deleted
//     file's blocks can never be served by mistake. EvictFile reclaims their
//     memory early.
package cache

import (
	"container/list"
	"sync"
)

const numShards = 16

type key struct{ file, off uint64 }

type entry struct {
	k    key
	data []byte
}

type shard struct {
	mu       sync.Mutex
	lru      *list.List // front = most recent
	items    map[key]*list.Element
	bytes    int64
	capacity int64
	// Counters live in the shard (guarded by mu) instead of global atomics: a
	// shared atomic counter bounces its cache line between all cores on every
	// Get and was measurably the bottleneck under parallel reads.
	hits, misses, evictions uint64
}

// Cache is a sharded LRU block cache. The zero value is not usable; use New.
type Cache struct {
	shards [numShards]shard
}

// New returns a cache holding about capacityBytes of block data. A capacity
// <= 0 yields a cache that stores nothing (every Get misses).
func New(capacityBytes int64) *Cache {
	c := &Cache{}
	per := capacityBytes / numShards
	for i := range c.shards {
		c.shards[i] = shard{lru: list.New(), items: map[key]*list.Element{}, capacity: per}
	}
	return c
}

func (c *Cache) shardFor(k key) *shard {
	// Mix both fields; offsets are multiples of the block size so use high bits.
	h := k.file*0x9E3779B97F4A7C15 ^ (k.off>>9)*0xC2B2AE3D27D4EB4F
	return &c.shards[(h>>32)%numShards]
}

// Get returns the cached block, marking it most recently used.
func (c *Cache) Get(file, off uint64) ([]byte, bool) {
	k := key{file, off}
	s := c.shardFor(k)
	s.mu.Lock()
	el, ok := s.items[k]
	if ok {
		s.lru.MoveToFront(el)
		data := el.Value.(*entry).data
		s.hits++
		s.mu.Unlock()
		return data, true
	}
	s.misses++
	s.mu.Unlock()
	return nil, false
}

// Put inserts a block (which must not be modified afterwards), evicting least
// recently used blocks of the same shard if needed. A block larger than the
// shard budget is not cached.
func (c *Cache) Put(file, off uint64, block []byte) {
	k := key{file, off}
	s := c.shardFor(k)
	size := int64(len(block))
	s.mu.Lock()
	defer s.mu.Unlock()
	if size > s.capacity {
		return
	}
	if el, ok := s.items[k]; ok { // replace (e.g. two readers raced on the same miss)
		e := el.Value.(*entry)
		s.bytes += size - int64(len(e.data))
		e.data = block
		s.lru.MoveToFront(el)
	} else {
		s.items[k] = s.lru.PushFront(&entry{k, block})
		s.bytes += size
	}
	for s.bytes > s.capacity {
		back := s.lru.Back()
		e := back.Value.(*entry)
		s.lru.Remove(back)
		delete(s.items, e.k)
		s.bytes -= int64(len(e.data))
		s.evictions++
	}
}

// EvictFile drops every block of the given file (called when a table is deleted).
func (c *Cache) EvictFile(file uint64) {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		for k, el := range s.items {
			if k.file == file {
				s.bytes -= int64(len(el.Value.(*entry).data))
				s.lru.Remove(el)
				delete(s.items, k)
			}
		}
		s.mu.Unlock()
	}
}

// Stats is a point-in-time summary.
type Stats struct {
	Hits, Misses, Evictions uint64
	Bytes, Entries          int64
}

// HitRate returns hits/(hits+misses), or 0 with no traffic.
func (s Stats) HitRate() float64 {
	if s.Hits+s.Misses == 0 {
		return 0
	}
	return float64(s.Hits) / float64(s.Hits+s.Misses)
}

// Stats returns counters and current occupancy.
func (c *Cache) Stats() Stats {
	var st Stats
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		st.Hits += s.hits
		st.Misses += s.misses
		st.Evictions += s.evictions
		st.Bytes += s.bytes
		st.Entries += int64(len(s.items))
		s.mu.Unlock()
	}
	return st
}
