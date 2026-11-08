package cache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func blk(n int, fill byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = fill
	}
	return b
}

// With a single-shard-sized key set we can check exact LRU order by forcing all
// keys into one shard via brute-force search for colliding keys.
func sameShardKeys(c *Cache, n int) []uint64 {
	target := c.shardFor(key{1, 0})
	var offs []uint64
	for off := uint64(0); len(offs) < n; off += 4096 {
		if c.shardFor(key{1, off}) == target {
			offs = append(offs, off)
		}
	}
	return offs
}

func TestLRUOrder(t *testing.T) {
	c := New(16 * 300) // 300 bytes per shard
	offs := sameShardKeys(c, 4)
	for i := 0; i < 3; i++ {
		c.Put(1, offs[i], blk(100, byte(i))) // fills the shard exactly (3 x 100)
	}
	c.Get(1, offs[0])              // touch #0 => #1 is now least recently used
	c.Put(1, offs[3], blk(100, 3)) // must evict #1
	if _, ok := c.Get(1, offs[1]); ok {
		t.Fatal("LRU victim was not evicted")
	}
	for _, i := range []int{0, 2, 3} {
		if _, ok := c.Get(1, offs[i]); !ok {
			t.Fatalf("entry %d wrongly evicted", i)
		}
	}
	if st := c.Stats(); st.Evictions != 1 {
		t.Fatalf("evictions=%d", st.Evictions)
	}
}

func TestCapacityBoundAndOversize(t *testing.T) {
	c := New(16 * 1000)
	for i := 0; i < 5000; i++ {
		c.Put(uint64(i%7), uint64(i)*4096, blk(200, 1))
	}
	if st := c.Stats(); st.Bytes > 16*1000 {
		t.Fatalf("over budget: %d bytes", st.Bytes)
	}
	c.Put(99, 0, blk(5000, 1)) // larger than a shard: silently not cached
	if _, ok := c.Get(99, 0); ok {
		t.Fatal("oversize block cached")
	}
	z := New(0)
	z.Put(1, 0, blk(10, 1))
	if _, ok := z.Get(1, 0); ok {
		t.Fatal("zero-capacity cache stored data")
	}
}

func TestReplaceAdjustsSize(t *testing.T) {
	c := New(16 * 1000)
	c.Put(1, 0, blk(100, 1))
	c.Put(1, 0, blk(300, 2))
	st := c.Stats()
	if st.Entries != 1 || st.Bytes != 300 {
		t.Fatalf("%+v", st)
	}
	if b, _ := c.Get(1, 0); b[0] != 2 {
		t.Fatal("stale value")
	}
}

func TestEvictFile(t *testing.T) {
	c := New(1 << 20)
	for off := uint64(0); off < 50; off++ {
		c.Put(1, off*4096, blk(64, 1))
		c.Put(2, off*4096, blk(64, 2))
	}
	c.EvictFile(1)
	st := c.Stats()
	if st.Entries != 50 || st.Bytes != 50*64 {
		t.Fatalf("%+v", st)
	}
	if _, ok := c.Get(1, 0); ok {
		t.Fatal("file 1 block survived")
	}
	if _, ok := c.Get(2, 0); !ok {
		t.Fatal("file 2 block lost")
	}
}

func TestHitRate(t *testing.T) {
	c := New(1 << 20)
	if c.Stats().HitRate() != 0 {
		t.Fatal("no traffic => 0")
	}
	c.Put(1, 0, blk(10, 1))
	c.Get(1, 0)
	c.Get(1, 0)
	c.Get(1, 4096)
	if hr := c.Stats().HitRate(); hr < 0.66 || hr > 0.67 {
		t.Fatalf("hit rate %f", hr)
	}
}

func TestConcurrent(t *testing.T) {
	c := New(64 << 10)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 20000; i++ {
				f, off := uint64(rng.Intn(4)), uint64(rng.Intn(200))*4096
				if b, ok := c.Get(f, off); ok {
					if len(b) != 512 || b[0] != byte(f) {
						t.Errorf("corrupt cache entry for %d/%d", f, off)
						return
					}
				} else {
					c.Put(f, off, blk(512, byte(f)))
				}
				if i%5000 == 0 {
					c.EvictFile(uint64(rng.Intn(4)))
				}
			}
		}(g)
	}
	wg.Wait()
	if st := c.Stats(); st.Bytes > 64<<10 {
		t.Fatalf("over budget %d", st.Bytes)
	}
}

func BenchmarkGetHit(b *testing.B) {
	c := New(64 << 20)
	for i := 0; i < 1000; i++ {
		c.Put(1, uint64(i)*4096, blk(4096, 1))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(1, uint64(i%1000)*4096)
	}
}

func BenchmarkGetHitParallel(b *testing.B) {
	c := New(64 << 20)
	for i := 0; i < 1000; i++ {
		c.Put(1, uint64(i)*4096, blk(4096, 1))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.Intn(1000)
		for pb.Next() {
			c.Get(1, uint64(i%1000)*4096)
			i++
		}
	})
}

func ExampleCache() {
	c := New(1 << 20)
	c.Put(7, 0, []byte("block"))
	b, ok := c.Get(7, 0)
	fmt.Println(string(b), ok)
	// Output: block true
}
