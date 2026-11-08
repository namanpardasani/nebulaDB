// Package bloom implements the Bloom filters stored inside SSTables.
//
// # What a Bloom filter buys an LSM tree
//
// A point lookup may have to consult many SSTables (every L0 file plus one file
// per deeper level). Most of them do not contain the key. A Bloom filter is a
// tiny in-memory bit set that answers "is this key definitely NOT in the file?"
// without any disk I/O. False negatives are impossible; false positives happen
// with probability p, which only costs one wasted block read.
//
// # The math
//
// With m bits, n keys and k hash probes, the false-positive rate is
//
//	p ≈ (1 - e^(-kn/m))^k
//
// which is minimised at k = (m/n)·ln 2. With b = m/n bits per key:
//
//	b = 10  =>  k = 7   =>  p ≈ 0.82%
//	b =  8  =>  k = 6   =>  p ≈ 2.2%
//	b = 15  =>  k = 10  =>  p ≈ 0.07%
//
// i.e. every extra ~4.8 bits per key divides p by ten. (TestFalsePositiveRate
// measures the real rate and compares it with this formula.)
//
// # Implementation
//
// Instead of k independent hashes we use double hashing: one 64-bit hash is
// split into h1, h2 and probe i tests bit (h1 + i·h2) mod m (Kirsch &
// Mitzenmacher showed this keeps the asymptotic false-positive rate).
// The hash is a fixed, documented function (FNV-1a + murmur3 finaliser) because
// filters are persisted: it must never depend on a per-process random seed.
//
// Filter byte layout:  [ bit array (m/8 bytes) ][ k : 1 byte ]
// k == 0 or k > 30 means "no usable filter" and MayContain returns true.
package bloom

import "math"

// Hash returns the stable 64-bit hash of key used for filter insertion/lookup.
func Hash(key []byte) uint64 {
	h := uint64(14695981039346656037) // FNV-1a
	for _, c := range key {
		h ^= uint64(c)
		h *= 1099511628211
	}
	// murmur3 fmix64: FNV alone has weak low/high bit avalanche for short keys.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// NumProbes returns the optimal number of probes for bitsPerKey, clamped to [1, 30].
func NumProbes(bitsPerKey int) int {
	k := int(float64(bitsPerKey)*math.Ln2 + 0.5)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

// ExpectedFalsePositiveRate is the theoretical p for the given bits per key
// (using the optimal k), for comparison with measurements.
func ExpectedFalsePositiveRate(bitsPerKey int) float64 {
	k := float64(NumProbes(bitsPerKey))
	return math.Pow(1-math.Exp(-k/float64(bitsPerKey)), k)
}

// Builder accumulates key hashes and produces a filter.
type Builder struct {
	hashes []uint64
}

// Add records a key (callers should add each distinct user key once).
func (b *Builder) Add(key []byte) { b.hashes = append(b.hashes, Hash(key)) }

// Len returns the number of keys added.
func (b *Builder) Len() int { return len(b.hashes) }

// Build returns the encoded filter sized for bitsPerKey, or nil if bitsPerKey
// is <= 0 or no keys were added.
func (b *Builder) Build(bitsPerKey int) []byte {
	if bitsPerKey <= 0 || len(b.hashes) == 0 {
		return nil
	}
	nbits := len(b.hashes) * bitsPerKey
	if nbits < 64 {
		nbits = 64 // tiny filters have terrible rates; 8 bytes is nothing
	}
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8
	k := NumProbes(bitsPerKey)
	out := make([]byte, nbytes+1)
	for _, h := range b.hashes {
		h1, h2 := uint32(h), uint32(h>>32)|1 // odd step: visits distinct bits
		for i := 0; i < k; i++ {
			pos := (uint64(h1) + uint64(i)*uint64(h2)) % uint64(nbits)
			out[pos>>3] |= 1 << (pos & 7)
		}
	}
	out[nbytes] = byte(k)
	return out
}

// MayContain reports whether key might be in the set the filter was built from.
// A malformed or empty filter conservatively answers true.
func MayContain(filter, key []byte) bool {
	if len(filter) < 2 {
		return true
	}
	k := int(filter[len(filter)-1])
	if k < 1 || k > 30 {
		return true
	}
	bits := filter[:len(filter)-1]
	nbits := uint64(len(bits)) * 8
	h := Hash(key)
	h1, h2 := uint32(h), uint32(h>>32)|1
	for i := 0; i < k; i++ {
		pos := (uint64(h1) + uint64(i)*uint64(h2)) % nbits
		if bits[pos>>3]&(1<<(pos&7)) == 0 {
			return false
		}
	}
	return true
}
