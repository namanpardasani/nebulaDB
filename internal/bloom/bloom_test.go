package bloom

import (
	"fmt"
	"math"
	"testing"
)

func build(n, bits int) []byte {
	var b Builder
	for i := 0; i < n; i++ {
		b.Add([]byte(fmt.Sprintf("present-%d", i)))
	}
	return b.Build(bits)
}

func TestNoFalseNegatives(t *testing.T) {
	for _, bits := range []int{1, 4, 10, 20} {
		f := build(5000, bits)
		for i := 0; i < 5000; i++ {
			if !MayContain(f, []byte(fmt.Sprintf("present-%d", i))) {
				t.Fatalf("bits=%d: false negative for key %d", bits, i)
			}
		}
	}
}

// Measures the real false-positive rate on 200k absent keys and checks it
// against the theoretical formula (within a generous tolerance: the formula is
// asymptotic and the hash is not perfectly random).
func TestFalsePositiveRate(t *testing.T) {
	const n, probes = 20000, 200000
	for _, bits := range []int{6, 8, 10, 12, 16} {
		f := build(n, bits)
		fp := 0
		for i := 0; i < probes; i++ {
			if MayContain(f, []byte(fmt.Sprintf("absent-%d", i))) {
				fp++
			}
		}
		got := float64(fp) / probes
		want := ExpectedFalsePositiveRate(bits)
		t.Logf("bits/key=%2d k=%2d filter=%6d bytes  measured FP=%.4f%%  theory=%.4f%%",
			bits, NumProbes(bits), len(f), got*100, want*100)
		if got > want*1.6+0.0005 || got < want*0.5-0.0005 {
			t.Errorf("bits=%d: measured %.5f vs theoretical %.5f", bits, got, want)
		}
	}
}

func TestEdgeCases(t *testing.T) {
	var b Builder
	if b.Build(10) != nil {
		t.Error("empty builder should produce no filter")
	}
	b.Add([]byte("x"))
	if b.Build(0) != nil {
		t.Error("0 bits/key disables the filter")
	}
	// Malformed filters never produce false negatives.
	for _, f := range [][]byte{nil, {}, {7}, {0, 0, 0}, {255, 255, 99}, {1, 2, 3, 31}} {
		if !MayContain(f, []byte("anything")) {
			t.Errorf("malformed filter %v rejected a key", f)
		}
	}
	if k := NumProbes(10); k != 7 {
		t.Errorf("k(10)=%d", k)
	}
	if math.Abs(ExpectedFalsePositiveRate(10)-0.0082) > 0.001 {
		t.Errorf("theory(10) = %f", ExpectedFalsePositiveRate(10))
	}
}

func TestHashStable(t *testing.T) {
	// Filters are persisted: the hash function must never change.
	if h := Hash([]byte("nebuladb")); h != 0xa24e4d5b41de4c19 {
		t.Fatalf("hash function changed (%#x): this would silently break every existing SSTable filter", h)
	}
	a, b := Hash([]byte("k1")), Hash([]byte("k1"))
	if a != b || a == Hash([]byte("k2")) {
		t.Fatal("hash unstable or colliding on trivial input")
	}
}

func FuzzMayContain(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 7}, []byte("key"))
	f.Fuzz(func(t *testing.T, filter, key []byte) { MayContain(filter, key) })
}

func BenchmarkMayContain(b *testing.B) {
	f := build(100000, 10)
	keys := make([][]byte, 1024)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("present-%d", i*7))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MayContain(f, keys[i&1023])
	}
}
