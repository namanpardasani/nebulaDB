package nebuladb_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/naman/nebuladb/pkg/nebuladb"
)

func TestEmbeddedAPI(t *testing.T) {
	dir := t.TempDir()
	db, err := nebuladb.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("nope")); !errors.Is(err, nebuladb.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	db.Set([]byte("a"), []byte("1"))
	if ok, _ := db.SetNX([]byte("a"), []byte("2")); ok {
		t.Fatal("SetNX overwrote")
	}
	db.SetTTL([]byte("t"), []byte("x"), time.Hour)
	if ttl, ok, _ := db.TTL([]byte("t")); !ok || ttl <= 59*time.Minute {
		t.Fatalf("ttl %v %v", ttl, ok)
	}
	if ttl, ok, _ := db.TTL([]byte("a")); !ok || ttl != 0 {
		t.Fatalf("persistent key ttl %v %v", ttl, ok)
	}
	if n, _ := db.Incr([]byte("c"), 5); n != 5 {
		t.Fatal(n)
	}
	db.MSet([]byte("m1"), []byte("x"), []byte("m2"), []byte("y"))
	if ok, _ := db.Delete([]byte("m1")); !ok {
		t.Fatal("delete")
	}
	db.Close()

	db, err = nebuladb.Open(dir, nil) // persists across reopen
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if v, _ := db.Get([]byte("a")); string(v) != "1" {
		t.Fatalf("got %q", v)
	}
	if v, _ := db.Get([]byte("c")); string(v) != "5" {
		t.Fatalf("got %q", v)
	}
	var all []string
	var next []byte
	for {
		ks, n, err := db.Keys(next, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range ks {
			all = append(all, string(k))
		}
		if n == nil {
			break
		}
		next = n
	}
	if fmt.Sprint(all) != "[a c m2 t]" {
		t.Fatalf("keys %v", all)
	}
	if db.Stats()["engine"] != "lsm" {
		t.Fatal("stats")
	}
}
