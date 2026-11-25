package storage

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// The layout identity is one atomic descriptor field set: written before a
// claim, durable across a close and a reopen, flipped to not-fresh, and
// bounded; a store that never set it reports none.
func TestLayoutIdentityIsDurableInTheDescriptor(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s")
	s, err := CreateWALStoreWithOptions(p, 64, 4096, WALStoreOptions{ChunkBlocks: 8})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := s.LayoutIdentity(); ok {
		t.Fatal("a new store reports a layout identity")
	}
	if err := s.SetLayoutIdentity("k-1234", true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWALStore(p)
	if err != nil {
		t.Fatal(err)
	}
	if id, fresh, ok := s.LayoutIdentity(); !ok || id != "k-1234" || !fresh {
		t.Fatalf("after reopen: %q fresh %t ok %t", id, fresh, ok)
	}
	if err := s.SetLayoutIdentity("k-1234", false); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", strings.Repeat("k", MaxLayoutClaimIDBytes+1), "a\xff"} {
		if err := s.SetLayoutIdentity(bad, false); err == nil {
			t.Errorf("claim id %q accepted", bad)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWALStore(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if id, fresh, ok := s.LayoutIdentity(); !ok || id != "k-1234" || fresh {
		t.Fatalf("after the fresh flip and reopen: %q fresh %t ok %t", id, fresh, ok)
	}
}

// The identity bytes decode strictly: unknown flags, a length out of
// bounds, bytes after the claim id, or bytes without the identity are
// refused; the round trip is exact.
func TestLayoutIdentityDecodeIsStrict(t *testing.T) {
	d := descriptorV3{LayoutSet: true, LayoutFresh: true, LayoutClaimID: "k"}
	b := d.encode()
	var got descriptorV3
	if err := decodeLayoutIdentityV3(&got, b[88:122]); err != nil || !got.LayoutSet || !got.LayoutFresh || got.LayoutClaimID != "k" {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	for name, mut := range map[string]func(x []byte){
		"unknown flag":       func(x []byte) { x[0] |= 0x80 },
		"zero length":        func(x []byte) { x[1] = 0 },
		"too long":           func(x []byte) { x[1] = MaxLayoutClaimIDBytes + 1 },
		"after the claim id": func(x []byte) { x[2+1] = 'z' },
		"without identity":   func(x []byte) { x[0] = 0 },
	} {
		x := append([]byte(nil), b[88:122]...)
		mut(x)
		if err := decodeLayoutIdentityV3(&descriptorV3{}, x); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// The identity is written once: another claim id on a root that has one,
// and fresh coming back after it was cleared, are refused and change
// nothing.
func TestLayoutIdentityIsImmutable(t *testing.T) {
	s, err := CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "s"), 64, 4096, WALStoreOptions{ChunkBlocks: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SetLayoutIdentity("k-1", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLayoutIdentity("k-2", true); err == nil {
		t.Fatal("a second claim id replaced the root's")
	}
	if err := s.SetLayoutIdentity("k-1", false); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLayoutIdentity("k-1", true); err == nil {
		t.Fatal("a root that held writes became fresh again")
	}
	if id, fresh, ok := s.LayoutIdentity(); !ok || id != "k-1" || fresh {
		t.Fatalf("after the refused changes: %q fresh %t", id, fresh)
	}
}

// Fresh is a property of the root: while it is set no guest write (single
// or batch) is acknowledged; once cleared (durably, before the write) the
// root takes writes, and a reopen keeps it cleared.
func TestLayoutFreshRefusesGuestWrites(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s")
	s, err := CreateWALStoreWithOptions(p, 64, 4096, WALStoreOptions{ChunkBlocks: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetLayoutIdentity("k-1", true); err != nil {
		t.Fatal(err)
	}
	blk := make([]byte, 4096)
	if _, err := s.Write(0, blk); !errors.Is(err, ErrLayoutFresh) {
		t.Fatalf("write to a fresh root: %v; want ErrLayoutFresh", err)
	}
	if _, err := s.WriteBatch(0, [][]byte{blk}); !errors.Is(err, ErrLayoutFresh) {
		t.Fatalf("batch write to a fresh root: %v; want ErrLayoutFresh", err)
	}
	if err := s.SetLayoutIdentity("k-1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(0, blk); err != nil {
		t.Fatalf("write after fresh cleared: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWALStore(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, blk); err != nil {
		t.Fatalf("write after reopen: %v", err)
	}
}
