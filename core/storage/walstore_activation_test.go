package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWALStoreProductionSelectorCreatesMappedV3(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	s, err := CreateWALStore(path, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if !s.FormatV3() {
		t.Fatal("production Create must select mapped v3/impl3")
	}
	d := s.v3.pub.current()
	if d.ImplVersion != 3 || d.WMode != WModeLagged || d.MapRootHash == [32]byte{} {
		t.Fatalf("production descriptor: impl=%d mode=%d map=%x", d.ImplVersion, d.WMode, d.MapRootHash)
	}
	if _, err := s.Write(3, bytes.Repeat([]byte{0x73}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Recover(); err != nil {
		_ = r.Close()
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if data, err := ro.Read(3); err != nil || !bytes.Equal(data, bytes.Repeat([]byte{0x73}, 4096)) {
		t.Fatalf("production readonly after reopen: %v", err)
	}
}
