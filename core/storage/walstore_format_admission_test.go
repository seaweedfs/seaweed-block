package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALStore_UnknownFormatWithoutFactoryRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := readSuperblock(f)
	if err != nil {
		t.Fatal(err)
	}
	sb.ImplVersion = 77
	if _, err = f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = sb.writeTo(f); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if x, err := OpenWALStore(path); err == nil {
		x.Close()
		t.Fatal("unknown format opened without accepting factory")
	}
	if x, err := OpenReadOnly(path); err == nil {
		x.Close()
		t.Fatal("unknown format readonly opened without accepting factory")
	}
}
