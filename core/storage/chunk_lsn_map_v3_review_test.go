package storage

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestChunkLSNMapV3_EntryBoundBeforeRootIO(t *testing.T) {
	d := mapDescriptorV3(t)
	d.BlockSize = 1
	d.VolumeSize = uint64(int(^uint(0)>>1))/16 + 1
	f, err := os.Create(filepath.Join(t.TempDir(), "closed-map"))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	// The geometry must be refused before even Stat, without allocating a root.
	_, _, err = readChunkLSNMapV3(f, d, 1)
	if !errors.Is(err, ErrMapInvalid) {
		t.Fatalf("entry bound must precede file I/O: %v", err)
	}
}

func TestChunkLSNMapV3_WholePageHashIsNotBodyDigest(t *testing.T) {
	d := mapDescriptorV3(t)
	m, err := createChunkLSNMapV3(filepath.Join(t.TempDir(), "map"), d, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	c, err := m.Prepare(1, 7, []chunkLSNEntryV3{{LSN: 7}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, 4096)
	if _, err = m.fd.ReadAt(page, int64(c.refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	// Deliberately wrong: SHA-256 of all 4096 bytes, including the embedded hash,
	// instead of the domain-separated 4064-byte body required by the byte contract.
	whole := sha256.Sum256(page)
	if whole == c.refs[0].Hash {
		t.Fatal("whole-page digest unexpectedly matches body digest")
	}
	copy(page[4064:], whole[:])
	if _, err = m.fd.WriteAt(page, int64(c.refs[0].Offset)); err != nil {
		t.Fatal(err)
	}
	root, err := encodeMapRootV3(d, 1, 1, 7, []mapPageRefV3{{Offset: c.refs[0].Offset, Hash: whole}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.fd.WriteAt(root, int64(c.rootOffset)); err != nil {
		t.Fatal(err)
	}
	d = c.descriptor(d)
	// Root, reference and embedded digest agree. Only page-body validation rejects it.
	d.MapRootHash = mapDigestV3(mapRootDomainV3, root[:128])
	if _, _, err = readChunkLSNMapV3(m.fd, d, 1); !errors.Is(err, ErrMapInvalid) {
		t.Fatalf("whole-page hash accepted: %v", err)
	}
}

func TestChunkLSNMapV3_PreflightErrorDoesNotLatch(t *testing.T) {
	d := mapDescriptorV3(t)
	m, err := createChunkLSNMapV3(filepath.Join(t.TempDir(), "map"), d, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if c, err := m.Prepare(0, 7, []chunkLSNEntryV3{{LSN: 7}}, nil); c != nil || !errors.Is(err, ErrMapInvalid) {
		t.Fatalf("invalid generation = %v, %v", c, err)
	}
	if c, err := m.Prepare(1, 7, []chunkLSNEntryV3{{LSN: 7}}, nil); err != nil || c == nil {
		t.Fatalf("valid retry after preflight error = %v, %v", c, err)
	}
}
