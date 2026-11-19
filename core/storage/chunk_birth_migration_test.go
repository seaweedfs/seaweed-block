package storage

import (
	"bytes"
	"os"
	"testing"
)

func TestChunkBirthMigrationCarriesImageFrontier(t *testing.T) {
	s, path := newChunkTestStore(t)
	d := s.v3.pub.current()
	mapPath := s.v3.ownedMap.fd.Name()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	src := MigrationSourceV3{UUID: d.UUID, VolumeSize: d.VolumeSize, ExtentSize: d.ExtentSize, BlockSize: d.BlockSize, OldWALOffset: 4096, OldWALSize: d.WALSize + 4096, CreatedAt: d.CreatedAt, CheckpointLSN: 7, ChunkBlocks: ctChunkBlocks}
	plan, err := PrepareMigrationV3(mapPath, src)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fd.Close() })
	// Actual offline migration's map and slot encoder, on a small flushed
	// image. No legacy engine or fabricated in-memory owned-map entries.
	for _, part := range []struct {
		offset int64
		data   []byte
	}{{MigrationSlotBOffset, plan.PreparedB}, {MigrationSlotAOffset, plan.CommittedA}, {MigrationSlotBOffset, plan.RolloverB}} {
		if _, err := fd.WriteAt(part.data, part.offset); err != nil {
			t.Fatal(err)
		}
		if err := fd.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	want := makeBlock(ctBlockSize, 0x57)
	if _, err := fd.WriteAt(want, int64(d.WALOffset+d.WALSize)); err != nil {
		t.Fatal(err)
	}
	if err := fd.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenWALStoreWithOptions(path, WALStoreOptions{ChunkBlocks: ctChunkBlocks})
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { closeBounded(t, r) })
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	l := mustAttach(t, r, f, ctConfig(1))
	newest, err := r.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0x41))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if cut.Head.Chunks[0].BirthLSN != 7 || cut.Head.Chunks[1].BirthLSN != newest {
		t.Fatalf("migrated image birth lost: %+v", cut.Head)
	}
	if !bytes.Equal(f.imageOf(t, cut.Head)[:ctBlockSize], want) {
		t.Fatal("migration cut changed inherited bytes")
	}
}

func TestChunkBirthSeededFrontierAnchorsDirectImage(t *testing.T) {
	s, _ := newChunkTestStore(t)
	if err := s.SeedChunkFrontier(7); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteExtentDirect(5, makeBlock(ctBlockSize, 0x57)); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	l := mustAttach(t, s, f, ctConfig(1))
	newest, err := s.Write(0, makeBlock(ctBlockSize, 0x41))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if cut.Head.Chunks[1].BirthLSN != 7 || cut.Head.Chunks[0].BirthLSN != newest {
		t.Fatalf("seeded image birth lost: %+v", cut.Head)
	}
}
