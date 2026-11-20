package storage

import (
	"bytes"
	"os"
	"testing"
)

func TestNativeReplacementAllBlocksAfterCheckpoint(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	for lba, fill := range map[uint32]byte{1: 0x11, 7: 0x77} {
		if _, err := s.Write(lba, blk(fill)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.CheckpointLSN() != 2 || s.dm.len() != 0 {
		t.Fatalf("setup: checkpoint=%d dirty=%d", s.CheckpointLSN(), s.dm.len())
	}
	all, err := s.AllBlocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || !bytes.Equal(all[1], blk(0x11)) || !bytes.Equal(all[7], blk(0x77)) {
		t.Fatal("checkpointed LBAs missing from AllBlocks or bytes changed")
	}
}

func TestNativeReplacementMutationsAfterClose(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	mapPath := s.v3.ownedMap.fd.Name()
	if _, err := s.Write(1, blk(0x11)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, p := range []string{path, mapPath} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		before[p] = b
	}
	ops := map[string]func() error{
		"Write":             func() error { _, e := s.Write(1, blk(0x22)); return e },
		"WriteBatch":        func() error { _, e := s.WriteBatch(1, [][]byte{blk(0x22), blk(0x33)}); return e },
		"ApplyEntry":        func() error { return s.ApplyEntry(1, blk(0x22), 2) },
		"WriteExtentDirect": func() error { return s.WriteExtentDirect(1, blk(0x22)) },
		"SeedChunkFrontier": func() error { return s.SeedChunkFrontier(7) },
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			if err := op(); err == nil {
				t.Fatal("closed store accepted mutation")
			}
		})
	}
	for p, want := range before {
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("closed mutation changed %s", p)
		}
	}
}
