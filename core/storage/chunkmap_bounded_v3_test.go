package storage

import (
	"os"
	"testing"
)

// The v3 owned-map file must stay bounded under steady writes (stability
// track; claude06's study, measured 2026-11-19: ~8.5 KiB per checkpoint and
// never reclaimed). Each cycle writes, syncs and checkpoints, so every cycle
// publishes a new map generation with a changed page. After 300 checkpoints
// the file must not exceed a small multiple of one full map (pages + root,
// each 4 KiB aligned): the live map of the two descriptor slots plus one
// in-flight candidate, with slack. RED on main 44b2beb2: the file grows by
// one page and one root per checkpoint (~2.4 MB here).
func TestChunkMapV3FileStaysBoundedUnderSteadyCheckpoints(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	shape, err := mapShapeForV3(s.v3.pub.current(), s.v3.ownedMap.chunkBlocks)
	if err != nil {
		t.Fatal(err)
	}
	rootBytes := (120 + 40*shape.pages + 4095) &^ 4095
	fullMap := int64(shape.pages*4096 + rootBytes)
	bound := 8 * fullMap
	const cycles = 300
	for i := 0; i < cycles; i++ {
		writeSynced(t, s, uint32(i%64), uint32(i%64))
		if err := s.flusher.flushOnceInternal(false); err != nil {
			t.Fatal(err)
		}
	}
	gen := s.v3.pub.current().MapGeneration
	if gen < cycles {
		t.Fatalf("setup: %d map generations after %d checkpoint cycles", gen, cycles)
	}
	fi, err := os.Stat(path + ".chunkmap")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() > bound {
		t.Fatalf("chunkmap is %d B after %d generations; bound %d B (8 x one full map of %d B)", fi.Size(), gen, bound, fullMap)
	}
}
