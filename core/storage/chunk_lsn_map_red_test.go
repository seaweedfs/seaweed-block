package storage

import (
	"bytes"
	"testing"
)

// An accepted old/equal-LSN ApplyEntry is still a visible mutation. Once
// flushed out of the WAL, it must not disappear from a later chunk cut.
func TestFormatV3_RedOldLSNMutationIncludedInCut(t *testing.T) {
	for _, lsn := range []uint64{1, 2} {
		t.Run(map[uint64]string{1: "old", 2: "equal"}[lsn], func(t *testing.T) {
			remote := newFakeChunkRemote()
			s, _ := newChunkTestStore(t)
			cfg := ctConfig(1)
			cfg.FreshFromHead = true
			layer := mustAttach(t, s, remote, cfg)
			if _, err := s.Write(0, makeBlock(ctBlockSize, 0x11)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(1, makeBlock(ctBlockSize, 0x22)); err != nil {
				t.Fatal(err)
			}
			if _, err := layer.Cut(ctTimeout(t)); err != nil {
				t.Fatal(err)
			}
			want := makeBlock(ctBlockSize, 0xB9)
			const changedLBA = 8 // different chunk, not covered by the next new write
			if err := s.ApplyEntry(changedLBA, want, lsn); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			got, err := s.Read(changedLBA)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("accepted mutation not visible locally: %v", err)
			}
			// Advance the normal stream so a behind-HEAD refusal cannot mask the
			// stale-LSN chunk omission; the new write is in a different chunk.
			if _, err := s.Write(20, makeBlock(ctBlockSize, 0x33)); err != nil {
				t.Fatal(err)
			}
			result, err := layer.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			image := remote.imageOf(t, result.Head)
			start := changedLBA * ctBlockSize
			if !bytes.Equal(image[start:start+ctBlockSize], want) {
				t.Fatal("accepted old/equal-LSN mutation missing from published HEAD after flush")
			}
		})
	}
}
