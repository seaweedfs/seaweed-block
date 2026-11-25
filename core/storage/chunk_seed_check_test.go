package storage

import (
	"testing"
)

// CanSeedChunkFrontier is the seed's own precondition: for every case the
// check and the seed agree (a refusal by one is a refusal by the other).
func TestCanSeedChunkFrontierAgreesWithTheSeed(t *testing.T) {
	for _, c := range []struct {
		name  string
		lsn   uint64
		write bool // a local write beyond the checkpoint first
		ok    bool
	}{
		{"a position with room for a next LSN", 7, false, true},
		{"no room for a next LSN", ^uint64(0), false, false},
		{"local writes beyond the checkpoint", 7, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, _ := newChunkTestStore(t)
			if c.write {
				if _, err := s.Write(0, makeBlock(ctBlockSize, 0x41)); err != nil {
					t.Fatal(err)
				}
			}
			check := s.CanSeedChunkFrontier(c.lsn)
			seed := s.SeedChunkFrontier(c.lsn)
			if (check == nil) != c.ok || (seed == nil) != c.ok {
				t.Fatalf("lsn %d: check %v, seed %v; want ok=%t for both", c.lsn, check, seed, c.ok)
			}
		})
	}
}
