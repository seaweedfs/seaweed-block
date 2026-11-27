package storage

import "testing"

// Public recycle knobs retain their meaning; setup uses actual acknowledged
// writes and complete checkpoints rather than fabricating legacy fields.
func TestWALStoreV3RecycleFloorCompatibilityMatrix(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		floor                           uint64
		active, install, disable, prior bool
		want                            uint64
	}{
		{name: "nil", want: 5},
		{name: "inactive", floor: 2, install: true, want: 5},
		{name: "active", floor: 2, active: true, install: true, want: 2},
		{name: "above", floor: 9, active: true, install: true, want: 5},
		{name: "disable", floor: 2, active: true, install: true, disable: true, want: 5},
		{name: "no_regression", floor: 1, active: true, install: true, prior: true, want: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			for i := 1; i <= 5; i++ {
				writeSynced(t, s, uint32(i), uint32(i))
				if tc.prior && i == 3 {
					if err := s.flusher.flushOnce(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if tc.install {
				s.SetRecycleFloorSource(&fakeRecycleFloorSource{floor: tc.floor, anyActive: tc.active})
			}
			if tc.disable {
				s.SetRecycleFloorSource(nil)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			if cp := s.CheckpointLSN(); cp != tc.want {
				t.Fatalf("checkpoint=%d want %d", cp, tc.want)
			}
			for i := 1; i <= 5; i++ {
				got, err := s.Read(uint32(i))
				if err != nil || got[0] != byte(i) {
					t.Fatalf("LBA%d: %v", i, err)
				}
			}
		})
	}
}

func TestWALStoreV3RetentionCompatibilityMatrix(t *testing.T) {
	tailWith := func(retain uint64) uint64 {
		s, _ := newMappedCheckpointStoreV3(t)
		s.SetRecoveryRetentionLSNs(retain)
		for i := 1; i <= 5; i++ {
			writeSynced(t, s, uint32(i), uint32(i))
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
		return s.v3.pub.current().RetainedTail
	}
	for _, tc := range []struct{ retain, tail uint64 }{{0, 20830}, {1, 16664}, {4, 4166}} {
		if got := tailWith(tc.retain); got != tc.tail {
			t.Fatalf("retention%d: tail=%d want %d", tc.retain, got, tc.tail)
		}
	}
	// The window is capped at a quarter of the WAL in block-sized records
	// (64 KiB / (4 x 4 KiB) = 4 here): a larger setting retains exactly
	// what the cap retains, so retention can never fill the WAL.
	if capped, atCap := tailWith(10), tailWith(v3TestWAL/(4*v3TestBlock)); capped != atCap {
		t.Fatalf("retention 10 on a %d-byte WAL: tail=%d, want the cap's tail %d", v3TestWAL, capped, atCap)
	}
}
