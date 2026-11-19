package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Write six native WAL rings with the real background flusher and no client
// Sync. Local persistence/reclaim must not depend on the guest issuing Flush.
func TestWALStoreV3NoClientSyncSixRings(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		for _, mapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode-%d/mapped-%v", mode, mapped), func(t *testing.T) {
				const walSize = 64 * 1024
				const count = 6 * walSize / 4096
				path := filepath.Join(t.TempDir(), "volume")
				var s *WALStore
				var err error
				if mapped {
					s, err = createMappedWALStoreV3ForTest(path, count, 4096, walSize, mode, 4)
				} else {
					s, err = createWALStoreV3ForTest(path, count, 4096, walSize, mode)
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				// Bound the RED and a genuinely stalled local flusher without a 30s test.
				// The healthy background path must drain this small geometry before it.
				s.admissionTimeout = 2 * time.Second
				for i := uint32(0); i < count; i++ {
					if _, err := s.Write(i, blk(byte(i+1))); err != nil {
						t.Fatalf("no-client-Sync write %d/%d: %v", i, count, err)
					}
				}
				for i := uint32(0); i < count; i++ {
					got, err := s.Read(i)
					if err != nil || !bytes.Equal(got, blk(byte(i+1))) {
						t.Fatalf("readback %d: %v", i, err)
					}
				}
			})
		}
	}
}
