// T3c scenario 2 of 4 — WAL replay correctness.
//
// Variant of crash-recovery that stresses the WAL replay path
// rather than the extent-read path: writes N blocks without
// waiting for checkpoint advance, then restarts. Recover must
// replay the WAL entries and ALL N blocks must be readable.
//
// Differs from crash-recovery in that it does NOT pause between
// Write and Close — so Recover sees WAL entries that haven't yet
// been checkpoint-flushed to the extent region.
//
// Pins ledger row INV-DURABLE-WAL-REPLAY-001.

package durable_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// TestT3c_Scenario_WALReplay_NAckedWrites pins that N acked
// writes fast-enough to land in the WAL are all recoverable.
func TestT3c_Scenario_WALReplay_NAckedWrites(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			view := newStubView(healthyProj(id))

			cfg := durable.ProviderConfig{
				Impl:        impl,
				StorageRoot: root,
				BlockSize:   4096,
				NumBlocks:   64,
			}

			// Phase 1: rapid N writes + Sync + immediate Close.
			p1, err := durable.NewDurableProvider(cfg, view)
			if err != nil {
				t.Fatalf("NewDurableProvider #1: %v", err)
			}
			ctx := context.Background()
			backend, err := p1.Open(ctx, "v1")
			if err != nil {
				t.Fatalf("Open #1: %v", err)
			}
			if _, err := p1.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatalf("RecoverVolume #1: %v", err)
			}

			const n = 32
			const blockSize = 4096
			blocks := make([][]byte, n)
			for i := 0; i < n; i++ {
				blocks[i] = make([]byte, blockSize)
				seed := byte(i * 43)
				for j := range blocks[i] {
					blocks[i][j] = seed + byte(j)
				}
				offset := int64(i) * blockSize
				if _, err := backend.Write(ctx, offset, blocks[i]); err != nil {
					t.Fatalf("Write[%d]: %v", i, err)
				}
			}
			// Sync acks everything written so far.
			if err := backend.Sync(ctx); err != nil {
				t.Fatalf("Sync: %v", err)
			}
			if err := p1.Close(); err != nil {
				t.Fatalf("p1.Close: %v", err)
			}

			// Phase 2: restart. Recover MUST replay all N from WAL
			// or extent (impl-dependent) so every LBA is readable.
			p2, err := durable.NewDurableProvider(cfg, view)
			if err != nil {
				t.Fatalf("NewDurableProvider #2: %v", err)
			}
			defer p2.Close()

			b2, err := p2.Open(ctx, "v1")
			if err != nil {
				t.Fatalf("Open #2: %v", err)
			}
			report, err := p2.RecoverVolume(ctx, "v1")
			if err != nil {
				t.Fatalf("RecoverVolume #2: %v (evidence=%s)", err, report.Evidence)
			}
			// Recovered LSN must reflect all N writes.
			if report.RecoveredLSN < n {
				t.Errorf("RecoveredLSN=%d, want >= %d (all N writes)", report.RecoveredLSN, n)
			}

			// Every LBA must read back its original bytes.
			missing := 0
			for i := 0; i < n; i++ {
				got := make([]byte, blockSize)
				if _, err := b2.Read(ctx, int64(i)*blockSize, got); err != nil {
					t.Fatalf("Read[%d]: %v", i, err)
				}
				if !bytes.Equal(got, blocks[i]) {
					missing++
					if missing <= 3 {
						t.Errorf("LBA %d lost after restart (impl=%s); first diff @ idx %d",
							i, impl, firstDiffIdx(got, blocks[i]))
					}
				}
			}
			if missing > 0 {
				t.Fatalf("%d / %d acked writes did NOT survive WAL replay (impl=%s)",
					missing, n, impl)
			}
			_ = fmt.Sprint // keep fmt import available for future diagnostics
		})
	}
}
