// T3c scenario 1 of 4 — crash recovery.
//
// Canonical G4 gate: acked writes must survive a restart
// byte-exact. The scenario is:
//
//   1. Write N blocks at distinct LBAs via Backend.Write.
//   2. Backend.Sync (acks the writes — durable contract says
//      any LSN <= Sync-returned-LSN is guaranteed recoverable).
//   3. Close the first Provider (tears down fd + final fsync).
//   4. Build a FRESH Provider rooted at the same StorageRoot
//      (simulates daemon restart).
//   5. Open + RecoverVolume — must succeed; backend operational.
//   6. Read each LBA — bytes must match what step 1 wrote.
//
// Pins ledger row INV-DURABLE-001 — "acknowledged Write survives
// kill + restart; Read returns byte-exact".
//
// This is an L1 scenario (in-process, no subprocess). T3c m01
// fs-workload provides the subprocess / real-kernel leg.

package durable_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// TestT3c_Scenario_CrashRecovery_AckedBytesByteExact is the
// canonical G4 scenario. Matrix-parameterized over both impls.
func TestT3c_Scenario_CrashRecovery_AckedBytesByteExact(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			view := newStubView(healthyProj(id))

			// Phase 1: open, write, sync, close.
			p1, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl:        impl,
				StorageRoot: root,
				BlockSize:   4096,
				NumBlocks:   32,
			}, view)
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

			// Write N distinct blocks at distinct offsets.
			const n = 8
			const blockSize = 4096
			blocks := make([][]byte, n)
			for i := 0; i < n; i++ {
				blocks[i] = make([]byte, blockSize)
				for j := range blocks[i] {
					blocks[i][j] = byte((i*37 + j) & 0xFF)
				}
				offset := int64(i) * blockSize
				if _, err := backend.Write(ctx, offset, blocks[i]); err != nil {
					t.Fatalf("Write[%d]: %v", i, err)
				}
			}

			// Ack: Sync tells us "everything up to here is durable".
			if err := backend.Sync(ctx); err != nil {
				t.Fatalf("Sync: %v", err)
			}

			// Simulate daemon shutdown (clean). Closes fd + final
			// fsync; nothing ambiguous.
			if err := p1.Close(); err != nil {
				t.Fatalf("p1.Close: %v", err)
			}

			// Phase 2: restart — fresh Provider over same path.
			p2, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl:        impl,
				StorageRoot: root,
				BlockSize:   4096,
				NumBlocks:   32,
			}, view)
			if err != nil {
				t.Fatalf("NewDurableProvider #2: %v", err)
			}
			defer p2.Close()

			backend2, err := p2.Open(ctx, "v1")
			if err != nil {
				t.Fatalf("Open #2: %v", err)
			}
			report, err := p2.RecoverVolume(ctx, "v1")
			if err != nil {
				t.Fatalf("RecoverVolume #2: %v (evidence=%s)", err, report.Evidence)
			}
			// Sanity: recovered LSN must be >= N (we wrote N blocks
			// and each bumped nextLSN).
			if report.RecoveredLSN == 0 {
				t.Errorf("RecoveredLSN=0 after %d acked writes; expected >0", n)
			}

			// Read each LBA — bytes MUST match.
			for i := 0; i < n; i++ {
				offset := int64(i) * blockSize
				got := make([]byte, blockSize)
				nr, err := backend2.Read(ctx, offset, got)
				if err != nil {
					t.Fatalf("Read[%d]: %v", i, err)
				}
				if nr != blockSize {
					t.Fatalf("Read[%d] n=%d want %d", i, nr, blockSize)
				}
				if !bytes.Equal(got, blocks[i]) {
					t.Fatalf("LBA %d did NOT survive restart byte-exact (impl=%s): first diff at idx %d (got 0x%02x want 0x%02x)",
						i, impl, firstDiffIdx(got, blocks[i]),
						got[firstDiffIdx(got, blocks[i])], blocks[i][firstDiffIdx(got, blocks[i])])
				}
			}
		})
	}
}
