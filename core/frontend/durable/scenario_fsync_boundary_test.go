// T3c scenario 3 of 4 — fsync boundary.
//
// LogicalStorage contract §2: "Unacked data may disappear after
// restart but cannot corrupt acked data: any LBA whose write
// completed before a successful Sync reads back identical bytes
// after Recover."
//
// Scenario:
//   1. Write a set of "acked" blocks and Sync — these MUST survive.
//   2. Write a set of "unacked" blocks without Sync.
//   3. Close the Provider (clean close; implicit final fsync is
//      legal per the contract — the invariant is "no corruption",
//      not "unacked always disappears").
//   4. Reopen + Recover.
//   5. For every acked LBA: bytes MUST match (byte-exact).
//   6. For every unacked LBA: bytes are EITHER the original
//      write OR zero — never partial / never another block's
//      bytes.
//
// Pins ledger row INV-DURABLE-FSYNC-BOUNDARY-001.

package durable_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

func TestT3c_Scenario_FsyncBoundary_AckedSurvivesUnackedMayVanish(t *testing.T) {
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
				NumBlocks:   32,
			}

			p1, err := durable.NewDurableProvider(cfg, view)
			if err != nil {
				t.Fatalf("NewDurableProvider #1: %v", err)
			}
			ctx := context.Background()
			b1, err := p1.Open(ctx, "v1")
			if err != nil {
				t.Fatalf("Open #1: %v", err)
			}
			if _, err := p1.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatalf("RecoverVolume #1: %v", err)
			}

			const blockSize = 4096
			const nAcked = 5
			const nUnacked = 5

			// Acked writes.
			ackedBlocks := make([][]byte, nAcked)
			for i := 0; i < nAcked; i++ {
				ackedBlocks[i] = make([]byte, blockSize)
				for j := range ackedBlocks[i] {
					ackedBlocks[i][j] = byte((i*11 + j) & 0xFF)
				}
				if _, err := b1.Write(ctx, int64(i)*blockSize, ackedBlocks[i]); err != nil {
					t.Fatalf("acked Write[%d]: %v", i, err)
				}
			}
			// The Sync boundary. Everything written BEFORE this
			// point is durable; afterward is unacked.
			if err := b1.Sync(ctx); err != nil {
				t.Fatalf("Sync: %v", err)
			}

			// Unacked writes — at LBAs nAcked..nAcked+nUnacked-1.
			unackedBlocks := make([][]byte, nUnacked)
			for i := 0; i < nUnacked; i++ {
				unackedBlocks[i] = make([]byte, blockSize)
				for j := range unackedBlocks[i] {
					unackedBlocks[i][j] = byte((i*17 + j + 0x80) & 0xFF)
				}
				lba := nAcked + i
				if _, err := b1.Write(ctx, int64(lba)*blockSize, unackedBlocks[i]); err != nil {
					t.Fatalf("unacked Write[%d]: %v", i, err)
				}
			}
			// NO Sync here. Close to end of phase.
			if err := p1.Close(); err != nil {
				t.Fatalf("p1.Close: %v", err)
			}

			// Phase 2: reopen.
			p2, err := durable.NewDurableProvider(cfg, view)
			if err != nil {
				t.Fatalf("NewDurableProvider #2: %v", err)
			}
			defer p2.Close()
			b2, err := p2.Open(ctx, "v1")
			if err != nil {
				t.Fatalf("Open #2: %v", err)
			}
			if _, err := p2.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatalf("RecoverVolume #2: %v", err)
			}

			// Invariant 1: every acked LBA MUST be byte-exact.
			for i := 0; i < nAcked; i++ {
				got := make([]byte, blockSize)
				if _, err := b2.Read(ctx, int64(i)*blockSize, got); err != nil {
					t.Fatalf("acked Read[%d]: %v", i, err)
				}
				if !bytes.Equal(got, ackedBlocks[i]) {
					t.Fatalf("ACKED LBA %d did NOT survive (impl=%s) — contract §2 violation",
						i, impl)
				}
			}

			// Invariant 2: every unacked LBA is EITHER original or
			// zero — never partial / foreign bytes.
			zero := make([]byte, blockSize)
			for i := 0; i < nUnacked; i++ {
				lba := nAcked + i
				got := make([]byte, blockSize)
				if _, err := b2.Read(ctx, int64(lba)*blockSize, got); err != nil {
					t.Fatalf("unacked Read[%d]: %v", i, err)
				}
				isOriginal := bytes.Equal(got, unackedBlocks[i])
				isZero := bytes.Equal(got, zero)
				if !isOriginal && !isZero {
					t.Fatalf("UNACKED LBA %d (impl=%s) is neither original write nor zero — CORRUPTION: first non-zero byte %d = 0x%02x",
						lba, impl, firstNonZero(got), got[firstNonZero(got)])
				}
			}
		})
	}
}

func firstNonZero(p []byte) int {
	for i, b := range p {
		if b != 0 {
			return i
		}
	}
	return -1
}
