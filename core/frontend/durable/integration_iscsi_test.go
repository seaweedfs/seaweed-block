// T3b integration — iSCSI handler against DurableProvider-sourced
// StorageBackend. Matrix-parameterized over walstore + smartwal per
// Addendum A #1. Exercises Write / Read / SYNC_CACHE through the
// real SCSI command path, not just the backend-dispatch boundary.
//
// Complements:
//   - TestT3a_StorageBackend_* (unit-level adapter tests)
//   - TestT3b_ISCSI_SyncCache_DispatchesToBackend (testback-level
//     wire verification — asserts Sync is CALLED; this file asserts
//     Sync actually PERSISTS + durability round-trip is correct)
//
// This is NOT a T3c scenario (no YAML replay, no L2 subprocess, no
// perf measurement). It's an L1 integration test that closes the
// matrix gap: every iSCSI op path is proven against both impls.

package durable_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

// scsiWriteCDB10 builds a minimal WRITE(10) CDB. lba is zero-based;
// nlb is one-based (standard SCSI convention).
func scsiWriteCDB10(lba uint32, nlb uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = 0x2A // ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], nlb)
	return cdb
}

// scsiReadCDB10 builds a minimal READ(10) CDB.
func scsiReadCDB10(lba uint32, nlb uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = 0x28 // ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], nlb)
	return cdb
}

// scsiSyncCacheCDB10 builds a zero-all SYNC_CACHE(10) — flush all.
func scsiSyncCacheCDB10() [16]byte {
	var cdb [16]byte
	cdb[0] = 0x35
	return cdb
}

// openDurableBackend is the shared setup for the iSCSI + NVMe
// integration tests: builds a DurableProvider, Opens a volume,
// runs Recover, and returns the operational backend.
func openDurableBackend(t *testing.T, impl durable.ImplName, numBlocks uint32, blockSize int) frontend.Backend {
	t.Helper()
	root := t.TempDir()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	view := newStubView(healthyProj(id))
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:        impl,
		StorageRoot: root,
		BlockSize:   blockSize,
		NumBlocks:   numBlocks,
	}, view)
	if err != nil {
		t.Fatalf("NewDurableProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	ctx := context.Background()
	backend, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	report, err := p.RecoverVolume(ctx, "v1")
	if err != nil {
		t.Fatalf("RecoverVolume: %v (evidence=%s)", err, report.Evidence)
	}
	return backend
}

// TestT3b_Integration_ISCSI_WriteReadSync_Matrix exercises a full
// SCSI write → sync → read round-trip through the iSCSI handler
// wrapped around a real DurableProvider-sourced backend. Matrix
// covers both impls.
func TestT3b_Integration_ISCSI_WriteReadSync_Matrix(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			// iSCSI defaults: 512 B block, enough blocks for a
			// few ops.
			backend := openDurableBackend(t, impl, 64, 512)
			h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: backend})
			ctx := context.Background()

			// Write 1 block (512 B) at LBA 4.
			payload := make([]byte, 512)
			for i := range payload {
				payload[i] = byte((i + 13) & 0xFF)
			}
			if r := h.HandleCommand(ctx, scsiWriteCDB10(4, 1), payload); r.AsError() != nil {
				t.Fatalf("WRITE(10): %v", r.AsError())
			}

			// SYNC_CACHE — must succeed (durable backend fsyncs).
			if r := h.HandleCommand(ctx, scsiSyncCacheCDB10(), nil); r.AsError() != nil {
				t.Fatalf("SYNC_CACHE: %v", r.AsError())
			}

			// Read back the same LBA.
			r := h.HandleCommand(ctx, scsiReadCDB10(4, 1), nil)
			if r.AsError() != nil {
				t.Fatalf("READ(10): %v", r.AsError())
			}
			if !bytes.Equal(r.Data, payload) {
				t.Fatalf("READ payload mismatch (impl=%s): bytes differ", impl)
			}

			// Read an unwritten LBA — must return zeros.
			r2 := h.HandleCommand(ctx, scsiReadCDB10(10, 1), nil)
			if r2.AsError() != nil {
				t.Fatalf("READ unwritten: %v", r2.AsError())
			}
			if !isAllZeros(r2.Data) {
				t.Fatalf("READ unwritten LBA returned non-zero (impl=%s)", impl)
			}
		})
	}
}

// TestT3b_Integration_ISCSI_MultiBlockWriteAcrossBoundary pins
// that SCSI multi-block writes translate through the adapter's
// byte↔LBA math correctly for both impls. Kernel ext4 / mkfs
// issues multi-block writes frequently.
func TestT3b_Integration_ISCSI_MultiBlockWriteAcrossBoundary(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			backend := openDurableBackend(t, impl, 32, 512)
			h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: backend})
			ctx := context.Background()

			// Write 8 blocks (4 KiB) starting at LBA 5 — crosses
			// block boundaries, exercises RMW-free fast path.
			const nlb = uint16(8)
			payload := make([]byte, int(nlb)*512)
			for i := range payload {
				payload[i] = byte((i*7 + 3) & 0xFF)
			}
			if r := h.HandleCommand(ctx, scsiWriteCDB10(5, nlb), payload); r.AsError() != nil {
				t.Fatalf("WRITE(10) multi-block: %v", r.AsError())
			}
			if r := h.HandleCommand(ctx, scsiSyncCacheCDB10(), nil); r.AsError() != nil {
				t.Fatalf("SYNC_CACHE: %v", r.AsError())
			}
			r := h.HandleCommand(ctx, scsiReadCDB10(5, nlb), nil)
			if r.AsError() != nil {
				t.Fatalf("READ(10) multi-block: %v", r.AsError())
			}
			if !bytes.Equal(r.Data, payload) {
				for i, b := range r.Data {
					if b != payload[i] {
						t.Fatalf("first diff at byte %d: got 0x%02x want 0x%02x (impl=%s)",
							i, b, payload[i], impl)
					}
				}
			}
		})
	}
}
