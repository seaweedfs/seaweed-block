// Ownership: sw port from V2 scsi_test.go (synccache subset).
// Batch 10.5 — port plan §4, locked 2026-04-22.
//
// SYNC_CACHE(10/16) is a no-op Good stub in T2 per port plan
// §3.3 N2 — memback is non-durable. Durable behavior is T3 scope.
// These tests pin the "accept + don't touch backend" contract
// so a later refactor can't accidentally drop the opcode case.
package iscsi_test

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

func TestT2Batch10_5_SyncCache10_NoOpGood_NoBackendCall(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})

	var cdb [16]byte
	cdb[0] = iscsi.ScsiSyncCache10
	// LBA + transferLen bytes can be populated — handler ignores
	// them for T2 no-op behavior.
	r := h.HandleCommand(context.Background(), cdb, nil)

	if r.Status != iscsi.StatusGood {
		t.Fatalf("SYNC_CACHE(10): status=0x%02x want Good", r.Status)
	}
	if rec.WriteCount() != 0 || rec.ReadCount() != 0 {
		t.Fatal("SYNC_CACHE(10) reached backend; should be local no-op (memback is non-durable, T3 owns real flush)")
	}
}

func TestT2Batch10_5_SyncCache16_NoOpGood_NoBackendCall(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})

	var cdb [16]byte
	cdb[0] = iscsi.ScsiSyncCache16
	r := h.HandleCommand(context.Background(), cdb, nil)

	if r.Status != iscsi.StatusGood {
		t.Fatalf("SYNC_CACHE(16): status=0x%02x want Good", r.Status)
	}
	if rec.WriteCount() != 0 || rec.ReadCount() != 0 {
		t.Fatal("SYNC_CACHE(16) reached backend; should be local no-op")
	}
}
