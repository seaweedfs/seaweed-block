// T3b iSCSI SYNCHRONIZE_CACHE wire test — mini plan §2 #6.
// Pins that SYNC_CACHE(10) and SYNC_CACHE(16) dispatch to
// backend.Sync(ctx).

package iscsi_test

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// syncCacheCDB builds a SYNCHRONIZE_CACHE(10) CDB (opcode 0x35).
// All fields zeroed — spec-legal "flush everything" form.
func syncCacheCDB10() [16]byte {
	var cdb [16]byte
	cdb[0] = 0x35 // ScsiSyncCache10
	return cdb
}

// syncCacheCDB16 builds a SYNCHRONIZE_CACHE(16) CDB (opcode 0x91).
func syncCacheCDB16() [16]byte {
	var cdb [16]byte
	cdb[0] = 0x91 // ScsiSyncCache16
	return cdb
}

func TestT3b_ISCSI_SyncCache_DispatchesToBackend(t *testing.T) {
	cases := []struct {
		name string
		cdb  [16]byte
	}{
		{"SYNC_CACHE_10", syncCacheCDB10()},
		{"SYNC_CACHE_16", syncCacheCDB16()},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			rec := testback.NewRecordingBackend(frontend.Identity{
				VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			})
			h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
			ctx := context.Background()

			pre := rec.SyncCount()
			r := h.HandleCommand(ctx, c.cdb, nil)
			if r.AsError() != nil {
				t.Fatalf("%s: got err=%v, want Good", c.name, r.AsError())
			}
			post := rec.SyncCount()
			if post-pre != 1 {
				t.Errorf("%s: Sync invoked %d times, want 1", c.name, post-pre)
			}
		})
	}
}
