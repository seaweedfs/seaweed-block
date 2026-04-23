// T3b NVMe Flush wire test — mini plan §2 #7.
// Pins that ioFlush (opcode 0x00) dispatches to backend.Sync(ctx).

package nvme_test

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

func TestT3b_NVMe_Flush_DispatchesToBackend(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: rec})
	ctx := context.Background()

	pre := rec.SyncCount()
	res := h.Handle(ctx, nvme.IOCommand{
		Opcode: 0x00, // ioFlush
		NSID:   1,
	})
	if res.AsError() != nil {
		t.Fatalf("Flush: got err=%v, want Success", res.AsError())
	}
	post := rec.SyncCount()
	if post-pre != 1 {
		t.Errorf("Flush dispatched to Sync %d times, want 1", post-pre)
	}
}
