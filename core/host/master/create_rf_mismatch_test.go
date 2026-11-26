package master

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// One replicated factor per cluster: an RF2 create on an RF3 master (expected
// slots 3) is refused with FailedPrecondition and records nothing, while an
// RF3 and an RF1 create are admitted; and every existing volume keeps failing
// over (no volume of another slot count can stop the controller's decisions).
func TestAnRF2CreateOnAnRF3MasterIsRefusedAndFailoverContinues(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	svc := newServices(h)
	_, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{VolumeId: "v-rf2", SizeBytes: 1 << 20, ReplicationFactor: 2})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RF2 create on an RF3 master: %v; want FailedPrecondition", err)
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("v-rf2"); ok {
		t.Fatal("the refused RF2 create left a volume record")
	}
	for _, rf := range []int32{3, 1} {
		id := map[int32]string{3: "v-rf3-new", 1: "v-rf1-new"}[rf]
		if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{VolumeId: id, SizeBytes: 1 << 20, ReplicationFactor: rf}); err != nil {
			t.Fatalf("RF%d create on an RF3 master: %v", rf, err)
		}
	}
	time.Sleep(250 * time.Millisecond) // past the boot grace
	ineligible := restarting
	ineligible.eligible = false
	unsupportedTick(t, h, ineligible)
	expectFailoverToR2(t, h, "an existing RF3 volume after the refused RF2 create")
}
