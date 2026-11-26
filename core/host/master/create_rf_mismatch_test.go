package master

import (
	"context"
	"strings"
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
	h := newBootGraceMaster(t, Config{LifecycleProductLoop: true, ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
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

// The third door: a replicated volume created while the lifecycle product
// loop is off (which alone binds and promotes it) would never get a primary,
// so CreateVolume refuses it naming the flag; with the loop on the same
// create is admitted, and an RF1 create is admitted either way.
func TestAReplicatedCreateWithTheProductLoopOffIsRefused(t *testing.T) {
	for _, loop := range []bool{false, true} {
		h, err := New(Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0", LifecycleProductLoop: loop})
		if err != nil {
			t.Fatal(err)
		}
		h.Start()
		svc := newServices(h)
		_, err = svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{VolumeId: "v-rf3", SizeBytes: 1 << 20, ReplicationFactor: 3})
		if loop && err != nil {
			t.Fatalf("RF3 create with the loop on: %v", err)
		}
		if !loop && (status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "--lifecycle-product-loop-interval")) {
			t.Fatalf("RF3 create with the loop off: %v; want FailedPrecondition naming the flag", err)
		}
		if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{VolumeId: "v-rf1", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
			t.Fatalf("RF1 create (loop %v): %v", loop, err)
		}
		closeTestMaster(t, h)
	}
}
