package master

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/geometry"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// CreateVolume, through the master's gRPC service as production serves it,
// creates a volume of exactly MaxNewVolumeBytes and refuses one chunk more
// with InvalidArgument naming the maximum; the refused id gets no record.
func TestCreateVolumeRefusesAVolumeAboveTheMaximum(t *testing.T) {
	m, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, LayoutCreateVolume: testLayoutCreateVolume,
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	defer closeTestMaster(t, m)
	conn, err := grpc.NewClient(m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lc := control.NewLifecycleServiceClient(conn)

	if _, err := lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "at-max", SizeBytes: geometry.MaxNewVolumeBytes, ReplicationFactor: 1}); err != nil {
		t.Fatalf("a volume of exactly the maximum (%d bytes) is refused: %v", geometry.MaxNewVolumeBytes, err)
	}
	_, err = lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "over-max", SizeBytes: geometry.MaxNewVolumeBytes + 4<<20, ReplicationFactor: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("one chunk above the maximum: got %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, geometry.ErrVolumeTooLarge.Error()) || !strings.Contains(msg, strconv.FormatUint(geometry.MaxNewVolumeBytes, 10)) {
		t.Fatalf("refusal does not name the maximum: %q", msg)
	}
	if _, ok := m.Lifecycle().Volumes.GetVolume("over-max"); ok {
		t.Fatal("a refused volume has a lifecycle record")
	}
}
