package master

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/lifecycleclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type identityRPC struct{ svc *services }

func (c identityRPC) GetVolume(ctx context.Context, q *control.GetLifecycleVolumeRequest, _ ...grpc.CallOption) (*control.CreateVolumeResponse, error) {
	return c.svc.GetVolume(ctx, q)
}

func TestGetLifecycleVolumeResolvesOnlyTheDurableIdentity(t *testing.T) {
	dir := t.TempDir()
	const id = "1234567890abcdef1234567890abcdef"
	for _, name := range []string{"v", "legacy"} {
		spec := lifecycle.VolumeSpec{VolumeID: name, SizeBytes: 1 << 20, ReplicationFactor: 1}
		if name == "v" {
			spec.LayoutID = id
		}
		raw, _ := json.Marshal(lifecycle.VolumeRecord{Spec: spec})
		if err := os.WriteFile(filepath.Join(dir, name+".json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	store, err := lifecycle.OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := newServices(&Host{lifecycle: &LifecycleStores{Volumes: store}})
	resolved, err := lifecycleclient.ResolveVolume(context.Background(), identityRPC{svc}, "v")
	if err != nil || resolved.String() != id {
		t.Fatalf("resolve: %v %s", err, resolved)
	}
	if _, err := lifecycleclient.ResolveVolume(context.Background(), identityRPC{svc}, "legacy"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("legacy identity was invented: %v", err)
	}
	if _, err := lifecycleclient.ResolveVolume(context.Background(), identityRPC{svc}, "missing"); status.Code(err) != codes.NotFound {
		t.Fatalf("missing volume: %v", err)
	}
	if len(store.ListVolumes()) != 2 {
		t.Fatal("lookup mutated lifecycle intent")
	}
	ref, err := svc.host.WorkspaceLayoutVolume(context.Background(), "v")
	if err != nil || ref.Name != "v" || ref.ID.String() != id {
		t.Fatalf("HEAD identity resolver: %+v %v", ref, err)
	}
	for _, name := range []string{"legacy", "missing"} {
		if _, err := svc.host.WorkspaceLayoutVolume(context.Background(), name); err == nil {
			t.Fatalf("HEAD identity invented for %s", name)
		}
	}
	var unready *Host
	if _, err := unready.WorkspaceLayoutVolume(context.Background(), "v"); err == nil {
		t.Fatal("resolver accepted before Host construction")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.host.WorkspaceLayoutVolume(ctx, "v"); err != context.Canceled {
		t.Fatalf("cancelled lookup: %v", err)
	}
}
