package master

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

type collisionBlobs struct{}

func (collisionBlobs) Put(context.Context, []byte) (string, error) {
	return "", errors.New("unexpected payload upload")
}
func (collisionBlobs) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("unexpected payload read")
}

// A full-id mismatch in an exclusively-created VOLUME is a directory collision,
// not a replay. The next create attempt must use a newly minted directory.
func TestCreateVolumeExclusiveCollisionRedraws(t *testing.T) {
	ctx := context.Background()
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	h.cfg.WorkspaceRPC = true
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), collisionBlobs{}, "/collision")
	if err != nil {
		t.Fatal(err)
	}
	var first, foreign chunkstore.VolumeRef
	var old layout.Volume
	attempts := 0
	h.cfg.LayoutCreateVolume = func(ctx context.Context, spec lifecycle.VolumeSpec) error {
		id, err := layout.ParseVolumeID(spec.LayoutID)
		if err != nil {
			return err
		}
		ref := chunkstore.VolumeRef{Name: spec.VolumeID, ID: id}
		g := layout.Geometry{SizeBytes: spec.SizeBytes, BlockSize: spec.BlockSize, ChunkBytes: spec.ChunkBytes}
		attempts++
		if attempts == 1 {
			first = ref
			foreign = ref
			foreign.ID[15] ^= 1
			if _, err := cat.CreateVolume(ctx, foreign, g); err != nil {
				return err
			}
			old, err = cat.ReadVolume(ctx, foreign)
			if err != nil {
				return err
			}
		}
		_, err = cat.CreateVolume(ctx, ref, g)
		return err
	}
	req := &control.CreateVolumeRequest{VolumeId: "v", SizeBytes: 1 << 20, ReplicationFactor: 1}
	// A retry is allowed, but it must heal the collision rather than stay stuck.
	resp, err := newServices(h).CreateVolume(ctx, req)
	if err != nil {
		resp, err = newServices(h).CreateVolume(ctx, req)
	}
	if err != nil {
		t.Fatalf("exclusive-create collision did not redraw: attempts=%d first=%s err=%v", attempts, first.ID, err)
	}
	id, err := layout.ParseVolumeID(resp.GetLayoutId())
	if err != nil {
		t.Fatal(err)
	}
	next := chunkstore.VolumeRef{Name: "v", ID: id}
	if !resp.GetStoreReady() || id == first.ID || next.Dir() == first.Dir() {
		t.Fatalf("collision reused identity/directory: first=%s next=%s ready=%v", first.ID, id, resp.GetStoreReady())
	}
	got, err := cat.ReadVolume(ctx, foreign)
	if err != nil || got != old {
		t.Fatalf("redraw altered the colliding volume: %v %v", got, err)
	}
	replay, err := newServices(h).CreateVolume(ctx, req)
	if err != nil || replay.GetLayoutId() != id.String() {
		t.Fatalf("settled identity changed on replay: %v %v", replay, err)
	}
}
