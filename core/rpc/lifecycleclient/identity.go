// Package lifecycleclient resolves volume names through the master.
package lifecycleclient

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

type VolumeResolver interface {
	GetVolume(context.Context, *control.GetLifecycleVolumeRequest, ...grpc.CallOption) (*control.CreateVolumeResponse, error)
}

// ResolveVolume never creates an id or falls back to a name-only filer path.
func ResolveVolume(ctx context.Context, rpc VolumeResolver, name string) (layout.VolumeID, error) {
	r, err := rpc.GetVolume(ctx, &control.GetLifecycleVolumeRequest{VolumeId: name})
	if err != nil {
		return layout.VolumeID{}, err
	}
	if r.GetVolumeId() != name {
		return layout.VolumeID{}, fmt.Errorf("master resolved volume %q as %q", name, r.GetVolumeId())
	}
	id, err := layout.ParseVolumeID(r.GetLayoutId())
	if err != nil {
		return layout.VolumeID{}, fmt.Errorf("volume %q has no valid master layout identity; recreate: %w", name, err)
	}
	return id, nil
}
