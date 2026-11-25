package main

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

// layoutVolumeCreator writes only the master's initial VOLUME and unclaimed
// HEAD. layout.CreateVolume performs exclusive writes and heals a replay
// stopped between them without minting another identity.
func layoutVolumeCreator(cat *chunkstore.BCatalog) func(context.Context, lifecycle.VolumeSpec) error {
	return func(ctx context.Context, spec lifecycle.VolumeSpec) error {
		if cat == nil {
			return fmt.Errorf("master: layout catalog is unavailable")
		}
		id, err := layout.ParseVolumeID(spec.LayoutID)
		if err != nil {
			return err
		}
		_, err = cat.CreateVolume(ctx, chunkstore.VolumeRef{Name: spec.VolumeID, ID: id}, layout.Geometry{
			SizeBytes: spec.SizeBytes, BlockSize: spec.BlockSize, ChunkBytes: spec.ChunkBytes,
		})
		return err
	}
}
