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

func layoutVolumeReader(cat *chunkstore.BCatalog) func(context.Context, lifecycle.VolumeSpec) (layout.Volume, error) {
	return func(ctx context.Context, spec lifecycle.VolumeSpec) (layout.Volume, error) {
		id, err := layout.ParseVolumeID(spec.LayoutID)
		if err != nil {
			return layout.Volume{}, err
		}
		return cat.ReadVolume(ctx, chunkstore.VolumeRef{Name: spec.VolumeID, ID: id})
	}
}

func layoutHeadReader(cat *chunkstore.BCatalog) func(context.Context, lifecycle.VolumeSpec) (layout.Head, error) {
	return func(ctx context.Context, spec lifecycle.VolumeSpec) (layout.Head, error) {
		id, err := layout.ParseVolumeID(spec.LayoutID)
		if err != nil {
			return layout.Head{}, err
		}
		h, found, err := cat.ReadHead(ctx, chunkstore.VolumeRef{Name: spec.VolumeID, ID: id})
		if err != nil {
			return layout.Head{}, err
		}
		if !found {
			return layout.Head{}, layout.ErrHeadAbsent
		}
		return h, nil
	}
}

func layoutTargetCreator(cat *chunkstore.BCatalog) func(context.Context, lifecycle.VolumeSpec, lifecycle.VolumeSpec, string) error {
	return func(ctx context.Context, source, target lifecycle.VolumeSpec, snapshot string) error {
		sid, err := layout.ParseVolumeID(source.LayoutID)
		if err != nil {
			return err
		}
		tid, err := layout.ParseVolumeID(target.LayoutID)
		if err != nil {
			return err
		}
		_, err = cat.Restore(ctx,
			chunkstore.VolumeRef{Name: source.VolumeID, ID: sid}, snapshot,
			chunkstore.VolumeRef{Name: target.VolumeID, ID: tid})
		return err
	}
}
