package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func lifecycleClient(t *testing.T, addr string) control.LifecycleServiceClient {
	t.Helper()
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return control.NewLifecycleServiceClient(c)
}

func startDeleteMaster(t *testing.T, cfg master.Config) (*master.Host, func()) {
	t.Helper()
	if cfg.WorkspaceRPC && cfg.LayoutCreateVolume == nil {
		store := newMemChunkStore()
		catalog, err := chunkstore.NewBCatalog(store, store, defaultChunkPathCLI)
		if err != nil {
			t.Fatal(err)
		}
		cfg.LayoutCreateVolume = func(ctx context.Context, spec lifecycle.VolumeSpec) error {
			id, err := layout.ParseVolumeID(spec.LayoutID)
			if err != nil {
				return err
			}
			_, err = catalog.CreateVolume(ctx, chunkstore.VolumeRef{Name: spec.VolumeID, ID: id},
				layout.Geometry{SizeBytes: spec.SizeBytes, BlockSize: spec.BlockSize, ChunkBytes: spec.ChunkBytes})
			return err
		}
	}
	h, err := master.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	return h, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Close(ctx)
	}
}
