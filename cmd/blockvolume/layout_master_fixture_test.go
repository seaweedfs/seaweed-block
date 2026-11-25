//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// layoutMasterWriter gives an in-process master the layout writer a
// production master gets from --chunk-filer: CreateVolume writes the VOLUME
// and the unclaimed HEAD under base on the test filer before the record is
// store-ready.
func layoutMasterWriter(t *testing.T, cluster *w4WeedCluster, base string) func(context.Context, lifecycle.VolumeSpec) error {
	t.Helper()
	cat, client, err := chunkstore.DialBCatalog(context.Background(), chunkstore.SWFSConfig{FilerGRPC: cluster.filerGRPC, AssignPath: base})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return func(ctx context.Context, spec lifecycle.VolumeSpec) error {
		id, err := layout.ParseVolumeID(spec.LayoutID)
		if err != nil {
			return err
		}
		_, err = cat.CreateVolume(ctx, chunkstore.VolumeRef{Name: spec.VolumeID, ID: id}, layout.Geometry{
			SizeBytes: spec.SizeBytes, BlockSize: spec.BlockSize, ChunkBytes: spec.ChunkBytes})
		return err
	}
}

// createStoreReadyVolume creates a volume through the master's public create,
// the path production uses, and requires the record to come back store-ready.
func createStoreReadyVolume(t *testing.T, h *master.Host, volume string, sizeBytes uint64) layout.VolumeID {
	t.Helper()
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: volume, SizeBytes: sizeBytes, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("create %s through the master: %v", volume, err)
	}
	if !resp.GetStoreReady() {
		t.Fatalf("create %s: the master answered store_ready=false", volume)
	}
	id, err := layout.ParseVolumeID(resp.GetLayoutId())
	if err != nil {
		t.Fatalf("master-minted layout id %q: %v", resp.GetLayoutId(), err)
	}
	if rec, ok := h.Lifecycle().Volumes.GetVolume(volume); !ok || !rec.StoreReady {
		t.Fatal(fmt.Sprintf("lifecycle record of %s is not store-ready after the create", volume))
	}
	return id
}
