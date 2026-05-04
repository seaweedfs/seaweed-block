package iscsi_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

type p2ProjectionView struct {
	proj frontend.Projection
}

func (v p2ProjectionView) Projection() frontend.Projection { return v.proj }

// TestP2_ISCSI_SustainedWriteSyncCache_SurvivesSmartWALReopen pins the
// product-backed write/flush path that Linux filesystems depend on:
// WRITE(10) reaches durable.StorageBackend, SYNCHRONIZE CACHE reaches
// storage.Sync, and synced blocks survive a clean close/reopen.
func TestP2_ISCSI_SustainedWriteSyncCache_SurvivesSmartWALReopen(t *testing.T) {
	const (
		blockSize        = 4096
		numBlocks        = 128
		scsiBlocksPerIO  = uint16(blockSize / int(iscsi.DefaultBlockSize))
		writeCount       = 48
		syncEvery        = 8
		maxBurst         = 128 * 1024
		frontendByteSize = numBlocks * blockSize
	)

	path := filepath.Join(t.TempDir(), "smartwal.bin")
	store, err := smartwal.CreateStore(path, numBlocks, blockSize)
	if err != nil {
		t.Fatalf("smartwal.CreateStore: %v", err)
	}

	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	view := p2ProjectionView{proj: frontend.Projection{
		VolumeID:        id.VolumeID,
		ReplicaID:       id.ReplicaID,
		Epoch:           id.Epoch,
		EndpointVersion: id.EndpointVersion,
		Healthy:         true,
	}}
	backend := durable.NewStorageBackend(store, view, id)
	backend.SetOperational(true, "p2 sustained sync test")

	tg, addr := startP2MemoryTarget(t, backend, frontendByteSize, maxBurst)
	cli := dialAndLogin(t, addr)

	expected := make(map[uint32][]byte, writeCount)
	for i := 0; i < writeCount; i++ {
		lba := uint32(i)
		payload := bytes.Repeat([]byte{byte(i + 1)}, blockSize)
		status, _, traces := cli.scsiCmdFullWithR2TTrace(t, writeCDB10(lba*uint32(scsiBlocksPerIO), scsiBlocksPerIO), nil, payload, 0)
		expectGood(t, status, "sustained WRITE(10)")
		if len(traces) == 0 {
			t.Fatalf("write %d: expected R2T path for 4KiB payload", i)
		}
		expected[lba] = payload

		if (i+1)%syncEvery == 0 {
			status, _ := cli.scsiCmd(t, syncCacheCDB10(), nil, 0)
			expectGood(t, status, "SYNCHRONIZE CACHE(10)")
		}
	}
	status, _ := cli.scsiCmd(t, syncCacheCDB10(), nil, 0)
	expectGood(t, status, "final SYNCHRONIZE CACHE(10)")

	cli.logout(t)
	if err := tg.Close(); err != nil {
		t.Fatalf("target Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("smartwal close: %v", err)
	}

	reopened, err := smartwal.OpenStore(path)
	if err != nil {
		t.Fatalf("smartwal.OpenStore: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatalf("smartwal Recover: %v", err)
	}

	for _, lba := range []uint32{0, 7, 8, 23, 47} {
		got, err := reopened.Read(lba)
		if err != nil {
			t.Fatalf("reopened Read lba=%d: %v", lba, err)
		}
		if !bytes.Equal(got, expected[lba]) {
			t.Fatalf("reopened data mismatch at lba=%d", lba)
		}
	}
}
