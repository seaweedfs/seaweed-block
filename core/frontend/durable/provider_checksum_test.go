package durable_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestProviderC12_RuntimeChecksumFaultLatchesAfterIORelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	view := newStubView(healthyProj(id))
	root := t.TempDir()
	var backend *durable.StorageBackend
	reports := 0
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: root, BlockSize: 4096, NumBlocks: 16, RequireDataChecksums: true, ReportDataChecksumFailure: func(err error) {
		if !errors.Is(err, storage.ErrDataChecksum) {
			t.Errorf("wrong fault: %v", err)
		}
		reports++
		if err := backend.StopIO(ctx, "data_checksum"); err != nil {
			t.Errorf("reporter was called while own IO lock held: %v", err)
		}
	}}, view)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	raw, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	backend = b.(*durable.StorageBackend)
	want := bytes.Repeat([]byte{0x49}, 4096)
	if err = raw.WriteExtentDirect(3, want); err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Sync(); err != nil {
		t.Fatal(err)
	}
	statuses := p.DurableStatuses()
	if len(statuses) != 1 || statuses[0].StorageFormatVersion != 2 || !statuses[0].DataChecksums {
		t.Fatalf("missing protected status: %+v", statuses)
	}
	// Header offsets are the stable documented v2 superblock layout.
	f, err := os.OpenFile(filepath.Join(root, "v1.bin"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	header := make([]byte, 4096)
	if _, err = f.ReadAt(header, 0); err != nil {
		t.Fatal(err)
	}
	extent := binary.LittleEndian.Uint64(header[40:48]) + binary.LittleEndian.Uint64(header[48:56])
	if _, err = f.WriteAt([]byte{0x48}, int64(extent+3*4096+17)); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	n, err := b.Read(ctx, 3*4096, got)
	if n != 0 || !errors.Is(err, syscall.EIO) || reports != 1 {
		t.Fatalf("runtime fault not refused/reported: n=%d err=%v reports=%d", n, err, reports)
	}
	if st := p.DurableStatuses()[0]; st.Operational || st.Evidence != "data_checksum" {
		t.Fatalf("corrupt backend still operational: %+v", st)
	}
	if _, err = b.Read(ctx, 0, got); err == nil {
		t.Fatal("later IO admitted after checksum fault")
	}
}
