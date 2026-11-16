package volume

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

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// The real callback chain, including a runtime fault after a primary was
// already ready: provider read -> checksum error -> Host -> heartbeat.
func TestC12RuntimeChecksumFaultReportsTypedHeartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, _ := watchdogHost(t, time.Hour)
	h.applyFact(selfPrimaryFact(3))
	h.wd.reason(true, 3, false, "") // disarm the startup watchdog: this is runtime
	root := t.TempDir()
	dp, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStore, StorageRoot: root, BlockSize: 4096, NumBlocks: 16,
		ReportDataChecksumFailure: h.ReportDataChecksumFailure,
	}, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	defer dp.Close()
	raw, err := dp.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = dp.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	b, err := dp.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{0x71}, 4096)
	if err = raw.WriteExtentDirect(3, want); err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Sync(); err != nil {
		t.Fatal(err)
	}
	if n, err := b.Read(ctx, 3*4096, make([]byte, 4096)); err != nil || n != 4096 {
		t.Fatalf("baseline: %d %v", n, err)
	}
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
	if _, err = f.WriteAt([]byte{0x72}, int64(extent+3*4096+19)); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if n, err := b.Read(ctx, 3*4096, make([]byte, 4096)); n != 0 || !errors.Is(err, syscall.EIO) {
		t.Fatalf("corruption returned bytes: %d %v", n, err)
	}
	if blocked, _ := h.localReadinessBlock(); !blocked {
		t.Fatal("runtime fault did not block host readiness")
	}
	if slot := reportSlot(t, h); slot.Eligible || slot.UnhealthyReason != "data_checksum" {
		t.Fatalf("runtime heartbeat eligible=%v reason=%q; want false data_checksum", slot.Eligible, slot.UnhealthyReason)
	}
	if _, err = b.Read(ctx, 0, make([]byte, 4096)); err == nil {
		t.Fatal("backend admitted subsequent IO")
	}
}
