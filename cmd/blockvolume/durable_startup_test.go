package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

// #133 (review of #139): blockvolume's startup open-and-replay step
// reports a classifiable failure of EITHER step through the host and
// faults fail-closed. These drive openAndRecoverDurable, the function
// main runs, so removing its reporting fails them.

type spyHost struct {
	blocked  []string
	cleared  int
	reported []string // the classified reasons ReportLocalRecoveryFailure returned
}

func (s *spyHost) BlockLocalReadiness(reason string) { s.blocked = append(s.blocked, reason) }
func (s *spyHost) ClearLocalReadinessBlock()         { s.cleared++ }
func (s *spyHost) ReportLocalRecoveryFailure(err error) string {
	r := volume.LocalRecoveryHealthReason(err)
	s.reported = append(s.reported, r)
	return r
}

type fakeStartup struct {
	ensureErr  error
	recoverErr error
	report     durable.RecoveryReport
	recovered  bool
}

func (f *fakeStartup) EnsureStorage(string) (storage.LogicalStorage, error) { return nil, f.ensureErr }
func (f *fakeStartup) RecoverVolume(context.Context, string) (durable.RecoveryReport, error) {
	f.recovered = true
	return f.report, f.recoverErr
}

func TestDurableStartup_ClassifiableOpenFailureFaultsAndReports(t *testing.T) {
	h := &spyHost{}
	dp := &fakeStartup{ensureErr: &fs.PathError{Op: "open", Path: "/vol/v1.bin", Err: syscall.EIO}}
	outcome, evidence, err := openAndRecoverDurable(dp, h, "v1", time.Second)
	if outcome != durableStartupFaulted || err == nil {
		t.Fatalf("classifiable open failure: outcome=%v err=%v; want faulted", outcome, err)
	}
	if len(h.blocked) != 1 || len(h.reported) != 1 || h.reported[0] != volume.UnhealthyIOError {
		t.Fatalf("host calls: blocked=%v reported=%v; want one block and io_error reported", h.blocked, h.reported)
	}
	if dp.recovered {
		t.Fatal("replay ran after a failed open")
	}
	if evidence == "" {
		t.Fatal("no evidence for the faulted open")
	}
}

func TestDurableStartup_UnclassifiedOpenFailureStaysFatal(t *testing.T) {
	h := &spyHost{}
	dp := &fakeStartup{ensureErr: errors.New("durable: bad config")}
	outcome, _, err := openAndRecoverDurable(dp, h, "v1", time.Second)
	if outcome != durableStartupFatal || err == nil {
		t.Fatalf("unclassified open failure: outcome=%v err=%v; want fatal (the earlier exit)", outcome, err)
	}
	if len(h.blocked) != 0 || len(h.reported) != 0 {
		t.Fatalf("host calls for a fatal open: blocked=%v reported=%v; want none", h.blocked, h.reported)
	}
}

func TestDurableStartup_ReplayFailureFaultsAndReports(t *testing.T) {
	h := &spyHost{}
	dp := &fakeStartup{recoverErr: storage.NewWALIntegrityFailure(nil, "crc"), report: durable.RecoveryReport{Evidence: "recover failed: crc"}}
	outcome, evidence, _ := openAndRecoverDurable(dp, h, "v1", time.Second)
	if outcome != durableStartupFaulted || evidence != "recover failed: crc" {
		t.Fatalf("replay failure: outcome=%v evidence=%q", outcome, evidence)
	}
	if len(h.reported) != 1 || h.reported[0] != volume.UnhealthyWALCorrupt || len(h.blocked) != 1 {
		t.Fatalf("host calls: blocked=%v reported=%v; want one block and wal_corrupt", h.blocked, h.reported)
	}
}

func TestDurableStartup_CleanRecoveryClearsBlock(t *testing.T) {
	h := &spyHost{}
	outcome, _, err := openAndRecoverDurable(&fakeStartup{report: durable.RecoveryReport{Evidence: "recovered LSN=3"}}, h, "v1", time.Second)
	if outcome != durableStartupReady || err != nil || h.cleared != 1 || len(h.reported) != 0 {
		t.Fatalf("clean recovery: outcome=%v err=%v cleared=%d reported=%v", outcome, err, h.cleared, h.reported)
	}
}

type healthyView struct{}

func (healthyView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", Healthy: true}
}

// Real provider, open path: the volume file is truncated (2 bytes), so
// the provider's 4-byte magic read fails with io.ErrUnexpectedEOF
// (io_error).
func TestDurableStartup_RealProviderOpenFailureIsIOError(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "v1.bin"), []byte{0x53, 0x57}, 0o644); err != nil {
		t.Fatal(err)
	}
	dp, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplSmartWAL, StorageRoot: root}, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	defer dp.Close()
	h := &spyHost{}
	outcome, _, oErr := openAndRecoverDurable(dp, h, "v1", 5*time.Second)
	if outcome != durableStartupFaulted || len(h.reported) != 1 || h.reported[0] != volume.UnhealthyIOError {
		t.Fatalf("truncated volume file: outcome=%v err=%v reported=%v; want faulted with io_error", outcome, oErr, h.reported)
	}
}

// Real provider, replay path: a smartwal volume written through the
// provider, then an extent corrupted on disk; the next start's replay
// fails with wal_corrupt.
func TestDurableStartup_RealProviderCorruptReplayIsWALCorrupt(t *testing.T) {
	root := t.TempDir()
	cfg := durable.ProviderConfig{Impl: durable.ImplSmartWAL, StorageRoot: root, BlockSize: 4096, NumBlocks: 16}
	dp, err := durable.NewDurableProvider(cfg, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	st, err := dp.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < 3; i++ {
		data := make([]byte, 4096)
		data[0] = byte(0xA0 + i)
		if _, err := st.Write(i, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := dp.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "v1.bin")
	layout, rec, err := smartwal.LatestRecord(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0x5A}, layout.ExtentStart+int64(rec.LBA)*int64(layout.BlockSize)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	dp2, err := durable.NewDurableProvider(cfg, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	defer dp2.Close()
	h := &spyHost{}
	outcome, evidence, _ := openAndRecoverDurable(dp2, h, "v1", 5*time.Second)
	if outcome != durableStartupFaulted || len(h.reported) != 1 || h.reported[0] != volume.UnhealthyWALCorrupt {
		t.Fatalf("corrupt replay: outcome=%v evidence=%q reported=%v; want faulted with wal_corrupt", outcome, evidence, h.reported)
	}
}
