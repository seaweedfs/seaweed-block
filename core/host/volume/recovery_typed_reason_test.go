package volume

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

// #133: a local durable recovery failure is classified into a typed
// unhealthy reason, which a primary-assigned replica reports at once.

func TestLocalRecoveryHealthReason_Classifies(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"extent checksum", &storage.DataChecksumFailure{LBA: 3, Detail: "CRC mismatch"}, UnhealthyDataChecksum},
		{"checksum wrapped as substrate IO", storage.NewSubstrateIOFailure(storage.ErrDataChecksum, "read"), UnhealthyDataChecksum},
		{"wal integrity", storage.NewWALIntegrityFailure(nil, "crc mismatch"), UnhealthyWALCorrupt},
		{"wal integrity wrapped", fmt.Errorf("recover: %w", storage.NewWALIntegrityFailure(nil, "x")), UnhealthyWALCorrupt},
		{"integrity sentinel", fmt.Errorf("x: %w", storage.ErrWALIntegrityFault), UnhealthyWALCorrupt},
		{"substrate io", storage.NewSubstrateIOFailure(errors.New("read"), "scan"), UnhealthyIOError},
		{"path error", &fs.PathError{Op: "read", Path: "/v", Err: syscall.EIO}, UnhealthyIOError},
		// io_error is broad by contract: a path error that says the local
		// storage is unusable (missing, not permitted) is io_error too; it
		// is not a claim that the disk is physically damaged.
		{"path ENOENT", &fs.PathError{Op: "open", Path: "/v", Err: syscall.ENOENT}, UnhealthyIOError},
		{"path EACCES", &fs.PathError{Op: "open", Path: "/v", Err: syscall.EACCES}, UnhealthyIOError},
		{"integrity wrapping a path error", storage.NewWALIntegrityFailure(&fs.PathError{Op: "read", Path: "/v", Err: syscall.EIO}, "x"), UnhealthyWALCorrupt},
		{"unexpected eof", fmt.Errorf("decode: %w", io.ErrUnexpectedEOF), UnhealthyIOError},
		{"eio", fmt.Errorf("x: %w", syscall.EIO), UnhealthyIOError},
		{"wal recycled", storage.NewWALRecycledFailure(nil, "x"), ""},
		{"unclassified", errors.New("something else"), ""},
	} {
		if got := LocalRecoveryHealthReason(tc.err); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

// The actual caller chain: a corrupted smartwal store, durable.Recover
// (what DurableProvider.RecoverVolume runs at startup), then the host's
// ReportLocalRecoveryFailure; a primary-assigned replica reports
// wal_corrupt at once, without waiting for the recover timeout.
func TestLocalRecoveryFailure_CorruptWALReportsWALCorruptAtOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v.sw")
	s, err := smartwal.CreateStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < 3; i++ {
		data := make([]byte, 4096)
		data[0] = byte(0xA0 + i)
		if _, err := s.Write(i, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
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
	reopened, err := smartwal.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report := durable.Recover(reopened, "v1")
	if report.Err == nil {
		t.Fatal("fixture: recovery of the corrupted store succeeded")
	}

	h, _ := watchdogHost(t, time.Hour) // the timeout is far away
	h.BlockLocalReadiness(report.Evidence)
	if got := h.ReportLocalRecoveryFailure(report.Err); got != UnhealthyWALCorrupt {
		t.Fatalf("ReportLocalRecoveryFailure(%v)=%q; want %s", report.Err, got, UnhealthyWALCorrupt)
	}
	h.applyFact(selfPrimaryFact(1)) // refused by the readiness block, but primary-assigned
	if s := reportSlot(t, h); s.Eligible || s.UnhealthyReason != UnhealthyWALCorrupt {
		t.Fatalf("primary-assigned after corrupt recovery: eligible=%t reason=%q; want ineligible, %s at once", s.Eligible, s.UnhealthyReason, UnhealthyWALCorrupt)
	}

	// A later clean recovery clears it.
	h.ClearLocalReadinessBlock()
	if s := reportSlot(t, h); !s.Eligible || s.UnhealthyReason != "" {
		t.Fatalf("after a clean recovery: eligible=%t reason=%q; want cleared", s.Eligible, s.UnhealthyReason)
	}
}

// Scope and fallback: a replica that is not primary-assigned is not
// flagged by a local failure; an unclassified failure reports nothing
// early and recovery_timeout still covers it.
func TestLocalRecoveryFailure_ScopeAndUnclassifiedFallback(t *testing.T) {
	h, clk := watchdogHost(t, 10*time.Second)
	h.ReportLocalRecoveryFailure(storage.NewSubstrateIOFailure(errors.New("read"), "replay"))
	if s := reportSlot(t, h); !s.Eligible || s.UnhealthyReason != "" {
		t.Fatalf("not primary-assigned: %+v; want not flagged", s)
	}
	h.applyFact(selfPrimaryFact(1))
	if s := reportSlot(t, h); s.UnhealthyReason != UnhealthyIOError {
		t.Fatalf("primary-assigned with an I/O recovery failure: reason=%q; want %s", s.UnhealthyReason, UnhealthyIOError)
	}

	h2, clk2 := watchdogHost(t, 10*time.Second)
	h2.ReportLocalRecoveryFailure(errors.New("unclassified"))
	h2.applyFact(selfPrimaryFact(1))
	if s := reportSlot(t, h2); s.UnhealthyReason != "" {
		t.Fatalf("unclassified failure inside the timeout: reason=%q; want none yet", s.UnhealthyReason)
	}
	clk2.advance(11 * time.Second)
	if s := reportSlot(t, h2); s.UnhealthyReason != UnhealthyRecoveryTimeout {
		t.Fatalf("unclassified failure past the timeout: reason=%q; want %s", s.UnhealthyReason, UnhealthyRecoveryTimeout)
	}
	_ = clk
}
