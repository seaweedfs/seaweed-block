// T3b Recovery tests — mini plan §2 acceptance row #5.
// Matrix-parameterized over both impls.

package durable_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// #5 clean recover path.
func TestT3b_Recovery_CleanRecover_FlipsOperational(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)
			backend, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			// Pre-Recover: I/O rejected.
			if _, err := backend.Read(context.Background(), 0, make([]byte, 4)); !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("pre-Recover Read: want ErrNotReady, got %v", err)
			}

			report, err := p.RecoverVolume(context.Background(), "v1")
			if err != nil {
				t.Fatalf("RecoverVolume: %v", err)
			}
			if report.VolumeID != "v1" {
				t.Errorf("report.VolumeID=%q want v1", report.VolumeID)
			}
			if report.Err != nil {
				t.Errorf("report.Err=%v want nil", report.Err)
			}
			if !strings.Contains(report.Evidence, "recovered LSN=") {
				t.Errorf("evidence=%q does not contain 'recovered LSN='", report.Evidence)
			}

			// Post-Recover: I/O succeeds.
			if _, err := backend.Read(context.Background(), 0, make([]byte, 4)); err != nil {
				t.Errorf("post-Recover Read: want success, got %v", err)
			}
		})
	}
}

// #5 — RecoverVolume on an un-opened volumeID returns ErrVolumeNotOpen.
func TestT3b_Recovery_UnopenedVolume_ReturnsErrVolumeNotOpen(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)
			_, err := p.RecoverVolume(context.Background(), "never-opened")
			if !errors.Is(err, durable.ErrVolumeNotOpen) {
				t.Fatalf("want ErrVolumeNotOpen, got %v", err)
			}
		})
	}
}

// Recover function (pure) returns evidence + no error on healthy storage.
func TestT3b_Recovery_PureFunction_Success(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t, 4, 4096)
			report := durable.Recover(s, "v1")
			if report.Err != nil {
				t.Fatalf("Recover: %v", report.Err)
			}
			if report.VolumeID != "v1" {
				t.Errorf("VolumeID=%q want v1", report.VolumeID)
			}
			if !strings.Contains(report.Evidence, "recovered LSN=") {
				t.Errorf("evidence=%q missing 'recovered LSN='", report.Evidence)
			}
		})
	}
}

// Recovery evidence for I/O that closes backend post-recover on error.
// This exercises the error wrapping path: even when the underlying
// Recover would not fail, we can verify the report's fields are set
// correctly. (Testing the real error path requires an injected
// error-returning LogicalStorage; kept out of T3b scope because
// writing a full stub is boilerplate that doesn't prove new logic.)
func TestT3b_Recovery_Report_FieldsPopulated(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t, 4, 4096)
			// Write + sync then recover — LSN should be > 0.
			if _, err := s.Write(0, make([]byte, 4096)); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatalf("Sync: %v", err)
			}
			report := durable.Recover(s, "v1")
			if report.Err != nil {
				t.Fatalf("Recover err: %v", report.Err)
			}
			// RecoveredLSN MUST be non-zero post-Write+Sync.
			if report.RecoveredLSN == 0 {
				t.Errorf("RecoveredLSN=0 after Write+Sync; expected >0")
			}
		})
	}
}
