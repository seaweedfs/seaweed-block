package durable

// Package durable G-int.5 — Recovery wrapper (read-side only).
//
// Recover wraps LogicalStorage.Recover() into a RecoveryReport
// that the Provider can act on (flip SetOperational) and the host
// can surface via /status (read-side readiness report, no new
// publication path).
//
// §3.2 authority-boundary discipline: Recovery does NOT publish
// epoch, does NOT advance lineage, does NOT mint assignment
// state. It observes on-disk state, computes "is this node ready
// to serve I/O", and reports the answer. Master authority remains
// the sole publisher of Epoch / EndpointVersion.
//
// Option 3 from audit §10.5: no local epoch mirror in superblock.
// Per-I/O fence check in StorageBackend (identity vs projection)
// is the sole authority-drift guard. Recovery compares nothing
// against master-published state.

import (
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// RecoveryReport is the outcome of one Recover call. Evidence is
// the short string passed to SetOperational + surfaced via
// /status for operator diagnostics.
type RecoveryReport struct {
	VolumeID     string
	RecoveredLSN uint64
	Evidence     string

	// Err is non-nil iff Recover returned an error. Callers MUST
	// check this: evidence alone is human-readable and not a
	// programmatic signal.
	Err error
}

// Recover calls LogicalStorage.Recover() and packages the result
// into a RecoveryReport. Never panics; all failures land on
// RecoveryReport.Err.
//
// Pure function — no side-effects on the backend. The Provider
// (or caller) is responsible for flipping the adapter's
// operational gate based on the report.
func Recover(s storage.LogicalStorage, volumeID string) RecoveryReport {
	lsn, err := s.Recover()
	if err != nil {
		return RecoveryReport{
			VolumeID: volumeID,
			Evidence: fmt.Sprintf("recover failed: %v", err),
			Err:      err,
		}
	}
	return RecoveryReport{
		VolumeID:     volumeID,
		RecoveredLSN: lsn,
		Evidence:     fmt.Sprintf("recovered LSN=%d", lsn),
	}
}
