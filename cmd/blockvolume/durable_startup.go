package main

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// durableStartup is the part of the durable provider the startup open
// and replay step uses.
type durableStartup interface {
	EnsureStorage(volumeID string) (storage.LogicalStorage, error)
	RecoverVolume(ctx context.Context, volumeID string) (durable.RecoveryReport, error)
}

// localRecoveryHost is the part of the volume host the startup step
// reports to.
type localRecoveryHost interface {
	BlockLocalReadiness(reason string)
	ClearLocalReadinessBlock()
	ReportLocalRecoveryFailure(err error) string
}

type durableStartupOutcome int

const (
	durableStartupReady   durableStartupOutcome = iota // storage open and replayed
	durableStartupFaulted                              // fail closed, keep serving status and heartbeats
	durableStartupFatal                                // unclassified open failure: exit as before
)

// openAndRecoverDurable opens the volume's durable storage and replays it
// at startup (#133). On a classifiable failure in either step (open or
// replay), it blocks local readiness, records the typed reason
// (wal_corrupt / io_error) through ReportLocalRecoveryFailure, and returns
// durableStartupFaulted: the process stays up fail-closed, with frontend
// I/O refused, and a primary assignment reports the reason at once. A
// replay failure that cannot be classified is faulted too, as before, and
// is covered by the recovery_timeout fallback. An open failure that
// cannot be classified returns durableStartupFatal, keeping the earlier
// exit.
func openAndRecoverDurable(dp durableStartup, h localRecoveryHost, volumeID string, recoverTimeout time.Duration) (durableStartupOutcome, string, error) {
	if _, err := dp.EnsureStorage(volumeID); err != nil {
		if volume.LocalRecoveryHealthReason(err) == "" {
			return durableStartupFatal, "", err
		}
		evidence := "durable storage open failed: " + err.Error()
		h.BlockLocalReadiness(evidence)
		h.ReportLocalRecoveryFailure(err)
		return durableStartupFaulted, evidence, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), recoverTimeout)
	defer cancel()
	report, err := dp.RecoverVolume(ctx, volumeID)
	if err != nil {
		h.BlockLocalReadiness(report.Evidence)
		h.ReportLocalRecoveryFailure(err)
		return durableStartupFaulted, report.Evidence, err
	}
	h.ClearLocalReadinessBlock()
	return durableStartupReady, report.Evidence, nil
}
