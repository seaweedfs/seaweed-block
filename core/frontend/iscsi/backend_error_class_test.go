package iscsi_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type classifiedErrorBackend struct {
	frontend.Backend
	err error
}

func (b classifiedErrorBackend) Read(context.Context, int64, []byte) (int, error)  { return 0, b.err }
func (b classifiedErrorBackend) Write(context.Context, int64, []byte) (int, error) { return 0, b.err }
func (b classifiedErrorBackend) Sync(context.Context) error                        { return b.err }

// INV-FE-ERROR-RETRY-CLASS-001: transient backend errors must never become a media failure. Both
// protocol handlers consume the same backend condition; neither returns data.
func TestBackendErrorClassesAcrossFrontends(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		err                             error
		status, key, asc, ascq, sct, sc uint8
		deadline                        bool
	}{
		{name: "wal-pressure", err: fmt.Errorf("%w: %w", frontend.ErrBackendBusy, storage.ErrWALPressureTimeout), status: 0x08, sct: 0, sc: 0x82},
		{name: "hydration-pending", err: fmt.Errorf("%w: %w", frontend.ErrBackendBusy, storage.ErrChunkHydrationPending), status: 0x08, sct: 0, sc: 0x82},
		{name: "observer-deadline", err: fmt.Errorf("%w: %w: %w", frontend.ErrBackendBusy, durable.ErrReplicationAckUnavailable, context.DeadlineExceeded), status: 0x08, sct: 0, sc: 0x82, deadline: true},
		{name: "unknown", err: errors.New("unrecognized backend cause"), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "pressure-without-busy-marker", err: storage.ErrWALPressureTimeout, status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "hydration-without-busy-marker", err: storage.ErrChunkHydrationPending, status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "observer-deadline-without-busy-marker", err: errors.Join(durable.ErrReplicationAckUnavailable, context.DeadlineExceeded), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6, deadline: true},
		{name: "unknown-with-busy", err: errors.Join(errors.New("unrecognized backend cause"), frontend.ErrBackendBusy), status: 0x08, sct: 0, sc: 0x82},
		{name: "unknown-with-not-ready", err: errors.Join(errors.New("unrecognized backend cause"), frontend.ErrNotReady), status: 0x02, key: 0x02, asc: 0x04, ascq: 0x01, sct: 0, sc: 0x82},
		{name: "unknown-with-stale", err: errors.Join(errors.New("unrecognized backend cause"), frontend.ErrStalePrimary), status: 0x02, key: 0x02, asc: 0x04, ascq: 0x0a, sct: 3, sc: 3},
		{name: "unknown-with-closed", err: errors.Join(errors.New("unrecognized backend cause"), frontend.ErrBackendClosed), status: 0x02, key: 0x02, asc: 0x04, ascq: 0x03, sct: 3, sc: 2},
		{name: "unknown-with-physical", err: errors.Join(errors.New("unrecognized backend cause"), syscall.EIO), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "latched-arbitrary-io", err: errors.Join(frontend.ErrBackendBusy, storage.NewSubstrateIOFailure(errors.New("map persist failed"), "latched store")),
			status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "latched-arbitrary-integrity", err: errors.Join(frontend.ErrBackendBusy, storage.NewWALIntegrityFailure(errors.New("descriptor corrupt"), "latched store")),
			status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "counter-exhausted", err: errors.Join(frontend.ErrBackendBusy, storage.ErrCounterExhausted),
			status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "local-short-read", err: errors.Join(frontend.ErrBackendBusy, storage.NewWALIntegrityFailure(io.ErrUnexpectedEOF, "local file read")),
			status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "unknown-recovery-kind", err: errors.Join(frontend.ErrBackendBusy, &storage.RecoveryFailure{Kind: storage.StorageRecoveryFailureUnknown, Cause: errors.New("unknown recovery cause")}),
			status: 0x08, sct: 0, sc: 0x82},
		{name: "recycled-recovery-kind", err: errors.Join(frontend.ErrBackendBusy, storage.NewWALRecycledFailure(errors.New("recycled"), "history")),
			status: 0x08, sct: 0, sc: 0x82},
		{name: "latched-after-nonpermanent-envelope", err: errors.Join(frontend.ErrNotReady,
			&storage.RecoveryFailure{Kind: storage.StorageRecoveryFailureUnknown},
			storage.NewSubstrateIOFailure(errors.New("map persistence latched"), "store")),
			status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "latched-under-nonpermanent-envelope", err: errors.Join(frontend.ErrNotReady,
			&storage.RecoveryFailure{Kind: storage.StorageRecoveryFailureUnknown,
				Cause: storage.NewWALIntegrityFailure(errors.New("descriptor latched"), "store")}),
			status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "fsync-fault", err: errors.Join(frontend.ErrBackendBusy, storage.ErrFsyncFailed), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "append-fault", err: errors.Join(frontend.ErrBackendBusy, storage.ErrAppendFailed), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "integrity-fault", err: errors.Join(frontend.ErrBackendBusy, storage.ErrWALIntegrityFault), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "invalid-map", err: errors.Join(frontend.ErrBackendBusy, storage.ErrMapInvalid), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "no-space", err: errors.Join(frontend.ErrBackendBusy, syscall.ENOSPC), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "read-only", err: errors.Join(frontend.ErrBackendBusy, syscall.EROFS), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "hydration-short-remote-body", err: errors.Join(frontend.ErrBackendBusy, storage.ErrChunkHydrationPending, io.ErrUnexpectedEOF), status: 0x08, sct: 0, sc: 0x82},
		{name: "not-ready", err: frontend.ErrNotReady, status: 0x02, key: 0x02, asc: 0x04, ascq: 0x01, sct: 0, sc: 0x82},
		{name: "sync-quorum-shortfall", err: fmt.Errorf("%w: %w: acknowledgements 1/2", durable.ErrReplicationAckUnavailable, replication.ErrDurabilityQuorumLost), status: 0x02, key: 0x02, asc: 0x04, ascq: 0x01, sct: 0, sc: 0x82},
		{name: "sync-all-shortfall", err: fmt.Errorf("%w: sync target LSN 9 acknowledgements 2/3", replication.ErrDurabilityBarrierFailed), status: 0x02, key: 0x02, asc: 0x04, ascq: 0x01, sct: 0, sc: 0x82},
		{name: "shortfall-with-media-error", err: errors.Join(replication.ErrDurabilityQuorumLost, syscall.EIO), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "busy", err: frontend.ErrBackendBusy, status: 0x08, sct: 0, sc: 0x82},
		{name: "stale", err: frontend.ErrStalePrimary, status: 0x02, key: 0x02, asc: 0x04, ascq: 0x0a, sct: 3, sc: 3},
		{name: "closed", err: frontend.ErrBackendClosed, status: 0x02, key: 0x02, asc: 0x04, ascq: 0x03, sct: 3, sc: 2},
		{name: "permanent", err: syscall.EIO, status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "deadline", err: context.DeadlineExceeded, status: 0x02, key: 0x02, asc: 0x04, ascq: 0x01, sct: 0, sc: 0x82, deadline: true},
		{name: "deadline-and-physical", err: errors.Join(context.DeadlineExceeded, syscall.EIO), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6, deadline: true},
		{name: "deadline-and-busy", err: errors.Join(context.DeadlineExceeded, frontend.ErrBackendBusy), status: 0x08, sct: 0, sc: 0x82, deadline: true},
		{name: "deadline-busy-physical-last", err: errors.Join(context.DeadlineExceeded, frontend.ErrBackendBusy, syscall.EIO), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6, deadline: true},
		{name: "deadline-busy-physical-first", err: errors.Join(syscall.EIO, frontend.ErrBackendBusy, context.DeadlineExceeded), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6, deadline: true},
		{name: "deadline-busy-nested-physical", err: errors.Join(frontend.ErrBackendBusy, fmt.Errorf("durability: %w", errors.Join(context.DeadlineExceeded, syscall.EIO))), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6, deadline: true},
		{name: "deadline-and-not-ready", err: errors.Join(context.DeadlineExceeded, frontend.ErrNotReady), status: 0x02, key: 0x02, asc: 0x04, ascq: 0x01, sct: 0, sc: 0x82, deadline: true},
		{name: "deadline-not-ready-physical-last", err: errors.Join(context.DeadlineExceeded, frontend.ErrNotReady, syscall.EIO), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6, deadline: true},
		{name: "deadline-not-ready-physical-first", err: errors.Join(syscall.EIO, frontend.ErrNotReady, context.DeadlineExceeded), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6, deadline: true},
		{name: "deadline-not-ready-nested-physical", err: errors.Join(frontend.ErrNotReady, fmt.Errorf("durability: %w", errors.Join(context.DeadlineExceeded, syscall.EIO))), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6, deadline: true},
	} {
		for _, op := range []string{"read", "write", "sync"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				ctx := context.Background()
				if tc.deadline {
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
					defer cancel()
				}
				b := classifiedErrorBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v"}), err: fmt.Errorf("backend: %w", tc.err)}
				sh := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: b})
				nh := nvme.NewIOHandler(nvme.HandlerConfig{Backend: b})
				var cdb [16]byte
				var payload []byte
				nc := nvme.IOCommand{NSID: 1, NLB: 1}
				switch op {
				case "read":
					cdb[0] = iscsi.ScsiRead10
					cdb[8] = 1
					nc.Opcode = 2
				case "write":
					cdb[0] = iscsi.ScsiWrite10
					cdb[8] = 1
					payload = make([]byte, 512)
					nc.Opcode = 1
					nc.Data = payload
				case "sync":
					cdb[0] = iscsi.ScsiSyncCache10
					nc.Opcode = 0
				}
				got := sh.HandleCommand(ctx, cdb, payload)
				ng := nh.Handle(ctx, nc)
				asc := tc.asc
				if op == "sync" && tc.key == 0x03 {
					asc = 0x0c
				}
				if got.Status != tc.status || got.SenseKey != tc.key || got.ASC != asc || got.ASCQ != tc.ascq {
					t.Errorf("SCSI got status/key/ASC/ASCQ %02x/%02x/%02x/%02x want %02x/%02x/%02x/%02x", got.Status, got.SenseKey, got.ASC, got.ASCQ, tc.status, tc.key, asc, tc.ascq)
				}
				if ng.SCT != tc.sct || ng.SC != tc.sc {
					t.Errorf("NVMe got %x/%x want %x/%x", ng.SCT, ng.SC, tc.sct, tc.sc)
				}
				if len(got.Data) != 0 || len(ng.Data) != 0 {
					t.Fatal("error returned data")
				}
				if tc.name == "not-ready" || tc.name == "busy" {
					if got.SenseKey == iscsi.SenseMediumError || ng.SCT != nvme.SCTGeneric || ng.SC != nvme.SCNamespaceNotReady {
						t.Fatal("transient condition is not retryable on both protocols")
					}
				}
			})
		}
	}
}

type unknownCompletionError struct{ message string }

func (e unknownCompletionError) Error() string { return e.message }

type otherUnknownCompletionError struct{}

func (otherUnknownCompletionError) Error() string { return "unknown completion B" }

// INV-FE-ERROR-RETRY-CLASS-001: no marker means
// permanent. Unknown leaf types are logged once across wrappers and protocols.
func TestUnclassifiedBackendErrorIsPermanentAndLoggedOncePerType(t *testing.T) {
	// The log-once cache is process-wide; give each test repetition a fresh
	// process rather than exposing an implementation-only reset operation.
	if os.Getenv("FRONTEND_UNCLASSIFIED_LOG_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUnclassifiedBackendErrorIsPermanentAndLoggedOncePerType$", "-test.count=1")
		cmd.Env = append(os.Environ(), "FRONTEND_UNCLASSIFIED_LOG_CHILD=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("log-once child: %v\n%s", err, output)
		}
		return
	}
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	for _, cause := range []error{
		fmt.Errorf("wrapped: %w", unknownCompletionError{"unknown completion A"}),
		errors.Join(unknownCompletionError{"same type, other value"}, otherUnknownCompletionError{}),
	} {
		b := classifiedErrorBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v"}), err: cause}
		scsi := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: b}).HandleCommand(context.Background(), [16]byte{iscsi.ScsiSyncCache10}, nil)
		nv := nvme.NewIOHandler(nvme.HandlerConfig{Backend: b}).Handle(context.Background(), nvme.IOCommand{NSID: 1})
		if scsi.Status != 0x02 || scsi.SenseKey != 0x03 || scsi.ASC != 0x0c || nv.SCT != 0 || nv.SC != 6 {
			t.Fatalf("marker-free completion not permanent: SCSI=%+v NVMe=%+v", scsi, nv)
		}
	}
	if strings.Count(logs.String(), "frontend: unclassified backend error type=") != 2 || !strings.Contains(logs.String(), "unknown completion A") || !strings.Contains(logs.String(), "unknown completion B") {
		t.Fatalf("expected one log for each leaf type across both frontends: %q", logs.String())
	}
}
