package iscsi_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
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
		{name: "not-ready", err: frontend.ErrNotReady, status: 0x02, key: 0x02, asc: 0x04, ascq: 0x01, sct: 0, sc: 0x82},
		{name: "busy", err: frontend.ErrBackendBusy, status: 0x08, sct: 0, sc: 0x82},
		{name: "stale", err: frontend.ErrStalePrimary, status: 0x02, key: 0x02, asc: 0x04, ascq: 0x0a, sct: 3, sc: 3},
		{name: "closed", err: frontend.ErrBackendClosed, status: 0x02, key: 0x02, asc: 0x04, ascq: 0x03, sct: 3, sc: 2},
		{name: "permanent", err: errors.New("media read failure"), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6},
		{name: "deadline", err: context.DeadlineExceeded, status: 0x02, key: 0x02, asc: 0x04, ascq: 0x01, sct: 0, sc: 0x82, deadline: true},
		{name: "deadline-and-physical", err: errors.Join(context.DeadlineExceeded, errors.New("media sync failure")), status: 0x02, key: 0x03, asc: 0x11, sct: 0, sc: 6, deadline: true},
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
