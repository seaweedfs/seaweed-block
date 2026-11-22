package nvme

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"testing"
	"time"
)

type retryClassBackend struct {
	frontend.Backend
	err error
}

func (b retryClassBackend) Read(context.Context, int64, []byte) (int, error)  { return 0, b.err }
func (b retryClassBackend) Write(context.Context, int64, []byte) (int, error) { return 0, b.err }
func (b retryClassBackend) Sync(context.Context) error                        { return b.err }

// INV-FE-ERROR-RETRY-CLASS-001: validate the completion encoding, not only the handler's status code.
func TestBackendRetryClassClearsWireDNR(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		dnr  bool
	}{
		{"not-ready", frontend.ErrNotReady, false},
		{"busy", frontend.ErrBackendBusy, false},
		{"permanent", errors.New("physical media fault"), true},
		{"deadline-busy", errors.Join(context.DeadlineExceeded, frontend.ErrBackendBusy), false},
		{"deadline-not-ready", errors.Join(context.DeadlineExceeded, frontend.ErrNotReady), false},
		{"deadline-busy-physical", errors.Join(context.DeadlineExceeded, frontend.ErrBackendBusy, errors.New("physical sync failure")), true},
		{"deadline-not-ready-physical", errors.Join(context.DeadlineExceeded, frontend.ErrNotReady, errors.New("physical sync failure")), true},
	} {
		for _, op := range []uint8{ioRead, ioWrite, ioFlush} {
			t.Run(tc.name+string(rune('0'+op)), func(t *testing.T) {
				h := NewIOHandler(HandlerConfig{Backend: retryClassBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v"}), err: tc.err}})
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				result := h.Handle(ctx, IOCommand{Opcode: op, NSID: 1, NLB: 1, Data: make([]byte, 512)})
				if dnr := result.completionStatus()&(1<<15) != 0; dnr != tc.dnr {
					t.Fatalf("DNR=%t want %t, result=%+v", dnr, tc.dnr, result)
				}
			})
		}
	}
}
