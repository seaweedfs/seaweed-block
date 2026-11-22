package nvme

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"testing"
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
	} {
		for _, op := range []uint8{ioRead, ioWrite, ioFlush} {
			t.Run(tc.name+string(rune('0'+op)), func(t *testing.T) {
				h := NewIOHandler(HandlerConfig{Backend: retryClassBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v"}), err: tc.err}})
				result := h.Handle(context.Background(), IOCommand{Opcode: op, NSID: 1, NLB: 1, Data: make([]byte, 512)})
				if dnr := result.completionStatus()&(1<<15) != 0; dnr != tc.dnr {
					t.Fatalf("DNR=%t want %t, result=%+v", dnr, tc.dnr, result)
				}
			})
		}
	}
}
