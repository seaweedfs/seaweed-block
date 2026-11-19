package nvme

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type readyReadBackend struct {
	frontend.Backend
	err error
}

func (b *readyReadBackend) Read(context.Context, int64, []byte) (int, error) { return 0, b.err }

func TestSessionReadNotReadyIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		sct, sc uint8
		dnr     bool
	}{
		{"attach-pending", fmt.Errorf("awaiting HEAD attach: %w", frontend.ErrNotReady), SCTGeneric, SCNamespaceNotReady, false},
		{"physical-fault", errors.New("physical read failure"), SCTGeneric, SCInternalError, true},
		{"closed", frontend.ErrBackendClosed, SCTPathRelated, SCPathAsymAccessInaccessible, true},
		{"stale", frontend.ErrStalePrimary, SCTPathRelated, SCPathAsymAccessTransition, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &readyReadBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"}), err: tc.err}
			s := &Session{handler: NewIOHandler(HandlerConfig{Backend: backend}), respCh: make(chan *response, 1), done: make(chan struct{})}
			req := &Request{}
			req.capsule.OpCode = ioRead
			req.capsule.NSID = 1
			s.handleRead(context.Background(), req)
			resp := <-s.respCh
			if want := MakeStatusField(tc.sct, tc.sc, tc.dnr); resp.resp.Status != want {
				t.Fatalf("wire status=%#x want=%#x", resp.resp.Status, want)
			}
		})
	}
}
