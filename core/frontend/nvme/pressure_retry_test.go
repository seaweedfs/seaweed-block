package nvme

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"testing"
)

type pressureWriteBackend struct {
	frontend.Backend
	err error
}

func (b *pressureWriteBackend) Write(context.Context, int64, []byte) (int, error) { return 0, b.err }
func TestSessionPressureTimeoutIsRetryableNamespaceNotReady(t *testing.T) {
	b := &pressureWriteBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"}), err: frontend.ErrBackendBusy}
	s := &Session{handler: NewIOHandler(HandlerConfig{Backend: b}), respCh: make(chan *response, 1), done: make(chan struct{})}
	for _, busy := range []bool{true, false} {
		if !busy {
			b.err = errors.New("sticky physical I/O fault")
		}
		req := &Request{payload: make([]byte, 512)}
		req.capsule.OpCode = ioWrite
		req.capsule.NSID = 1
		s.handleWrite(context.Background(), req)
		resp := <-s.respCh
		want := MakeStatusField(SCTGeneric, SCInternalError, true)
		if busy {
			want = MakeStatusField(SCTGeneric, SCNamespaceNotReady, false)
		}
		if resp.resp.Status != want {
			t.Fatalf("busy=%v wire status=%#x want%#x", busy, resp.resp.Status, want)
		}
	}
}
