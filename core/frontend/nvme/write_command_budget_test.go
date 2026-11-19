package nvme

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type expiredDispatchBackend struct {
	frontend.Backend
	deadline time.Time
	calls    int
}

func (b *expiredDispatchBackend) Write(ctx context.Context, _ int64, _ []byte) (int, error) {
	b.calls++
	b.deadline, _ = ctx.Deadline()
	if ctx.Err() == context.DeadlineExceeded {
		return 0, frontend.ErrBackendBusy
	}
	return 512, nil
}
func TestSessionWriteBudgetIncludesDispatchQueue(t *testing.T) {
	b := &expiredDispatchBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})}
	s := &Session{handler: NewIOHandler(HandlerConfig{Backend: b}), respCh: make(chan *response, 1), done: make(chan struct{})}
	arrived := time.Now().Add(-5 * time.Second)
	req := &Request{payload: make([]byte, 512), receivedAt: arrived, dispatchReadyAt: time.Now()}
	req.capsule.OpCode = ioWrite
	req.capsule.NSID = 1
	s.handleWrite(context.Background(), req)
	resp := <-s.respCh
	if b.calls != 1 || !b.deadline.Equal(arrived.Add(frontend.WriteCommandBudget)) || resp.resp.Status != MakeStatusField(SCTGeneric, SCNamespaceNotReady, false) {
		t.Fatalf("deadline=%v status=%#x calls=%d", b.deadline, resp.resp.Status, b.calls)
	}
}
