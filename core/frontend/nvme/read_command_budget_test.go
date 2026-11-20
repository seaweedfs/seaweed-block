package nvme

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type budgetReadBackend struct {
	frontend.Backend
	deadline time.Time
	physical bool
}

func (b *budgetReadBackend) Read(ctx context.Context, _ int64, p []byte) (int, error) {
	b.deadline, _ = ctx.Deadline()
	<-ctx.Done()
	if b.physical {
		return 0, errors.Join(ctx.Err(), errors.New("physical read failure"))
	}
	return 0, frontend.ErrBackendBusy
}

func TestSessionReadBudgetIncludesQueueResidence(t *testing.T) {
	for _, physical := range []bool{false, true} {
		b := &budgetReadBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"}), physical: physical}
		s := &Session{handler: NewIOHandler(HandlerConfig{Backend: b}), respCh: make(chan *response, 1), done: make(chan struct{})}
		arrival := time.Now().Add(-5 * time.Second)
		req := &Request{receivedAt: arrival, dispatchReadyAt: time.Now()}
		req.capsule.OpCode, req.capsule.NSID = ioRead, 1
		s.handleRead(context.Background(), req)
		resp := <-s.respCh
		want := MakeStatusField(SCTGeneric, SCNamespaceNotReady, false)
		if physical {
			want = MakeStatusField(SCTGeneric, SCInternalError, true)
		}
		if !b.deadline.Equal(arrival.Add(frontend.ReadCommandBudget)) || resp.resp.Status != want || len(resp.c2hData) != 0 {
			t.Fatalf("physical=%t deadline=%v status=%#x data=%d", physical, b.deadline, resp.resp.Status, len(resp.c2hData))
		}
	}
}
