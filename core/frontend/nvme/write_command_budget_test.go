package nvme

import (
	"context"
	"errors"
	"fmt"
	"syscall"
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

// An already-issued write is joined even if its deadline passes. A subsequent
// FUA wait may return deadline exceeded, but must not turn transient expiry
// into a permanent device error. Physical failures still win.
type joinedBudgetBackend struct {
	frontend.Backend
	fua, physical bool
}

func (b *joinedBudgetBackend) result(ctx context.Context) error {
	if b.physical {
		return errors.Join(syscall.EIO, ctx.Err())
	}
	return fmt.Errorf("durability wait: %w", ctx.Err())
}
func (b *joinedBudgetBackend) Write(ctx context.Context, _ int64, p []byte) (int, error) {
	<-ctx.Done()
	if b.fua {
		return len(p), nil
	}
	return 0, b.result(ctx)
}
func (b *joinedBudgetBackend) Sync(ctx context.Context) error { return b.result(ctx) }
func TestSessionWriteBudgetDeadlineIsRetryableIncludingFUA(t *testing.T) {
	for _, fua := range []bool{false, true} {
		for _, physical := range []bool{false, true} {
			t.Run(fmt.Sprintf("fua=%t/physical=%t", fua, physical), func(t *testing.T) {
				b := &joinedBudgetBackend{Backend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"}), fua: fua, physical: physical}
				s := &Session{handler: NewIOHandler(HandlerConfig{Backend: b}), respCh: make(chan *response, 1), done: make(chan struct{})}
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				defer cancel()
				req := &Request{payload: make([]byte, 512)}
				req.capsule.OpCode = ioWrite
				req.capsule.NSID = 1
				if fua {
					req.capsule.D12 |= 1 << 30
				}
				s.handleWrite(ctx, req)
				got := (<-s.respCh).resp.Status
				want := MakeStatusField(SCTGeneric, SCNamespaceNotReady, false)
				if physical {
					want = MakeStatusField(SCTGeneric, SCInternalError, true)
				}
				if got != want {
					t.Fatalf("completion=%#x want%#x", got, want)
				}
			})
		}
	}
}
