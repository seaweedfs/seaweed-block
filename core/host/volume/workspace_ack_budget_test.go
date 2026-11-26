package volume

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

// slowAckClient is a master that answers the revocation ACK after delay
// (its bounded HEAD read and write), honouring the call's deadline.
type slowAckClient struct {
	delay time.Duration
	acked chan struct{}
	valid time.Duration // the revoking lease's validity (default 30 s)
	seen  *ackCall      // what the ACK call was entered with (optional)
}

// ackCall records the ACK call as dispatched: entered with a live context,
// and the deadline that context carried.
type ackCall struct {
	mu          sync.Mutex
	entered     bool
	live        bool
	deadline    time.Time
	hasDeadline bool
}

func (c slowAckClient) RenewWorkspaceLease(context.Context, *control.WorkspaceIdentity, ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	valid := c.valid
	if valid == 0 {
		valid = 30 * time.Second
	}
	return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(valid), Revoking: true}, nil
}

func (c slowAckClient) AckWorkspaceRevoked(ctx context.Context, _ *control.WorkspaceRevocationAck, _ ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	if c.seen != nil {
		c.seen.mu.Lock()
		c.seen.entered, c.seen.live = true, ctx.Err() == nil
		c.seen.deadline, c.seen.hasDeadline = ctx.Deadline()
		c.seen.mu.Unlock()
	}
	select {
	case <-time.After(c.delay):
		close(c.acked)
		return &control.WorkspaceEmpty{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A master that takes 2.5 s to answer the ACK (above its 2 s HEAD read
// alone, still inside its bounded steps) gets a completed ACK: the holder's
// ACK bound exceeds the master's.
func TestWorkspaceRevocationAckOutlastsTheMastersBoundedSteps(t *testing.T) {
	if WorkspaceAckTimeout <= 2*time.Second+500*time.Millisecond {
		t.Fatalf("WorkspaceAckTimeout %v does not exceed the master's 2 s HEAD read plus its write and reconcile", WorkspaceAckTimeout)
	}
	client := slowAckClient{delay: 2500 * time.Millisecond, acked: make(chan struct{})}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := g.Run(ctx); err != nil {
		t.Fatalf("a 2.5 s ACK answer failed the revocation: %v", err)
	}
	select {
	case <-client.acked:
	default:
		t.Fatal("the master's ACK answer was not awaited")
	}
}

// The lease deadline still bounds the ACK: with the revoking lease valid for
// 1 s and a master that answers after 3 s, the ACK call is dispatched with a
// live context whose deadline is the lease deadline (earlier than the 5 s
// ACK bound and the caller's), it ends with DeadlineExceeded at about 1 s,
// and afterwards the guard holds no lease and no ACK completed.
func TestWorkspaceRevocationAckIsCutAtTheLeaseDeadline(t *testing.T) {
	seen := &ackCall{}
	client := slowAckClient{delay: 3 * time.Second, acked: make(chan struct{}), valid: time.Second, seen: seen}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	parent := 20 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), parent)
	defer cancel()
	start := time.Now()
	err = g.Run(ctx)
	elapsed := time.Since(start)

	seen.mu.Lock()
	entered, live, deadline, hasDeadline := seen.entered, seen.live, seen.deadline, seen.hasDeadline
	seen.mu.Unlock()
	if !entered || !live {
		t.Fatalf("the ACK was not dispatched with a live context (entered %v, live %v); err %v", entered, live, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v; want DeadlineExceeded at the lease deadline", err)
	}
	if !hasDeadline || !deadline.Before(start.Add(WorkspaceAckTimeout)) || !deadline.Before(start.Add(parent)) || deadline.After(start.Add(1500*time.Millisecond)) {
		t.Fatalf("the ACK's deadline %v (set %v) is not the 1 s lease deadline: start %v, ACK bound %v, parent %v", deadline, hasDeadline, start, WorkspaceAckTimeout, parent)
	}
	if elapsed >= 2500*time.Millisecond {
		t.Fatalf("the ACK ran %v: it was not cut at the 1 s lease deadline", elapsed)
	}
	if g.Held() {
		t.Fatal("the guard still holds the lease after its deadline")
	}
	select {
	case <-client.acked:
		t.Fatal("an ACK completed after the lease deadline")
	default:
	}
}
