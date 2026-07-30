package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/transport"
)

var (
	ErrPeerQueueSaturated    = errors.New("replication: peer work queue saturated")
	ErrPeerQueueClosed       = errors.New("replication: peer work queue closed")
	ErrBarrierBelowTargetLSN = errors.New("replication: barrier acknowledgement below target LSN")
)

type peerWorkKind uint8

const (
	peerWorkWrite peerWorkKind = iota + 1
	peerWorkBarrier
)

type peerWorkResult struct {
	peerID   string
	eligible bool
	ackLSN   uint64
	err      error
}

type peerWork struct {
	kind      peerWorkKind
	write     LocalWrite
	targetLSN uint64
	result    chan<- peerWorkResult
}

type peerWorkQueueOps struct {
	ship       func(context.Context, LocalWrite) (bool, error)
	barrier    func(context.Context, uint64) (uint64, error)
	invalidate func(string)
}

// peerWorkQueue is one lineage-scoped, bounded FIFO. Writes and barriers share
// the same worker so a barrier can never overtake an earlier write.
type peerWorkQueue struct {
	peerID string
	ops    peerWorkQueueOps

	ctx    context.Context
	cancel context.CancelFunc
	jobs   chan peerWork
	done   chan struct{}

	mu          sync.Mutex
	closed      bool
	terminalErr error
}

func newPeerWorkQueue(peer *ReplicaPeer, depth int) *peerWorkQueue {
	return newPeerWorkQueueWithOps(peer.Target().ReplicaID, depth, peerWorkQueueOps{
		ship: func(ctx context.Context, write LocalWrite) (bool, error) {
			eligible := peer.State() == ReplicaHealthy
			err := peer.ShipEntry(ctx, transport.RecoveryLineage{}, write.LBA, write.LSN, write.Data)
			return eligible, err
		},
		barrier: func(ctx context.Context, targetLSN uint64) (uint64, error) {
			ack, err := peer.Barrier(ctx, targetLSN)
			if err != nil {
				return 0, err
			}
			if ack.AchievedLSN < targetLSN {
				err := fmt.Errorf("%w: peer %s achieved %d, required %d",
					ErrBarrierBelowTargetLSN, peer.Target().ReplicaID, ack.AchievedLSN, targetLSN)
				peer.Invalidate(err.Error())
				return ack.AchievedLSN, err
			}
			return ack.AchievedLSN, nil
		},
		invalidate: peer.Invalidate,
	})
}

func newPeerWorkQueueWithOps(peerID string, depth int, ops peerWorkQueueOps) *peerWorkQueue {
	if depth < 1 {
		depth = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := &peerWorkQueue{
		peerID: peerID,
		ops:    ops,
		ctx:    ctx,
		cancel: cancel,
		jobs:   make(chan peerWork, depth),
		done:   make(chan struct{}),
	}
	go q.run()
	return q
}

func (q *peerWorkQueue) enqueueWrite(write LocalWrite, result chan<- peerWorkResult) (int, error) {
	return q.enqueue(peerWork{kind: peerWorkWrite, write: write, result: result})
}

func (q *peerWorkQueue) enqueueBarrier(targetLSN uint64, result chan<- peerWorkResult) (int, error) {
	return q.enqueue(peerWork{kind: peerWorkBarrier, targetLSN: targetLSN, result: result})
}

func (q *peerWorkQueue) enqueue(work peerWork) (int, error) {
	q.mu.Lock()
	if q.closed {
		err := q.closedErrorLocked()
		q.mu.Unlock()
		return 0, err
	}
	select {
	case q.jobs <- work:
		depth := len(q.jobs)
		q.mu.Unlock()
		return depth, nil
	default:
		err := fmt.Errorf("%w: peer %s depth %d", ErrPeerQueueSaturated, q.peerID, cap(q.jobs))
		q.closed = true
		q.terminalErr = err
		q.cancel()
		q.mu.Unlock()
		if q.ops.invalidate != nil {
			q.ops.invalidate(err.Error())
		}
		return 0, err
	}
}

func (q *peerWorkQueue) closeAndWait() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.terminalErr = fmt.Errorf("%w: peer %s", ErrPeerQueueClosed, q.peerID)
		q.cancel()
	}
	q.mu.Unlock()
	<-q.done
}

func (q *peerWorkQueue) isTerminal() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

func (q *peerWorkQueue) run() {
	defer close(q.done)
	defer q.failPending()
	for {
		select {
		case <-q.ctx.Done():
			return
		default:
		}

		select {
		case <-q.ctx.Done():
			return
		case work := <-q.jobs:
			if err := q.ctx.Err(); err != nil {
				q.complete(work, peerWorkResult{peerID: q.peerID, err: q.closedError()})
				return
			}
			result := peerWorkResult{peerID: q.peerID}
			switch work.kind {
			case peerWorkWrite:
				result.eligible, result.err = q.ops.ship(q.ctx, work.write)
			case peerWorkBarrier:
				result.ackLSN, result.err = q.ops.barrier(q.ctx, work.targetLSN)
			default:
				result.err = fmt.Errorf("replication: peer %s unknown queue work kind %d", q.peerID, work.kind)
			}
			q.complete(work, result)
		}
	}
}

func (q *peerWorkQueue) failPending() {
	err := q.closedError()
	for {
		select {
		case work := <-q.jobs:
			q.complete(work, peerWorkResult{peerID: q.peerID, err: err})
		default:
			return
		}
	}
}

func (q *peerWorkQueue) complete(work peerWork, result peerWorkResult) {
	if work.result != nil {
		work.result <- result
	}
}

func (q *peerWorkQueue) closedError() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closedErrorLocked()
}

func (q *peerWorkQueue) closedErrorLocked() error {
	if q.terminalErr != nil {
		return q.terminalErr
	}
	return fmt.Errorf("%w: peer %s", ErrPeerQueueClosed, q.peerID)
}
