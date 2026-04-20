package authority

import (
	"context"
	"sync"
)

// assignmentQueue is the bounded "latest desired state per volume"
// publication queue. It is intentionally not a history-preserving
// log: the convergence package cares about the latest desired
// authority move per volume, not every intermediate planning thought.
type assignmentQueue struct {
	mu    sync.Mutex
	items map[string]AssignmentAsk
	order []string
	wake  chan struct{}
}

func newAssignmentQueue() *assignmentQueue {
	return &assignmentQueue{
		items: make(map[string]AssignmentAsk),
		wake:  make(chan struct{}, 1),
	}
}

func (q *assignmentQueue) enqueue(ask AssignmentAsk) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, exists := q.items[ask.VolumeID]; !exists {
		q.order = append(q.order, ask.VolumeID)
	}
	q.items[ask.VolumeID] = ask
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *assignmentQueue) discard(volumeID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.items, volumeID)
}

func (q *assignmentQueue) next(ctx context.Context) (AssignmentAsk, error) {
	for {
		q.mu.Lock()
		if len(q.order) > 0 {
			vid := q.order[0]
			q.order = q.order[1:]
			ask, ok := q.items[vid]
			if !ok {
				q.mu.Unlock()
				continue
			}
			delete(q.items, vid)
			q.mu.Unlock()
			return ask, nil
		}
		q.mu.Unlock()
		select {
		case <-q.wake:
		case <-ctx.Done():
			return AssignmentAsk{}, ctx.Err()
		}
	}
}
