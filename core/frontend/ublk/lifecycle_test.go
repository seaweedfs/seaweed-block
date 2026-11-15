package ublk

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A device already present at our id is never reclaimed automatically:
// Start refuses with an error naming the id (and refusePreExisting has no
// way to send a command). An absent id is accepted.
func TestRefusePreExisting_RefusesAnyExistingID(t *testing.T) {
	if err := refusePreExisting(7, false); err != nil {
		t.Fatalf("absent id refused: %v", err)
	}
	err := refusePreExisting(7, true)
	if err == nil || !strings.Contains(err.Error(), "device 7") || !strings.Contains(err.Error(), "/dev/ublkc7") {
		t.Fatalf("err %v, want a refusal naming device 7", err)
	}
}

type fakeQueue struct {
	id       int
	done     chan struct{}
	released bool
	err      error
}

func (q *fakeQueue) doneCh() <-chan struct{} { return q.done }
func (q *fakeQueue) release()                { q.released = true }
func (q *fakeQueue) name() string            { return fmt.Sprintf("queue %d", q.id) }
func (q *fakeQueue) result() error           { return q.err }

// A queue that did not stop keeps its mappings: its thread may still read
// descriptors or write buffers, so they are leaked, never unmapped, and
// the shutdown reports the failure. Stopped queues are released.
func TestSettleQueues_NeverReleasesAQueueThatMayStillRun(t *testing.T) {
	stopped := &fakeQueue{id: 0, done: make(chan struct{})}
	close(stopped.done)
	stuck := &fakeQueue{id: 1, done: make(chan struct{})}
	late := &fakeQueue{id: 2, done: make(chan struct{})}
	close(late.done)
	err := settleQueues([]settler{stopped, stuck, late}, 50*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "queue 1") {
		t.Fatalf("err %v, want queue 1 reported", err)
	}
	if stuck.released {
		t.Fatal("a queue that may still run had its mappings released")
	}
	if !stopped.released || !late.released {
		t.Fatalf("stopped queues not released: %v %v", stopped.released, late.released)
	}
}

// A queue thread that ended abnormally (its ring failed, or closing the
// ring failed) reports that error through the shutdown.
func TestSettleQueues_SurfacesAQueueThreadError(t *testing.T) {
	q := &fakeQueue{id: 3, done: make(chan struct{}), err: errors.New("io_uring_enter: EBADF")}
	close(q.done)
	err := settleQueues([]settler{q}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "EBADF") {
		t.Fatalf("err %v, want the queue thread's error surfaced", err)
	}
	if !q.released {
		t.Fatal("a stopped queue was not released")
	}
}
