//go:build linux

package ublk

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"
)

type cmdRecorder struct{ cmds []uint32 }

func (c *cmdRecorder) command(cmdOp uint32, devID uint32, buf []byte, data uint64) (int32, error) {
	c.cmds = append(c.cmds, cmdOp)
	return 0, nil
}

func (c *cmdRecorder) deleted() bool {
	for _, op := range c.cmds {
		if op == uCmdDelDev {
			return true
		}
	}
	return false
}

// The Close path deletes its device when every queue thread has exited,
// even if one of them ended with an error (the error is reported); only a
// queue whose thread is still running keeps its mappings and suppresses
// DEL_DEV.
func TestTeardown_DeletesTheDeviceUnlessAQueueIsStillRunning(t *testing.T) {
	stoppedWithErr := &fakeQueue{id: 0, done: make(chan struct{}), err: errors.New("io_uring_enter: EBADF")}
	close(stoppedWithErr.done)
	d := &Device{devID: 4, path: "/dev/ublkb4", ctrlFD: -1, charFD: -1}
	ctl := &cmdRecorder{}
	err := d.teardownWith([]settler{stoppedWithErr}, ctl, true)
	if err == nil || !strings.Contains(err.Error(), "EBADF") {
		t.Fatalf("teardown err %v, want the queue error reported", err)
	}
	if !ctl.deleted() {
		t.Fatalf("a device whose queues all stopped was not deleted (commands %v)", ctl.cmds)
	}

	running := &fakeQueue{id: 1, done: make(chan struct{})}
	d2 := &Device{devID: 5, path: "/dev/ublkb5", ctrlFD: -1, charFD: -1}
	ctl2 := &cmdRecorder{}
	done := make(chan error, 1)
	go func() { done <- d2.teardownWith([]settler{running}, ctl2, true) }()
	select {
	case err := <-done:
		if err == nil || ctl2.deleted() || running.released {
			t.Fatalf("running queue: err %v deleted %v released %v; want an error, no DEL_DEV, mappings kept", err, ctl2.deleted(), running.released)
		}
	case <-time.After(queueStopTimeout + 5*time.Second):
		t.Fatal("teardown did not return")
	}
}

// A completion that is neither a new request nor the expected abort ends
// the tag and is reported as the queue's error, never dropped silently.
func TestQueue_UnexpectedCompletionIsReported(t *testing.T) {
	q := &queue{id: 0, depth: 1, ioBytes: 4096, descs: make([]byte, ioDescSize), bufs: make([]byte, 4096), h: handler{backend: &failingBackend{}, size: testSize}}
	commits := 0
	commit := func(int, int32) error { commits++; return nil }

	still, err := q.onCQE(context.Background(), cqe{UserData: 0, Res: ioResAbort}, commit)
	if still || err != nil || q.err != nil || commits != 0 {
		t.Fatalf("abort: still=%v err=%v q.err=%v commits=%d, want the tag ended quietly", still, err, q.err, commits)
	}
	still, err = q.onCQE(context.Background(), cqe{UserData: 0, Res: -int32(syscall.EIO)}, commit)
	if still || commits != 0 {
		t.Fatalf("unexpected result: still=%v commits=%d, want the tag ended", still, commits)
	}
	if q.err == nil || !strings.Contains(q.err.Error(), "-5") {
		t.Fatalf("unexpected completion -EIO not reported (q.err %v, err %v)", q.err, err)
	}
}
