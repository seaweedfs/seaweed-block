package ublk

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// recordingController records control commands; GET_DEV_INFO fills the
// device info with srvPID (or fails with infoErr).
type recordingController struct {
	cmds    []uint32
	srvPID  int32
	infoErr error
}

func (c *recordingController) command(cmdOp uint32, devID uint32, buf []byte, data uint64) (int32, error) {
	c.cmds = append(c.cmds, cmdOp)
	if cmdOp == uCmdGetDevInfo {
		if c.infoErr != nil {
			return 0, c.infoErr
		}
		if len(buf) >= int(unsafe.Sizeof(devInfo{})) {
			info := (*devInfo)(unsafe.Pointer(&buf[0]))
			info.DevID, info.SrvPID = devID, c.srvPID
		}
	}
	return 0, nil
}

func (c *recordingController) destructive() []string {
	var out []string
	for _, op := range c.cmds {
		switch op {
		case uCmdStopDev:
			out = append(out, "STOP")
		case uCmdDelDev:
			out = append(out, "DEL")
		}
	}
	return out
}

// A device found at our id is deleted only with proof that its server is
// dead; when its owner may be alive, or cannot be established, Start
// refuses and touches nothing.
func TestReclaimStale_DeletesOnlyWithProofTheOwnerIsDead(t *testing.T) {
	dead := func(int32) (bool, error) { return false, nil }
	live := func(int32) (bool, error) { return true, nil }
	unknown := func(int32) (bool, error) { return false, errors.New("cannot signal") }
	for _, tc := range []struct {
		name     string
		exists   bool
		c        *recordingController
		alive    func(int32) (bool, error)
		refuse   bool
		wantDest string
	}{
		{"no device", false, &recordingController{srvPID: 42}, dead, false, ""},
		{"owner process gone", true, &recordingController{srvPID: 42}, dead, false, "STOP,DEL"},
		{"owner alive", true, &recordingController{srvPID: 42}, live, true, ""},
		{"owner liveness unknown", true, &recordingController{srvPID: 42}, unknown, true, ""},
		{"never started (no owner pid)", true, &recordingController{srvPID: 0}, dead, true, ""},
		{"device info unreadable", true, &recordingController{infoErr: errors.New("EPERM")}, dead, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := reclaimStale(tc.c, 7, tc.exists, tc.alive)
			if tc.refuse != (err != nil) {
				t.Fatalf("refused=%v (err %v), want refused=%v", err != nil, err, tc.refuse)
			}
			if got := strings.Join(tc.c.destructive(), ","); got != tc.wantDest {
				t.Fatalf("destructive commands %q, want %q", got, tc.wantDest)
			}
		})
	}
}

type fakeQueue struct {
	id       int
	done     chan struct{}
	released bool
}

func (q *fakeQueue) doneCh() <-chan struct{} { return q.done }
func (q *fakeQueue) release()                { q.released = true }
func (q *fakeQueue) name() string            { return fmt.Sprintf("queue %d", q.id) }

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
