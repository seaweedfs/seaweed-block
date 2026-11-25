package volume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// orderLog is one sequence of the host's role calls and adapter assignments.
type orderLog struct {
	mu  sync.Mutex
	seq []string
}

func (o *orderLog) add(s string) { o.mu.Lock(); o.seq = append(o.seq, s); o.mu.Unlock() }
func (o *orderLog) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.seq...)
}
func (o *orderLog) index(s string) int {
	for i, e := range o.snapshot() {
		if e == s {
			return i
		}
	}
	return -1
}

// orderListener fails its first replica role call, then records every role
// call it admits.
type orderListener struct {
	log   *orderLog
	mu    sync.Mutex
	fails int
}

func (l *orderListener) FenceAsPrimary(e uint64) error {
	l.log.add(fmt.Sprintf("primary@%d", e))
	return nil
}
func (l *orderListener) AdmitAsReplica(e uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fails > 0 {
		l.fails--
		return errors.New("replica floor record write: permission denied")
	}
	l.log.add(fmt.Sprintf("replica@%d", e))
	return nil
}

// heldAdapter records each assignment when it returns; the one at holdAt
// waits in OnAssignment until release is closed.
type heldAdapter struct {
	log              *orderLog
	holdAt           uint64
	entered, release chan struct{}
}

func (a *heldAdapter) OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog {
	if info.Epoch == a.holdAt {
		close(a.entered)
		<-a.release
	}
	a.log.add(fmt.Sprintf("assign@%d", info.Epoch))
	return adapter.ApplyLog{}
}

// The retry's completion of a skipped supporting assignment at epoch 2 is
// held in its first step while a newer fact at epoch 3 arrives: this host's
// own primary line, or a line that excludes it. The completion must finish
// before the newer transition takes its role, so no older completion lands
// after it: the adapter does not go back to epoch 2, the primary watchdog
// keeps epoch 3, and no supporting admission or marker for 2 follows.
func TestRoleRetryCompletionNeverLandsAfterANewerTransition(t *testing.T) {
	min, max := roleRetryMin, roleRetryMax
	roleRetryMin, roleRetryMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { roleRetryMin, roleRetryMax = min, max })

	selfPrimary := func() *control.AssignmentFact {
		return &control.AssignmentFact{ServerId: "sB", VolumeId: "vol1", ReplicaId: "rB", Epoch: 3, EndpointVersion: 1, DataAddr: "127.0.0.1:5", CtrlAddr: "127.0.0.1:6", PeerSetGeneration: 3}
	}
	excluding := func() *control.AssignmentFact {
		return &control.AssignmentFact{VolumeId: "vol1", ReplicaId: "rA", Epoch: 3, EndpointVersion: 1, DataAddr: "127.0.0.1:3", CtrlAddr: "127.0.0.1:4", PeerSetGeneration: 3,
			Peers: []*control.ReplicaDescriptor{{ReplicaId: "rC", Epoch: 3, EndpointVersion: 1, DataAddr: "127.0.0.1:7", CtrlAddr: "127.0.0.1:8"}}}
	}
	for name, newer := range map[string]func() *control.AssignmentFact{"self-primary": selfPrimary, "excluding": excluding} {
		t.Run(name, func(t *testing.T) {
			markers := make(chan adapter.AssignmentInfo, 8)
			h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "sB", VolumeID: "vol1", ReplicaID: "rB", DataAddr: "127.0.0.1:5", CtrlAddr: "127.0.0.1:6", ReadyMarker: markers})
			if err != nil {
				t.Fatal(err)
			}
			log := &orderLog{}
			adpt := &heldAdapter{log: log, holdAt: 2, entered: make(chan struct{}), release: make(chan struct{})}
			h.adpt = adpt
			if err := h.SetSelfFence(&orderListener{log: log, fails: 1}); err != nil {
				t.Fatal(err)
			}
			h.applyFact(supportingFact(2)) // its role call fails: the assignment waits for the retry

			ctx, cancel := context.WithCancel(context.Background())
			h.wg.Add(1)
			go h.runRoleRetry(ctx)
			defer func() { cancel(); h.wg.Wait() }()

			select {
			case <-adpt.entered: // the retry's role call succeeded; its completion is held
			case <-time.After(2 * time.Second):
				t.Fatal("the retry never completed the skipped assignment")
			}
			done := make(chan struct{})
			go func() { h.applyFact(newer()); close(done) }()
			time.Sleep(100 * time.Millisecond) // the newer fact runs as far as it can
			close(adpt.release)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("the newer fact never finished")
			}

			deadline := time.Now().Add(2 * time.Second)
			for log.index("assign@2") < 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			seq := log.snapshot()
			old, newerRole := log.index("assign@2"), log.index("primary@3")
			if old < 0 || newerRole < 0 {
				t.Fatalf("sequence %v; want the completion at 2 and the newer role call at 3", seq)
			}
			if old > newerRole {
				t.Fatalf("sequence %v: the completion for epoch 2 landed after the newer transition at 3", seq)
			}
			if name == "self-primary" {
				if last := seq[len(seq)-1]; last != "assign@3" {
					t.Fatalf("sequence %v: the adapter ended at %s; want assign@3", seq, last)
				}
				h.wd.mu.Lock()
				pe := h.wd.primaryEpoch
				h.wd.mu.Unlock()
				if pe != 3 {
					t.Fatalf("primary watchdog epoch %d after the primary line at 3; want 3 (an old supporting admission cleared it)", pe)
				}
			}
			close(markers)
			var order []uint64
			for m := range markers {
				order = append(order, m.Epoch)
			}
			for i := 1; i < len(order); i++ {
				if order[i] < order[i-1] {
					t.Fatalf("ready markers %v: an old marker followed a newer one", order)
				}
			}
		})
	}
}
