package volume

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// assignedEpochs records the assignments the host applied to its adapter.
type assignedEpochs struct {
	mu     sync.Mutex
	epochs []uint64
}

func (a *assignedEpochs) OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.epochs = append(a.epochs, info.Epoch)
	return adapter.ApplyLog{}
}

func (a *assignedEpochs) last() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.epochs) == 0 {
		return 0
	}
	return a.epochs[len(a.epochs)-1]
}

func supportingEpoch(h *Host) uint64 {
	h.wd.mu.Lock()
	defer h.wd.mu.Unlock()
	return h.wd.supportingEpoch
}

// A supporting fact whose replica role call failed is completed when the
// retry succeeds, with no new fact delivered: the adapter takes the line's
// identity and the supporting admission is recorded. A newer fact delivered
// while the role call still fails supersedes the pending one.
func TestRoleRetryCompletesTheSkippedSupportingAssignment(t *testing.T) {
	min, max := roleRetryMin, roleRetryMax
	roleRetryMin, roleRetryMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { roleRetryMin, roleRetryMax = min, max })

	for name, c := range map[string]struct {
		facts []uint64 // supporting facts delivered while the role call fails
		want  uint64
	}{
		"one fact, then the retry succeeds":       {[]uint64{2}, 2},
		"a newer fact supersedes the pending one": {[]uint64{2, 3}, 3},
	} {
		t.Run(name, func(t *testing.T) {
			h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "sB", VolumeID: "vol1", ReplicaID: "rB", DataAddr: "127.0.0.1:5", CtrlAddr: "127.0.0.1:6"})
			if err != nil {
				t.Fatal(err)
			}
			adpt := &assignedEpochs{}
			h.adpt = adpt
			ln := &flakyListener{fails: len(c.facts), err: errors.New("replica floor record write: permission denied")}
			if err := h.SetSelfFence(ln); err != nil {
				t.Fatal(err)
			}
			for _, e := range c.facts {
				h.applyFact(supportingFact(e))
			}
			if got := adpt.last(); got != 0 {
				t.Fatalf("the adapter took epoch %d while the role call failed", got)
			}
			if failed, _ := h.RoleAdmissionFailure(); !failed {
				t.Fatal("the failed role call was not latched")
			}

			ctx, cancel := context.WithCancel(context.Background())
			h.wg.Add(1)
			go h.runRoleRetry(ctx)
			defer func() { cancel(); h.wg.Wait() }()

			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && supportingEpoch(h) != c.want {
				time.Sleep(5 * time.Millisecond)
			}
			if got := supportingEpoch(h); got != c.want {
				t.Fatalf("supporting admission at epoch %d after the retry; want %d (no new fact delivered)", got, c.want)
			}
			if got := adpt.last(); got != c.want {
				t.Fatalf("adapter epoch %d after the retry; want %d", got, c.want)
			}
			if failed, reason := h.RoleAdmissionFailure(); failed {
				t.Fatalf("latch still set: %s", reason)
			}
		})
	}
}
