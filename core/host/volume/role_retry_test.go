package volume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// flakyListener fails its replica role call with err until fails is used up.
type flakyListener struct {
	mu    sync.Mutex
	fails int
	err   error
	calls int
}

func (f *flakyListener) FenceAsPrimary(uint64) error { return nil }
func (f *flakyListener) AdmitAsReplica(uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fails > 0 {
		f.fails--
		return f.err
	}
	return nil
}

func (f *flakyListener) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// A replica role call that failed (its floor could not be persisted) is
// retried until it succeeds, which clears the latch; a call refused for good
// (ErrRoleRefused) is not retried and keeps its reason.
func TestReplicaRoleCallIsRetriedUntilItSucceeds(t *testing.T) {
	min, max := roleRetryMin, roleRetryMax
	roleRetryMin, roleRetryMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { roleRetryMin, roleRetryMax = min, max })

	for name, c := range map[string]struct {
		err       error
		recovered bool
	}{
		"floor not persisted, then the disk recovers": {errors.New("replica floor record write: permission denied"), true},
		"refused for good":                            {fmt.Errorf("replication ingress closed: %w", ErrRoleRefused), false},
	} {
		t.Run(name, func(t *testing.T) {
			h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "s3", VolumeID: "v1", ReplicaID: "r3", DataAddr: "127.0.0.1:19403", CtrlAddr: "127.0.0.1:19404"})
			if err != nil {
				t.Fatal(err)
			}
			ln := &flakyListener{fails: 2, err: c.err}
			if err := h.SetSelfFence(ln); err != nil {
				t.Fatal(err)
			}
			if err := h.fenceSelfWithPeers(selfRoleReplica, 2, true); err == nil {
				t.Fatal("the first role call did not fail")
			}
			ctx, cancel := context.WithCancel(context.Background())
			h.wg.Add(1)
			go h.runRoleRetry(ctx)
			defer func() { cancel(); h.wg.Wait() }()

			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if failed, _ := h.RoleAdmissionFailure(); !failed {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			failed, reason := h.RoleAdmissionFailure()
			if c.recovered {
				if failed || ln.count() != 3 {
					t.Fatalf("latch %t (%s) after %d calls; want cleared on the 3rd", failed, reason, ln.count())
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
			if !failed || ln.count() != 1 {
				t.Fatalf("a refused role call: latch %t, %d calls; want latched and never retried", failed, ln.count())
			}
		})
	}
}
