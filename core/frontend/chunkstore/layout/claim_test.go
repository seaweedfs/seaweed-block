package layout

import (
	"context"
	"errors"
	"testing"
)

var newer = HolderID{Epoch: 4, Holder: "srv-b"}

// fakeLease records its calls; ended says whether old leases are over.
type fakeLease struct {
	ended bool
	calls []uint64
	check func() // runs inside the call
}

func (l *fakeLease) OldLeaseEnded(_ context.Context, epoch uint64) error {
	l.calls = append(l.calls, epoch)
	if l.check != nil {
		l.check()
	}
	if !l.ended {
		return errors.New("the previous holder's lease has not ended")
	}
	return nil
}

func TestClaim_Closure(t *testing.T) {
	for name, h := range map[string]Head{
		"idle":     idleHead(),
		"snap(s)":  snapHead("s1"),
		"clean(c)": {Gen: 7, Epoch: 3, Holder: "srv-a", Activity: Activity{Kind: ActClean, Session: "c1"}},
	} {
		next, changed, err := h.Claim(newer)
		if err != nil || !changed || next.Epoch != 4 || next.Holder != "srv-b" || next.Activity != (Activity{Kind: ActIdle}) {
			t.Fatalf("Claim over %s: %+v %t %v; want epoch 4, srv-b, idle", name, next, changed, err)
		}
		if (next.Current == nil) != (h.Current == nil) || next.Position != h.Position || len(next.Snapshots) != len(h.Snapshots) {
			t.Fatalf("Claim over %s changed the image or snapshots: %+v", name, next)
		}
		if h.Epoch != 3 {
			t.Fatal("Claim changed its receiver")
		}
		if again, changed, err := next.Claim(newer); err != nil || changed || again.Epoch != 4 {
			t.Fatalf("Claim of a HEAD I hold at that epoch: %+v %t %v; want a replay", again, changed, err)
		}
	}
	h := idleHead()
	for name, tc := range map[string]struct {
		me   HolderID
		want error
	}{
		"older epoch":                {HolderID{Epoch: 2, Holder: "srv-b"}, ErrStaleEpoch},
		"same epoch, another server": {HolderID{Epoch: 3, Holder: "srv-b"}, ErrNotHolder},
	} {
		if _, _, err := h.Claim(tc.me); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v; want %v", name, err, tc.want)
		}
	}
	if _, _, err := h.Claim(HolderID{Epoch: 5}); err == nil {
		t.Error("Claim without a holder accepted")
	}
}

// The lease wait runs after HEAD is read and before it is updated, on
// every claim that changes HEAD; a wait not yet over refuses and writes
// nothing.
func TestRunClaim_WaitsForTheOldLease(t *testing.T) {
	ctx := context.Background()
	h := snapHead("s1")
	st := &memHead{h: &h}
	lease := &fakeLease{}
	if _, err := RunClaim(ctx, st, lease, newer); err == nil {
		t.Fatal("claim before the old lease ended succeeded")
	}
	if st.updates != 0 || st.h.Epoch != 3 || len(lease.calls) != 1 || lease.calls[0] != 4 {
		t.Fatalf("refused claim: updates %d, HEAD epoch %d, lease calls %v", st.updates, st.h.Epoch, lease.calls)
	}
	lease.ended = true
	lease.check = func() {
		if st.updates != 0 {
			t.Error("the lease wait ran after the HEAD update")
		}
	}
	got, err := RunClaim(ctx, st, lease, newer)
	if err != nil || got.Epoch != 4 || st.h.Holder != "srv-b" || st.h.Activity.Kind != ActIdle {
		t.Fatalf("claim: %+v %v", got, err)
	}
	if _, err := RunClaim(ctx, st, nil, newer); err == nil {
		t.Fatal("claim without a lease wait accepted")
	}
	if _, err := RunClaim(ctx, &memHead{}, lease, newer); !errors.Is(err, ErrHeadAbsent) {
		t.Fatalf("claim of an absent HEAD: %v; want ErrHeadAbsent, nothing created", err)
	}
}

// After a claim the old holder's open session is void: its commit and its
// abort are refused, and a new Begin needs the new epoch.
func TestRunClaim_VoidsTheOldSession(t *testing.T) {
	ctx := context.Background()
	f, h, req := commitFixture(t)
	st := &memHead{h: &h}
	if _, err := RunClaim(ctx, st, &fakeLease{ended: true}, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := runCommit(st, f, req); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("the old holder's commit after a claim: %v; want ErrNotHolder", err)
	}
	if _, err := RunAbort(ctx, st, me, "s1"); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("the old holder's abort after a claim: %v; want ErrNotHolder", err)
	}
	if _, err := RunBegin(ctx, st, me, "s9"); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("the old holder's begin after a claim: %v; want ErrNotHolder", err)
	}
	if got, err := RunBegin(ctx, st, newer, "t1"); err != nil || got.Activity.Session != "t1" {
		t.Fatalf("the new holder's begin: %+v %v", got, err)
	}
}

func TestRunClaim_InterruptAtEveryStep(t *testing.T) {
	interruptHead(t, snapHead("s1"), func(st HeadStore) error {
		_, err := RunClaim(context.Background(), st, &fakeLease{ended: true}, newer)
		return err
	}, func(t *testing.T, h Head) {
		if h.Epoch != 4 || h.Holder != "srv-b" || h.Activity != (Activity{Kind: ActIdle}) || h.Position != 40 {
			t.Fatalf("HEAD after Claim: %+v", h)
		}
	})
}
