package layout

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The start rules, carried from the store-independent tests of the same
// rule on the current store: a holder's local data is identified by the
// claim id its local root's marker carries, together with the epoch.

func startHead(epoch uint64, holder, claim string) Head {
	return Head{Gen: 7, VolumeID: idFor("v1"), Epoch: epoch, Holder: holder, ClaimID: claim, Activity: Activity{Kind: ActIdle}}
}

func start(t *testing.T, h Head, line HolderID, local LocalRoot) (*memHead, Head, error) {
	t.Helper()
	st := &memHead{h: &h}
	got, err := Start(context.Background(), st, &fakeLease{ended: true}, line, local)
	return st, got, err
}

// A fresh local root at the holder's own epoch cannot prove it holds the
// acknowledged suffix: refused before any claim, with the likely cause and
// the way back, HEAD unchanged; a retry on the same (still fresh) root is
// refused too.
func TestStart_OwnEpochFreshRootRefusedBeforeClaim(t *testing.T) {
	h := startHead(3, "s1/r1", "k-old")
	line := HolderID{Epoch: 3, Holder: "s1/r1"}
	fresh := LocalRoot{ClaimID: "k-new", Fresh: true}
	st, _, err := start(t, h, line, fresh)
	if !errors.Is(err, ErrMissingLocalRoot) {
		t.Fatalf("own-epoch fresh root: %v; want ErrMissingLocalRoot", err)
	}
	for _, words := range []string{"wrong or unmounted data directory", "forced move (also to the same host)"} {
		if !strings.Contains(err.Error(), words) {
			t.Fatalf("refusal %q lacks %q", err, words)
		}
	}
	if st.updates != 0 || st.h.ClaimID != "k-old" || st.h.Epoch != 3 {
		t.Fatalf("refused start changed HEAD: %+v (%d writes)", st.h, st.updates)
	}
	if _, err := Start(context.Background(), st, &fakeLease{ended: true}, line, fresh); !errors.Is(err, ErrMissingLocalRoot) || st.updates != 0 {
		t.Fatalf("retry on the same fresh root: %v (%d writes)", err, st.updates)
	}
}

// The same root restarting at its own epoch continues: its marker is the
// claim id HEAD holds. No claim is written.
func TestStart_OwnEpochSameRootContinues(t *testing.T) {
	st, got, err := start(t, startHead(3, "s1/r1", "k3"), HolderID{Epoch: 3, Holder: "s1/r1"}, LocalRoot{ClaimID: "k3"})
	if err != nil || st.updates != 0 || got.ClaimID != "k3" {
		t.Fatalf("same-root restart: %+v %v (%d writes); want continue without a write", got, err, st.updates)
	}
}

// A fresh root whose claim was written but whose reply was lost restarts
// with the same marker: HEAD holds that claim id at this epoch, so it
// continues.
func TestStart_LostClaimReplyContinues(t *testing.T) {
	st, got, err := start(t, startHead(5, "s1/r1", "k5"), HolderID{Epoch: 5, Holder: "s1/r1"}, LocalRoot{ClaimID: "k5", Fresh: true})
	if err != nil || st.updates != 0 || got.Epoch != 5 {
		t.Fatalf("restart after a lost claim reply: %+v %v (%d writes)", got, err, st.updates)
	}
}

// After a same-host forced move the successor (epoch 4, its own claim id)
// holds HEAD; the old root (claim id of epoch 3, with a long unpublished
// suffix) remounted under epoch 4 is refused as stale and fenced; HEAD and
// its image are unchanged.
func TestStart_SameHostForcedSuccessorRefusesTheRemountedOldRoot(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("successor image"))}
	h := startHead(4, "s1/r1", "k4")
	h.Current = &rn
	st, _, err := start(t, h, HolderID{Epoch: 4, Holder: "s1/r1"}, LocalRoot{ClaimID: "k3"})
	if !errors.Is(err, ErrStaleLocalRoot) || !errors.Is(err, ErrNotHolder) {
		t.Fatalf("remounted old root: %v; want ErrStaleLocalRoot and fenced (ErrNotHolder)", err)
	}
	if st.updates != 0 || st.h.ClaimID != "k4" || *st.h.Current != rn {
		t.Fatalf("refused start changed HEAD: %+v", st.h)
	}
}

// A fresh root at a higher issued epoch, on the same server or another,
// claims with its marker's claim id.
func TestStart_FreshRootAtHigherEpochClaims(t *testing.T) {
	for _, server := range []string{"s1/r1", "s2/r1"} {
		st, got, err := start(t, startHead(3, "s1/r1", "k3"), HolderID{Epoch: 4, Holder: server}, LocalRoot{ClaimID: "k4", Fresh: true})
		if err != nil || st.updates != 1 || got.Epoch != 4 || got.Holder != server || got.ClaimID != "k4" {
			t.Fatalf("fresh root at epoch 4 on %s: %+v %v (%d writes)", server, got, err, st.updates)
		}
	}
}

// An older line is fenced; the same epoch held by another server is not
// this holder's; an absent HEAD is refused (nothing is created).
func TestStart_Refusals(t *testing.T) {
	if _, _, err := start(t, startHead(4, "s1/r1", "k4"), HolderID{Epoch: 3, Holder: "s1/r1"}, LocalRoot{ClaimID: "k3"}); !errors.Is(err, ErrStaleEpoch) {
		t.Errorf("older line: %v; want ErrStaleEpoch", err)
	}
	if _, _, err := start(t, startHead(4, "s1/r1", "k4"), HolderID{Epoch: 4, Holder: "s2/r1"}, LocalRoot{ClaimID: "k9", Fresh: true}); !errors.Is(err, ErrNotHolder) {
		t.Errorf("same epoch, another server: %v; want ErrNotHolder", err)
	}
	st := &memHead{}
	if _, err := Start(context.Background(), st, &fakeLease{ended: true}, HolderID{Epoch: 1, Holder: "s1/r1"}, LocalRoot{ClaimID: "k1", Fresh: true}); !errors.Is(err, ErrHeadAbsent) || st.updates != 0 {
		t.Errorf("absent HEAD: %v", err)
	}
	if _, _, err := start(t, startHead(3, "s1/r1", "k3"), HolderID{Epoch: 4, Holder: "s1/r1"}, LocalRoot{}); err == nil {
		t.Error("start without a local claim id accepted")
	}
}
