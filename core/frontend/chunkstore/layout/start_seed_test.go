package layout

import (
	"context"
	"errors"
	"testing"
)

// A fresh local root seeds its frontier at HEAD's position once it starts,
// so Start refuses a position its store cannot take BEFORE any claim: a
// refusal writes nothing. The rule comes from the local store
// (LocalRoot.Seedable); here a store that takes positions below 5. (A
// position of MaxUint64 cannot be written at all: the Encode test below.)

var seedGeometry = Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}

func seedsBelow5(position uint64) error {
	if position >= 5 {
		return errors.New("the store cannot seed this position")
	}
	return nil
}

func TestStart_FreshRootRefusesAnUnseedablePositionBeforeClaim(t *testing.T) {
	unclaimed := Unclaimed(idFor("v1"), nil)
	unclaimed.Gen = 1
	for name, c := range map[string]struct {
		h    Head
		line HolderID
	}{
		"higher epoch": {startHead(3, "s1/r1", "k-old"), HolderID{Epoch: 4, Holder: "s2/r1"}},
		"unclaimed":    {unclaimed, HolderID{Epoch: 1, Holder: "s1/r1"}},
	} {
		t.Run(name, func(t *testing.T) {
			local := LocalRoot{Geometry: seedGeometry, ClaimID: "k-new", Fresh: true, Seedable: seedsBelow5}
			// control: a position the store can take is claimed
			ok := c.h
			ok.Position = 3
			st, _, err := start(t, ok, c.line, local)
			if err != nil || st.updates == 0 {
				t.Fatalf("control, position 3: %v (%d writes); want a claim", err, st.updates)
			}
			bad := c.h
			bad.Position = 9
			before := bad
			st, _, err = start(t, bad, c.line, local)
			if !errors.Is(err, ErrUnseedablePosition) {
				t.Fatalf("HEAD position 9: %v; want ErrUnseedablePosition", err)
			}
			if st.updates != 0 || st.h.Gen != before.Gen || st.h.Epoch != before.Epoch || st.h.ClaimID != before.ClaimID {
				t.Fatalf("a refused start wrote HEAD: %+v (%d writes)", st.h, st.updates)
			}
		})
	}
}

// The check is never skipped: a fresh root that does not say which
// positions it can seed is refused, HEAD unchanged.
func TestStart_FreshRootWithoutASeedRuleIsRefused(t *testing.T) {
	st, _, err := start(t, startHead(3, "s1/r1", "k-old"), HolderID{Epoch: 4, Holder: "s2/r1"}, LocalRoot{Geometry: seedGeometry, ClaimID: "k-new", Fresh: true})
	if !errors.Is(err, ErrUnseedablePosition) || st.updates != 0 {
		t.Fatalf("fresh root without Seedable: %v (%d writes); want ErrUnseedablePosition, no write", err, st.updates)
	}
}

// The claim re-checks against the very HEAD it updates: a position that
// turned unseedable between Start's read and the claim is refused by the
// claim's own condition, HEAD unchanged.
func TestStart_TheClaimRechecksTheSeedRule(t *testing.T) {
	h := startHead(3, "s1/r1", "k-old")
	h.Position = 9
	st := &memHead{h: &h}
	calls := 0
	local := LocalRoot{Geometry: seedGeometry, ClaimID: "k-new", Fresh: true, Seedable: func(p uint64) error {
		calls++
		if calls > 1 {
			return errors.New("the store changed")
		}
		return nil
	}}
	_, err := Start(context.Background(), st, &fakeLease{ended: true}, HolderID{Epoch: 4, Holder: "s2/r1"}, local)
	if !errors.Is(err, ErrUnseedablePosition) || st.updates != 0 || calls < 2 {
		t.Fatalf("claim re-check: %v (%d writes, %d checks); want the claim's own check to refuse, no write", err, st.updates, calls)
	}
}

// No commit produces a position with no room for a next LSN; a HEAD already
// at that position (an older writer) keeps it and is not re-checked.
func TestCommit_RefusesToProduceAPositionWithNoNextLSN(t *testing.T) {
	me := HolderID{Epoch: 3, Holder: "s1/r1", ClaimID: "k3"}
	h := startHead(3, "s1/r1", "k3")
	h.State, h.Activity, h.Position = StateLive, Activity{Kind: ActSnap, Session: "s1"}, 9
	root := RootName{Hash: Hash{1}}
	if _, _, err := h.Commit(me, "s1", root, 10, "", SnapshotMeta{}, StateLive); err != nil {
		t.Fatalf("control, commit at 10: %v", err)
	}
	if _, _, err := h.Commit(me, "s1", root, ^uint64(0), "", SnapshotMeta{}, StateLive); !errors.Is(err, ErrNoNextPosition) {
		t.Fatalf("commit at MaxUint64: %v; want ErrNoNextPosition", err)
	}
	h.Position = ^uint64(0)
	if _, _, err := h.Commit(me, "s1", root, ^uint64(0), "", SnapshotMeta{}, StateDetached); err != nil {
		t.Fatalf("a HEAD already at MaxUint64 stops there (detach): %v", err)
	}
}
