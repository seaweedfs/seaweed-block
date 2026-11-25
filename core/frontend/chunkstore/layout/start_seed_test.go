package layout

import (
	"context"
	"errors"
	"testing"
)

// A fresh local root seeds its frontier at HEAD's position once it starts,
// so Start refuses a position its store cannot take BEFORE any claim: a
// refusal writes nothing. The rule comes from the local store
// (LocalRoot.Seedable); here a store that has no room past MaxUint64.

var seedGeometry = Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}

func noRoomPastMax(position uint64) error {
	if position == ^uint64(0) {
		return errors.New("no room for a next LSN")
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
			local := LocalRoot{Geometry: seedGeometry, ClaimID: "k-new", Fresh: true, Seedable: noRoomPastMax}
			// control: a position the store can take is claimed
			ok := c.h
			ok.Position = 9
			st, _, err := start(t, ok, c.line, local)
			if err != nil || st.updates == 0 {
				t.Fatalf("control, position 9: %v (%d writes); want a claim", err, st.updates)
			}
			bad := c.h
			bad.Position = ^uint64(0)
			before := bad
			st, _, err = start(t, bad, c.line, local)
			if !errors.Is(err, ErrUnseedablePosition) {
				t.Fatalf("HEAD position MaxUint64: %v; want ErrUnseedablePosition", err)
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

// The kernel never writes a position with no room for a next LSN; a HEAD
// that carries one (an older or broken writer) is still read.
func TestHead_EncodeRefusesAPositionWithNoNextLSN(t *testing.T) {
	h := startHead(3, "s1/r1", "k3")
	h.State = StateLive
	h.Position = ^uint64(0) - 1
	if _, err := h.Encode(); err != nil {
		t.Fatalf("control, position MaxUint64-1: %v", err)
	}
	h.Position = ^uint64(0)
	if _, err := h.Encode(); !errors.Is(err, ErrNoNextPosition) || !errors.Is(err, ErrMalformed) {
		t.Fatalf("position MaxUint64: %v; want ErrNoNextPosition (ErrMalformed)", err)
	}
}
