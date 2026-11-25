package layout

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// The final image is committed through the same single HEAD update as a
// snapshot. The caller supplies the position synced after closing guest writes.
func TestCommitDetached_ProofAndLostReply(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "before apply", true: "reply lost"}[applied], func(t *testing.T) {
			o, h, req := commitFixture(t)
			req.State = StateDetached
			st := &memHead{h: &h, killAt: 1, after: applied}
			if _, err := RunCommit(context.Background(), st, o, "v1", me, req); !errors.Is(err, errKilled) {
				t.Fatal("expected interrupted commit", err)
			}
			st.restart()
			proof, found, err := st.ReadHead(context.Background())
			if err != nil || !found || !proof.heldBy(me) {
				t.Fatal("stop cannot decide without reading its claim", proof, found, err)
			}
			if applied {
				if proof.State != StateDetached || proof.Position != req.Position || proof.Activity.Kind != ActIdle {
					t.Fatal("HEAD lacks the final synced image proof", proof)
				}
				if _, err := Start(context.Background(), st, &fakeLease{}, me, LocalRoot{ClaimID: me.ClaimID, Position: req.Position}); !errors.Is(err, ErrDetached) || !strings.Contains(err.Error(), "new grant") {
					t.Fatal("same-epoch start did not refuse with the cure", err)
				}
			} else if proof.State != StateLive || proof.Position != h.Position {
				t.Fatal("unapplied stop changed the published image", proof)
			}
		})
	}
}

// Every holder method is enumerated: adding a transition requires a detached
// refusal probe too. Claim is the sole exception, only at a higher epoch.
func TestDetachedHEAD_RefusesEveryHolderTransition(t *testing.T) {
	h := idleHead()
	h.State = StateDetached
	rn := RootName{Hash: Sum([]byte("image"))}
	h.Current = &rn
	steps := map[string]func() error{
		"Begin":      func() error { _, _, e := h.Begin(me, "s"); return e },
		"Abort":      func() error { _, _, e := h.Abort(me, "s"); return e },
		"Commit":     func() error { _, _, e := h.Commit(me, "s", rn, h.Position, "", SnapshotMeta{}, StateLive); return e },
		"BeginClean": func() error { _, _, e := h.BeginClean(me, "c"); return e },
		"EndClean":   func() error { _, _, e := h.EndClean(me, "c"); return e },
		"Claim":      func() error { _, _, e := h.Claim(me); return e },
	}
	for name, step := range steps {
		if err := step(); !errors.Is(err, ErrDetached) {
			t.Errorf("%s did not refuse detached HEAD: %v", name, err)
		}
	}
	ht, holderT := reflect.TypeOf(Head{}), reflect.TypeOf(HolderID{})
	for i := 0; i < ht.NumMethod(); i++ {
		m := ht.Method(i)
		if m.Type.NumIn() > 1 && m.Type.In(1) == holderT {
			if _, ok := steps[m.Name]; !ok {
				t.Errorf("holder transition %s has no detached probe", m.Name)
			}
		}
	}
	newHolder := me
	newHolder.Epoch++
	next, changed, err := h.Claim(newHolder)
	if err != nil || !changed || next.State != StateLive || next.Position != h.Position || *next.Current != rn {
		t.Fatal("higher grant did not reopen the same published image", next, changed, err)
	}
	if next, changed, err := h.MarkDeleting(); err != nil || !changed || next.State != StateDeleting {
		t.Fatal("detached volume could not enter deletion", next, changed, err)
	}
	if _, _, err := h.MarkDeleted(); err == nil {
		t.Fatal("detached volume skipped the deleting transition")
	}
	if raw, err := h.Encode(); err != nil {
		t.Fatal("detached proof not encodable", err)
	} else if got, err := DecodeHead(raw, h.Gen, h.VolumeID); err != nil || got.State != h.State {
		t.Fatal("detached proof not preserved", got, err)
	}
}

func TestDetachedHEAD_NewGrantStillWaitsForTheOldLease(t *testing.T) {
	h := idleHead()
	h.State = StateDetached
	rn := RootName{Hash: Sum([]byte("image"))}
	h.Current = &rn
	st := &memHead{h: &h}
	line := me
	line.Epoch++
	lease := &fakeLease{}
	local := LocalRoot{ClaimID: me.ClaimID, Position: h.Position}
	if _, err := Start(context.Background(), st, lease, line, local); err == nil || st.updates != 0 || len(lease.calls) != 1 {
		t.Fatal("higher epoch bypassed old lease", err, st.updates, lease.calls)
	}
	lease.ended = true
	next, err := Start(context.Background(), st, lease, line, local)
	if err != nil || next.State != StateLive || next.Epoch != line.Epoch || st.updates != 1 {
		t.Fatal("higher grant could not reopen after lease end", next, err)
	}
}
