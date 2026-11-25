package layout

import (
	"context"
	"errors"
	"testing"
)

func cleanHead(session string) Head {
	h := snapHead("x")
	h.Activity = Activity{Kind: ActClean, Session: session}
	return h
}

func TestBeginClean_Closure(t *testing.T) {
	h := idleHead()
	next, changed, err := h.BeginClean(me, "c1")
	if err != nil || !changed || next.Activity != (Activity{Kind: ActClean, Session: "c1"}) {
		t.Fatalf("BeginClean from idle: %+v %t %v", next, changed, err)
	}
	if again, changed, err := next.BeginClean(me, "c1"); err != nil || changed || again.Activity != next.Activity {
		t.Fatalf("BeginClean of the open cleaning: %+v %t %v; want a replay", again, changed, err)
	}
	for name, tc := range map[string]struct {
		h    Head
		me   HolderID
		want error
	}{
		"snapshot open":  {snapHead("s1"), me, ErrBusy},
		"other cleaning": {cleanHead("c2"), me, ErrBusy},
		"other epoch":    {h, HolderID{Epoch: 4, Holder: "srv-a"}, ErrNotHolder},
		"other holder":   {h, HolderID{Epoch: 3, Holder: "srv-b"}, ErrNotHolder},
	} {
		if _, _, err := tc.h.BeginClean(tc.me, "c1"); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v; want %v", name, err, tc.want)
		}
	}
	if _, _, err := h.BeginClean(me, ""); err == nil {
		t.Error("BeginClean without a session id accepted")
	}
}

func TestEndClean_Closure(t *testing.T) {
	h := cleanHead("c1")
	next, changed, err := h.EndClean(me, "c1")
	if err != nil || !changed || next.Activity != (Activity{Kind: ActIdle}) || next.Position != h.Position || len(next.Snapshots) != len(h.Snapshots) {
		t.Fatalf("EndClean: %+v %t %v", next, changed, err)
	}
	if again, changed, err := next.EndClean(me, "c1"); err != nil || changed {
		t.Fatalf("EndClean over idle: %+v %t %v; want a replay", again, changed, err)
	}
	for name, tc := range map[string]struct {
		h    Head
		me   HolderID
		want error
	}{
		"snapshot open":            {snapHead("c1"), me, ErrNoSession},
		"other cleaning":           {cleanHead("c2"), me, ErrNoSession},
		"other epoch":              {h, HolderID{Epoch: 4, Holder: "srv-a"}, ErrNotHolder},
		"same epoch, other holder": {h, HolderID{Epoch: 3, Holder: "srv-b"}, ErrNotHolder},
	} {
		if _, _, err := tc.h.EndClean(tc.me, "c1"); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v; want %v", name, err, tc.want)
		}
	}
}

// EndClean over idle succeeds and writes nothing whatever came before (a
// cleaning ended, or never begun): it means only that no activity remains.
func TestEndClean_IdleMeansNoActivityRemains(t *testing.T) {
	for name, h := range map[string]Head{
		"never begun":   idleHead(),
		"already ended": func() Head { n, _, _ := cleanHead("c1").EndClean(me, "c1"); return n }(),
	} {
		next, changed, err := h.EndClean(me, "c1")
		if err != nil || changed || next.Activity.Kind != ActIdle {
			t.Errorf("EndClean over idle (%s): %+v %t %v; want unchanged, no error", name, next, changed, err)
		}
	}
}

// A cleaning and a snapshot exclude each other: no snapshot begins while a
// cleaning is open, no cleaning begins while a snapshot is open; a claim
// voids an open cleaning.
func TestClean_ExcludesSnapshot(t *testing.T) {
	ctx := context.Background()
	h := idleHead()
	st := &memHead{h: &h}
	if _, err := RunBeginClean(ctx, st, me, "c1"); err != nil {
		t.Fatal(err)
	}
	if _, err := RunBegin(ctx, st, me, "s1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("Begin during a cleaning: %v; want ErrBusy", err)
	}
	if _, err := RunEndClean(ctx, st, me, "c1"); err != nil {
		t.Fatal(err)
	}
	if _, err := RunBegin(ctx, st, me, "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := RunBeginClean(ctx, st, me, "c2"); !errors.Is(err, ErrBusy) {
		t.Fatalf("BeginClean during a snapshot: %v; want ErrBusy", err)
	}
	c := cleanHead("c1")
	st2 := &memHead{h: &c}
	if _, err := RunClaim(ctx, st2, &fakeLease{ended: true}, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := RunEndClean(ctx, st2, me, "c1"); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("the old holder's EndClean after a claim: %v; want ErrNotHolder", err)
	}
}

func TestRunBeginClean_InterruptAtEveryStep(t *testing.T) {
	interruptHead(t, idleHead(), func(st HeadStore) error {
		_, err := RunBeginClean(context.Background(), st, me, "c1")
		return err
	}, func(t *testing.T, h Head) {
		if h.Activity != (Activity{Kind: ActClean, Session: "c1"}) {
			t.Fatalf("HEAD after BeginClean: %+v", h.Activity)
		}
	})
}

func TestRunEndClean_InterruptAtEveryStep(t *testing.T) {
	interruptHead(t, cleanHead("c1"), func(st HeadStore) error {
		_, err := RunEndClean(context.Background(), st, me, "c1")
		return err
	}, func(t *testing.T, h Head) {
		if h.Activity != (Activity{Kind: ActIdle}) {
			t.Fatalf("HEAD after EndClean: %+v", h.Activity)
		}
	})
}
