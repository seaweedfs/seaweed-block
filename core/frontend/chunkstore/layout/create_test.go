package layout

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// memNewVolume is a volume directory: VOLUME and HEAD, each created
// exclusively; killAt kills the process at its n-th create, before the
// write applies or (after) once it applied with its reply lost.
type memNewVolume struct {
	*memHead
	vol           *Volume
	creates       int
	killAt        int
	after, killed bool
}

func newMemNewVolume() *memNewVolume { return &memNewVolume{memHead: &memHead{}} }

func (m *memNewVolume) kill() (bool, error) {
	if m.killed {
		return false, errKilled
	}
	m.creates++
	if m.creates == m.killAt {
		m.killed = true
		return m.after, errKilled
	}
	return true, nil
}

func (m *memNewVolume) ReadVolumeRecord(context.Context) (Volume, bool, error) {
	if m.killed {
		return Volume{}, false, errKilled
	}
	if m.vol == nil {
		return Volume{}, false, nil
	}
	return *m.vol, true, nil
}

func (m *memNewVolume) CreateVolumeRecord(_ context.Context, v Volume) error {
	if m.vol != nil && !m.killed {
		return ErrExists
	}
	apply, err := m.kill()
	if apply {
		vv := v
		m.vol = &vv
	}
	return err
}

func (m *memNewVolume) ReadHead(ctx context.Context) (Head, bool, error) {
	if m.killed {
		return Head{}, false, errKilled
	}
	return m.memHead.ReadHead(ctx)
}

func (m *memNewVolume) CreateHead(_ context.Context, h Head) error {
	if m.memHead.h != nil && !m.killed {
		return ErrExists
	}
	apply, err := m.kill()
	if apply {
		if _, eerr := h.Encode(); eerr != nil {
			return eerr
		}
		n := h.clone()
		n.Gen = 1
		m.memHead.h = &n
	}
	return err
}

func (m *memNewVolume) restart() { m.killed, m.killAt = false, 0 }

func TestUnclaimedHead_Shape(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	for name, h := range map[string]Head{
		"new volume":     Unclaimed(idFor("v"), nil),
		"restore target": Unclaimed(idFor("v"), &rn),
		"tombstone":      {VolumeID: idFor("v"), Activity: Activity{Kind: ActIdle}, State: StateDeleted},
	} {
		if _, err := h.Encode(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, h := range map[string]Head{
		"epoch without holder":    {VolumeID: idFor("v"), Epoch: 1, Activity: Activity{Kind: ActIdle}},
		"claim id without holder": {VolumeID: idFor("v"), ClaimID: "k", Activity: Activity{Kind: ActIdle}},
		"activity without holder": {VolumeID: idFor("v"), Activity: Activity{Kind: ActSnap, Session: "s"}},
		"holder without epoch":    {VolumeID: idFor("v"), Holder: "a", ClaimID: "k", Activity: Activity{Kind: ActIdle}},
		"holder without claim id": {VolumeID: idFor("v"), Epoch: 1, Holder: "a", Activity: Activity{Kind: ActIdle}},
	} {
		if _, err := h.Encode(); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v; want ErrMalformed", name, err)
		}
	}
	// No holder step applies to an unclaimed HEAD, not even for an empty caller.
	h := Unclaimed(idFor("v"), nil)
	for _, me := range []HolderID{{}, {Epoch: 1, Holder: "a", ClaimID: "k"}} {
		if _, _, err := h.Begin(me, "s"); !errors.Is(err, ErrNotHolder) {
			t.Errorf("Begin by %+v on an unclaimed HEAD: %v; want ErrNotHolder", me, err)
		}
	}
}

// The first Start claims an unclaimed HEAD only from a fresh root.
func TestStart_ClaimsAnUnclaimedHeadFromAFreshRoot(t *testing.T) {
	ctx := context.Background()
	line := HolderID{Epoch: 1, Holder: "s1/r1"}
	h := Unclaimed(idFor("v1"), nil)
	h.Gen = 1
	st := &memHead{h: &h}
	if _, err := Start(ctx, st, &fakeLease{ended: true}, line, LocalRoot{ClaimID: "k1"}); !errors.Is(err, ErrStaleLocalRoot) || st.updates != 0 {
		t.Fatalf("non-fresh root on an unclaimed HEAD: %v (%d writes); want ErrStaleLocalRoot", err, st.updates)
	}
	got, err := Start(ctx, st, &fakeLease{ended: true}, line, LocalRoot{ClaimID: "k1", Fresh: true})
	if err != nil || got.Epoch != 1 || got.Holder != "s1/r1" || got.ClaimID != "k1" {
		t.Fatalf("fresh root on an unclaimed HEAD: %+v %v", got, err)
	}
}

func TestCreateVolume(t *testing.T) {
	ctx := context.Background()
	v := Volume{ID: idFor("v1"), Name: "v1", Format: VolumeFormat}
	st := newMemNewVolume()
	h, err := CreateVolume(ctx, st, v)
	if err != nil || h.VolumeID != v.ID || h.Holder != "" || h.Epoch != 0 || h.Current != nil || st.vol == nil {
		t.Fatalf("CreateVolume: %+v %v", h, err)
	}
	if again, err := CreateVolume(ctx, st, v); err != nil || again.VolumeID != v.ID || st.creates != 2 {
		t.Fatalf("CreateVolume again: %v, %d creates; want a replay", err, st.creates)
	}
	other := newMemNewVolume()
	other.vol = &Volume{ID: idFor("x"), Name: "v1", Format: VolumeFormat}
	if _, err := CreateVolume(ctx, other, v); !errors.Is(err, ErrIdentity) || other.memHead.h != nil {
		t.Fatalf("CreateVolume over another id's VOLUME: %v; want ErrIdentity, no HEAD", err)
	}
}

// Killed at each create, before it applies and after with the reply lost,
// a re-run completes with VOLUME and the unclaimed HEAD.
func TestCreateVolume_InterruptAtEveryStep(t *testing.T) {
	ctx := context.Background()
	v := Volume{ID: idFor("v1"), Name: "v1", Format: VolumeFormat}
	for k := 1; k <= 2; k++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("create%d/after=%t", k, after), func(t *testing.T) {
				st := newMemNewVolume()
				st.killAt, st.after = k, after
				if _, err := CreateVolume(ctx, st, v); !errors.Is(err, errKilled) {
					t.Fatalf("killed run: %v", err)
				}
				st.restart()
				h, err := CreateVolume(ctx, st, v)
				if err != nil || h.VolumeID != v.ID || st.vol == nil || st.vol.ID != v.ID {
					t.Fatalf("re-run: %+v %v", h, err)
				}
			})
		}
	}
}

// CreateTarget records the dependent before it writes anything of the
// target, and its HEAD names the copied root; once the operator has
// tombstoned the target, the create refuses.
func TestCreateTarget(t *testing.T) {
	ctx := context.Background()
	o, h, _, _ := removeFixture(t)
	r, err := NewReader(removeBase, "v1", o.fetch)
	if err != nil {
		t.Fatal(err)
	}
	h.Activity = Activity{Kind: ActIdle}
	src := &memHead{h: h}
	v := Volume{ID: idFor("t"), Name: "t", Format: VolumeFormat}
	st := newMemNewVolume()
	var copied *RootName
	copyRoot := func(_ context.Context, rn RootName) error {
		if d := src.h.Snapshots["daily"].Dependents; len(d) != 1 || d[0].TargetID != v.ID {
			t.Fatalf("copy before the dependent is recorded: %+v", d)
		}
		copied = &rn
		return nil
	}
	got, err := CreateTarget(ctx, src, r, "daily", st, v, copyRoot)
	if err != nil || copied == nil || got.Current == nil || *got.Current != *copied || *copied != src.h.Snapshots["daily"].Root {
		t.Fatalf("CreateTarget: %+v %v", got, err)
	}
	empty := newMemNewVolume()
	if _, err := CreateVolume(ctx, empty, v); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateTarget(ctx, src, r, "daily", empty, v, func(context.Context, RootName) error { return nil }); !errors.Is(err, ErrExists) {
		t.Fatalf("CreateTarget over an empty volume created first: %v; want ErrExists", err)
	}
	tomb := newMemNewVolume()
	tomb.memHead.h = &Head{VolumeID: v.ID, Activity: Activity{Kind: ActIdle}, State: StateDeleted, Gen: 1}
	if _, err := CreateTarget(ctx, src, r, "daily", tomb, v, func(context.Context, RootName) error { return nil }); !errors.Is(err, ErrVolumeDeleting) {
		t.Fatalf("CreateTarget over the operator's tombstone: %v; want ErrVolumeDeleting", err)
	}
}
