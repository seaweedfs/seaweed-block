package layout

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestDeleteSnapshot_Closure(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{"daily": {Root: rn}, "pinned": {Root: rn, Dependents: deps(rn, "t1")}}
	next, changed, err := h.DeleteSnapshot("daily")
	if err != nil || !changed || len(next.Snapshots) != 1 {
		t.Fatalf("DeleteSnapshot: %+v %t %v", next.Snapshots, changed, err)
	}
	if _, ok := h.Snapshots["daily"]; !ok {
		t.Fatal("DeleteSnapshot changed its receiver")
	}
	if _, changed, err := next.DeleteSnapshot("daily"); err != nil || changed {
		t.Fatalf("DeleteSnapshot of a missing name: %t %v; want a replay", changed, err)
	}
	if _, _, err := h.DeleteSnapshot("pinned"); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("DeleteSnapshot with a dependent: %v; want ErrHasDependents", err)
	}
	d := deletingHead()
	d.Snapshots = map[string]Snapshot{"daily": {Root: rn}}
	if _, _, err := d.DeleteSnapshot("daily"); !errors.Is(err, ErrVolumeDeleting) {
		t.Fatalf("DeleteSnapshot on a deleting volume: %v", err)
	}
}

func TestRunDeleteSnapshot_InterruptAtEveryStep(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{"daily": {Root: rn}}
	interruptHead(t, h, func(st HeadStore) error { _, err := RunDeleteSnapshot(context.Background(), st, "daily"); return err },
		func(t *testing.T, h Head) {
			if _, ok := h.Snapshots["daily"]; ok {
				t.Fatal("snapshot still in HEAD")
			}
		})
}

func TestAddDependent_Closure(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{"daily": {Root: rn}}
	t1 := Dependent{Target: "t1", TargetID: idFor("t1")}
	next, changed, err := h.AddDependent("daily", t1, rn)
	if err != nil || !changed {
		t.Fatalf("AddDependent: %v", err)
	}
	if d := next.Snapshots["daily"].Dependents; len(d) != 1 || d[0].TargetID != idFor("t1") || d[0].Root != rn {
		t.Fatalf("dependents %+v; want t1 with the snapshot's root", d)
	}
	if _, err := next.Encode(); err != nil {
		t.Fatalf("HEAD after AddDependent does not encode: %v", err)
	}
	if _, changed, err := next.AddDependent("daily", t1, rn); err != nil || changed {
		t.Fatalf("AddDependent again: %t %v; want a replay", changed, err)
	}
	if _, _, err := h.AddDependent("weekly", t1, rn); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("AddDependent of a missing snapshot: %v", err)
	}
	if _, _, err := h.AddDependent("daily", t1, RootName{Hash: Sum([]byte("other"))}); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("AddDependent on a snapshot whose root changed since the check: %v", err)
	}
	if _, _, err := h.AddDependent("daily", Dependent{Target: "t1"}, rn); err == nil {
		t.Fatal("AddDependent without the target's id accepted")
	}
	d := deletingHead()
	d.Snapshots = map[string]Snapshot{"daily": {Root: rn}}
	if _, _, err := d.AddDependent("daily", t1, rn); !errors.Is(err, ErrVolumeDeleting) {
		t.Fatalf("AddDependent on a deleting volume: %v", err)
	}
}

// RunAddDependent refuses a snapshot that names another volume's chunks:
// a restore target of a restore target would make the dependency deeper
// than one.
func TestRunAddDependent_RefusesAForeignSource(t *testing.T) {
	ctx := context.Background()
	o, h, _, _ := removeFixture(t)
	foreign := manifest(Ref{Owner: "src", OwnerID: idFor("src"), Chunk: NameChunk(1, []byte("x"))})
	data := map[string][]byte{}
	frn := writeImage(t, data, foreign, nil)
	for p, b := range data {
		o.objs[p] = &memObject{data: b, meta: ObjectMeta{Session: "s0"}}
	}
	h.Snapshots["restored"] = Snapshot{Root: frn}
	r, err := NewReader(removeBase, "v1", o.fetch)
	if err != nil {
		t.Fatal(err)
	}
	st := &memHead{h: h}
	if _, err := RunAddDependent(ctx, st, r, "restored", Dependent{Target: "t", TargetID: idFor("t")}); !errors.Is(err, ErrForeignSource) || st.updates != 0 {
		t.Fatalf("AddDependent on a snapshot naming foreign chunks: %v (%d writes); want ErrForeignSource", err, st.updates)
	}
	if _, err := RunAddDependent(ctx, st, r, "daily", Dependent{Target: "t", TargetID: idFor("t")}); err != nil || len(st.h.Snapshots["daily"].Dependents) != 1 {
		t.Fatalf("AddDependent on an own snapshot: %v", err)
	}
}

func TestRunAddDependent_InterruptAtEveryStep(t *testing.T) {
	o, h, _, _ := removeFixture(t)
	r, err := NewReader(removeBase, "v1", o.fetch)
	if err != nil {
		t.Fatal(err)
	}
	h.Activity = Activity{Kind: ActIdle}
	interruptHead(t, *h, func(st HeadStore) error {
		_, err := RunAddDependent(context.Background(), st, r, "daily", Dependent{Target: "t", TargetID: idFor("t")})
		return err
	}, func(t *testing.T, h Head) {
		if d := h.Snapshots["daily"].Dependents; len(d) != 1 || d[0].TargetID != idFor("t") {
			t.Fatalf("dependents %+v", d)
		}
	})
}

func TestRetireSnapshot_Closure(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{
		"auto":    {Root: rn, Meta: SnapshotMeta{CreatedNs: 1, Class: ClassSemantic}},
		"pinned":  {Root: rn, Meta: SnapshotMeta{CreatedNs: 1, Class: ClassPinned}},
		"held":    {Root: rn, Meta: SnapshotMeta{CreatedNs: 1, ExpiresNs: 100}},
		"depends": {Root: rn, Dependents: deps(rn, "t1")},
	}
	if next, changed, err := h.RetireSnapshot("auto", 50); err != nil || !changed || len(next.Snapshots) != 3 {
		t.Fatalf("retire an unpinned snapshot: %t %v", changed, err)
	}
	for name, now := range map[string]int64{"pinned": 50, "held": 50} {
		if _, _, err := h.RetireSnapshot(name, now); !errors.Is(err, ErrPinned) {
			t.Errorf("retire %s at %d: %v; want ErrPinned", name, now, err)
		}
	}
	if _, changed, err := h.RetireSnapshot("held", 100); err != nil || !changed {
		t.Fatalf("retire after the expiry: %t %v", changed, err)
	}
	if _, _, err := h.RetireSnapshot("depends", 50); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("retire a depended-on snapshot: %v", err)
	}
	if _, changed, err := h.RetireSnapshot("gone", 50); err != nil || changed {
		t.Fatalf("retire a missing name: %t %v; want a replay", changed, err)
	}
	// The operator's delete is not refused by a pin.
	if _, changed, err := h.DeleteSnapshot("pinned"); err != nil || !changed {
		t.Fatalf("operator delete of a pinned snapshot: %t %v", changed, err)
	}
}

func TestRunRetireSnapshot_InterruptAtEveryStep(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{"auto": {Root: rn, Meta: SnapshotMeta{CreatedNs: 1}}}
	interruptHead(t, h, func(st HeadStore) error { _, err := RunRetireSnapshot(context.Background(), st, "auto", 2); return err },
		func(t *testing.T, h Head) {
			if _, ok := h.Snapshots["auto"]; ok {
				t.Fatal("snapshot still in HEAD")
			}
		})
}

// A snapshot's retention facts are written by the commit that creates the
// name, kept across every later transition and encoding, never changed by
// a replay; invalid facts are refused.
func TestSnapshotMeta_WrittenOnceByTheCommit(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Activity = Activity{Kind: ActSnap, Session: "s1"}
	me := HolderID{Epoch: h.Epoch, Holder: h.Holder, ClaimID: h.ClaimID}
	meta := SnapshotMeta{CreatedNs: 7, Class: ClassScheduled, ExpiresNs: 9, Labels: map[string]string{"swb.schedule.keep": "3"}}
	next, _, err := h.Commit(me, "s1", rn, 1, "daily", meta, StateLive)
	if err != nil {
		t.Fatal(err)
	}
	b, err := next.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeHead(b, 2, next.VolumeID)
	if err != nil || back.Snapshots["daily"].Meta.Class != ClassScheduled || back.Snapshots["daily"].Meta.Labels["swb.schedule.keep"] != "3" || back.Snapshots["daily"].Meta.CreatedNs != 7 {
		t.Fatalf("meta after encode/decode: %+v %v", back.Snapshots["daily"], err)
	}
	cl := back.clone()
	meta.Labels["swb.schedule.keep"] = "99"
	if cl.Snapshots["daily"].Meta.Labels["swb.schedule.keep"] != "3" || next.Snapshots["daily"].Meta.Labels["swb.schedule.keep"] != "3" {
		t.Fatal("the caller's labels map aliases HEAD's")
	}
	again, changed, err := next.Commit(me, "s1", rn, 1, "daily", SnapshotMeta{Class: ClassPinned}, StateLive)
	if err != nil || changed || again.Snapshots["daily"].Meta.Class != ClassScheduled {
		t.Fatalf("a replay with other facts: %t %v %+v; want a replay keeping the first facts", changed, err, again.Snapshots["daily"].Meta)
	}
	bad := map[string]SnapshotMeta{
		"class":      {Class: "forever"},
		"time":       {CreatedNs: -1},
		"labels":     {Labels: map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}},
		"long label": {Labels: map[string]string{"k": string(make([]byte, MaxLabelBytes+1))}},
	}
	for name, m := range bad {
		if _, _, err := h.Commit(me, "s1", rn, 1, "x", m, StateLive); !errors.Is(err, ErrMalformed) {
			t.Errorf("commit with a bad %s: %v; want ErrMalformed", name, err)
		}
	}
}

// HEAD at its snapshot cap, each snapshot with the largest facts, stays
// inline (measured and bounded here).
func TestHead_SizeAtTheSnapshotCap(t *testing.T) {
	h := idleHead()
	h.Snapshots = map[string]Snapshot{}
	long := func(c byte) string {
		b := make([]byte, MaxLabelBytes)
		for i := range b {
			b[i] = c
		}
		return string(b)
	}
	for i := 0; i < MaxSnapshots; i++ {
		labels := map[string]string{}
		for j := 0; j < MaxSnapshotLabels; j++ {
			labels[fmt.Sprintf("%s%02d", long('k')[:MaxLabelBytes-2], j)] = long('v')
		}
		name := fmt.Sprintf("%s-%04d", long('n')[:58], i)
		h.Snapshots[name] = Snapshot{Root: RootName{Hash: Sum([]byte(name))}, Meta: SnapshotMeta{CreatedNs: 1 << 62, Class: ClassScheduled, ExpiresNs: 1 << 62, Labels: labels}}
	}
	b, err := h.Encode()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("HEAD at %d snapshots with the largest facts: %d bytes", MaxSnapshots, len(b))
	if len(b) > 128<<10 {
		t.Fatalf("HEAD at the cap is %d bytes", len(b))
	}
}

// Reachability, removal and the volume delete never read a snapshot's
// retention facts: wall-clock times and classes decide policy only.
func TestGuard_SafetyIgnoresSnapshotMeta(t *testing.T) {
	for _, f := range []string{"remove.go", "read.go", "delete.go", "objects.go", "create.go"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, banned := range []string{".Meta", "SnapshotMeta", "CreatedNs", "ExpiresNs", "PinnedAt"} {
			if strings.Contains(string(b), banned) {
				t.Errorf("%s uses %s: reachability, removal and delete must not read retention facts", f, banned)
			}
		}
	}
}
