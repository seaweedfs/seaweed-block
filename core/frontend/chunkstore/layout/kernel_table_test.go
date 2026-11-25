package layout

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// The kernel's state x entry-point table (spec section 0, made
// executable): every entry point called, as this volume's holder, on HEAD
// in every state, with its result. A new HEAD state or a new entry point
// fails TestKernelTable_Covers until its row or column is filled.
//
// Results: "write" (HEAD changed or was created), "noop" (no write, no
// error: a replay or nothing to do), or the kernel refusal's name.

// kernelStates are HEAD's states: absent, unclaimed, live with each
// activity, detached, deleting, deleted.
var kernelStates = []string{"absent", "unclaimed", "idle", "snap", "clean", "detached", "deleting", "deleted"}

// kernelEntries are the kernel's entry points (DeleteVolume is their
// composition: Reserve, MarkDeleting, release, objects, MarkDeleted).
var kernelEntries = []string{
	"CreateVolume", "CreateTarget", "StartHigher", "StartHigherCannotStart", "StartOwn", "StartOwnOtherFresh", "StartOwnOtherFreshCannotStart", "StartOwnOtherOld",
	"Claim", "Begin", "Commit", "CommitNew", "CommitLower", "Abort", "BeginClean", "EndClean",
	"RemoveUnreachable", "DeleteSnapshot", "RetireSnapshot", "AddDependent", "ReleaseDependent",
	"MarkDeleting", "MarkDeleted", "DeletedHeadCreate",
}

var kernelRefusals = []struct {
	name string
	err  error
}{
	// the local-root refusals wrap ErrNotHolder: name them first
	{"StaleLocalRoot", ErrStaleLocalRoot}, {"MissingLocalRoot", ErrMissingLocalRoot}, {"CannotStartFrom", ErrCannotStartFrom},
	{"HeadAbsent", ErrHeadAbsent}, {"NotHolder", ErrNotHolder}, {"Busy", ErrBusy}, {"NoSession", ErrNoSession},
	{"VolumeDeleting", ErrVolumeDeleting}, {"Detached", ErrDetached}, {"StaleEpoch", ErrStaleEpoch},
	{"Exists", ErrExists}, {"HasDependents", ErrHasDependents}, {"TargetLive", ErrTargetLive},
	{"NotPrepared", ErrNotPrepared},
	{"Identity", ErrIdentity}, {"NoSnapshot", ErrNoSnapshot}, {"Malformed", ErrMalformed},
	{"HolderActive", ErrHolderActive}, {"NotDeleting", ErrNotDeleting}, {"PositionBehind", ErrPositionBehind},
}

// cannotStart is a local root whose post-claim step fails on any HEAD.
func cannotStart(Head) error { return errors.New("the local root cannot start from this HEAD") }

func resultOf(err error, wrote bool) string {
	if err == nil {
		if wrote {
			return "write"
		}
		return "noop"
	}
	for _, k := range kernelRefusals {
		if errors.Is(err, k.err) {
			return k.name
		}
	}
	return "other: " + err.Error()
}

var tableGeometry = Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}

// kernelCell sets HEAD in state and runs entry once.
func kernelCell(t *testing.T, state, entry string) string {
	t.Helper()
	ctx := context.Background()
	o, h, _, orphans := removeFixture(t)
	h.Position = 10 // a volume with commits behind it (CommitLower commits below it)
	switch state {
	case "unclaimed":
		// a restore target never started: its current root names the
		// source's chunks (here the fixture's own image)
		u := Unclaimed(h.VolumeID, h.Current)
		u.Gen = 1
		h = &u
	case "idle":
		h.Activity = Activity{Kind: ActIdle}
	case "snap":
		h.Activity = Activity{Kind: ActSnap, Session: "s1"}
	case "clean":
		h.Activity = Activity{Kind: ActClean, Session: "c1"}
	case "detached":
		h.Activity, h.State = Activity{Kind: ActIdle}, StateDetached
	case "deleting":
		h.Activity, h.State = Activity{Kind: ActIdle}, StateDeleting
	case "deleted":
		h.Activity, h.State = Activity{Kind: ActIdle}, StateDeleted
	case "maxposition": // an older writer left HEAD at the last position
		h.Activity, h.Position = Activity{Kind: ActIdle}, ^uint64(0)
	case "maxposition-deleting":
		h.Activity, h.State, h.Position = Activity{Kind: ActIdle}, StateDeleting, ^uint64(0)
	}
	if entry == "ReleaseDependent" && h.Snapshots != nil {
		d := h.Snapshots["daily"]
		d.Dependents = []Dependent{{Target: "t", TargetID: idFor("t"), Root: d.Root}}
		h.Snapshots["daily"] = d
	}
	nv := newMemNewVolume()
	vol := Volume{ID: idFor("v1"), Name: "v1", Geometry: tableGeometry, Format: VolumeFormat}
	if state != "absent" {
		nv.memHead.h = h
		nv.vol = &vol
	}
	before, objectWrites := 0, len(o.writes)
	if nv.memHead.h != nil {
		before = int(nv.memHead.h.Gen)
	}
	r, err := NewReader(removeBase, "v1", o.fetch)
	if err != nil {
		t.Fatal(err)
	}
	// An idle source volume with a snapshot "daily" naming only its own
	// chunks, for the entry points that read or write a source.
	_, sh, _, _ := removeFixture(t)
	sh.Activity = Activity{Kind: ActIdle}
	src := newMemNewVolume()
	src.memHead.h, src.vol = sh, &vol

	switch entry {
	case "CreateVolume":
		_, err = CreateVolume(ctx, nv, vol)
	case "CreateTarget":
		_, err = CreateTarget(ctx, src, r, "daily", nv, Volume{ID: idFor("v1"), Name: "v1", Geometry: tableGeometry, Format: VolumeFormat}, func(context.Context, RootName) error { return nil })
	case "StartHigher": // a higher granted epoch, a fresh root
		_, err = Start(ctx, nv, &fakeLease{ended: true}, HolderID{Epoch: 4, Holder: "srv-a"}, LocalRoot{Geometry: tableGeometry, ClaimID: "k-new", Fresh: true, CanStartFrom: anyStart})
	case "StartOwn": // HEAD's epoch and server, the root carrying HEAD's claim id
		_, err = Start(ctx, nv, &fakeLease{ended: true}, HolderID{Epoch: me.Epoch, Holder: me.Holder}, LocalRoot{Geometry: tableGeometry, ClaimID: me.ClaimID, Position: h.Position})
	case "StartOwnOtherFresh": // HEAD's epoch and server, a fresh root of another claim
		_, err = Start(ctx, nv, &fakeLease{ended: true}, HolderID{Epoch: me.Epoch, Holder: me.Holder}, LocalRoot{Geometry: tableGeometry, ClaimID: "k-other", Fresh: true, CanStartFrom: anyStart})
	case "StartHigherCannotStart": // a higher granted epoch, a fresh root whose store cannot seed HEAD's position
		_, err = Start(ctx, nv, &fakeLease{ended: true}, HolderID{Epoch: 4, Holder: "srv-a"}, LocalRoot{Geometry: tableGeometry, ClaimID: "k-new", Fresh: true, CanStartFrom: cannotStart})
	case "StartOwnOtherFreshCannotStart":
		_, err = Start(ctx, nv, &fakeLease{ended: true}, HolderID{Epoch: me.Epoch, Holder: me.Holder}, LocalRoot{Geometry: tableGeometry, ClaimID: "k-other", Fresh: true, CanStartFrom: cannotStart})
	case "StartOwnOtherOld": // HEAD's epoch and server, an older root of another claim
		_, err = Start(ctx, nv, &fakeLease{ended: true}, HolderID{Epoch: me.Epoch, Holder: me.Holder}, LocalRoot{Geometry: tableGeometry, ClaimID: "k-other", Position: h.Position})
	case "Claim":
		_, err = RunClaim(ctx, nv, &fakeLease{ended: true}, HolderID{Epoch: 4, Holder: "srv-b", ClaimID: "k-b"})
	case "Begin":
		_, err = RunBegin(ctx, nv, me, "s2")
	case "Commit":
		var cur map[uint64]Ref
		if h.Current != nil {
			img, oerr := r.Open(ctx, *h.Current)
			if oerr != nil {
				t.Fatal(oerr)
			}
			cur = map[uint64]Ref{}
			for _, sh := range img.Shards() {
				seg, _, _ := img.Segment(ctx, sh)
				for i, ref := range seg.Chunks {
					cur[i] = ref
				}
			}
		}
		prepared(t, o, cur, "s1")
		objectWrites = len(o.writes)
		_, err = RunCommit(ctx, nv, o, "v1", me, CommitRequest{Session: "s1", Prev: cur, Next: cur, Position: h.Position, State: StateLive})
	case "CommitLower": // HEAD's image at a position below HEAD's
		var cur map[uint64]Ref
		if h.Current != nil {
			img, oerr := r.Open(ctx, *h.Current)
			if oerr != nil {
				t.Fatal(oerr)
			}
			cur = map[uint64]Ref{}
			for _, sh := range img.Shards() {
				seg, _, _ := img.Segment(ctx, sh)
				for i, ref := range seg.Chunks {
					cur[i] = ref
				}
			}
		}
		lower := h.Position
		if lower > 0 {
			lower-- // (a HEAD at 0, absent or never committed, has no lower position)
		}
		prepared(t, o, cur, "s1")
		objectWrites = len(o.writes)
		_, err = RunCommit(ctx, nv, o, "v1", me, CommitRequest{Session: "s1", Prev: cur, Next: cur, Position: lower, State: StateLive})
	case "CommitNew": // a new image (not a replay of an applied commit)
		var cur map[uint64]Ref
		if h.Current != nil {
			img, oerr := r.Open(ctx, *h.Current)
			if oerr != nil {
				t.Fatal(oerr)
			}
			cur = map[uint64]Ref{}
			for _, sh := range img.Shards() {
				seg, _, _ := img.Segment(ctx, sh)
				for i, ref := range seg.Chunks {
					cur[i] = ref
				}
			}
		}
		next := map[uint64]Ref{}
		for i, ref := range cur {
			next[i] = ref
		}
		next[9] = ref("v1", 9, "new")
		prepared(t, o, next, "s1")
		objectWrites = len(o.writes)
		_, err = RunCommit(ctx, nv, o, "v1", me, CommitRequest{Session: "s1", Prev: cur, Next: next, Position: h.Position + 1, State: StateLive})
	case "Abort":
		_, err = RunAbort(ctx, nv, me, "s1")
	case "BeginClean":
		_, err = RunBeginClean(ctx, nv, me, "c2")
	case "EndClean":
		_, err = RunEndClean(ctx, nv, me, "c1")
	case "RemoveUnreachable":
		_, err = RemoveUnreachable(ctx, nv, r, o, me, "c1", 0, orphans)
	case "DeleteSnapshot":
		_, err = RunDeleteSnapshot(ctx, nv, "daily")
	case "RetireSnapshot":
		_, err = RunRetireSnapshot(ctx, nv, "daily", 1)
	case "AddDependent":
		_, err = RunAddDependent(ctx, nv, r, "daily", Dependent{Target: "t", TargetID: idFor("t")})
	case "ReleaseDependent":
		_, err = RunReleaseDependent(ctx, nv, idFor("t"))
	case "MarkDeleting":
		_, err = RunMarkDeleting(ctx, nv, &fakeMaster{})
	case "MarkDeleted":
		_, err = RunMarkDeleted(ctx, nv)
	case "DeletedHeadCreate":
		_, err = OperatorReleaseDependent(ctx, src, nv.memHead, r, idFor("v1"))
	default:
		t.Fatalf("no call for entry %q", entry)
	}
	after := 0
	if nv.memHead.h != nil {
		after = int(nv.memHead.h.Gen)
	}
	return resultOf(err, after != before || len(o.writes) != objectWrites)
}

// kernelExpected is the table, reviewed cell by cell against the spec
// (section 0's state x event table): every entry point refuses a deleting
// or deleted HEAD except the delete's own steps; a detached HEAD (a clean
// stop) refuses the holder's transaction steps until a new claim, and
// takes the operator's snapshot management (delete, retire, restore
// source) and a claim; an unclaimed HEAD takes only the first claim and
// the creates' replays.
var kernelExpected = map[string]map[string]string{
	"CreateVolume":                  {"absent": "write", "unclaimed": "noop", "idle": "noop", "snap": "noop", "clean": "noop", "detached": "noop", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"CreateTarget":                  {"absent": "write", "unclaimed": "Exists", "idle": "Exists", "snap": "Exists", "clean": "Exists", "detached": "Exists", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"StartHigher":                   {"absent": "HeadAbsent", "unclaimed": "write", "idle": "write", "snap": "write", "clean": "write", "detached": "write", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"StartHigherCannotStart":        {"absent": "HeadAbsent", "unclaimed": "CannotStartFrom", "idle": "CannotStartFrom", "snap": "CannotStartFrom", "clean": "CannotStartFrom", "detached": "CannotStartFrom", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"StartOwnOtherFreshCannotStart": {"absent": "HeadAbsent", "unclaimed": "CannotStartFrom", "idle": "MissingLocalRoot", "snap": "MissingLocalRoot", "clean": "MissingLocalRoot", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"StartOwn":                      {"absent": "HeadAbsent", "unclaimed": "StaleLocalRoot", "idle": "noop", "snap": "write", "clean": "write", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"StartOwnOtherFresh":            {"absent": "HeadAbsent", "unclaimed": "write", "idle": "MissingLocalRoot", "snap": "MissingLocalRoot", "clean": "MissingLocalRoot", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"StartOwnOtherOld":              {"absent": "HeadAbsent", "unclaimed": "StaleLocalRoot", "idle": "StaleLocalRoot", "snap": "StaleLocalRoot", "clean": "StaleLocalRoot", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"Claim":                         {"absent": "HeadAbsent", "unclaimed": "write", "idle": "write", "snap": "write", "clean": "write", "detached": "write", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"Begin":                         {"absent": "HeadAbsent", "unclaimed": "NotHolder", "idle": "write", "snap": "Busy", "clean": "Busy", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"Commit":                        {"absent": "HeadAbsent", "unclaimed": "NotHolder", "idle": "noop", "snap": "write", "clean": "NoSession", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"CommitLower":                   {"absent": "HeadAbsent", "unclaimed": "NotHolder", "idle": "NoSession", "snap": "PositionBehind", "clean": "NoSession", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"CommitNew":                     {"absent": "HeadAbsent", "unclaimed": "NotHolder", "idle": "NoSession", "snap": "write", "clean": "NoSession", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"Abort":                         {"absent": "HeadAbsent", "unclaimed": "NotHolder", "idle": "noop", "snap": "write", "clean": "NoSession", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"BeginClean":                    {"absent": "HeadAbsent", "unclaimed": "NotHolder", "idle": "write", "snap": "Busy", "clean": "Busy", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"EndClean":                      {"absent": "HeadAbsent", "unclaimed": "NotHolder", "idle": "noop", "snap": "NoSession", "clean": "write", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"RemoveUnreachable":             {"absent": "HeadAbsent", "unclaimed": "NotHolder", "idle": "NoSession", "snap": "NoSession", "clean": "write", "detached": "Detached", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"DeleteSnapshot":                {"absent": "HeadAbsent", "unclaimed": "noop", "idle": "write", "snap": "write", "clean": "write", "detached": "write", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"RetireSnapshot":                {"absent": "HeadAbsent", "unclaimed": "noop", "idle": "write", "snap": "write", "clean": "write", "detached": "write", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"AddDependent":                  {"absent": "HeadAbsent", "unclaimed": "NoSnapshot", "idle": "write", "snap": "write", "clean": "write", "detached": "write", "deleting": "VolumeDeleting", "deleted": "VolumeDeleting"},
	"ReleaseDependent":              {"absent": "HeadAbsent", "unclaimed": "noop", "idle": "write", "snap": "write", "clean": "write", "detached": "write", "deleting": "unreachable", "deleted": "unreachable"},
	"MarkDeleting":                  {"absent": "HeadAbsent", "unclaimed": "write", "idle": "write", "snap": "HolderActive", "clean": "HolderActive", "detached": "write", "deleting": "noop", "deleted": "noop"},
	"MarkDeleted":                   {"absent": "HeadAbsent", "unclaimed": "NotDeleting", "idle": "NotDeleting", "snap": "NotDeleting", "clean": "NotDeleting", "detached": "NotDeleting", "deleting": "write", "deleted": "noop"},
	"DeletedHeadCreate":             {"absent": "write", "unclaimed": "TargetLive", "idle": "TargetLive", "snap": "TargetLive", "clean": "TargetLive", "detached": "TargetLive", "deleting": "noop", "deleted": "noop"},
}

// kernelUnreachable are cells no reachable HEAD gives (a deleting source
// has no dependents: MarkDeleting refuses one that has, TestDeleteVolume).
var kernelUnreachable = map[string]map[string]bool{"ReleaseDependent": {"deleting": true, "deleted": true}}

func TestKernelTable(t *testing.T) {
	for _, e := range kernelEntries {
		for _, s := range kernelStates {
			want, ok := kernelExpected[e][s]
			if !ok {
				t.Errorf("no expected result for %s on %s HEAD", e, s)
				continue
			}
			if kernelUnreachable[e][s] {
				if want != "unreachable" {
					t.Errorf("%s on %s HEAD is unreachable; the table says %s", e, s, want)
				}
				continue
			}
			if got := kernelCell(t, s, e); got != want {
				t.Errorf("%s on %s HEAD: %s; want %s", e, s, got, want)
			}
		}
	}
}

// kernelEntryFuncs maps each table row to the exported function it calls;
// kernelComposed are exported entry points that only compose the rows.
var kernelEntryFuncs = map[string]string{
	"CreateVolume": "CreateVolume", "CreateTarget": "CreateTarget", "StartHigher": "Start", "StartOwn": "Start",
	"StartOwnOtherFresh": "Start", "StartOwnOtherOld": "Start", "StartHigherCannotStart": "Start", "StartOwnOtherFreshCannotStart": "Start", "Claim": "RunClaim",
	"Begin": "RunBegin", "Commit": "RunCommit", "CommitNew": "RunCommit", "CommitLower": "RunCommit", "Abort": "RunAbort", "BeginClean": "RunBeginClean", "EndClean": "RunEndClean",
	"RemoveUnreachable": "RemoveUnreachable", "DeleteSnapshot": "RunDeleteSnapshot", "RetireSnapshot": "RunRetireSnapshot",
	"AddDependent": "RunAddDependent", "ReleaseDependent": "RunReleaseDependent", "MarkDeleting": "RunMarkDeleting",
	"MarkDeleted": "RunMarkDeleted", "DeletedHeadCreate": "OperatorReleaseDependent",
}

var kernelComposed = map[string]string{"DeleteVolume": "Reserve, MarkDeleting, ReleaseDependent per source, the objects, MarkDeleted"}

// kernelStateConsts maps every State*/Act* constant to the table's
// columns it gives.
var kernelStateConsts = map[string][]string{
	"StateLive": {"unclaimed", "idle", "snap", "clean"}, "StateDetached": {"detached"}, "StateDeleting": {"deleting"}, "StateDeleted": {"deleted"},
	"ActIdle": {"unclaimed", "idle", "detached", "deleting", "deleted"}, "ActSnap": {"snap"}, "ActClean": {"clean"},
}

// TestKernelTable_Covers fails when a new entry point (an exported Run*,
// Create*, Start, Remove* or Operator* function) or a new HEAD state or
// activity constant appears without its row or column in the table.
func TestKernelTable_Covers(t *testing.T) {
	funcs, consts := map[string]bool{}, map[string]bool{}
	for fname, src := range packageSources(t, false) {
		file, err := parser.ParseFile(token.NewFileSet(), fname, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			switch x := d.(type) {
			case *ast.FuncDecl:
				n := x.Name.Name
				if x.Recv == nil && (strings.HasPrefix(n, "Run") || strings.HasPrefix(n, "Create") || n == "Start" || strings.HasPrefix(n, "Remove") || strings.HasPrefix(n, "Operator") || n == "DeleteVolume") {
					funcs[n] = true
				}
			case *ast.GenDecl:
				for _, sp := range x.Specs {
					vs, ok := sp.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, id := range vs.Names {
						n := id.Name
						if (strings.HasPrefix(n, "State") && len(n) > 5) || (strings.HasPrefix(n, "Act") && len(n) > 3) {
							consts[n] = true
						}
					}
				}
			}
		}
	}
	rows := map[string]bool{}
	for _, f := range kernelEntryFuncs {
		rows[f] = true
	}
	for f := range funcs {
		if !rows[f] && kernelComposed[f] == "" {
			t.Errorf("entry point %s has no row in the kernel table", f)
		}
	}
	for _, e := range kernelEntries {
		if !funcs[kernelEntryFuncs[e]] {
			t.Errorf("table row %s calls %s, which is not an entry point any more", e, kernelEntryFuncs[e])
		}
	}
	cols := map[string]bool{}
	for _, s := range kernelStates {
		cols[s] = true
	}
	for c := range consts {
		cs, ok := kernelStateConsts[c]
		if !ok {
			t.Errorf("HEAD state or activity %s has no column in the kernel table", c)
		}
		for _, s := range cs {
			if !cols[s] {
				t.Errorf("%s gives column %s, which the table lacks", c, s)
			}
		}
	}
}

// TestKernelTable_Observe prints the observed table (go test -run
// Observe -v), the source the expected table was reviewed from.
func TestKernelTable_Observe(t *testing.T) {
	if os.Getenv("KERNEL_TABLE_OBSERVE") == "" {
		t.Skip("set KERNEL_TABLE_OBSERVE=1 to print the observed table")
	}
	var b strings.Builder
	for _, e := range kernelEntries {
		fmt.Fprintf(&b, "%q: {", e)
		for i, s := range kernelStates {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%q: %q", s, kernelCell(t, s, e))
		}
		b.WriteString("},\n")
	}
	t.Log("\n" + b.String())
}

// A HEAD an older or broken writer left at position MaxUint64 can still be
// deleted: the guard against that position sits where a position is
// produced (Commit), not on every transition, so the operator is never
// stuck with a HEAD nothing can remove.
func TestKernelTable_HeadAtMaxPosition(t *testing.T) {
	for _, c := range []struct{ state, entry, want string }{
		{"maxposition", "MarkDeleting", "write"},
		{"maxposition-deleting", "MarkDeleted", "write"},
	} {
		if got := kernelCell(t, c.state, c.entry); got != c.want {
			t.Errorf("%s on a HEAD at position MaxUint64 (%s): %s; want %s", c.entry, c.state, got, c.want)
		}
	}
}
