package authority

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The HEAD time a forced move kept, as the filer reported it with HEAD.
var headFilerTime = time.Date(2026, 11, 21, 1, 2, 3, 0, time.UTC)

// pastBarrier moves now past the forced barrier of v's current lease.
func pastBarrier(t *testing.T, p *Publisher, now *time.Time) {
	t.Helper()
	st := p.WorkspaceState("v")
	if st == nil || st.Lease == nil {
		t.Fatal("no lease")
	}
	barrier := time.Unix(0, st.Lease.ExpiresAtNS)
	if d := p.leaseDeadlineLockedForTest("v"); d.After(barrier) {
		barrier = d
	}
	*now = barrier.Add(p.workspaceSkew(st.Lease)).Add(time.Second)
}

// INV-WS-LOST-RANGE-READOUT-001: a forced move stores the kept HEAD's filer
// time with its range; the readout returns it, with the move record's HEAD
// generation, also after a reload. A range with a HEAD time keeps the v1
// checksum over the ranges, so a binary that predates HEAD times still
// verifies the file; a range without one is written exactly as before.
func TestWorkspaceForcedMoveStoresHeadFilerTime(t *testing.T) {
	for _, known := range []bool{true, false} {
		t.Run(map[bool]string{true: "filer time known", false: "filer time unknown"}[known], func(t *testing.T) {
			disk, dir := newTempStore(t)
			p, now, barrier := forcedLostRangeFixture(t, disk, false)
			*now = barrier.Add(time.Second)
			head := forcedLostRangeHead
			if known {
				head.FilerMtimeNS = headFilerTime.UnixNano()
			}
			r, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, head)
			if err != nil {
				t.Fatal(err)
			}
			want := WorkspaceLostRangeReadout{WorkspaceLostRange: WorkspaceLostRange{RequestID: "f", NewEpoch: r.NewEpoch, OldReplicaID: "old", OldEpoch: 1, LostAfterLSN: 42, LostUntilNS: barrier.UnixNano(), HeadTimeNS: head.FilerMtimeNS},
				VolumeID: "v", HeadGen: 7, OldHost: "old-node", NewHost: "new-node"}
			for name, q := range map[string]*Publisher{"running": p, "reloaded": NewPublisher(nil, WithStore(disk))} {
				got, err := q.WorkspaceLostRangeReadouts("v", "")
				if err != nil || len(got) != 1 || got[0] != want {
					t.Fatalf("%s: readout %+v %v; want [%+v]", name, got, err, want)
				}
			}
			raw, err := os.ReadFile(filepath.Join(dir, "v"+lostRangeFileExtension))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "head_times") != known {
				t.Fatalf("head times present=%v in %s", !known, raw)
			}
			// What a binary that predates HEAD times reads and checks.
			type oldRange struct {
				RequestID    string
				NewEpoch     uint64
				OldReplicaID string
				OldEpoch     uint64
				LostAfterLSN uint64
				LostUntilNS  int64
			}
			var old struct {
				Version  int        `json:"v"`
				VolumeID string     `json:"volume"`
				Ranges   []oldRange `json:"ranges"`
				Checksum string     `json:"checksum"`
			}
			if err := json.Unmarshal(raw, &old); err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(struct {
				VolumeID string
				Ranges   []oldRange
			}{old.VolumeID, old.Ranges})
			sum := sha256.Sum256(body)
			if old.Version != 1 || old.Checksum != hex.EncodeToString(sum[:]) || len(old.Ranges) != 1 || old.Ranges[0].LostAfterLSN != 42 {
				t.Fatalf("an older binary would not verify the file: %s", raw)
			}
		})
	}
}

// A HEAD time changed on disk without its checksum fails the file's checks:
// the readout then fails, never "no loss" and never a wrong time.
func TestWorkspaceLostRangeHeadTimeTamperFailsClosed(t *testing.T) {
	disk, dir := newTempStore(t)
	p, now, barrier := forcedLostRangeFixture(t, disk, false)
	*now = barrier.Add(time.Second)
	head := forcedLostRangeHead
	head.FilerMtimeNS = headFilerTime.UnixNano()
	if _, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, head); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "v"+lostRangeFileExtension)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	env["head_times"].([]any)[0].(map[string]any)["HeadTimeNS"] = float64(headFilerTime.Add(time.Hour).UnixNano())
	tampered, _ := json.Marshal(env)
	if err := os.WriteFile(path, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	reloaded := NewPublisher(nil, WithStore(disk))
	if _, err := reloaded.WorkspaceLostRangeReadouts("v", ""); !errors.Is(err, ErrWorkspaceLostRangeMissing) {
		t.Fatalf("tampered head time: %v; want ErrWorkspaceLostRangeMissing", err)
	}
	if !loadErrorsMention(reloaded, "head times checksum mismatch") {
		t.Fatalf("load errors %v do not name the failed check", reloaded.LoadErrors())
	}
}

func loadErrorsMention(p *Publisher, s string) bool {
	for _, e := range p.LoadErrors() {
		if strings.Contains(e.Error(), s) {
			return true
		}
	}
	return false
}

// Accumulation: a second forced move with no cut between (HEAD unchanged,
// still at the first holder's epoch) keeps the same LostAfterLSN with a
// later LostUntil; a forced move after a new cut starts at the new HEAD. The
// readout lists every range in issue order, or one by request id; replays
// return the stored ranges, never recomputed ones.
func TestWorkspaceLostRangeAccumulatesInIssueOrder(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	p, now, barrier := forcedLostRangeFixture(t, disk, false)
	*now = barrier.Add(time.Second)
	h1 := forcedLostRangeHead
	h1.FilerMtimeNS = headFilerTime.UnixNano()
	f1, err := p.MoveWorkspace(ctx, forcedLostRangeReq, h1) // old-node -> new-node, epoch 1 -> 2
	if err != nil {
		t.Fatal(err)
	}
	pastBarrier(t, p, now)
	f2req := WorkspaceMoveRequest{RequestID: "f2", VolumeID: "v", ExpectedReplicaID: "old", TargetReplicaID: "old", ExpectedHost: "new-node", TargetHost: "third-node",
		ExpectedEpoch: f1.NewEpoch, TargetDataAddr: "td", TargetCtrlAddr: "tc", Kind: "forced", Force: true}
	f2, err := p.MoveWorkspace(ctx, f2req, h1) // the grantee never cut: HEAD unchanged
	if err != nil {
		t.Fatal(err)
	}
	pastBarrier(t, p, now)
	h3 := WorkspaceHead{Gen: 8, Epoch: f2.NewEpoch, Holder: "third-node/old", RemoteLSN: 50, FilerMtimeNS: headFilerTime.Add(time.Minute).UnixNano()}
	f3req := WorkspaceMoveRequest{RequestID: "f3", VolumeID: "v", ExpectedReplicaID: "old", TargetReplicaID: "old", ExpectedHost: "third-node", TargetHost: "old-node",
		ExpectedEpoch: f2.NewEpoch, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
	if _, err := p.MoveWorkspace(ctx, f3req, h3); err != nil {
		t.Fatal(err)
	}
	got, err := p.WorkspaceLostRangeReadouts("v", "")
	if err != nil || len(got) != 3 {
		t.Fatalf("readout %+v %v; want three ranges", got, err)
	}
	if got[0].RequestID != "f" || got[1].RequestID != "f2" || got[2].RequestID != "f3" {
		t.Fatalf("not in issue order: %+v", got)
	}
	if got[1].LostAfterLSN != got[0].LostAfterLSN || got[1].LostUntilNS <= got[0].LostUntilNS || got[1].HeadGen != 7 || got[1].HeadTimeNS != h1.FilerMtimeNS {
		t.Fatalf("no cut between: want the same LostAfterLSN, a later LostUntil and the same HEAD: %+v then %+v", got[0], got[1])
	}
	if got[2].LostAfterLSN != 50 || got[2].HeadGen != 8 || got[2].HeadTimeNS != h3.FilerMtimeNS || got[2].LostUntilNS <= got[1].LostUntilNS {
		t.Fatalf("after a cut: want a range from the new HEAD: %+v", got[2])
	}
	one, err := p.WorkspaceLostRangeReadouts("v", "f2")
	if err != nil || len(one) != 1 || one[0] != got[1] {
		t.Fatalf("by request id: %+v %v; want [%+v]", one, err, got[1])
	}
	if _, err := p.WorkspaceLostRangeReadouts("v", "nope"); !errors.Is(err, ErrWorkspaceNoForcedMove) {
		t.Fatalf("unknown request id: %v", err)
	}
	// A replay much later, after a reload, returns the stored grant and the
	// stored range: nothing is recomputed from the replay's clock or HEAD.
	*now = now.Add(time.Hour)
	q := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return *now }))
	r2, err := q.MoveWorkspace(ctx, f2req, WorkspaceHead{Gen: 99, Epoch: 9, Holder: "x/y", RemoteLSN: 999, FilerMtimeNS: 1})
	if err != nil || r2 != f2 {
		t.Fatalf("replay %+v %v; want the stored grant %+v", r2, err, f2)
	}
	again, err := q.WorkspaceLostRangeReadouts("v", "")
	if err != nil || len(again) != 3 || again[0] != got[0] || again[1] != got[1] || again[2] != got[2] {
		t.Fatalf("after reload and replay: %+v %v; want %+v", again, err, got)
	}
}

// Fail closed: an issued forced move whose range file is missing, or fails
// its checksum, makes the readout fail (all ranges, or its own); it never
// returns an empty "no loss".
func TestWorkspaceLostRangeReadoutFailsClosed(t *testing.T) {
	for _, damage := range []string{"missing", "checksum"} {
		t.Run(damage, func(t *testing.T) {
			disk, dir := newTempStore(t)
			p, now, barrier := forcedLostRangeFixture(t, disk, false)
			*now = barrier.Add(time.Second)
			if _, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "v"+lostRangeFileExtension)
			if damage == "missing" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else {
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(strings.Replace(string(raw), `"LostAfterLSN":42`, `"LostAfterLSN":43`, 1)), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			reloaded := NewPublisher(nil, WithStore(disk))
			for _, id := range []string{"", "f"} {
				got, err := reloaded.WorkspaceLostRangeReadouts("v", id)
				if !errors.Is(err, ErrWorkspaceLostRangeMissing) || got != nil {
					t.Fatalf("request %q: %+v %v; want ErrWorkspaceLostRangeMissing", id, got, err)
				}
			}
			if damage == "checksum" && !loadErrorsMention(reloaded, "checksum mismatch") {
				t.Fatalf("load errors %v", reloaded.LoadErrors())
			}
		})
	}
}

// skipRangeStore drops the lost range write but reports success: the
// mutant "grant issued, range write skipped".
type skipRangeStore struct{ *FileAuthorityStore }

func (skipRangeStore) PutWorkspaceLostRanges(string, []WorkspaceLostRange) error { return nil }

// The mutant "grant issued, range write skipped" is detected: the move
// issues (the store claimed the range durable), but the readout of a master
// that reloads from disk fails closed instead of reporting no loss. (The
// order itself, range before issue, is TestWorkspaceForcedMoveLostRangeOrdering.)
func TestWorkspaceLostRangeSkippedWriteIsDetected(t *testing.T) {
	disk, _ := newTempStore(t)
	p, now, barrier := forcedLostRangeFixture(t, skipRangeStore{disk}, false)
	*now = barrier.Add(time.Second)
	if r, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead); err != nil || !r.Issued {
		t.Fatalf("fixture: %+v %v", r, err)
	}
	if _, err := NewPublisher(nil, WithStore(disk)).WorkspaceLostRangeReadouts("v", "f"); !errors.Is(err, ErrWorkspaceLostRangeMissing) {
		t.Fatalf("skipped range write not detected: %v", err)
	}
}

// countWritesStore counts every durable write, authority and lost range.
type countWritesStore struct {
	*FileAuthorityStore
	writes atomic.Int64
}

func (s *countWritesStore) Put(r DurableRecord) error {
	s.writes.Add(1)
	return s.FileAuthorityStore.Put(r)
}

func (s *countWritesStore) PutWorkspaceLostRanges(v string, rs []WorkspaceLostRange) error {
	s.writes.Add(1)
	return s.FileAuthorityStore.PutWorkspaceLostRanges(v, rs)
}

// The preview runs the forced admission and writes nothing: before the
// barrier it is refused and names the barrier (when it becomes admissible);
// after it, it is admitted, and the move then records exactly that barrier
// as LostUntil. A replay previews as admitted with the stored bound.
func TestWorkspaceForcedMovePreviewWritesNothing(t *testing.T) {
	disk, dir := newTempStore(t)
	store := &countWritesStore{FileAuthorityStore: disk}
	p, now, barrier := forcedLostRangeFixture(t, store, false)
	before, state := store.writes.Load(), p.WorkspaceState("v")
	*now = barrier.Add(-time.Nanosecond)
	pv := p.PreviewWorkspaceForcedMove(forcedLostRangeReq)
	if pv.Admitted || pv.LostUntilNS != barrier.UnixNano() || pv.NowNS != now.UnixNano() || !strings.Contains(pv.Reason, "has not elapsed") {
		t.Fatalf("preview before the barrier: %+v", pv)
	}
	*now = barrier.Add(time.Second)
	pv = p.PreviewWorkspaceForcedMove(forcedLostRangeReq)
	if !pv.Admitted || pv.LostUntilNS != barrier.UnixNano() || pv.Replay {
		t.Fatalf("preview after the barrier: %+v", pv)
	}
	bad := forcedLostRangeReq
	bad.ExpectedEpoch = 5
	if pv := p.PreviewWorkspaceForcedMove(bad); pv.Admitted || !strings.Contains(pv.Reason, "missing or changed authority/lease") {
		t.Fatalf("preview at a stale epoch: %+v", pv)
	}
	clean := forcedLostRangeReq
	clean.Kind, clean.Force = "clean", false
	if pv := p.PreviewWorkspaceForcedMove(clean); pv.Admitted {
		t.Fatalf("preview of a clean move: %+v", pv)
	}
	if !WorkspaceMoveHeadAdmits(forcedLostRangeReq, forcedLostRangeHead) || WorkspaceMoveHeadAdmits(forcedLostRangeReq, WorkspaceHead{Gen: 9, Epoch: 1, Holder: "other/old"}) {
		t.Fatal("the preview's HEAD rule differs from the move's")
	}
	if got := store.writes.Load(); got != before {
		t.Fatalf("preview wrote %d times", got-before)
	}
	if after := p.WorkspaceState("v"); len(after.Moves) != len(state.Moves) || *after.Lease != *state.Lease {
		t.Fatalf("preview changed state: %+v -> %+v", state, after)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "*"+lostRangeFileExtension)); len(m) != 0 {
		t.Fatalf("preview wrote a lost range file: %v", m)
	}
	r, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.WorkspaceLostRanges("v"); len(got) != 1 || got[0].LostUntilNS != pv.LostUntilNS {
		t.Fatalf("move recorded %+v; the preview named %d", got, pv.LostUntilNS)
	}
	writes := store.writes.Load()
	*now = now.Add(time.Hour)
	if pv := p.PreviewWorkspaceForcedMove(forcedLostRangeReq); !pv.Admitted || !pv.Replay || pv.LostUntilNS != barrier.UnixNano() {
		t.Fatalf("preview of the issued request %+v (grant %+v)", pv, r)
	}
	if store.writes.Load() != writes {
		t.Fatal("replay preview wrote")
	}
}
