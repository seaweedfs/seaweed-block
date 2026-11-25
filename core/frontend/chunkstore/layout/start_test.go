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
	fresh := LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k-new", Fresh: true, Seedable: anySeedable}
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
	st, got, err := start(t, startHead(3, "s1/r1", "k3"), HolderID{Epoch: 3, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k3"})
	if err != nil || st.updates != 0 || got.ClaimID != "k3" {
		t.Fatalf("same-root restart: %+v %v (%d writes); want continue without a write", got, err, st.updates)
	}
}

// A fresh root whose claim was written but whose reply was lost restarts
// with the same marker: HEAD holds that claim id at this epoch, so it
// continues.
func TestStart_LostClaimReplyContinues(t *testing.T) {
	st, got, err := start(t, startHead(5, "s1/r1", "k5"), HolderID{Epoch: 5, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k5", Fresh: true, Seedable: anySeedable})
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
	st, _, err := start(t, h, HolderID{Epoch: 4, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k3"})
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
		st, got, err := start(t, startHead(3, "s1/r1", "k3"), HolderID{Epoch: 4, Holder: server}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k4", Fresh: true, Seedable: anySeedable})
		if err != nil || st.updates != 1 || got.Epoch != 4 || got.Holder != server || got.ClaimID != "k4" {
			t.Fatalf("fresh root at epoch 4 on %s: %+v %v (%d writes)", server, got, err, st.updates)
		}
	}
}

// An older line is fenced; the same epoch held by another server is not
// this holder's; an absent HEAD is refused (nothing is created).
func TestStart_Refusals(t *testing.T) {
	if _, _, err := start(t, startHead(4, "s1/r1", "k4"), HolderID{Epoch: 3, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k3"}); !errors.Is(err, ErrStaleEpoch) {
		t.Errorf("older line: %v; want ErrStaleEpoch", err)
	}
	if _, _, err := start(t, startHead(4, "s1/r1", "k4"), HolderID{Epoch: 4, Holder: "s2/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k9", Fresh: true, Seedable: anySeedable}); !errors.Is(err, ErrNotHolder) {
		t.Errorf("same epoch, another server: %v; want ErrNotHolder", err)
	}
	st := &memHead{}
	if _, err := Start(context.Background(), st, &fakeLease{ended: true}, HolderID{Epoch: 1, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k1", Fresh: true, Seedable: anySeedable}); !errors.Is(err, ErrHeadAbsent) || st.updates != 0 {
		t.Errorf("absent HEAD: %v", err)
	}
	if _, _, err := start(t, startHead(3, "s1/r1", "k3"), HolderID{Epoch: 4, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}}); err == nil {
		t.Error("start without a local claim id accepted")
	}
}

// A same-host forced move put a fresh root Y at epoch 4 (claim k4, its
// image published); the old root X (claim k3, data) is remounted while the
// master grants epoch 5: X's data predates Y's image, so it is refused as
// stale and fenced, HEAD unchanged. At a higher epoch only a fresh root, or
// the root whose claim id HEAD holds (the continuing lineage), claims.
func TestStart_HigherEpochRefusesAnOlderLineage(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("image published at epoch 4"))}
	h := startHead(4, "s1/r1", "k4")
	h.Current = &rn
	st, _, err := start(t, h, HolderID{Epoch: 5, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k3"})
	if !errors.Is(err, ErrStaleLocalRoot) || !errors.Is(err, ErrNotHolder) {
		t.Fatalf("old root at a higher epoch: %v; want ErrStaleLocalRoot, fenced", err)
	}
	if st.updates != 0 || st.h.ClaimID != "k4" || st.h.Epoch != 4 || *st.h.Current != rn {
		t.Fatalf("refused start changed HEAD: %+v (%d writes)", st.h, st.updates)
	}
	// The continuing lineage (the root HEAD's claim id names) claims at the
	// higher epoch, keeping its data.
	st2, got, err := start(t, h, HolderID{Epoch: 5, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k4"})
	if err != nil || st2.updates != 1 || got.Epoch != 5 || got.ClaimID != "k4" {
		t.Fatalf("continuing lineage at epoch 5: %+v %v (%d writes)", got, err, st2.updates)
	}
}

// A root carrying HEAD's claim id but synced only below HEAD's published
// position is an old copy of the right directory: refused as stale, at its
// own epoch and at a higher one, HEAD unchanged.
func TestStart_OldCopyOfTheRightRootRefused(t *testing.T) {
	h := startHead(4, "s1/r1", "k4")
	h.Position = 100
	for _, epoch := range []uint64{4, 5} {
		st, _, err := start(t, h, HolderID{Epoch: epoch, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k4", Position: 99})
		if !errors.Is(err, ErrStaleLocalRoot) || st.updates != 0 {
			t.Fatalf("root at position 99 under HEAD at 100, epoch %d: %v (%d writes); want ErrStaleLocalRoot, no write", epoch, err, st.updates)
		}
		st, _, err = start(t, h, HolderID{Epoch: epoch, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k4", Position: 100})
		if err != nil {
			t.Fatalf("root at HEAD's position, epoch %d: %v", epoch, err)
		}
	}
}

// Killed at the claim's HEAD write (before it applies, and applied with
// its reply lost), the holder restarts with the same local root: the claim
// id it writes is the one its marker already carries, so the re-run either
// claims or finds its own claim and continues; it is never refused.
func TestStart_InterruptAtTheClaim(t *testing.T) {
	for _, fresh := range []bool{true, false} {
		local := LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k3", Fresh: fresh, Seedable: anySeedable}
		for _, after := range []bool{false, true} {
			h := startHead(3, "s1/r1", "k3")
			if fresh {
				h = startHead(3, "s1/r1", "k-previous")
				local.ClaimID = "k-fresh"
			}
			st := &memHead{h: &h, killAt: 1, after: after}
			line := HolderID{Epoch: 4, Holder: "s1/r1"}
			if _, err := Start(context.Background(), st, &fakeLease{ended: true}, line, local); !errors.Is(err, errKilled) {
				t.Fatalf("killed start: %v", err)
			}
			st.restart()
			got, err := Start(context.Background(), st, &fakeLease{ended: true}, line, local)
			if err != nil || got.Epoch != 4 || got.ClaimID != local.ClaimID {
				t.Fatalf("restart (fresh %t, applied %t): %+v %v; want claimed or continued at epoch 4 with %q", fresh, after, got, err, local.ClaimID)
			}
		}
	}
}

// A fresh root stays fresh until its restore's base is durable: claimed at
// a higher epoch, crashed before the base is down, restarted at the same
// epoch with the same marker and still fresh, it continues the restore.
// Reported as non-fresh at position 0 under HEAD's published position, the
// same root would be refused as an old copy; the caller reports Fresh until
// the base is durable.
func TestStart_FreshAcrossACrashContinuesTheRestore(t *testing.T) {
	h := startHead(3, "s1/r1", "k-prev")
	h.Position = 50
	st := &memHead{h: &h}
	line := HolderID{Epoch: 4, Holder: "s1/r1"}
	if _, err := Start(context.Background(), st, &fakeLease{ended: true}, line, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k4", Fresh: true, Seedable: anySeedable}); err != nil {
		t.Fatal(err)
	}
	got, err := Start(context.Background(), st, &fakeLease{ended: true}, line, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k4", Fresh: true, Seedable: anySeedable})
	if err != nil || got.ClaimID != "k4" || got.Epoch != 4 {
		t.Fatalf("restart of a still-fresh root: %+v %v; want continue", got, err)
	}
	if _, err := Start(context.Background(), st, &fakeLease{ended: true}, line, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k4", Position: 0}); !errors.Is(err, ErrStaleLocalRoot) {
		t.Fatalf("the same root reported non-fresh at 0 under 50: %v; want ErrStaleLocalRoot", err)
	}
}

// A deleting volume does not start: at the holder's own epoch Start falls
// through to the claim, which refuses a deleting HEAD.
func TestStart_DeletingVolumeRefused(t *testing.T) {
	h := startHead(3, "s1/r1", "k3")
	h.State = StateDeleting
	for _, line := range []HolderID{{Epoch: 3, Holder: "s1/r1"}, {Epoch: 4, Holder: "s1/r1"}} {
		st, _, err := start(t, h, line, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k3"})
		if !errors.Is(err, ErrVolumeDeleting) || st.updates != 0 {
			t.Fatalf("start at epoch %d of a deleting volume: %v (%d writes); want ErrVolumeDeleting", line.Epoch, err, st.updates)
		}
	}
}

// A planned move away and back finds the old directory on the first host:
// another root claimed in between, so it is refused as stale, and the
// error names the cure.
func TestStart_StaleRootNamesTheCure(t *testing.T) {
	_, _, err := start(t, startHead(5, "s2/r1", "k5"), HolderID{Epoch: 6, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k3"})
	if !errors.Is(err, ErrStaleLocalRoot) || !strings.Contains(err.Error(), StaleLocalRootCure) {
		t.Fatalf("old directory after a move away and back: %v; want ErrStaleLocalRoot naming %q", err, StaleLocalRootCure)
	}
}

// Start validates the local root against the very HEAD its claim updates:
// if HEAD changes after Start's first view (another lineage claims, or the
// published position rises past the local root), the claim is refused,
// never written over the newer HEAD.
func TestStart_ValidatesAgainstTheHeadItUpdates(t *testing.T) {
	for name, change := range map[string]func(h *Head){
		"another lineage claims":   func(h *Head) { h.Epoch, h.Holder, h.ClaimID = 4, "s2/r1", "k-other" },
		"published position rises": func(h *Head) { h.Position = 100 },
	} {
		t.Run(name, func(t *testing.T) {
			h := startHead(3, "s1/r1", "k3")
			h.Position = 50
			st := &memHead{h: &h}
			st.afterRead = func() { change(st.h) }
			_, err := Start(context.Background(), st, &fakeLease{ended: true}, HolderID{Epoch: 5, Holder: "s1/r1"}, LocalRoot{Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k3", Position: 50})
			if !errors.Is(err, ErrStaleLocalRoot) || st.updates != 0 {
				t.Fatalf("start after HEAD changed under it: %v (%d writes); want ErrStaleLocalRoot, nothing written", err, st.updates)
			}
		})
	}
}
