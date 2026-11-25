package durable_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func viewAs(replica string, epoch uint64) *stubView {
	return newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: replica, Epoch: epoch, EndpointVersion: 1}))
}

func writeSync(t *testing.T, s storage.LogicalStorage, lba uint32, fill byte) {
	t.Helper()
	if _, err := s.Write(lba, block(fill)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
}

func headHolder(t *testing.T, remote *storage.MemChunkRemote) storage.ChunkHead {
	t.Helper()
	h, err := remote.GetHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// movedAwayAndBack runs A (epoch 3) cut then uncut synced writes, A lost;
// B (epoch 4, fresh) claims, writes LBA 0 = 0xB1, syncs, cuts, closes. It
// returns the remote and A's surviving root.
func movedAwayAndBack(t *testing.T) (*testutil.LayoutFixture, string) {
	t.Helper()
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	rootA, rootB := t.TempDir(), t.TempDir()

	// 1. A (epoch 3) cuts LBA 0 and LBA 16, then keeps writing without a cut.
	pA := layoutChunkProvider(t, rootA, fixture, viewAs("rA", 3), 0)
	sA := openRecovered(t, pA)
	writeSync(t, sA, 0, 0xA1)
	writeSync(t, sA, 16, 0xA1)
	if _, err := pA.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatalf("A capture: %v", err)
	}
	writeSync(t, sA, 0, 0xA2)          // never cut
	if err := pA.Close(); err != nil { // A is lost; its root stays on disk
		t.Fatal(err)
	}

	// 2. A forced move to B (epoch 4, fresh root): B claims HEAD, writes,
	// syncs and cuts.
	pB := layoutChunkProvider(t, rootB, fixture, viewAs("rB", 4), 0)
	sB := openRecovered(t, pB)
	writeSync(t, sB, 0, 0xB1)
	if _, err := pB.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatalf("B capture: %v", err)
	}
	if err := pB.Close(); err != nil {
		t.Fatal(err)
	}
	return fixture, rootA
}

// A workspace moves A -> B -> A, and A's old durable root is still on disk.
// A must not serve its stale image or cut it over B's acknowledged, synced
// and published writes: the existing root is refused (typed, visible, not
// served, HEAD unchanged). Once the old root is removed, A attaches fresh
// from HEAD and reads B's data.
func TestWALStoreChunk_MoveBackOntoAnOldRootIsRefused(t *testing.T) {
	ctx := context.Background()
	fixture, rootA := movedAwayAndBack(t)
	before := layoutHead(t, fixture)

	// 3. Back to A (epoch 5) over its old root: refused.
	pA2 := layoutChunkProvider(t, rootA, fixture, viewAs("rA", 5), 0)
	if _, err := pA2.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	_, err := pA2.RecoverVolume(ctx, "v1")
	if !errors.Is(err, layout.ErrStaleLocalRoot) || !errors.Is(err, layout.ErrNotHolder) {
		t.Fatalf("move back onto the old root: %v; want ErrStaleLocalRoot (fenced)", err)
	}
	b, err := pA2.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Read(ctx, 0, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
		t.Errorf("read after a refused move back: %v; want ErrNotReady", err)
	}
	if _, err := b.Write(ctx, 0, block(0xA3)); !errors.Is(err, frontend.ErrNotReady) {
		t.Errorf("write after a refused move back: %v; want ErrNotReady", err)
	}
	if _, err := pA2.CaptureVolume(ctx, "v1"); err == nil {
		t.Error("a refused move back still cuts")
	}
	var evidence string
	for _, st := range pA2.DurableStatuses() {
		if st.VolumeID == "v1" {
			evidence = st.Evidence
		}
	}
	if !strings.Contains(evidence, layout.StaleLocalRootCure) {
		t.Errorf("status evidence %q does not tell the operator what to do with root %s", evidence, rootA)
	}
	if after := layoutHead(t, fixture); after.Gen != before.Gen || after.Holder != before.Holder || after.Epoch != before.Epoch {
		t.Errorf("HEAD changed by a refused move back: %+v -> %+v", before, after)
	}
	if h := layoutHeadBlock(t, fixture, 0); !bytes.Equal(h, block(0xB1)) {
		t.Errorf("HEAD LBA 0 = %02x after a refused move back; want B's %02x", h[0], 0xB1)
	}
	if err := pA2.Close(); err != nil {
		t.Fatal(err)
	}

	// 4. The operator removes the old root: A attaches fresh and serves B's
	// image (and A's own earlier published chunk).
	entries, err := os.ReadDir(rootA)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(rootA, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	pA3 := layoutChunkProvider(t, rootA, fixture, viewAs("rA", 5), 0)
	sA3 := openRecovered(t, pA3)
	if got, err := sA3.Read(0); err != nil || !bytes.Equal(got, block(0xB1)) {
		t.Fatalf("fresh attach after removing the root: LBA 0 err %v; want B's %02x", err, 0xB1)
	}
	if got, err := sA3.Read(16); err != nil || !bytes.Equal(got, block(0xA1)) {
		t.Fatalf("fresh attach after removing the root: LBA 16 err %v; want the published %02x", err, 0xA1)
	}
}

// The same holder reattaching its own root at a newer epoch is not a move
// back: it attaches, serves its data and cuts.
func TestWALStoreChunk_SameHolderReattachAtNewerEpoch(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	root := t.TempDir()
	p := chunkProvider(t, root, remote, viewAs("rA", 3), 0)
	s := openRecovered(t, p)
	writeSync(t, s, 0, 0xC1)
	if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	writeSync(t, s, 32, 0xC2) // local only
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := chunkProvider(t, root, remote, viewAs("rA", 4), 0)
	s2 := openRecovered(t, p2)
	if got, _ := s2.Read(32); !bytes.Equal(got, block(0xC2)) {
		t.Fatal("same-holder reattach lost its local write")
	}
	if _, err := p2.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatalf("same-holder reattach cannot cut: %v", err)
	}
	if !bytes.Equal(headBlock(t, remote, 32), block(0xC2)) {
		t.Fatal("HEAD lacks the reattached holder's write")
	}
}

// An existing root with no HEAD at all (generation 0) is this holder's own
// image: it attaches.
func TestWALStoreChunk_ExistingRootWithoutHeadAttaches(t *testing.T) {
	root := t.TempDir()
	p := chunkProvider(t, root, storage.NewMemChunkRemote(), viewAs("rA", 3), 0)
	s := openRecovered(t, p)
	writeSync(t, s, 0, 0xD1)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	empty := storage.NewMemChunkRemote() // no HEAD
	if h := headHolder(t, empty); h.Gen != 0 {
		t.Fatalf("empty remote HEAD gen %d", h.Gen)
	}
	p2 := chunkProvider(t, root, empty, viewAs("rA", 3), 0)
	s2 := openRecovered(t, p2)
	if got, _ := s2.Read(0); !bytes.Equal(got, block(0xD1)) {
		t.Fatal("existing root without a HEAD lost its data")
	}
}

// A holder whose HEAD a newer epoch has taken over never cuts over it.
func TestWALStoreChunk_CutRefusedAfterANewerEpochTakesOver(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	pA := chunkProvider(t, t.TempDir(), remote, viewAs("rA", 3), 0)
	sA := openRecovered(t, pA)
	writeSync(t, sA, 0, 0xE1)
	if _, err := pA.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	pB := chunkProvider(t, t.TempDir(), remote, viewAs("rB", 4), 0)
	openRecovered(t, pB) // takeover raises HEAD to epoch 4
	before := headHolder(t, remote)
	if before.Epoch != 4 || before.Holder != "rB" {
		t.Fatalf("takeover HEAD %+v", before)
	}
	writeSync(t, sA, 0, 0xE2)
	if _, err := pA.CaptureVolume(ctx, "v1"); err == nil {
		t.Fatal("the epoch-3 holder cut over the epoch-4 HEAD")
	}
	if after := headHolder(t, remote); after.Gen != before.Gen || after.Holder != "rB" {
		t.Fatalf("HEAD moved after the stale holder's cut: %+v -> %+v", before, after)
	}
	if !bytes.Equal(headBlock(t, remote, 0), block(0xE1)) {
		t.Fatal("HEAD lost the published image")
	}
}

// The refusal is visible on the deferred path too: A recovers before the
// master's assignment, and the attach that runs when it arrives is refused
// with the same evidence.
func TestWALStoreChunk_MoveBackRefusedOnDeferredAttach(t *testing.T) {
	ctx := context.Background()
	fixture, rootA := movedAwayAndBack(t)
	before := layoutHead(t, fixture)
	view := newStubView(frontend.Projection{})
	pA2 := layoutChunkProvider(t, rootA, fixture, view, 0)
	if _, err := pA2.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := pA2.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("recovery before the assignment: %v", err)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "rA", Epoch: 5, EndpointVersion: 1}))
	deadline := time.Now().Add(5 * time.Second)
	var st durable.VolumeStatus
	for time.Now().Before(deadline) {
		for _, s := range pA2.DurableStatuses() {
			if s.VolumeID == "v1" {
				st = s
			}
		}
		if strings.Contains(st.Evidence, "remove or quarantine the local root") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st.Operational || !strings.Contains(st.Evidence, "remove or quarantine the local root") {
		t.Fatalf("deferred attach of the old root: operational %t evidence %q", st.Operational, st.Evidence)
	}
	if after := layoutHead(t, fixture); after.Gen != before.Gen || after.Holder != before.Holder {
		t.Fatalf("HEAD changed by a refused deferred attach: %+v -> %+v", before, after)
	}
}
