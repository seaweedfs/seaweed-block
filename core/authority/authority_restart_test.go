package authority

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// ============================================================
// P14 S5 — Restart / Reload End-to-End Tests
//
// Covers proof matrix rows 1, 2, 3, 4, 10 through the full
// Publisher + Store path. Row 9 (single-owner lock), rows 5, 6,
// 7, 8, 11, 12 live in authority_store_test.go.
//
// A "restart" is simulated by:
//   1. Constructing Publisher A with the store, performing mints.
//   2. Closing Publisher A's context / throwing away the value.
//   3. Constructing Publisher B with the SAME store directory.
//   4. Asserting Publisher B's observable state matches A's last
//      persisted state.
// ============================================================

// Row 1: current authority line survives bounded restart
func TestDurableAuthority_ReloadReproducesCurrentLine(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Publisher A mints (v1, r1) and reassigns to r2.
	storeA, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatalf("store A: %v", err)
	}
	pubA := NewPublisher(NewStaticDirective(nil), WithStore(storeA))
	if err := pubA.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("apply Bind: %v", err)
	}
	if err := pubA.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r2",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("apply Reassign: %v", err)
	}
	beforeBasis, ok := pubA.LastAuthorityBasis("v1", "r2")
	if !ok || beforeBasis.Epoch != 2 || beforeBasis.EndpointVersion != 1 {
		t.Fatalf("Publisher A pre-restart basis wrong: %+v", beforeBasis)
	}
	_ = storeA.Close()

	// Phase 2: simulate restart — new store + new publisher on
	// the same directory.
	storeB, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatalf("store B: %v", err)
	}
	defer storeB.Close()
	pubB := NewPublisher(NewStaticDirective(nil), WithStore(storeB))

	// Reload must reproduce the current authority line.
	afterBasis, ok := pubB.LastAuthorityBasis("v1", "r2")
	if !ok {
		t.Fatal("Publisher B missing reloaded authority for r2")
	}
	if afterBasis != beforeBasis {
		t.Fatalf("reloaded basis mismatch: before=%+v after=%+v", beforeBasis, afterBasis)
	}
}

// Row 2: reload does not revive stale authority over newer
// stored truth.
//
// Scenario: Publisher A writes r1@1 then advances to r2@2
// via Reassign. The durable store now contains r2@2 (and the
// per-volume "current line" rule: only r2's record is on disk;
// r1's per-slot state is NOT persisted, per sketch §5). After
// restart, Publisher B must see r2@2 — NOT r1@1. Any
// hypothetical code path that reloaded a stale "pre-failover"
// record would lose r2's advance.
func TestDurableAuthority_StalePreFailoverRecordRejectedOnReload(t *testing.T) {
	dir := t.TempDir()

	storeA, _ := NewFileAuthorityStore(dir)
	pubA := NewPublisher(NewStaticDirective(nil), WithStore(storeA))
	_ = pubA.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	})
	_ = pubA.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r2",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentReassign,
	})
	_ = storeA.Close()

	// Restart.
	storeB, _ := NewFileAuthorityStore(dir)
	defer storeB.Close()
	pubB := NewPublisher(NewStaticDirective(nil), WithStore(storeB))

	// Post-restart: r2 must be the current volume authority line.
	line, ok := pubB.VolumeAuthorityLine("v1")
	if !ok {
		t.Fatal("VolumeAuthorityLine returned empty after restart")
	}
	if line.ReplicaID != "r2" || line.Epoch != 2 {
		t.Fatalf("expected r2@2 as current line, got %+v", line)
	}
	// r1 must NOT exist as a fresh per-slot record — per-volume
	// only means old slot state is intentionally not persisted.
	if _, ok := pubB.LastAuthorityBasis("v1", "r1"); ok {
		t.Fatal("old slot r1 must not reappear post-restart (per-volume current-line rule)")
	}
}

// Row 3: Publisher continues authority advance from reloaded
// truth rather than zero-value state.
func TestDurableAuthority_PublisherAdvancesFromReloaded(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: advance to epoch=3 (Bind + 2x Reassign).
	storeA, _ := NewFileAuthorityStore(dir)
	pubA := NewPublisher(NewStaticDirective(nil), WithStore(storeA))
	_ = pubA.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1", Intent: IntentBind})
	_ = pubA.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r2", DataAddr: "d2", CtrlAddr: "c2", Intent: IntentReassign})
	_ = pubA.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r3", DataAddr: "d3", CtrlAddr: "c3", Intent: IntentReassign})
	_ = storeA.Close()

	// Phase 2: restart. Continue advancing. The next Reassign
	// must mint epoch=4, NOT epoch=1.
	storeB, _ := NewFileAuthorityStore(dir)
	defer storeB.Close()
	pubB := NewPublisher(NewStaticDirective(nil), WithStore(storeB))
	if err := pubB.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1-new", CtrlAddr: "c1-new",
		Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("post-restart Reassign: %v", err)
	}
	after, ok := pubB.LastAuthorityBasis("v1", "r1")
	if !ok {
		t.Fatal("post-restart authority for r1 missing")
	}
	if after.Epoch != 4 {
		t.Fatalf("post-restart Reassign minted epoch=%d; want 4 (=max(3)+1)", after.Epoch)
	}
}

// Row 4: Controller after restart consumes reloaded current
// line, not zero-value truth.
func TestDurableAuthority_ControllerSeesReloadedLineAtBoot(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: mint a line.
	storeA, _ := NewFileAuthorityStore(dir)
	pubA := NewPublisher(NewStaticDirective(nil), WithStore(storeA))
	_ = pubA.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	})
	_ = pubA.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r2",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentReassign,
	})
	_ = storeA.Close()

	// Phase 2: reload. Attach a TopologyController using the
	// reloaded Publisher as its AuthorityBasisReader.
	storeB, _ := NewFileAuthorityStore(dir)
	defer storeB.Close()
	pubB := NewPublisher(NewStaticDirective(nil), WithStore(storeB))

	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	}, pubB)

	// The controller's view of the current volume line should
	// come from pubB (reloaded). Submit a snapshot where the
	// observed Authority matches the reloaded state — the
	// controller's basisMatchesLine must accept it. If reload
	// were broken and pubB returned zero-valued line, the stale
	// check would drop the snapshot.
	snap := ClusterSnapshot{
		CollectedRevision: 1,
		CollectedAt:       time.Now(),
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{{
			VolumeID: "v1",
			Authority: AuthorityBasis{
				Assigned: true, ReplicaID: "r2", Epoch: 2, EndpointVersion: 1,
				DataAddr: "d2", CtrlAddr: "c2",
			},
			Slots: []ReplicaCandidate{
				{ReplicaID: "r1", ServerID: "s1", DataAddr: "d1", CtrlAddr: "c1", Reachable: true, ReadyForPrimary: true, Eligible: true},
				{ReplicaID: "r2", ServerID: "s2", DataAddr: "d2", CtrlAddr: "c2", Reachable: true, ReadyForPrimary: true, Eligible: true},
				{ReplicaID: "r3", ServerID: "s3", DataAddr: "d3", CtrlAddr: "c3", Reachable: true, ReadyForPrimary: true, Eligible: true},
			},
		}},
	}
	if err := ctrl.SubmitObservedState(snap, SupportabilityReport{}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	// If the controller saw reloaded truth, the basis matched
	// and it had the chance to evaluate the volume. Any no-emit
	// outcome (current r2 is acceptable, unchanged) is fine —
	// what matters is the controller did NOT treat this as a
	// stale snapshot to drop.
	//
	// Evidence: LastUnsupported must not have a "snapshot
	// authority outside accepted topology" or similar message
	// that would indicate reloaded basis was wrong.
	if ev, ok := ctrl.LastUnsupported("v1"); ok {
		t.Fatalf("controller reported unsupported for reloaded v1: %+v", ev)
	}
}

// Row 10: Publisher rolls in-memory state back on store.Put error
func TestDurableAuthority_PutFailureRollsBackInMemoryState(t *testing.T) {
	failing := &failingStore{failOnPut: true}
	pub := NewPublisher(NewStaticDirective(nil), WithStore(failing))

	err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	})
	if err == nil {
		t.Fatal("apply must fail when store.Put fails")
	}
	if !errors.Is(err, errFailingStorePut) {
		t.Fatalf("error must wrap failing-store sentinel, got %v", err)
	}

	// In-memory state must NOT reflect the failed mint. A
	// subsequent Bind for the SAME key must succeed — the
	// original Bind was rolled back.
	failing.failOnPut = false
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("second Bind must succeed after rollback: %v", err)
	}
	basis, ok := pub.LastAuthorityBasis("v1", "r1")
	if !ok {
		t.Fatal("expected basis after successful second Bind")
	}
	if basis.Epoch != 1 {
		t.Fatalf("epoch after rollback+rebind: got %d want 1", basis.Epoch)
	}
}

// Bonus: reload with an empty store yields a fresh Publisher.
func TestDurableAuthority_EmptyStoreReloadIsFreshState(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileAuthorityStore(dir)
	defer store.Close()
	pub := NewPublisher(NewStaticDirective(nil), WithStore(store))

	// No state reloaded.
	if _, ok := pub.LastAuthorityBasis("v1", "r1"); ok {
		t.Fatal("empty store reload must produce no state")
	}
	if _, ok := pub.VolumeAuthorityLine("v1"); ok {
		t.Fatal("empty store reload must produce no volume line")
	}

	// A Bind on an empty-store Publisher behaves like a fresh
	// first-time Bind.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind on empty-reloaded publisher: %v", err)
	}
	basis, ok := pub.LastAuthorityBasis("v1", "r1")
	if !ok || basis.Epoch != 1 {
		t.Fatalf("fresh Bind post-empty-reload: got %+v", basis)
	}
}

// Bonus: end-to-end flow with Run + Bridge still works against
// a store-backed publisher.
func TestDurableAuthority_StoreBackedPublisherRunsNormalRoute(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileAuthorityStore(dir)
	defer store.Close()

	dirDir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "v1", ReplicaID: "r1",
			DataAddr: "d1", CtrlAddr: "c1",
			Intent: IntentBind},
	})
	pub := NewPublisher(dirDir, WithStore(store))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, cancelSub := pub.Subscribe("v1", "r1")
	defer cancelSub()
	go func() { _ = pub.Run(ctx) }()

	select {
	case got := <-ch:
		if got.Epoch != 1 {
			t.Fatalf("expected epoch=1 on run mint, got %+v", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("no publication received on store-backed route")
	}

	// Confirm the record landed on disk.
	recs, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(recs) != 1 || recs[0].VolumeID != "v1" || recs[0].Epoch != 1 {
		t.Fatalf("store did not receive the mint: %+v", recs)
	}
}

// ============================================================
// Fakes
// ============================================================

var errFailingStorePut = errors.New("failing-store: forced Put error")

type failingStore struct {
	failOnPut bool
	records   []DurableRecord
}

func (f *failingStore) Load() ([]DurableRecord, error) {
	out := make([]DurableRecord, len(f.records))
	copy(out, f.records)
	return out, nil
}
func (f *failingStore) LoadWithSkips() ([]DurableRecord, []error, error) {
	out := make([]DurableRecord, len(f.records))
	copy(out, f.records)
	return out, nil, nil
}
func (f *failingStore) Put(r DurableRecord) error {
	if f.failOnPut {
		return errFailingStorePut
	}
	f.records = append(f.records, r)
	return nil
}
func (f *failingStore) Close() error { return nil }

// Silence unused import warning if adapter is not referenced
// directly in this file's test bodies.
var _ = adapter.AssignmentInfo{}
