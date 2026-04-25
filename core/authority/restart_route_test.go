package authority

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// ============================================================
// P14 S7 — L1 Component Route Tests (restart)
//
// These tests close rows 1-7 of the S7 proof matrix (sketch
// §13.1). They exercise the restart-path composition:
//
//   FileAuthorityStore + StoreLock
//     -> Publisher(WithStore, reloaded synchronously)
//     -> TopologyController (reloaded publisher as reader)
//     -> ObservationHost (same reader; controller as sink)
//     -> VolumeReplicaAdapter (constructed BEFORE bridge)
//     -> VolumeBridge subscriptions (catch-up delivery feeds
//        the adapter immediately)
//
// The construction order is pinned by sketch §5; tests that
// deviate from it are route-drift and are rejected.
//
// NO test calls adapter.OnAssignment directly. Every delivery
// goes through VolumeBridge's per-(volumeID, replicaID)
// subscription.
// ============================================================

// ============================================================
// Test-side CommandExecutor
//
// restartExecutor mirrors the production adapter.CommandExecutor
// surface without doing any real transport work. It is hook-
// driven: each test configures exactly the fence / session
// behavior it wants, leaving the defaults in place for
// everything else.
// ============================================================

type restartExecutor struct {
	mu sync.Mutex

	onStart   adapter.OnSessionStart
	onClose   adapter.OnSessionClose
	onFence   adapter.OnFenceComplete

	// fenceMode drives what Fence() does with incoming calls.
	//   "autoSuccess" (default): fire OnFenceComplete{Success:true} inline
	//   "withhold":              do not fire the callback; test fires it later
	fenceMode string

	// withheldFences captures parameters of every Fence() call
	// made while fenceMode == "withhold", so tests can replay a
	// late callback later.
	withheldFences []adapter.FenceResult
}

func newRestartExecutor() *restartExecutor {
	return &restartExecutor{fenceMode: "autoSuccess"}
}

func (e *restartExecutor) SetOnSessionStart(fn adapter.OnSessionStart) { e.onStart = fn }
func (e *restartExecutor) SetOnSessionClose(fn adapter.OnSessionClose) { e.onClose = fn }
func (e *restartExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete) {
	e.mu.Lock()
	e.onFence = fn
	e.mu.Unlock()
}

func (e *restartExecutor) Probe(replicaID, dataAddr, ctrlAddr string, sessionID, epoch, endpointVersion uint64) adapter.ProbeResult {
	return adapter.ProbeResult{
		ReplicaID:         replicaID,
		Success:           true,
		EndpointVersion:   endpointVersion,
		TransportEpoch:    epoch,
		ReplicaFlushedLSN: 100,
		PrimaryTailLSN:    10,
		PrimaryHeadLSN:    100,
	}
}

func (e *restartExecutor) StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	return nil
}
func (e *restartExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	return nil
}
func (e *restartExecutor) StartRecoverySession(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64, contentKind engine.RecoveryContentKind, policy engine.RecoveryRuntimePolicy) error {
	return nil
}
func (e *restartExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {}
func (e *restartExecutor) PublishHealthy(replicaID string)                                     {}
func (e *restartExecutor) PublishDegraded(replicaID string, reason string)                     {}

func (e *restartExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	e.mu.Lock()
	mode := e.fenceMode
	cb := e.onFence
	result := adapter.FenceResult{
		ReplicaID:       replicaID,
		SessionID:       sessionID,
		Epoch:           epoch,
		EndpointVersion: endpointVersion,
		Success:         true,
	}
	if mode == "withhold" {
		e.withheldFences = append(e.withheldFences, result)
	}
	e.mu.Unlock()

	if mode == "autoSuccess" && cb != nil {
		cb(result)
	}
	return nil
}

// setFenceMode switches between autoSuccess and withhold.
func (e *restartExecutor) setFenceMode(mode string) {
	e.mu.Lock()
	e.fenceMode = mode
	e.mu.Unlock()
}

// ============================================================
// restartRoute — the full S7 stack, composed per sketch §5
// ============================================================

type restartRoute struct {
	dir      string
	clk      *fakeClock
	lock     *StoreLock
	store    AuthorityStore
	pub      *Publisher
	ctrl     *TopologyController
	host     *ObservationHost
	hostRdr  *snapshottingReader
	adpt     *adapter.VolumeReplicaAdapter
	exec     *restartExecutor
	ctx      context.Context
	cancel   context.CancelFunc
	volumeID string
	replicas []string
}

type restartRouteConfig struct {
	VolumeID    string
	Replicas    []string
	Topology    AcceptedTopology
	Directive   Directive
	RetryWindow time.Duration
	// FenceMode, if set, is applied to the restartExecutor
	// BEFORE the publisher / host live loops start. Avoids the
	// race where a post-construction setFenceMode call happens
	// after catch-up delivery has already dispatched a fence.
	// Empty string defaults to "autoSuccess".
	FenceMode string
}

// newRestartRoute composes the full S7 stack in the pinned
// order from sketch §5: lock -> store -> pub (reload) -> ctrl
// (reloaded reader) -> host (same reader) -> adapter -> bridge
// -> live loops. Any test that deviates from this order is
// rejected.
func newRestartRoute(t *testing.T, dir string, clk *fakeClock, cfg restartRouteConfig) *restartRoute {
	t.Helper()

	// Step 1: lock
	lock, err := AcquireStoreLock(dir)
	if err != nil {
		t.Fatalf("AcquireStoreLock: %v", err)
	}

	// Step 2: store
	store, err := NewFileAuthorityStore(dir)
	if err != nil {
		_ = lock.Release()
		t.Fatalf("NewFileAuthorityStore: %v", err)
	}

	// Step 3: pre-check already inside Publisher reload; just
	// call LoadWithSkips here as an explicit guard to match the
	// sparrow Bootstrap shape. Index-level failure fails closed.
	if _, _, err := store.LoadWithSkips(); err != nil {
		_ = store.Close()
		_ = lock.Release()
		t.Fatalf("LoadWithSkips: %v", err)
	}

	// Step 4: Publisher with WithStore; reload is synchronous.
	directive := cfg.Directive
	if directive == nil {
		directive = NewStaticDirective(nil)
	}
	pub := NewPublisher(directive, WithStore(store))

	// Step 5: Controller uses reloaded Publisher as its reader.
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
		RebalanceSkew:          100,
		RetryWindow:            cfg.RetryWindow,
	}, pub)
	if clk != nil {
		ctrl.SetNowForTest(clk.Now)
	}

	// Step 6: ObservationHost with the SAME reloaded publisher
	// as its builder reader, wrapped by snapshottingReader so
	// stale-observation tests can freeze the view.
	hostRdr := newSnapshottingReader(pub)
	now := time.Now
	if clk != nil {
		now = clk.Now
	}
	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 30 * time.Second, PendingGrace: 10 * time.Millisecond},
		Topology:  cfg.Topology,
		Sink:      ctrl,
		Reader:    hostRdr,
		Now:       now,
	})

	// Step 7: adapter BEFORE bridge (sketch §5 pin).
	exec := newRestartExecutor()
	if cfg.FenceMode != "" {
		// Apply fence-mode config BEFORE any live loop runs.
		// Setting it later would race with catch-up delivery.
		exec.setFenceMode(cfg.FenceMode)
	}
	adpt := adapter.NewVolumeReplicaAdapter(exec)

	ctx, cancel := context.WithCancel(context.Background())

	// Step 8: bridge subscribes — catch-up delivery goes to the
	// adapter that was constructed in step 7. Use the public
	// VolumeBridge helper (not direct Bridge goroutines) so the
	// test exercises the actual lifecycle / fan-out / cancellation
	// surface production code uses.
	if len(cfg.Replicas) > 0 {
		go VolumeBridge(ctx, pub, adpt, cfg.VolumeID, cfg.Replicas...)
	}

	// Step 9: live loops.
	go func() { _ = pub.Run(ctx) }()
	host.Start(ctx)

	return &restartRoute{
		dir:      dir,
		clk:      clk,
		lock:     lock,
		store:    store,
		pub:      pub,
		ctrl:     ctrl,
		host:     host,
		hostRdr:  hostRdr,
		adpt:     adpt,
		exec:     exec,
		ctx:      ctx,
		cancel:   cancel,
		volumeID: cfg.VolumeID,
		replicas: cfg.Replicas,
	}
}

// close tears down the route cleanly. The order mirrors
// production shutdown: stop host, cancel context, close store,
// release lock. Returns any non-cleanup error; cleanup-phase
// errors are logged as residual per sketch §8.5 and do not
// fail tests, but store write/reload errors remain hard.
func (r *restartRoute) close(t *testing.T) {
	t.Helper()
	r.host.Stop()
	r.cancel()
	// Small drain so any in-flight pub.Run / bridge goroutines
	// observe cancellation before we release the store.
	time.Sleep(20 * time.Millisecond)
	if err := r.store.Close(); err != nil {
		t.Fatalf("store.Close: %v", err) // hard failure — store write/reload error
	}
	if err := r.lock.Release(); err != nil {
		// Windows residual-risk scope: lock release during
		// teardown may race with the temp-dir cleanup. Log but
		// do not fail; the correctness surface is the reload
		// assertions, not the cleanup.
		t.Logf("restart-route close: lock release residual: %v", err)
	}
}

// simulateRestart tears down a running route and returns a
// fresh one composed over the same store directory. Mirrors
// the real-binary restart shape while staying in-process for
// L1 coverage.
func simulateRestart(t *testing.T, prev *restartRoute, cfg restartRouteConfig) *restartRoute {
	t.Helper()
	dir := prev.dir
	clk := prev.clk
	prev.close(t)
	return newRestartRoute(t, dir, clk, cfg)
}

// ============================================================
// Helpers reused across the S7 L1 tests
// ============================================================

func threeSlotTopology(volumeID string) AcceptedTopology {
	return AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume(volumeID)}}
}

func waitForAdapterMode(t *testing.T, adpt *adapter.VolumeReplicaAdapter, want engine.Mode, deadline time.Duration) engine.ReplicaProjection {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if p := adpt.Projection(); p.Mode == want {
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	p := adpt.Projection()
	t.Fatalf("adapter did not reach %s within deadline, final=%+v", want, p)
	return p
}

// ============================================================
// Row 1: Reloaded authority reaches adapter via VolumeBridge.
// ============================================================

func TestS7_ReloadedAuthority_ReanchorsAdapterViaVolumeBridge(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))

	// Seed the durable store by running one directive-driven
	// mint through a full composed stack, then tear it down. The
	// second composition (the "restart") must see the reloaded
	// line reach the adapter via bridge catch-up alone.
	seed := newRestartRoute(t, dir, clk, restartRouteConfig{
		VolumeID: "vr",
		Replicas: []string{"r1"},
		Topology: threeSlotTopology("vr"),
		Directive: NewStaticDirective([]AssignmentAsk{{
			VolumeID: "vr", ReplicaID: "r1",
			DataAddr: "d1", CtrlAddr: "c1",
			Intent: IntentBind,
		}}),
	})
	_ = waitForAdapterMode(t, seed.adpt, engine.ModeHealthy, 2*time.Second)

	// Restart — new empty directive, same store dir. No mint
	// may happen this time; everything the adapter sees must
	// come from the reloaded publisher via bridge catch-up.
	restarted := simulateRestart(t, seed, restartRouteConfig{
		VolumeID:  "vr",
		Replicas:  []string{"r1"},
		Topology:  threeSlotTopology("vr"),
		Directive: NewStaticDirective(nil),
	})
	defer restarted.close(t)

	// Reloaded publisher must hold the line.
	line, ok := restarted.pub.VolumeAuthorityLine("vr")
	if !ok {
		t.Fatal("reload: VolumeAuthorityLine missing on vr")
	}
	if line.Epoch != 1 || line.ReplicaID != "r1" {
		t.Fatalf("reload line: got %+v want r1@1", line)
	}

	// Adapter must reach Healthy via bridge catch-up delivery,
	// without any test-side OnAssignment call.
	_ = waitForAdapterMode(t, restarted.adpt, engine.ModeHealthy, 2*time.Second)
}

// ============================================================
// Row 2: Restart recomputes desired from durable authority +
// observation; transient desired is NOT persisted.
// ============================================================

func TestS7_Restart_RecomputesDesiredNotTransientState(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))

	seed := newRestartRoute(t, dir, clk, restartRouteConfig{
		VolumeID: "vr",
		Replicas: []string{"r1"},
		Topology: threeSlotTopology("vr"),
		Directive: NewStaticDirective([]AssignmentAsk{{
			VolumeID: "vr", ReplicaID: "r1",
			DataAddr: "d1", CtrlAddr: "c1",
			Intent: IntentBind,
		}}),
	})
	_ = waitForAdapterMode(t, seed.adpt, engine.ModeHealthy, 2*time.Second)

	// Drive observation so controller decides a Reassign(r2): r1
	// unreachable forces failover in the decision table. Hard
	// precondition: desired MUST be populated pre-restart; if
	// never populated, this test would otherwise pass vacuously.
	unreachableHBs := buildV1HeartbeatsVR(clk.Now(), map[string]bool{"r1": true})
	for _, hb := range unreachableHBs {
		_ = seed.host.IngestHeartbeat(hb)
	}
	deadline := time.Now().Add(2 * time.Second)
	preDesiredOK := false
	var preDesired DesiredAssignment
	for time.Now().Before(deadline) {
		if d, ok := seed.ctrl.DesiredFor("vr"); ok && d.Ask.ReplicaID == "r2" {
			preDesired = d
			preDesiredOK = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !preDesiredOK {
		t.Fatal("precondition: pre-restart desired Reassign(r2) was never populated — test cannot distinguish recompute from vacuous empty state")
	}

	// Restart. Immediately after construction, the new
	// controller's desired/stuck/unsupported maps MUST be empty
	// — no durable desired store.
	restarted := simulateRestart(t, seed, restartRouteConfig{
		VolumeID:  "vr",
		Replicas:  []string{"r1"},
		Topology:  threeSlotTopology("vr"),
		Directive: NewStaticDirective(nil),
	})
	defer restarted.close(t)

	if _, ok := restarted.ctrl.DesiredFor("vr"); ok {
		t.Fatal("after restart, desired must be empty until first post-restart observation refills it")
	}
	if _, ok := restarted.ctrl.LastConvergenceStuck("vr"); ok {
		t.Fatal("after restart, stuck evidence must be empty (no durable stuck store)")
	}
	if _, ok := restarted.ctrl.LastUnsupported("vr"); ok {
		t.Fatal("after restart, unsupported evidence must be empty")
	}

	// Re-ingest the SAME observation through the real host path.
	// Controller must recompute the same desired Reassign(r2)
	// from (reloaded authority + fresh observation).
	clk.Advance(1 * time.Second)
	for _, hb := range buildV1HeartbeatsVR(clk.Now(), map[string]bool{"r1": true}) {
		_ = restarted.host.IngestHeartbeat(hb)
	}
	deadline = time.Now().Add(2 * time.Second)
	postDesiredOK := false
	var postDesired DesiredAssignment
	for time.Now().Before(deadline) {
		if d, ok := restarted.ctrl.DesiredFor("vr"); ok && d.Ask.ReplicaID == "r2" {
			postDesired = d
			postDesiredOK = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !postDesiredOK {
		t.Fatal("after restart + same observation, desired was not recomputed to Reassign(r2)")
	}
	if postDesired.Ask.ReplicaID != preDesired.Ask.ReplicaID ||
		postDesired.Ask.Intent != preDesired.Ask.Intent {
		t.Fatalf("recomputed desired differs from pre-restart: pre=%+v post=%+v", preDesired.Ask, postDesired.Ask)
	}
}

// ============================================================
// Row 3: Stale pre-restart observation cannot move authority
// backward. The host's builder reader is frozen at a stale line
// (test simulation per sketch §9.1), while the controller's
// reader points at the live reloaded publisher. The stale
// observation falls into the basisMatchesLine-false / stale-
// basis path and does not cause any ask to be emitted.
// ============================================================

func TestS7_Restart_StaleObservationCannotMoveBackward(t *testing.T) {
	// Four sub-shapes; each starts from a fresh store to keep
	// assertions simple. All four expect: no ask reaches the
	// adapter beyond the single reloaded line.
	sub := []struct {
		name        string
		freeze      AuthorityBasis // fake stale line to feed through the host reader
	}{
		{"stale-epoch", AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 0, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}},
		{"stale-replica", AuthorityBasis{Assigned: true, ReplicaID: "r-old", Epoch: 1, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}},
		{"stale-endpointVersion", AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 0, DataAddr: "d1", CtrlAddr: "c1"}},
		{"unassigned", AuthorityBasis{Assigned: false}},
	}
	for _, c := range sub {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))

			seed := newRestartRoute(t, dir, clk, restartRouteConfig{
				VolumeID: "vr",
				Replicas: []string{"r1"},
				Topology: threeSlotTopology("vr"),
				Directive: NewStaticDirective([]AssignmentAsk{{
					VolumeID: "vr", ReplicaID: "r1",
					DataAddr: "d1", CtrlAddr: "c1",
					Intent: IntentBind,
				}}),
			})
			_ = waitForAdapterMode(t, seed.adpt, engine.ModeHealthy, 2*time.Second)

			restarted := simulateRestart(t, seed, restartRouteConfig{
				VolumeID:  "vr",
				Replicas:  []string{"r1"},
				Topology:  threeSlotTopology("vr"),
				Directive: NewStaticDirective(nil),
			})
			defer restarted.close(t)

			// Inject the stale view into the host's builder
			// reader. Controller's reader is still the live
			// reloaded publisher, so it sees the true line.
			restarted.hostRdr.forceSet("vr", c.freeze)

			// Ingest fresh heartbeats. Host rebuild will stamp
			// Authority from the frozen reader; controller will
			// see basisMatchesLine == false and skip emit.
			for _, hb := range buildV1HeartbeatsVR(clk.Now(), nil) {
				_ = restarted.host.IngestHeartbeat(hb)
			}
			// Small settle.
			time.Sleep(100 * time.Millisecond)

			// Publisher must still hold r1@1 — no backward move.
			line, ok := restarted.pub.VolumeAuthorityLine("vr")
			if !ok {
				t.Fatal("publisher line missing after stale observation")
			}
			if line.Epoch != 1 || line.ReplicaID != "r1" {
				t.Fatalf("stale observation moved authority backward: got %+v want r1@1", line)
			}
			// No desired should have been emitted — stale-basis
			// path does not call decideVolume.
			if d, ok := restarted.ctrl.DesiredFor("vr"); ok {
				t.Fatalf("stale observation emitted a desired ask: %+v", d)
			}
		})
	}
}

// ============================================================
// Row 4a (S7 restart-specific): old-slot per-replica state is
// NOT revived from the durable store.
//
// The durable AuthorityStore is one-record-per-volume (see
// authority_store.go §11-24: "one record per VolumeID; the
// store holds exactly one at any time"). When a failover
// Reassign advances the authority line from r1→r2, the
// per-volume file is overwritten; on restart, reloaded
// publisher state contains ONLY (vr, r2)@2.
//
// This is the explicit restart-contract claim S7 is
// responsible for: no old-slot per-replica line may be revived
// from durable storage. VolumeBridge for an old slot after
// restart therefore has nothing to deliver — that absence is
// by design, not an accident of the test.
//
// (The live-route monotonic-guard proof lives in
// TestS7_LiveRoute_OldSlotDeliveryRejectedByAdapter below,
// where in-memory publisher state legitimately retains both
// per-slot keys.)
// ============================================================

func TestS7_Restart_OldSlotNotRevivedFromDurable(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))

	// Seed: Bind r1@1, then failover Reassign to r2@2 on the
	// same volume. Both writes go through the real
	// publisher+store path.
	seed := newRestartRoute(t, dir, clk, restartRouteConfig{
		VolumeID: "vr",
		Replicas: []string{"r1", "r2"},
		Topology: threeSlotTopology("vr"),
		Directive: NewStaticDirective([]AssignmentAsk{
			{VolumeID: "vr", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1", Intent: IntentBind},
			{VolumeID: "vr", ReplicaID: "r2", DataAddr: "d2", CtrlAddr: "c2", Intent: IntentReassign},
		}),
	})
	// Wait until publisher line advances to r2@2.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if line, ok := seed.pub.VolumeAuthorityLine("vr"); ok && line.ReplicaID == "r2" && line.Epoch == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if line, ok := seed.pub.VolumeAuthorityLine("vr"); !ok || line.ReplicaID != "r2" || line.Epoch != 2 {
		t.Fatalf("seed failover did not mint r2@2: got ok=%v line=%+v", ok, line)
	}
	// Precondition: pre-restart publisher holds BOTH per-slot
	// keys in memory (confirming the live-route monotonic proof
	// below has a real setup to use).
	if _, ok := seed.pub.LastAuthorityBasis("vr", "r1"); !ok {
		t.Fatal("precondition: pre-restart publisher should hold (vr, r1)@1 in memory")
	}
	if _, ok := seed.pub.LastAuthorityBasis("vr", "r2"); !ok {
		t.Fatal("precondition: pre-restart publisher should hold (vr, r2)@2 in memory")
	}

	// Restart.
	restarted := simulateRestart(t, seed, restartRouteConfig{
		VolumeID:  "vr",
		Replicas:  []string{"r2"},
		Topology:  threeSlotTopology("vr"),
		Directive: NewStaticDirective(nil),
	})
	defer restarted.close(t)

	// Post-restart publisher MUST hold (vr, r2)@2 and MUST NOT
	// hold (vr, r1)@1 — the durable store is one-record-per-
	// volume, so the old slot is intentionally gone.
	if _, ok := restarted.pub.LastAuthorityBasis("vr", "r1"); ok {
		t.Fatal("old slot (vr, r1) must NOT be revived from durable reload (per-volume current-line rule)")
	}
	line, ok := restarted.pub.LastAuthorityBasis("vr", "r2")
	if !ok {
		t.Fatal("(vr, r2) must be present after restart")
	}
	if line.Epoch != 2 {
		t.Fatalf("(vr, r2) epoch after restart: got %d want 2", line.Epoch)
	}
	// And starting a VolumeBridge subscription for r1 on the
	// restarted publisher delivers NOTHING (no state for that
	// key). The adapter must still reach Healthy on r2@2 and
	// never see an r1 assignment through the real route.
	_ = waitForAdapterMode(t, restarted.adpt, engine.ModeHealthy, 2*time.Second)
	go VolumeBridge(restarted.ctx, restarted.pub, restarted.adpt, "vr", "r1")
	time.Sleep(100 * time.Millisecond)
	if p := restarted.adpt.Projection(); p.Epoch != 2 {
		t.Fatalf("adapter epoch after r1 bridge subscription: got %d want 2 (no state for (vr,r1) to deliver)", p.Epoch)
	}
}

// ============================================================
// Row 4b (live-route monotonic proof): old-slot AssignmentInfo
// delivery through VolumeBridge is rejected by the adapter's
// engine-level monotonic guard.
//
// NOT a restart test — restart deliberately discards the old
// slot (see row 4a). This is the complementary pre-restart
// proof: while the publisher is running in-memory, Reassign
// creates a new (vol, rN) key without deleting the old
// (vol, r1) key, so both per-replica keys legitimately exist.
// A late VolumeBridge subscription to the old slot must be
// routed to the adapter, and the adapter's engine-level
// monotonic guard must reject the lower-epoch assignment.
// ============================================================

func TestS7_LiveRoute_OldSlotDeliveryRejectedByAdapter(t *testing.T) {
	// No store, no restart — we want the in-memory publisher
	// state that legitimately holds both per-replica keys.
	dir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "vr", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1", Intent: IntentBind},
		{VolumeID: "vr", ReplicaID: "r2", DataAddr: "d2", CtrlAddr: "c2", Intent: IntentReassign},
	})
	pub := NewPublisher(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec := newRestartExecutor()
	adpt := adapter.NewVolumeReplicaAdapter(exec)

	go func() { _ = pub.Run(ctx) }()

	// Subscribe ONLY to r2 first. Adapter picks up r2@2 via the
	// real VolumeBridge catch-up path.
	go VolumeBridge(ctx, pub, adpt, "vr", "r2")

	_ = waitForAdapterMode(t, adpt, engine.ModeHealthy, 2*time.Second)
	projectionBefore := adpt.Projection()
	if projectionBefore.Epoch != 2 {
		t.Fatalf("adapter epoch before old-slot delivery: got %d want 2\nprojection=%+v",
			projectionBefore.Epoch, projectionBefore)
	}
	// Precondition: publisher still holds the in-memory
	// (vr, r1)@1 key (Reassign doesn't delete it).
	if r1Line, ok := pub.LastAuthorityBasis("vr", "r1"); !ok || r1Line.Epoch != 1 {
		t.Fatalf("precondition: in-memory publisher must still hold (vr, r1)@1; got ok=%v line=%+v", ok, r1Line)
	}

	// Now start a SECOND VolumeBridge subscription for r1 on
	// the same adapter. Publisher's in-memory (vr, r1)@1 IS
	// delivered as catch-up truth on the new subscription —
	// this is the real bridge-delivered old-slot AssignmentInfo.
	// The adapter's engine-level monotonic guard must reject it.
	go VolumeBridge(ctx, pub, adpt, "vr", "r1")

	time.Sleep(200 * time.Millisecond)

	projectionAfter := adpt.Projection()
	if projectionAfter.Epoch != 2 {
		t.Fatalf("old-slot r1@1 delivery regressed adapter epoch: before=%d after=%d full=%+v",
			projectionBefore.Epoch, projectionAfter.Epoch, projectionAfter)
	}
	if projectionAfter.Mode != projectionBefore.Mode {
		t.Fatalf("old-slot delivery changed adapter mode: %s -> %s", projectionBefore.Mode, projectionAfter.Mode)
	}
}

// ============================================================
// Row 5: Unsupported topology after restart records evidence
// through the observation path (conflicting primary claim).
// ============================================================

func TestS7_Restart_UnsupportedTopologyRecordsEvidence(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))

	seed := newRestartRoute(t, dir, clk, restartRouteConfig{
		VolumeID: "vr",
		Replicas: []string{"r1"},
		Topology: threeSlotTopology("vr"),
		Directive: NewStaticDirective([]AssignmentAsk{{
			VolumeID: "vr", ReplicaID: "r1",
			DataAddr: "d1", CtrlAddr: "c1",
			Intent: IntentBind,
		}}),
	})
	_ = waitForAdapterMode(t, seed.adpt, engine.ModeHealthy, 2*time.Second)

	restarted := simulateRestart(t, seed, restartRouteConfig{
		VolumeID:  "vr",
		Replicas:  []string{"r1"},
		Topology:  threeSlotTopology("vr"),
		Directive: NewStaticDirective(nil),
	})
	defer restarted.close(t)

	// Ingest conflicting-primary heartbeats: two servers claim
	// LocalRolePrimary for vr. Builder must mark the volume
	// Unsupported with ReasonConflictingPrimaryClaim, which the
	// controller records on its unsupported map.
	s1 := healthySlotForVR("r1")
	s1.LocalRoleClaim = LocalRolePrimary
	s2 := healthySlotForVR("r2")
	s2.LocalRoleClaim = LocalRolePrimary
	s3 := healthySlotForVR("r3")
	_ = restarted.host.IngestHeartbeat(serverObs("s1", clk.Now(), s1))
	_ = restarted.host.IngestHeartbeat(serverObs("s2", clk.Now(), s2))
	_ = restarted.host.IngestHeartbeat(serverObs("s3", clk.Now(), s3))

	// Poll for unsupported evidence to land on controller.
	var ev UnsupportedEvidence
	deadline := time.Now().Add(2 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		if e, ok := restarted.ctrl.LastUnsupported("vr"); ok && e.Reason != "" {
			ev = e
			found = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !found {
		t.Fatal("expected UnsupportedEvidence for conflicting primary claim after restart")
	}
	if ev.Reason == "" {
		t.Fatalf("evidence reason empty, got %+v", ev)
	}
}

// ============================================================
// Row 6: Fence path bounded through the real bridge + adapter
// route.
//
// L1 scope (sketch §10): prove the fence COMMAND reaches the
// executor through the real route (bridge catch-up delivery →
// adapter → Fence()), and that withholding the completion
// callback leaves projection non-Healthy. The full timeout-
// expiry proof is the adapter package's own test responsibility;
// sketch §10.1 Path B forbids S7 from adding a fence-
// reconfigure seam in core/adapter, so L1 cannot drive the
// watchdog to fire in bounded wall time. Residual noted in
// the S7 deliverable.
// ============================================================

func TestS7_Restart_FenceRouteBoundedWithoutCallback(t *testing.T) {
	// Seed run first so the restart path is exercised (this is a
	// RESTART test — the adapter receives reloaded authority via
	// bridge catch-up after a cross-process teardown in-miniature).
	dir := t.TempDir()
	clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))

	seed := newRestartRoute(t, dir, clk, restartRouteConfig{
		VolumeID: "vr",
		Replicas: []string{"r1"},
		Topology: threeSlotTopology("vr"),
		Directive: NewStaticDirective([]AssignmentAsk{{
			VolumeID: "vr", ReplicaID: "r1",
			DataAddr: "d1", CtrlAddr: "c1",
			Intent: IntentBind,
		}}),
	})
	_ = waitForAdapterMode(t, seed.adpt, engine.ModeHealthy, 2*time.Second)

	// Restart with fence-mode = withhold pinned at config time
	// (applied BEFORE live loops start, so the first catch-up-
	// driven fence on the reloaded adapter is captured, not
	// auto-completed).
	r := simulateRestart(t, seed, restartRouteConfig{
		VolumeID:  "vr",
		Replicas:  []string{"r1"},
		Topology:  threeSlotTopology("vr"),
		Directive: NewStaticDirective(nil),
		FenceMode: "withhold",
	})
	defer r.close(t)

	// A fence command must reach the executor via the real
	// bridge route within a reasonable window.
	deadline := time.Now().Add(3 * time.Second)
	fenceDispatched := false
	for time.Now().Before(deadline) {
		r.exec.mu.Lock()
		n := len(r.exec.withheldFences)
		r.exec.mu.Unlock()
		if n > 0 {
			fenceDispatched = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !fenceDispatched {
		t.Fatal("expected adapter to emit a Fence command via the real bridge route within 3s of reload")
	}

	// Projection must NOT be Healthy while fence completion is
	// withheld. Any "Healthy without callback" would indicate
	// the route was publishing liveness from incomplete truth.
	if p := r.adpt.Projection(); p.Mode == engine.ModeHealthy {
		t.Fatalf("adapter reached Healthy without a fence callback: %+v", p)
	}
}

// ============================================================
// Row 7: One volume's restart issue does not block another
// volume's convergence.
// ============================================================

func TestS7_Restart_PerVolumeIsolation(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))

	// Seed two volumes, both healthy after initial Bind.
	twoVolumeTopo := AcceptedTopology{Volumes: []VolumeExpected{
		threeSlotVolume("vA"),
		threeSlotVolume("vB"),
	}}
	_ = twoVolumeTopo
	seed := newRestartRoute(t, dir, clk, restartRouteConfig{
		VolumeID: "vA",
		Replicas: []string{"r1"},
		Topology: twoVolumeTopo,
		Directive: NewStaticDirective([]AssignmentAsk{
			{VolumeID: "vA", ReplicaID: "r1", DataAddr: "dA", CtrlAddr: "cA", Intent: IntentBind},
			{VolumeID: "vB", ReplicaID: "r1", DataAddr: "dB", CtrlAddr: "cB", Intent: IntentBind},
		}),
	})
	_ = waitForAdapterMode(t, seed.adpt, engine.ModeHealthy, 2*time.Second)

	restarted := simulateRestart(t, seed, restartRouteConfig{
		VolumeID:  "vA",
		Replicas:  []string{"r1"},
		Topology:  twoVolumeTopo,
		Directive: NewStaticDirective(nil),
	})
	defer restarted.close(t)

	// vB gets a conflicting-primary observation (unsupported).
	// vA gets healthy observation (should remain untouched).
	vBs1 := healthySlotFor("vB", "r1")
	vBs1.LocalRoleClaim = LocalRolePrimary
	vBs2 := healthySlotFor("vB", "r2")
	vBs2.LocalRoleClaim = LocalRolePrimary
	vBs3 := healthySlotFor("vB", "r3")

	vAs1 := healthySlotFor("vA", "r1")
	vAs2 := healthySlotFor("vA", "r2")
	vAs3 := healthySlotFor("vA", "r3")

	_ = restarted.host.IngestHeartbeat(serverObs("s1", clk.Now(), vAs1, vBs1))
	_ = restarted.host.IngestHeartbeat(serverObs("s2", clk.Now(), vAs2, vBs2))
	_ = restarted.host.IngestHeartbeat(serverObs("s3", clk.Now(), vAs3, vBs3))

	// vB must accrue unsupported evidence; vA must NOT be
	// blocked — its publisher line stays r1@1 and no desired
	// gets stuck.
	deadline := time.Now().Add(2 * time.Second)
	vBBlocked := false
	for time.Now().Before(deadline) {
		if _, ok := restarted.ctrl.LastUnsupported("vB"); ok {
			vBBlocked = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !vBBlocked {
		t.Fatal("vB did not record unsupported evidence")
	}

	// vA's authority line must still be intact.
	line, ok := restarted.pub.VolumeAuthorityLine("vA")
	if !ok || line.ReplicaID != "r1" || line.Epoch != 1 {
		t.Fatalf("vA publisher line disrupted by vB issue: got ok=%v %+v", ok, line)
	}
	// vA must have no unsupported evidence.
	if ev, ok := restarted.ctrl.LastUnsupported("vA"); ok {
		t.Fatalf("vA accrued unsupported evidence despite being healthy: %+v", ev)
	}
}

// ============================================================
// Helpers local to this file
// ============================================================

// buildV1HeartbeatsVR returns the three-server heartbeat set for
// volume "vr" (S7 tests) with r1/r2/r3 slots.
func buildV1HeartbeatsVR(at time.Time, unreachable map[string]bool) []HeartbeatMessage {
	replicas := []struct{ server, replica string }{
		{"s1", "r1"},
		{"s2", "r2"},
		{"s3", "r3"},
	}
	out := make([]HeartbeatMessage, 0, len(replicas))
	for _, r := range replicas {
		slot := healthySlotForVR(r.replica)
		if unreachable[r.replica] {
			slot.Reachable = false
			slot.ReadyForPrimary = false
		}
		out = append(out, serverObs(r.server, at, slot))
	}
	return out
}

func healthySlotForVR(replicaID string) SlotFact {
	return healthySlotFor("vr", replicaID)
}

func healthySlotFor(volumeID, replicaID string) SlotFact {
	return SlotFact{
		VolumeID:        volumeID,
		ReplicaID:       replicaID,
		DataAddr:        fmt.Sprintf("data-%s-%s", volumeID, replicaID),
		CtrlAddr:        fmt.Sprintf("ctrl-%s-%s", volumeID, replicaID),
		Reachable:       true,
		ReadyForPrimary: true,
		Eligible:        true,
	}
}

// forceSet stores an arbitrary basis on the snapshottingReader,
// bypassing Freeze() (which captures from underlying). Used by
// stale-observation tests to inject a synthetic stale line.
func (s *snapshottingReader) forceSet(volumeID string, b AuthorityBasis) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frozen = true
	s.perVol[volumeID] = b
	if b.ReplicaID != "" {
		s.perSlot[volumeID+"/"+b.ReplicaID] = b
	}
}
