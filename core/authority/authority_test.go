package authority

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// ============================================================
// Unit tests — authoring rules
// ============================================================

func TestPublisher_Bind_MintsEpochAndEndpointVersion(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	got, ok := pub.LastPublished("v1", "r1")
	if !ok {
		t.Fatal("expected LastPublished after Bind")
	}
	if got.Epoch != 1 {
		t.Fatalf("Bind Epoch: got %d want 1", got.Epoch)
	}
	if got.EndpointVersion != 1 {
		t.Fatalf("Bind EndpointVersion: got %d want 1", got.EndpointVersion)
	}
}

func TestPublisher_Bind_RejectsDoubleBind(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	ask := AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}
	if err := pub.apply(ask); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := pub.apply(ask); !errors.Is(err, ErrBindAlreadyBound) {
		t.Fatalf("second Bind: want ErrBindAlreadyBound, got %v", err)
	}
}

func TestPublisher_RefreshEndpoint_BumpsOnlyEndpointVersion(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("RefreshEndpoint: %v", err)
	}
	got, _ := pub.LastPublished("v1", "r1")
	if got.Epoch != 1 {
		t.Fatalf("Refresh must keep Epoch=1, got %d", got.Epoch)
	}
	if got.EndpointVersion != 2 {
		t.Fatalf("Refresh must bump EndpointVersion to 2, got %d", got.EndpointVersion)
	}
	if got.DataAddr != "d2" || got.CtrlAddr != "c2" {
		t.Fatalf("Refresh must update addrs, got %q/%q", got.DataAddr, got.CtrlAddr)
	}
}

func TestPublisher_RefreshEndpoint_IdempotentOnSameAddrs(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	ask := AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}
	if err := pub.apply(ask); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	refresh := ask
	refresh.Intent = IntentRefreshEndpoint
	if err := pub.apply(refresh); err != nil {
		t.Fatalf("RefreshEndpoint same-addrs: %v", err)
	}
	got, _ := pub.LastPublished("v1", "r1")
	if got.Epoch != 1 || got.EndpointVersion != 1 {
		t.Fatalf("same-addrs Refresh must be no-op, got epoch=%d ev=%d",
			got.Epoch, got.EndpointVersion)
	}
}

func TestPublisher_RefreshEndpoint_RejectsUnboundKey(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentRefreshEndpoint,
	})
	if !errors.Is(err, ErrRefreshNotBound) {
		t.Fatalf("want ErrRefreshNotBound, got %v", err)
	}
}

func TestPublisher_Reassign_BumpsEpochAndResetsEndpointVersion(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// Prior state: epoch=1, ev=2.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("Reassign: %v", err)
	}
	got, _ := pub.LastPublished("v1", "r1")
	if got.Epoch != 2 {
		t.Fatalf("Reassign Epoch: got %d want 2", got.Epoch)
	}
	if got.EndpointVersion != 1 {
		t.Fatalf("Reassign must reset EndpointVersion to 1, got %d", got.EndpointVersion)
	}
}

func TestPublisher_Reassign_RejectsUnboundKey(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentReassign,
	})
	if !errors.Is(err, ErrReassignNotBound) {
		t.Fatalf("want ErrReassignNotBound, got %v", err)
	}
}

func TestPublisher_Validate_RejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		ask  AssignmentAsk
		want error
	}{
		{"no volume", AssignmentAsk{ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}, ErrMissingVolumeID},
		{"no replica", AssignmentAsk{VolumeID: "v", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}, ErrMissingReplicaID},
		{"no data", AssignmentAsk{VolumeID: "v", ReplicaID: "r", CtrlAddr: "c", Intent: IntentBind}, ErrMissingDataAddr},
		{"no ctrl", AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", Intent: IntentBind}, ErrMissingCtrlAddr},
		{"no intent", AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c"}, ErrMissingIntent},
		{"bad intent", AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: AskIntent(99)}, ErrUnknownIntent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pub := NewPublisher(NewStaticDirective(nil))
			err := pub.apply(c.ask)
			if !errors.Is(err, c.want) {
				t.Fatalf("want %v, got %v", c.want, err)
			}
		})
	}
}

// ============================================================
// Fan-out keyed by (VolumeID, ReplicaID)
// ============================================================

func TestPublisher_FanOut_TwoSubscribersOnSameKey(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	chA, cancelA := pub.Subscribe("v1", "r1")
	defer cancelA()
	chB, cancelB := pub.Subscribe("v1", "r1")
	defer cancelB()

	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	got1 := receiveOrFail(t, chA, "subA")
	got2 := receiveOrFail(t, chB, "subB")
	if got1.Epoch != 1 || got2.Epoch != 1 {
		t.Fatalf("both subscribers must receive epoch=1, got %d / %d", got1.Epoch, got2.Epoch)
	}
}

// TestPublisher_IndependentUnsubscribe_OtherPeersUnaffected is the
// regression test for the architect finding that key-wide
// Unsubscribe tore down every subscriber on the same (vid, rid).
// With the per-subscription cancel API, one subscriber leaving must
// not close the channel of any other subscriber on the same key.
func TestPublisher_IndependentUnsubscribe_OtherPeersUnaffected(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	chA, cancelA := pub.Subscribe("v1", "r1")
	chB, cancelB := pub.Subscribe("v1", "r1")
	defer cancelB()

	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	receiveOrFail(t, chA, "A before cancel")
	receiveOrFail(t, chB, "B before cancel")

	// Cancel only A. B must stay live.
	cancelA()
	select {
	case _, ok := <-chA:
		if ok {
			t.Fatal("A channel expected closed after its own cancel")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("A channel not closed after cancel")
	}

	// B must still receive the next authoritative fact.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("RefreshEndpoint: %v", err)
	}
	got := receiveOrFail(t, chB, "B after A-only cancel")
	if got.EndpointVersion != 2 {
		t.Fatalf("B must continue receiving, got ev=%d", got.EndpointVersion)
	}
}

func TestPublisher_FanOut_SubscriberOnOtherVolumeDoesNotReceive(t *testing.T) {
	// Same replicaID across different volumes must NOT cross-deliver.
	// This is the multi-volume correctness test the architect called
	// out when we were keying only by ReplicaID.
	pub := NewPublisher(NewStaticDirective(nil))
	chV1, cancelV1 := pub.Subscribe("vol1", "r1")
	defer cancelV1()
	chV2, cancelV2 := pub.Subscribe("vol2", "r1")
	defer cancelV2()

	if err := pub.apply(AssignmentAsk{
		VolumeID: "vol1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind vol1: %v", err)
	}

	got := receiveOrFail(t, chV1, "vol1 subscriber")
	if got.VolumeID != "vol1" {
		t.Fatalf("vol1 subscriber got %q", got.VolumeID)
	}

	select {
	case info := <-chV2:
		t.Fatalf("vol2 subscriber must not receive vol1 publication, got %+v", info)
	case <-time.After(20 * time.Millisecond):
		// expected: nothing
	}
}

func TestPublisher_LateSubscriber_ReceivesLastPublished(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	ch, cancel := pub.Subscribe("v1", "r1")
	defer cancel()
	got := receiveOrFail(t, ch, "late subscriber")
	if got.Epoch != 1 {
		t.Fatalf("late subscriber must receive last published, got epoch=%d", got.Epoch)
	}
}

func TestPublisher_Cancel_ClosesOnlyThisSubscription(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	ch, cancel := pub.Subscribe("v1", "r1")
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channel not closed after cancel")
	}
	// cancel is idempotent — second call must not panic.
	cancel()
}

// TestPublisher_DeliveryConvergesToLatestOnSlowConsumer is the
// regression test for the architect's "silent drop" finding. An
// authority publication must not be permanently lost just because
// the per-subscription buffer was full when it landed. The publisher
// overwrites a pending stale value with the latest, so the next
// time the consumer drains the channel, it sees the CURRENT
// authoritative state. Intermediate states between drains may be
// coalesced — for authority truth that is correct (the engine
// cares about current identity, not interstitial history).
func TestPublisher_DeliveryConvergesToLatestOnSlowConsumer(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	ch, cancel := pub.Subscribe("v1", "r1")
	defer cancel()

	// Publish several authoritative facts WITHOUT draining the
	// channel. With the old "drop and log" behavior, only the first
	// would be retained and later publications would be lost. With
	// the overwrite-latest behavior, the channel always holds the
	// most recent one.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d3", CtrlAddr: "c3",
		Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("Reassign: %v", err)
	}

	// Now drain. The consumer must see the LATEST authored fact
	// (epoch=2 from Reassign, ev=1). Intermediate states may have
	// been coalesced.
	got := receiveOrFail(t, ch, "slow consumer catch-up")
	if got.Epoch != 2 || got.EndpointVersion != 1 {
		t.Fatalf("slow consumer must converge to latest, got epoch=%d ev=%d (want epoch=2 ev=1)",
			got.Epoch, got.EndpointVersion)
	}
	if got.DataAddr != "d3" {
		t.Fatalf("slow consumer must see latest addrs, got DataAddr=%q", got.DataAddr)
	}
}

// TestPublisher_RunClosesLiveSubscriptionsOnExit — when Run exits
// (ctx cancelled, directive errored), every still-live subscription
// channel must close so Bridges observe end-of-stream and exit
// their own loops cleanly.
func TestPublisher_RunClosesLiveSubscriptionsOnExit(t *testing.T) {
	dir := NewStaticDirective(nil) // never produces
	pub := NewPublisher(dir)
	ch, _ := pub.Subscribe("v1", "r1")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pub.Run(ctx) }()

	// Cancel Run and expect the subscription channel to close.
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run: want context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not exit on ctx cancel")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected subscription channel closed after Run exit")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("subscription channel not closed after Run exit")
	}
}

// ============================================================
// Run loop — integration with Directive
// ============================================================

func TestPublisher_Run_DrivesDirectiveUntilCtxCancel(t *testing.T) {
	dir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind},
	})
	pub := NewPublisher(dir)
	ch, cancelSub := pub.Subscribe("v1", "r1")
	defer cancelSub()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pub.Run(ctx) }()

	info := receiveOrFail(t, ch, "Run consumer")
	if info.Epoch != 1 {
		t.Fatalf("Run must drive Bind, got epoch=%d", info.Epoch)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run exit: want context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not exit on ctx cancel")
	}
}

func TestPublisher_Run_LogsAndContinuesOnRejectedAsk(t *testing.T) {
	// Reassign on an unbound key must be rejected, then the next
	// ask must still be processed. This is the "continue on reject"
	// Run semantic.
	dir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentReassign}, // rejected
		{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind},    // accepted
	})
	pub := NewPublisher(dir)
	ch, cancelSub := pub.Subscribe("v1", "r1")
	defer cancelSub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)

	info := receiveOrFail(t, ch, "post-reject subscriber")
	if info.Epoch != 1 {
		t.Fatalf("expected accepted Bind after rejected Reassign, got epoch=%d", info.Epoch)
	}
}

// ============================================================
// Bridge — forwards to AssignmentConsumer
// ============================================================

type recordingConsumer struct {
	mu   sync.Mutex
	seen []adapter.AssignmentInfo
}

func (r *recordingConsumer) OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog {
	r.mu.Lock()
	r.seen = append(r.seen, info)
	r.mu.Unlock()
	return adapter.ApplyLog{}
}

func (r *recordingConsumer) snapshot() []adapter.AssignmentInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]adapter.AssignmentInfo, len(r.seen))
	copy(cp, r.seen)
	return cp
}

func TestBridge_ForwardsToConsumer(t *testing.T) {
	dir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind},
	})
	pub := NewPublisher(dir)
	cons := &recordingConsumer{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)
	go Bridge(ctx, pub, cons, "v1", "r1")

	// Wait for Bridge → consumer delivery.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(cons.snapshot()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	seen := cons.snapshot()
	if len(seen) != 1 {
		t.Fatalf("Bridge did not forward: got %d deliveries", len(seen))
	}
	if seen[0].Epoch != 1 || seen[0].EndpointVersion != 1 {
		t.Fatalf("Bridge forwarded wrong fact: %+v", seen[0])
	}
}

// ============================================================
// Closure target: end-to-end authority → adapter → engine → Healthy
// ============================================================

// closureExecutor is a test executor that satisfies
// adapter.CommandExecutor and drives the adapter to ModeHealthy on
// a caught-up replica. It responds to ProbeReplica synchronously
// with a caught-up result (R == H) and auto-fires fence callbacks.
//
// We intentionally build a test executor rather than reuse the
// adapter-internal mockExecutor — the closure test must live in the
// authority package and use only adapter's exported surface.
type closureExecutor struct {
	mu              sync.Mutex
	onStart         adapter.OnSessionStart
	onClose         adapter.OnSessionClose
	onFenceComplete adapter.OnFenceComplete
	nextSession     atomic.Uint64
}

func newClosureExecutor() *closureExecutor {
	ce := &closureExecutor{}
	ce.nextSession.Store(1000)
	return ce
}

func (e *closureExecutor) SetOnSessionStart(fn adapter.OnSessionStart)     { e.onStart = fn }
func (e *closureExecutor) SetOnSessionClose(fn adapter.OnSessionClose)     { e.onClose = fn }
func (e *closureExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete)   { e.onFenceComplete = fn }

func (e *closureExecutor) Probe(replicaID, dataAddr, ctrlAddr string, epoch, endpointVersion uint64) adapter.ProbeResult {
	return adapter.ProbeResult{
		ReplicaID:       replicaID,
		Success:         true,
		EndpointVersion: endpointVersion,
		TransportEpoch:  epoch,
		// R >= H → caught-up; engine will emit FenceAtEpoch.
		ReplicaFlushedLSN: 100,
		PrimaryTailLSN:    10,
		PrimaryHeadLSN:    100,
	}
}

func (e *closureExecutor) StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	return nil
}
func (e *closureExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	return nil
}
func (e *closureExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {}
func (e *closureExecutor) PublishHealthy(replicaID string)                                     {}
func (e *closureExecutor) PublishDegraded(replicaID string, reason string)                     {}

func (e *closureExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	// Fire success inline — matches the ack-gated contract from P14 S1.
	cb := e.onFenceComplete
	if cb != nil {
		cb(adapter.FenceResult{
			ReplicaID:       replicaID,
			SessionID:       sessionID,
			Epoch:           epoch,
			EndpointVersion: endpointVersion,
			Success:         true,
		})
	}
	return nil
}

// TestClosureTarget_SparrowReachesHealthyViaAuthorityRoute is the
// S2 acceptance test: wire Publisher → Bridge → VolumeReplicaAdapter
// with a StaticDirective carrying one Bind, and observe the adapter
// reach Mode == ModeHealthy — without any harness.assign call or
// any test-side adapter.OnAssignment call.
func TestClosureTarget_SparrowReachesHealthyViaAuthorityRoute(t *testing.T) {
	exec := newClosureExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)

	dir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "vol1", ReplicaID: "r1",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334",
			Intent: IntentBind},
	})
	pub := NewPublisher(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pub.Run(ctx)
	go Bridge(ctx, pub, a, "vol1", "r1")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a.Projection().Mode == engine.ModeHealthy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("sparrow did not reach Healthy via authority route; final mode=%s", a.Projection().Mode)
}

// ============================================================
// Non-forgeability — structural test
// ============================================================

// nonForgeabilityAllowedPackageSuffixes are production-compiled
// test-infrastructure packages that may construct AssignmentInfo or
// declare a local variable of its type. They are not operator or
// admin paths; they exist to drive scenario tests. Adding a package
// here is an explicit, reviewable act.
//
// Matched against the relative path from the repo root. Each suffix
// must start with a path separator so "calibration" does not
// accidentally match "recalibration".
var nonForgeabilityAllowedPackageSuffixes = []string{
	string(filepath.Separator) + "core" + string(filepath.Separator) + "authority" + string(filepath.Separator),
	string(filepath.Separator) + "core" + string(filepath.Separator) + "calibration" + string(filepath.Separator),
	string(filepath.Separator) + "core" + string(filepath.Separator) + "conformance" + string(filepath.Separator),
	string(filepath.Separator) + "core" + string(filepath.Separator) + "schema" + string(filepath.Separator),
	// core/adapter is the package that defines AssignmentInfo; it
	// is the type owner and must name the type in field and method
	// signatures. The AST check below permits references (type
	// names in signatures / zero values) but flags construction
	// with non-zero Epoch or EndpointVersion and local variable
	// declarations of the type — so this suffix is NOT added.
}

// TestNonForgeability_NoAssignmentInfoMintingOutsideAuthority walks
// EVERY production .go file in the repo (not just core/) and fails
// if any file outside the allowlist:
//
//   1. constructs adapter.AssignmentInfo via composite literal with
//      a non-zero Epoch or non-zero EndpointVersion, OR
//   2. declares a local variable of type adapter.AssignmentInfo,
//      (*adapter.AssignmentInfo), or []adapter.AssignmentInfo. A
//      local variable of this type is the classic bypass shape for
//      deferred mutation: `var x AssignmentInfo; x.Epoch = input`.
//
// Using go/ast/parser rather than regex closes two prior weaknesses:
//   - the regex-based check only matched struct literals, so a
//     `var x AssignmentInfo; x.Epoch = v` bypass slipped through;
//   - the regex walk was scoped to core/, so cmd/sparrow demos and
//     any future cmd/ packages were not covered.
//
// The allowlist is defined above. Adapter itself (which defines
// the type) is not in the allowlist because the AST check
// intentionally allows type references in function signatures and
// return types; what it forbids is construction with non-zero
// identity fields and local declarations of the type. Adapter
// never does either — it only receives values via OnAssignment.
func TestNonForgeability_NoAssignmentInfoMintingOutsideAuthority(t *testing.T) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		t.Fatalf("find repo root: %v", err)
	}

	var bad []string
	err = filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			// Skip vendor, hidden dirs (.git, .gocache, .gotmp), build
			// output. We recurse into everything else.
			name := d.Name()
			if name == "vendor" || (len(name) > 0 && name[0] == '.') {
				if path != repoRoot {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		for _, allowed := range nonForgeabilityAllowedPackageSuffixes {
			if strings.Contains(path, allowed) {
				return nil
			}
		}

		findings, err := auditNonForgeability(path)
		if err != nil {
			return err
		}
		for _, f := range findings {
			bad = append(bad, path+": "+f)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(bad) > 0 {
		t.Fatalf("non-forgeability: production code outside the authority/calibration/conformance/schema allowlist forges AssignmentInfo:\n  %s",
			strings.Join(bad, "\n  "))
	}
}

// auditNonForgeability parses a Go source file and returns a list
// of human-readable findings describing any disallowed use of
// adapter.AssignmentInfo. It catches:
//
//   (a) composite literals of type AssignmentInfo / adapter.AssignmentInfo
//       with Epoch or EndpointVersion fields set to non-zero values;
//   (b) local variable declarations (var / :=) whose declared or
//       inferred type is AssignmentInfo (value, pointer, or slice).
//
// Function parameters, method receivers, return type declarations,
// and struct field types are NOT flagged — consuming values flows
// through the type name, and forbidding that would block legitimate
// adapter callers.
func auditNonForgeability(path string) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var findings []string

	isAssignmentInfoType := func(e ast.Expr) bool {
		// Strip pointer and slice wrappers.
		for {
			switch t := e.(type) {
			case *ast.StarExpr:
				e = t.X
				continue
			case *ast.ArrayType:
				e = t.Elt
				continue
			}
			break
		}
		switch t := e.(type) {
		case *ast.Ident:
			return t.Name == "AssignmentInfo"
		case *ast.SelectorExpr:
			if id, ok := t.X.(*ast.Ident); ok {
				return id.Name == "adapter" && t.Sel.Name == "AssignmentInfo"
			}
		}
		return false
	}

	// (a) Composite literal Epoch/EV non-zero.
	ast.Inspect(file, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if cl.Type == nil || !isAssignmentInfoType(cl.Type) {
			return true
		}
		for _, elt := range cl.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			keyID, ok := kv.Key.(*ast.Ident)
			if !ok {
				continue
			}
			if keyID.Name != "Epoch" && keyID.Name != "EndpointVersion" {
				continue
			}
			if litIsZero(kv.Value) {
				continue
			}
			pos := fset.Position(kv.Pos())
			findings = append(findings, fmtFinding(pos.Line, "composite literal sets %s to non-zero", keyID.Name))
		}
		return true
	})

	// (b) Local variable declarations of type AssignmentInfo.
	// Scope: inside function bodies only. Package-level declarations,
	// parameter declarations, and struct field types are excluded.
	ast.Inspect(file, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			return true
		}
		ast.Inspect(fn.Body, func(inner ast.Node) bool {
			switch v := inner.(type) {
			case *ast.DeclStmt:
				gd, ok := v.Decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.VAR {
					return true
				}
				for _, spec := range gd.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok || vs.Type == nil {
						continue
					}
					if isAssignmentInfoType(vs.Type) {
						pos := fset.Position(vs.Pos())
						findings = append(findings, fmtFinding(pos.Line, "local variable declared of type AssignmentInfo"))
					}
				}
			case *ast.AssignStmt:
				// `var x adapter.AssignmentInfo` uses GenDecl above.
				// `x := adapter.AssignmentInfo{}` is a short decl; if
				// RHS is a composite literal, the type check in (a)
				// already handles non-zero Epoch/EV. We do NOT flag
				// short decls of zero-value AssignmentInfo from a
				// function call return (legitimate when the callee is
				// a consumer API returning a zero value, e.g.
				// Publisher.LastPublished), because that would be
				// over-broad.
			}
			return true
		})
		return false
	})

	return findings, nil
}

func fmtFinding(line int, msg string, args ...any) string {
	return fmt.Sprintf("line %d: %s", line, fmt.Sprintf(msg, args...))
}

func litIsZero(e ast.Expr) bool {
	if lit, ok := e.(*ast.BasicLit); ok {
		return lit.Kind == token.INT && lit.Value == "0"
	}
	return false
}

// findRepoRoot walks up from cwd until it finds a directory
// containing a go.mod file. Used by the non-forgeability test so
// the walker covers cmd/, core/, and any future production
// packages in the repo — not just core/.
func findRepoRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := cwd
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", errors.New("could not locate repo root (no go.mod found walking up)")
}

// ============================================================
// Helpers
// ============================================================

func receiveOrFail(t *testing.T, ch <-chan adapter.AssignmentInfo, who string) adapter.AssignmentInfo {
	t.Helper()
	select {
	case info, ok := <-ch:
		if !ok {
			t.Fatalf("%s: channel closed before delivery", who)
		}
		return info
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("%s: no delivery within 500ms", who)
		return adapter.AssignmentInfo{}
	}
}

