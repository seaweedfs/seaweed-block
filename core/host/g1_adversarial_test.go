package host_test

// D2 — G1 adversarial L1 tests. Each test drives a single
// adversarial condition against the real in-process master host
// and asserts bounded fate.
//
// Per assignment §D2, every test documents:
//   Bad-state family: <PCDD-* ID>
//   Minimum repro:    <1-2 sentence description>
//   Owner layer:      engine | adapter | host | protocol
//   Expected bounded fate: <what happens>
//   Ledger row:       <INV-* or PCDD-* ID>

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// g1MasterWithTopo builds a master host bound to 127.0.0.1:0 with
// a three-slot topology for "v1". Returns the master and a
// cleanup-registered gRPC connection the test can use directly.
func g1MasterWithTopo(t *testing.T) (masterAddr string, conn *grpc.ClientConn) {
	t.Helper()
	m := newMasterHost(t, 5*time.Second)

	c, err := grpc.NewClient(m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial master: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return m.Addr(), c
}

// healthyReport returns a HeartbeatReport populated with one
// healthy slot for volume v1, owned by the given replica on the
// topology-matching server. Server IDs MUST match the topology
// the master was constructed with (topoV1() in route_test.go:
// r1→s1, r2→s2, r3→s3).
func healthyReport(serverID, replicaID string, sentAt time.Time) *control.HeartbeatReport {
	return &control.HeartbeatReport{
		ServerId:  serverID,
		SentAt:    timestamppb.New(sentAt),
		Reachable: true,
		Eligible:  true,
		Slots: []*control.HeartbeatSlot{{
			VolumeId:        "v1",
			ReplicaId:       replicaID,
			DataAddr:        "data-" + replicaID,
			CtrlAddr:        "ctrl-" + replicaID,
			Reachable:       true,
			ReadyForPrimary: true,
			Eligible:        true,
		}},
	}
}

// serverOf maps a replica ID to the topology-matching server ID
// for the topoV1() three-slot setup used by these tests.
func serverOf(replicaID string) string {
	switch replicaID {
	case "r1":
		return "s1"
	case "r2":
		return "s2"
	case "r3":
		return "s3"
	default:
		return "unknown"
	}
}

// TestG1_StaleHeartbeat_IgnoredNotApplied
//
// Bad-state family: PCDD-STALE-HB-001
// Minimum repro:    Send a heartbeat at T=now, then send a heartbeat from the
//                   same server with SentAt = T-10s. Observation store's
//                   out-of-order guard keeps the newer one; the stale report
//                   is silently dropped.
// Owner layer:      host (observation store is host-resident state)
// Expected bounded fate: Master snapshot does NOT regress; the stale HB
//                   fires no mutation callback; no assignment re-emitted.
// Ledger row:       PCDD-STALE-HB-001 (Component)
func TestG1_StaleHeartbeat_IgnoredNotApplied(t *testing.T) {
	masterAddr, conn := g1MasterWithTopo(t)
	obs := control.NewObservationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	now := time.Now()
	// Send fresh heartbeats for all three slots — volume becomes
	// supported; controller mints Bind(r1).
	for _, rid := range []string{"r1", "r2", "r3"} {
		if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf(rid), rid, now)); err != nil {
			t.Fatalf("healthy hb %s: %v", rid, err)
		}
	}
	// Wait for the mint to land.
	before := pollAssigned(t, masterAddr, "v1", 3*time.Second)
	beforeTuple := tupleOf(before)

	// Now send a STALE heartbeat from s-r1 with SentAt 10s in
	// the past. The observation store's `prev.ObservedAt.After(obs.ObservedAt)`
	// guard drops it with no mutation.
	staleAt := now.Add(-10 * time.Second)
	if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf("r1"), "r1", staleAt)); err != nil {
		t.Fatalf("stale hb: %v", err)
	}
	// Give any async ingest a chance to (not) happen.
	time.Sleep(100 * time.Millisecond)

	after, err := queryStatus(ctx, masterAddr, "v1")
	if err != nil {
		t.Fatalf("queryStatus after stale: %v", err)
	}
	afterTuple := tupleOf(after)
	if afterTuple != beforeTuple {
		t.Fatalf("stale heartbeat regressed authority state:\n  before=%s\n  after=%s",
			beforeTuple, afterTuple)
	}
}

// TestG1_ReconnectCatchesCurrentLine_NoReverse
//
// Bad-state family: (subscription correctness)
// Minimum repro:    Open a SubscribeAssignments stream, consume the current
//                   line, close the stream, reopen with the same subscription
//                   parameters. Second subscription MUST deliver the current
//                   line (catch-up) and MUST NOT deliver an older lex
//                   (Epoch, EndpointVersion) pair.
// Owner layer:      host (master's fan-in dedupe)
// Expected bounded fate: Second subscription's first delivery matches the
//                   most recently published line; no stale replay leaks.
// Ledger row:       INV-AUTH-001 (Component: monotonic lex dedupe)
func TestG1_ReconnectCatchesCurrentLine_NoReverse(t *testing.T) {
	masterAddr, conn := g1MasterWithTopo(t)
	obs := control.NewObservationServiceClient(conn)
	asgn := control.NewAssignmentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Drive to supported + Bind r1.
	now := time.Now()
	for _, rid := range []string{"r1", "r2", "r3"} {
		if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf(rid), rid, now)); err != nil {
			t.Fatalf("hb: %v", err)
		}
	}
	_ = pollAssigned(t, masterAddr, "v1", 3*time.Second)

	// First subscription — consume one fact, then close.
	firstCtx, firstCancel := context.WithCancel(ctx)
	firstStream, err := asgn.SubscribeAssignments(firstCtx, &control.SubscribeRequest{
		VolumeId: "v1", ReplicaId: "r1",
	})
	if err != nil {
		t.Fatalf("first subscribe: %v", err)
	}
	first, err := firstStream.Recv()
	if err != nil {
		t.Fatalf("first recv: %v", err)
	}
	firstEpoch := first.Epoch
	firstEV := first.EndpointVersion
	firstCancel() // close first stream

	// Brief pause to ensure master tears down the first sub.
	time.Sleep(50 * time.Millisecond)

	// Second subscription — should immediately catch up with
	// the current line (same Epoch/EV as the first, since no
	// mint happened in between).
	secondCtx, secondCancel := context.WithTimeout(ctx, 3*time.Second)
	defer secondCancel()
	secondStream, err := asgn.SubscribeAssignments(secondCtx, &control.SubscribeRequest{
		VolumeId: "v1", ReplicaId: "r1",
	})
	if err != nil {
		t.Fatalf("second subscribe: %v", err)
	}
	second, err := secondStream.Recv()
	if err != nil {
		t.Fatalf("second recv: %v", err)
	}
	// Lex ordering rule: second.(Epoch, EV) >= first.(Epoch, EV);
	// for a system with no intervening mint, they must be equal.
	if second.Epoch < firstEpoch || (second.Epoch == firstEpoch && second.EndpointVersion < firstEV) {
		t.Fatalf("reconnect reversed lex (Epoch, EV): first=(%d,%d) second=(%d,%d)",
			firstEpoch, firstEV, second.Epoch, second.EndpointVersion)
	}
}

// TestG1_MalformedHeartbeat_Rejected
//
// Bad-state family: PCDD-MALFORMED-001
// Minimum repro:    Send HeartbeatReport with missing required fields
//                   (empty ServerID, zero SentAt, nil slot, empty addrs).
//                   Each MUST be rejected at the RPC boundary with a
//                   named-field error before reaching ObservationHost.Ingest.
// Owner layer:      protocol (RPC validator)
// Expected bounded fate: gRPC error surfaces; master state unchanged.
// Ledger row:       PCDD-MALFORMED-001 (Component)
func TestG1_MalformedHeartbeat_Rejected(t *testing.T) {
	_, conn := g1MasterWithTopo(t)
	obs := control.NewObservationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cases := []struct {
		name    string
		report  *control.HeartbeatReport
		wantSub string // substring that MUST appear in the error message
	}{
		{
			name:    "empty-server-id",
			report:  &control.HeartbeatReport{SentAt: timestamppb.Now()},
			wantSub: "ServerID",
		},
		{
			name:    "zero-sent-at",
			report:  &control.HeartbeatReport{ServerId: "s1"},
			wantSub: "SentAt",
		},
		// NOTE: a literal nil slot inside a proto3 repeated field
		// is not reachable via the gRPC wire — the marshaller
		// materializes a zero-value struct. The defensive nil
		// guard in validateHeartbeat is for in-process callers
		// constructing the proto struct directly; it can't be
		// exercised through the RPC boundary test here.
		{
			name: "slot-missing-volume-id",
			report: &control.HeartbeatReport{
				ServerId: "s1", SentAt: timestamppb.Now(),
				Slots: []*control.HeartbeatSlot{{ReplicaId: "r1", DataAddr: "d", CtrlAddr: "c"}},
			},
			wantSub: "VolumeID",
		},
		{
			name: "slot-missing-addrs",
			report: &control.HeartbeatReport{
				ServerId: "s1", SentAt: timestamppb.Now(),
				Slots: []*control.HeartbeatSlot{{VolumeId: "v1", ReplicaId: "r1"}},
			},
			wantSub: "DataAddr",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := obs.ReportHeartbeat(ctx, c.report)
			if err == nil {
				t.Fatalf("malformed heartbeat accepted — MUST be rejected")
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Fatalf("error must name the violating field; want substring %q; got %v",
					c.wantSub, err)
			}
		})
	}
}

// TestG1_SubscribeBeforeTopologyKnown_FailsFastWithNamedError
//
// Bad-state family: (subscription correctness)
// Minimum repro:    Subscribe to a volume that is NOT in accepted topology.
//                   Master must return an error instead of silently streaming
//                   nothing.
// Owner layer:      host (subscription gate)
// Expected bounded fate: gRPC error with "not in accepted topology".
// Ledger row:       INV-RPC-001 (Component)
func TestG1_SubscribeBeforeTopologyKnown_FailsFastWithNamedError(t *testing.T) {
	_, conn := g1MasterWithTopo(t)
	asgn := control.NewAssignmentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := asgn.SubscribeAssignments(ctx, &control.SubscribeRequest{
		VolumeId:  "unknown-volume",
		ReplicaId: "r1",
	})
	if err != nil {
		// Some gRPC impls return error at call-site.
		if !strings.Contains(err.Error(), "accepted topology") {
			t.Fatalf("expected 'accepted topology' in error; got %v", err)
		}
		return
	}
	// Others return error on first Recv.
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("subscribe to unknown volume should fail")
	}
	if !strings.Contains(err.Error(), "accepted topology") {
		t.Fatalf("expected 'accepted topology' in error; got %v", err)
	}
}

// TestG1_ConcurrentHeartbeatSameReplica_Serialized
//
// Bad-state family: (concurrent RPC safety)
// Minimum repro:    Send N concurrent heartbeats for the same (volume,
//                   replica) over N goroutines. All accepted; master state
//                   reflects a single coherent snapshot, not a torn mix.
// Owner layer:      host (observation store mutex)
// Expected bounded fate: All RPCs return success; authority tuple stable.
// Ledger row:       INV-RPC-001 (Component)
func TestG1_ConcurrentHeartbeatSameReplica_Serialized(t *testing.T) {
	masterAddr, conn := g1MasterWithTopo(t)
	obs := control.NewObservationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Seed initial supported state.
	now := time.Now()
	for _, rid := range []string{"r1", "r2", "r3"} {
		if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf(rid), rid, now)); err != nil {
			t.Fatalf("seed hb: %v", err)
		}
	}
	baseline := tupleOf(pollAssigned(t, masterAddr, "v1", 3*time.Second))

	// Hammer r1 with concurrent heartbeats. All carry identical
	// field values; all are valid; observation store must
	// serialize them internally.
	const N = 16
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			_, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf("r1"), "r1", time.Now()))
			errs <- err
		}()
	}
	for i := 0; i < N; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent hb %d: %v", i, err)
		}
	}
	// Authority tuple must be stable — no churn induced by
	// concurrent identical-state heartbeats (S6 same-ask dedupe).
	time.Sleep(100 * time.Millisecond)
	after, err := queryStatus(ctx, masterAddr, "v1")
	if err != nil {
		t.Fatalf("queryStatus: %v", err)
	}
	if tupleOf(after) != baseline {
		t.Fatalf("concurrent heartbeats caused authority churn:\n  baseline=%s\n  after=%s",
			baseline, tupleOf(after))
	}
}

// TestG1_RPCTimeout_FailClosed
//
// Bad-state family: (bounded-fate on slow peers)
// Minimum repro:    Open a SubscribeAssignments stream but stop reading.
//                   Close client connection. Master's handler must exit
//                   via stream.Context().Done() and release per-replica
//                   publisher subscriptions.
// Owner layer:      host (stream lifecycle)
// Expected bounded fate: Master's handler returns within bounded time
//                   after client disconnect; subsequent subscribes work
//                   normally (no resource leak).
// Ledger row:       INV-RPC-001 (Component)
func TestG1_RPCTimeout_FailClosed(t *testing.T) {
	masterAddr, conn := g1MasterWithTopo(t)
	obs := control.NewObservationServiceClient(conn)
	asgn := control.NewAssignmentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now()
	for _, rid := range []string{"r1", "r2", "r3"} {
		if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf(rid), rid, now)); err != nil {
			t.Fatalf("hb: %v", err)
		}
	}
	_ = pollAssigned(t, masterAddr, "v1", 3*time.Second)

	// Open stream, receive first fact, then cancel the stream
	// context to simulate slow-client disconnect. Don't consume
	// any more from the stream.
	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := asgn.SubscribeAssignments(streamCtx, &control.SubscribeRequest{
		VolumeId: "v1", ReplicaId: "r1",
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("initial recv: %v", err)
	}
	streamCancel()

	// After cancellation, a subsequent fresh subscribe must
	// succeed quickly (the first handler released its publisher
	// subs and freed resources).
	time.Sleep(100 * time.Millisecond)
	freshCtx, freshCancel := context.WithTimeout(ctx, 3*time.Second)
	defer freshCancel()
	fresh, err := asgn.SubscribeAssignments(freshCtx, &control.SubscribeRequest{
		VolumeId: "v1", ReplicaId: "r1",
	})
	if err != nil {
		t.Fatalf("fresh subscribe: %v", err)
	}
	if _, err := fresh.Recv(); err != nil {
		t.Fatalf("fresh recv: %v", err)
	}
}

// TestG1_VolumeSubscribeBeforeMasterReady_BoundedWait
//
// Bad-state family: (bounded-fate on startup race)
// Minimum repro:    Subscribe to AssignmentService before any heartbeat
//                   has reached the master. Master holds the stream open;
//                   once the first assignment is minted (from subsequent
//                   heartbeats) it delivers via the held stream.
// Owner layer:      host (subscription lifecycle)
// Expected bounded fate: No error, no empty fact; delivery on mint.
// Ledger row:       INV-OBS-REALPROC-001 (Component)
func TestG1_VolumeSubscribeBeforeMasterReady_BoundedWait(t *testing.T) {
	masterAddr, conn := g1MasterWithTopo(t)
	_ = masterAddr
	obs := control.NewObservationServiceClient(conn)
	asgn := control.NewAssignmentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Subscribe FIRST, before any heartbeat — topology is in
	// accepted set so the call is permitted but no assignment
	// has been minted yet.
	stream, err := asgn.SubscribeAssignments(ctx, &control.SubscribeRequest{
		VolumeId: "v1", ReplicaId: "r1",
	})
	if err != nil {
		t.Fatalf("early subscribe: %v", err)
	}

	// Read the fact in a background goroutine so the main
	// goroutine can feed heartbeats without deadlock.
	type recvResult struct {
		fact *control.AssignmentFact
		err  error
	}
	done := make(chan recvResult, 1)
	go func() {
		f, e := stream.Recv()
		done <- recvResult{f, e}
	}()

	// Now send heartbeats — the mint should trigger delivery.
	now := time.Now()
	for _, rid := range []string{"r1", "r2", "r3"} {
		if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf(rid), rid, now)); err != nil {
			t.Fatalf("hb: %v", err)
		}
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("stream recv: %v", r.err)
		}
		if r.fact == nil || r.fact.Epoch == 0 {
			t.Fatalf("expected non-zero Epoch fact; got %+v", r.fact)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mint did not reach pre-existing subscription within 5s")
	}
}

// TestG1_DelayedHeartbeat_SupersededNotReverted
//
// Bad-state family: PCDD-DELAYED-HB-001
// Minimum repro:    Send a heartbeat at T; then send a heartbeat with
//                   SentAt = T-5s (delayed arrival, stale view). Observation
//                   store increments supersededCount AND authority state
//                   is not reverted. The counter distinguishes "recorded
//                   as superseded" from "silently dropped".
// Owner layer:      host (observation store + diagnostic counter)
// Expected bounded fate: supersededCount increments; authority tuple
//                   unchanged.
// Ledger row:       PCDD-DELAYED-HB-001 (Component)
func TestG1_DelayedHeartbeat_SupersededNotReverted(t *testing.T) {
	m := newMasterHost(t, 5*time.Second)
	conn, err := grpc.NewClient(m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	obs := control.NewObservationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	baseAt := time.Now()
	for _, rid := range []string{"r1", "r2", "r3"} {
		if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf(rid), rid, baseAt)); err != nil {
			t.Fatalf("seed hb: %v", err)
		}
	}
	before := tupleOf(pollAssigned(t, m.Addr(), "v1", 3*time.Second))
	beforeCount := m.ObservationHost().Store().SupersededCount()

	// Now send a DELAYED heartbeat from s1 — same server, same
	// replica state, but SentAt earlier than the one already in
	// the store. The observation store's out-of-order guard
	// drops it AND increments supersededCount.
	delayedAt := baseAt.Add(-5 * time.Second)
	if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf("r1"), "r1", delayedAt)); err != nil {
		t.Fatalf("delayed hb: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Counter must have advanced by exactly 1 — recorded as
	// superseded, not silently dropped.
	afterCount := m.ObservationHost().Store().SupersededCount()
	if afterCount != beforeCount+1 {
		t.Fatalf("supersededCount did not advance by 1: before=%d after=%d",
			beforeCount, afterCount)
	}
	// And authority state must not have reverted.
	after, err := queryStatus(ctx, m.Addr(), "v1")
	if err != nil {
		t.Fatalf("queryStatus: %v", err)
	}
	if tupleOf(after) != before {
		t.Fatalf("delayed heartbeat reverted authority state:\n  before=%s\n  after=%s",
			before, tupleOf(after))
	}
}

// TestG1_DeadPeer_BoundedDetection
//
// Bad-state family: (freshness-expiry bounded fate)
// Minimum repro:    Seed the store with healthy observations from all
//                   three replicas. Advance the observation host's clock
//                   past the freshness window. Snapshot build classifies
//                   the observations as expired; the volume transitions
//                   out of the supported set and the controller's evidence
//                   surfaces reflect it.
// Owner layer:      host (observation freshness clock)
// Expected bounded fate: Within one rebuild cycle after clock advance,
//                   the volume is no longer "supported"; controller
//                   doesn't keep minting from stale facts.
// Ledger row:       INV-OBS-REALPROC-001 (Component — expiry path)
func TestG1_DeadPeer_BoundedDetection(t *testing.T) {
	m := newMasterHost(t, 5*time.Second)
	conn, err := grpc.NewClient(m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	obs := control.NewObservationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Fresh subscribe-before-ingest sequence. Start time T.
	base := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)

	// Inject a mock clock into the observation host. The host
	// uses it for expiry computations; we advance it to trigger
	// freshness-expiry detection deterministically.
	//
	// Note: the SentAt field on heartbeats sets ObservedAt
	// (freshness anchor) — we pin it to `base` so the expiry
	// window elapses relative to our controlled clock.
	var (
		clockMu sync.Mutex
		clock   = base
	)
	m.ObservationHost().SetNowForTest(func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	})
	advance := func(d time.Duration) {
		clockMu.Lock()
		clock = clock.Add(d)
		clockMu.Unlock()
	}

	// Seed three healthy heartbeats with SentAt = base.
	for _, rid := range []string{"r1", "r2", "r3"} {
		if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf(rid), rid, base)); err != nil {
			t.Fatalf("seed hb: %v", err)
		}
	}
	// Controller mints Bind from the supported snapshot.
	_ = pollAssigned(t, m.Addr(), "v1", 3*time.Second)

	// Advance clock well past the default freshness window
	// (newMasterHost uses the master.Config default of 30s;
	// advancing 5 minutes is decisively past).
	advance(5 * time.Minute)

	// Trigger a rebuild. The store's mutation callback fires on
	// ingest; without a fresh ingest, we nudge by sending a
	// status query which doesn't rebuild — instead send one
	// fresh heartbeat from a different server that is NOT in
	// topology, which forces a rebuild without changing the
	// authority-volume slot set. Simpler: send r1 again with
	// SentAt = clock (which is newer) and let the rebuild
	// classify the OTHER two as expired.
	//
	// Actually simplest: just advance clock and wait for the
	// host's GC / rebuild loop to pick up the expiry. The host
	// rebuilds on ingest; without ingest, it won't rebuild.
	// We send one more heartbeat — from r1 at current clock —
	// to trigger a rebuild that evaluates expiry for r2 and r3.
	clockMu.Lock()
	refreshAt := clock
	clockMu.Unlock()
	if _, err := obs.ReportHeartbeat(ctx, healthyReport(serverOf("r1"), "r1", refreshAt)); err != nil {
		t.Fatalf("refresh hb: %v", err)
	}

	// Poll: within a bounded window, the observation host's
	// LastBuild must report r2 and r3 as either Pending (grace
	// elapsed) or Unsupported (missing from supported set).
	// v1 should no longer be in the "supported" set since two
	// of its three expected slots are expired.
	deadline := time.Now().Add(3 * time.Second)
	var built bool
	for time.Now().Before(deadline) {
		lastBuild, ok := m.ObservationHost().LastBuild()
		if !ok {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		// v1 must NOT be in the supported snapshot any more —
		// two of its slots expired past freshness.
		supported := false
		for _, v := range lastBuild.Snapshot.Volumes {
			if v.VolumeID == "v1" {
				supported = true
				break
			}
		}
		if !supported {
			built = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !built {
		t.Fatal("v1 still in supported snapshot after clock advanced past freshness window")
	}
}

// Deferred G1 tests (not in this patch):
//
//   - TestG1_DelayedHeartbeat_SupersededNotReverted
//       Requires explicit observation-layer surface for "heartbeat
//       arrived but was superseded by a later one"; current
//       observation store drops silently via the prev.ObservedAt
//       guard. Needs a design decision: is silent drop acceptable
//       evidence for PCDD-DELAYED-HB-001, or does the surface
//       need a new "superseded count" diagnostic? Escalating per
//       assignment §6 rule 6.
//
//   - TestG1_DeadPeer_BoundedDetection
//       Requires access to the observation store's expiry clock
//       (FreshnessWindow). Can be injected via master.Config but
//       needs a mechanism to advance the clock OR to wait the
//       real freshness window. Real-time wait isn't L1; clock
//       injection requires a small host-layer hook. Escalating.
//
// Both deferred tests need an architect / PM call before I can
// write them cleanly; the assignment §6 rule 6 says sw should
// escalate a missing bad-state family rather than invent a
// surface.
//
// The remaining D2 tests (#1, #3, #5, #6, #7, #8) are the six
// implemented above. Assignment specifies 8 tests total; 6 of
// 8 land in this patch with 2 explicitly escalated.

// _ references to keep the authority import alive in case the
// file evolves to consume it directly.
var _ = authority.AcceptedTopology{}
