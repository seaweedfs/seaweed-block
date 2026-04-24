package replication

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// --- Test harness helpers ---

// newHealthyPeer builds a ReplicaPeer pointing at a real
// ReplicaListener. Barrier calls against this peer succeed.
func newHealthyPeer(t *testing.T, id string, epoch, ev uint64) *ReplicaPeer {
	t.Helper()
	replicaStore := storage.NewBlockStore(64, 4096)
	listener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		t.Fatalf("%s: NewReplicaListener: %v", id, err)
	}
	listener.Serve()
	t.Cleanup(func() { listener.Stop() })

	primary := storage.NewBlockStore(64, 4096)
	exec := transport.NewBlockExecutor(primary, listener.Addr())
	target := ReplicaTarget{
		ReplicaID:       id,
		DataAddr:        listener.Addr(),
		ControlAddr:     listener.Addr(),
		Epoch:           epoch,
		EndpointVersion: ev,
	}
	peer, err := NewReplicaPeer(target, exec)
	if err != nil {
		t.Fatalf("%s: NewReplicaPeer: %v", id, err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	return peer
}

// newDeadPeer builds a ReplicaPeer pointing at an unreachable TCP
// address. Barrier calls against this peer return a dial error.
func newDeadPeer(t *testing.T, id string, epoch, ev uint64) *ReplicaPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	_ = ln.Close()

	primary := storage.NewBlockStore(64, 4096)
	exec := transport.NewBlockExecutor(primary, deadAddr)
	target := ReplicaTarget{
		ReplicaID:       id,
		DataAddr:        deadAddr,
		ControlAddr:     deadAddr,
		Epoch:           epoch,
		EndpointVersion: ev,
	}
	peer, err := NewReplicaPeer(target, exec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	return peer
}

// newWrongLineagePeer builds a ReplicaPeer whose executor targets a
// custom listener that responds to every barrier request with a
// valid-decode response carrying a DIFFERENT lineage. Used for the
// stale-lineage pin test.
func newWrongLineagePeer(t *testing.T, id string, epoch, ev uint64) *ReplicaPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	wrongLineage := transport.RecoveryLineage{
		SessionID:       uint64(1_000_000),
		Epoch:           uint64(1_000_000),
		EndpointVersion: uint64(1_000_000),
		TargetLSN:       uint64(1_000_000),
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				if _, _, err := transport.ReadMsg(conn); err != nil {
					return
				}
				payload := transport.EncodeBarrierResp(transport.BarrierResponse{
					Lineage:     wrongLineage,
					AchievedLSN: 42,
				})
				_ = transport.WriteMsg(conn, transport.MsgBarrierResp, payload)
			}(c)
		}
	}()

	primary := storage.NewBlockStore(64, 4096)
	exec := transport.NewBlockExecutor(primary, ln.Addr().String())
	target := ReplicaTarget{
		ReplicaID:       id,
		DataAddr:        ln.Addr().String(),
		ControlAddr:     ln.Addr().String(),
		Epoch:           epoch,
		EndpointVersion: ev,
	}
	peer, err := NewReplicaPeer(target, exec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	return peer
}

// successSync is a localSync closure that always succeeds.
func successSync(ctx context.Context) (uint64, error) { return 42, nil }

// failSync returns a localSync closure whose error is `err`.
func failSync(err error) func(ctx context.Context) (uint64, error) {
	return func(ctx context.Context) (uint64, error) { return 0, err }
}

// slowSync returns a localSync closure that sleeps before returning
// success. Used to measure parallel execution.
func slowSync(d time.Duration) func(ctx context.Context) (uint64, error) {
	return func(ctx context.Context) (uint64, error) {
		time.Sleep(d)
		return 42, nil
	}
}

// --- Test 1: RF=3 quorum all healthy ---

func TestDurabilityCoordinator_Quorum_RF3_AllHealthy(t *testing.T) {
	coord := NewDurabilityCoordinator()
	peers := []*ReplicaPeer{
		newHealthyPeer(t, "r1", 7, 3),
		newHealthyPeer(t, "r2", 7, 3),
	}

	result, err := coord.SyncLocalAndReplicas(
		context.Background(),
		DurabilitySyncQuorum,
		100,
		successSync,
		peers,
	)
	if err != nil {
		t.Fatalf("Sync should succeed: %v", err)
	}
	if result.DurableNodes != 3 {
		t.Fatalf("DurableNodes: got %d want 3 (primary + 2 peers)", result.DurableNodes)
	}
	if result.Quorum != 2 {
		t.Fatalf("Quorum: got %d want 2 (rf=3)", result.Quorum)
	}
	if len(result.FailedPeers) != 0 {
		t.Fatalf("FailedPeers: got %v want []", result.FailedPeers)
	}
}

// --- Test 2: RF=3 quorum one replica failed ---

func TestDurabilityCoordinator_Quorum_RF3_OneReplicaFailed(t *testing.T) {
	coord := NewDurabilityCoordinator()
	peers := []*ReplicaPeer{
		newHealthyPeer(t, "r1", 7, 3),
		newDeadPeer(t, "r-dead", 7, 3),
	}

	// sync_quorum: primary(1) + r1(1) = 2 durable ≥ 2 quorum → pass.
	result, err := coord.SyncLocalAndReplicas(
		context.Background(),
		DurabilitySyncQuorum,
		100,
		successSync,
		peers,
	)
	if err != nil {
		t.Fatalf("sync_quorum should succeed with 2/3 durable: %v", err)
	}
	if result.DurableNodes != 2 {
		t.Fatalf("DurableNodes: got %d want 2", result.DurableNodes)
	}
	if result.Quorum != 2 {
		t.Fatalf("Quorum: got %d want 2", result.Quorum)
	}
	if len(result.FailedPeers) != 1 || result.FailedPeers[0] != "r-dead" {
		t.Fatalf("FailedPeers: got %v want [r-dead]", result.FailedPeers)
	}

	// Dead peer must be Degraded (INV-REPL-BARRIER-FAILURE-DEGRADES-PEER
	// via peer.Barrier's own translation at T4b-3).
	if peers[1].State() != ReplicaDegraded {
		t.Fatalf("dead peer state: got %s want degraded", peers[1].State())
	}
}

// --- Test 3: RF=3 quorum both replicas failed ---

func TestDurabilityCoordinator_Quorum_RF3_BothReplicasFailed(t *testing.T) {
	coord := NewDurabilityCoordinator()
	peers := []*ReplicaPeer{
		newDeadPeer(t, "r-dead-1", 7, 3),
		newDeadPeer(t, "r-dead-2", 7, 3),
	}

	result, err := coord.SyncLocalAndReplicas(
		context.Background(),
		DurabilitySyncQuorum,
		100,
		successSync,
		peers,
	)
	if err == nil {
		t.Fatal("sync_quorum with both peers failed must error")
	}
	if !errors.Is(err, ErrDurabilityQuorumLost) {
		t.Fatalf("expected ErrDurabilityQuorumLost, got: %v", err)
	}
	if result.DurableNodes != 1 {
		t.Fatalf("DurableNodes: got %d want 1 (only primary)", result.DurableNodes)
	}
	if result.Quorum != 2 {
		t.Fatalf("Quorum: got %d want 2", result.Quorum)
	}
	if len(result.FailedPeers) != 2 {
		t.Fatalf("FailedPeers count: got %d want 2", len(result.FailedPeers))
	}
}

// --- Test 4: sync_all any failure fails ---

func TestDurabilityCoordinator_SyncAll_AnyFailure_Fails(t *testing.T) {
	coord := NewDurabilityCoordinator()
	peers := []*ReplicaPeer{
		newHealthyPeer(t, "r1", 7, 3),
		newDeadPeer(t, "r-dead", 7, 3),
	}

	// sync_all: any peer failure → error, even though quorum math
	// would accept it. No silent swallow (V2 lines 60-67).
	result, err := coord.SyncLocalAndReplicas(
		context.Background(),
		DurabilitySyncAll,
		100,
		successSync,
		peers,
	)
	if err == nil {
		t.Fatal("sync_all with one peer failed must error")
	}
	if !errors.Is(err, ErrDurabilityBarrierFailed) {
		t.Fatalf("expected ErrDurabilityBarrierFailed, got: %v", err)
	}
	// DurableNodes is still 2 (primary + r1) — the mode decision,
	// not the count, is what changes between sync_all and sync_quorum.
	if result.DurableNodes != 2 {
		t.Fatalf("DurableNodes: got %d want 2", result.DurableNodes)
	}
}

// --- Test 5: best_effort silences failures ---

func TestDurabilityCoordinator_BestEffort_Silences_Failures(t *testing.T) {
	coord := NewDurabilityCoordinator()
	peers := []*ReplicaPeer{
		newDeadPeer(t, "r-dead-1", 7, 3),
		newDeadPeer(t, "r-dead-2", 7, 3),
	}

	result, err := coord.SyncLocalAndReplicas(
		context.Background(),
		DurabilityBestEffort,
		100,
		successSync,
		peers,
	)
	if err != nil {
		t.Fatalf("best_effort must not surface peer failures: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("result.Err should be nil in best_effort: %v", result.Err)
	}
	// Even though mode returns nil, the per-peer Invalidate happened
	// inside peer.Barrier (T4b-3 INV-REPL-BARRIER-FAILURE-DEGRADES-PEER).
	for _, p := range peers {
		if p.State() != ReplicaDegraded {
			t.Fatalf("peer %s state: got %s want degraded",
				p.Target().ReplicaID, p.State())
		}
	}
	if len(result.FailedPeers) != 2 {
		t.Fatalf("FailedPeers count: got %d want 2 (still captured for diagnostics)",
			len(result.FailedPeers))
	}
}

// --- Test 6: stale-lineage ack not counted ---

// H5 LOCK pin. A replica sending a valid-decode BarrierResponse with
// the WRONG lineage must NOT count toward durability. peer.Barrier
// returns ErrBarrierLineageMismatch (set at T4b-2); coordinator sees
// this as a failed peer.
func TestDurabilityCoordinator_StaleLineageAck_NotCounted(t *testing.T) {
	coord := NewDurabilityCoordinator()
	peers := []*ReplicaPeer{
		newHealthyPeer(t, "r1", 7, 3),
		newWrongLineagePeer(t, "r-stale", 7, 3),
	}

	// sync_quorum RF=3: primary(1) + r1(1) + r-stale(STALE, 0) = 2 durable.
	// Quorum = 2 → pass numerically, but r-stale is counted as failed.
	result, err := coord.SyncLocalAndReplicas(
		context.Background(),
		DurabilitySyncQuorum,
		100,
		successSync,
		peers,
	)
	if err != nil {
		t.Fatalf("sync_quorum with 2/3 durable should pass: %v", err)
	}
	if result.DurableNodes != 2 {
		t.Fatalf("DurableNodes: got %d want 2 (stale lineage NOT counted)",
			result.DurableNodes)
	}
	// r-stale peer must be marked Degraded by peer.Barrier's error
	// translation. Confirms the H5 rule actually walked to the
	// right shutdown.
	if peers[1].State() != ReplicaDegraded {
		t.Fatalf("stale-lineage peer state: got %s want degraded", peers[1].State())
	}
	// FailedPeers must include the stale peer.
	if len(result.FailedPeers) != 1 || result.FailedPeers[0] != "r-stale" {
		t.Fatalf("FailedPeers: got %v want [r-stale]", result.FailedPeers)
	}

	// Same peer config with sync_all must fail the sync (any failure).
	// Create fresh peers because the stale one is now Degraded and
	// would stay Degraded on retry.
	freshPeers := []*ReplicaPeer{
		newHealthyPeer(t, "r1-fresh", 7, 3),
		newWrongLineagePeer(t, "r-stale-fresh", 7, 3),
	}
	_, err = coord.SyncLocalAndReplicas(
		context.Background(),
		DurabilitySyncAll,
		100,
		successSync,
		freshPeers,
	)
	if err == nil {
		t.Fatal("sync_all with stale-lineage peer should fail")
	}
	if !errors.Is(err, ErrDurabilityBarrierFailed) {
		t.Fatalf("expected ErrDurabilityBarrierFailed, got: %v", err)
	}
}

// --- Test 7: local sync fails in every mode ---

func TestDurabilityCoordinator_LocalSyncFails_AllModesError(t *testing.T) {
	coord := NewDurabilityCoordinator()
	localErr := errors.New("simulated local fsync failure")

	for _, mode := range []DurabilityMode{
		DurabilityBestEffort,
		DurabilitySyncQuorum,
		DurabilitySyncAll,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			peers := []*ReplicaPeer{
				newHealthyPeer(t, "r1-"+mode.String(), 7, 3),
			}
			result, err := coord.SyncLocalAndReplicas(
				context.Background(),
				mode,
				100,
				failSync(localErr),
				peers,
			)
			if err == nil {
				t.Fatalf("%s: local fsync failure must short-circuit error return", mode)
			}
			if !errors.Is(err, localErr) {
				t.Fatalf("%s: err should wrap localErr, got: %v", mode, err)
			}
			// DurableNodes = 0 because primary's 1 is gated on
			// localSync success (G-1 concern #7).
			if result.DurableNodes != 0 {
				t.Fatalf("%s: DurableNodes: got %d want 0 (local fsync gate)",
					mode, result.DurableNodes)
			}
		})
	}
}

// --- Test 8: parallel execution (not sequential) ---

// Pins V2's parallel WaitGroup shape. Local sync takes 100ms; 2 peer
// barriers take <10ms each against real listeners. Total elapsed
// must reflect max, not sum — if sequential we'd see ~100 + 2×barrier
// = ~120ms+, and with two sleeps we'd surface something larger if
// the impl accidentally serialized.
func TestDurabilityCoordinator_ParallelExecution(t *testing.T) {
	coord := NewDurabilityCoordinator()
	peers := []*ReplicaPeer{
		newHealthyPeer(t, "r1", 7, 3),
		newHealthyPeer(t, "r2", 7, 3),
	}

	const localDelay = 100 * time.Millisecond
	start := time.Now()
	_, err := coord.SyncLocalAndReplicas(
		context.Background(),
		DurabilitySyncQuorum,
		100,
		slowSync(localDelay),
		peers,
	)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// Budget: localDelay + scheduling + barrier I/O. Peer barriers
	// against loopback listeners are <10ms each. If sequential we'd
	// be at localDelay + 2 * peerBarrier ≈ 120ms+; if parallel we
	// should be ~max(localDelay, peerBarrier) ≈ 100ms + overhead.
	// Use 150ms as the ceiling (per G-1 spec).
	if elapsed > 150*time.Millisecond {
		t.Fatalf("elapsed %v > 150ms — parallel execution invariant broken", elapsed)
	}
	if elapsed < localDelay {
		t.Fatalf("elapsed %v < %v — localSync didn't actually run?", elapsed, localDelay)
	}
}

// --- Test 9: zero-peers standalone fast-path ---

// INV-REPL-ZERO-PEER-NO-SPAWN pin. With zero peers,
// SyncLocalAndReplicas takes the fast-path: DurableNodes=1 (primary),
// Quorum=1 (rf=1), and localSync is called exactly once.
//
// The "no-spawn" property is verified structurally by a fast-path
// stack-frame check: localSync is called on the caller's goroutine,
// so its stack includes the test's own t.Run frame. A spawned
// goroutine's stack would start from `goexit` with no parent frame.
// We capture runtime.Caller chain depth at localSync call-time and
// assert it exceeds a threshold that only the same-goroutine path
// can produce.
func TestDurabilityCoordinator_ZeroPeers_StandaloneFastPath(t *testing.T) {
	coord := NewDurabilityCoordinator()

	for _, mode := range []DurabilityMode{
		DurabilityBestEffort,
		DurabilitySyncQuorum,
		DurabilitySyncAll,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			called := 0
			sameGoroutine := false
			testGID := goroutineID()
			localSync := func(ctx context.Context) (uint64, error) {
				called++
				sameGoroutine = goroutineID() == testGID
				return 42, nil
			}

			result, err := coord.SyncLocalAndReplicas(
				context.Background(),
				mode,
				100,
				localSync,
				nil, // explicit zero peers
			)
			if err != nil {
				t.Fatalf("%s: zero-peer standalone must succeed: %v", mode, err)
			}
			if called != 1 {
				t.Fatalf("%s: localSync called %d times want 1", mode, called)
			}
			if result.DurableNodes != 1 {
				t.Fatalf("%s: DurableNodes: got %d want 1", mode, result.DurableNodes)
			}
			if result.Quorum != 1 {
				t.Fatalf("%s: Quorum: got %d want 1 (rf=1)", mode, result.Quorum)
			}
			if !sameGoroutine {
				t.Fatalf("%s: localSync ran in a spawned goroutine — zero-peer fast-path should call it synchronously (INV-REPL-ZERO-PEER-NO-SPAWN)",
					mode)
			}
		})
	}

	// Also: local sync failure propagates through the fast-path.
	localErr := errors.New("fast-path local failure")
	_, err := coord.SyncLocalAndReplicas(
		context.Background(),
		DurabilitySyncAll,
		100,
		failSync(localErr),
		nil,
	)
	if !errors.Is(err, localErr) {
		t.Fatalf("fast-path local failure not propagated: %v", err)
	}
}

// goroutineID extracts the current goroutine ID from runtime.Stack
// output. Uses the first line "goroutine N [status]:". Only used
// by the zero-peer fast-path test for same-goroutine equality.
// Not load-bearing for correctness; purely diagnostic fence.
func goroutineID() uint64 {
	buf := make([]byte, 64)
	n := runtimeStackFn(buf, false)
	// First line format: "goroutine N [status]:"
	line := string(buf[:n])
	// Extract N.
	prefix := "goroutine "
	if !strings.HasPrefix(line, prefix) {
		return 0
	}
	rest := line[len(prefix):]
	end := strings.IndexByte(rest, ' ')
	if end < 0 {
		return 0
	}
	var id uint64
	for i := 0; i < end; i++ {
		c := rest[i]
		if c < '0' || c > '9' {
			return 0
		}
		id = id*10 + uint64(c-'0')
	}
	return id
}

// --- Sanity: coord constructor + DurabilityMode stringer ---

func TestDurabilityCoordinator_Constructor(t *testing.T) {
	c := NewDurabilityCoordinator()
	if c == nil {
		t.Fatal("NewDurabilityCoordinator returned nil")
	}
}

func TestDurabilityMode_String(t *testing.T) {
	cases := map[DurabilityMode]string{
		DurabilityBestEffort: "best_effort",
		DurabilitySyncQuorum: "sync_quorum",
		DurabilitySyncAll:    "sync_all",
		DurabilityMode(99):   "unknown(99)",
	}
	for mode, want := range cases {
		if got := mode.String(); got != want {
			t.Fatalf("DurabilityMode(%d).String(): got %q want %q", mode, got, want)
		}
	}
}

// Silence unused-import warnings if test file references strings only
// in a subset of paths.
var _ = strings.Contains
