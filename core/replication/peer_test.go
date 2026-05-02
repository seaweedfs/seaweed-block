package replication

// Completion oracle: recover(a,b) band — NOT recover(a) closure.
// See sw-block/design/recover-semantics-adjustment-plan.md §8.1.

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// setupPeerWithRealReplica stands up an in-process real replica
// (ReplicaListener + BlockStore) and a fresh executor aimed at it,
// then constructs a peer pointed at the listener. Returns the peer,
// the replica's store (for assertions), and the listener addr.
//
// This is the "real *transport.BlockExecutor + real *transport.
// ReplicaListener" setup required by T4a-3 context note: ReplicaPeer
// is a leaf; its tests drive against the real transport layer, not
// against a ReplicationVolume caller stub.
func setupPeerWithRealReplica(t *testing.T) (*ReplicaPeer, *storage.BlockStore, *transport.ReplicaListener) {
	t.Helper()
	replicaStore := storage.NewBlockStore(64, 4096)
	listener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		t.Fatalf("NewReplicaListener: %v", err)
	}
	listener.Serve()
	t.Cleanup(func() { listener.Stop() })

	primaryStore := storage.NewBlockStore(64, 4096)
	exec := transport.NewBlockExecutor(primaryStore, listener.Addr())

	target := ReplicaTarget{
		ReplicaID:       "r-peer-test",
		DataAddr:        listener.Addr(),
		ControlAddr:     listener.Addr(),
		Epoch:           5,
		EndpointVersion: 2,
	}
	peer, err := NewReplicaPeer(target, exec)
	if err != nil {
		t.Fatalf("NewReplicaPeer: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	return peer, replicaStore, listener
}

func waitReplicaBlock(t *testing.T, replica *storage.BlockStore, lba uint32, want []byte) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := replica.Read(lba)
		if bytes.Equal(got, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := replica.Read(lba)
	t.Fatalf("replica LBA %d data mismatch: got %x want %x", lba, got, want)
}

// TestReplicaPeer_ShipEntry_Happy — one entry through a fully-wired
// peer arrives at the replica byte-exact; peer stays Healthy.
func TestReplicaPeer_ShipEntry_Happy(t *testing.T) {
	peer, replica, _ := setupPeerWithRealReplica(t)

	data := make([]byte, 4096)
	data[0], data[1] = 0xAB, 0xCD

	if err := peer.ShipEntry(context.Background(), transport.RecoveryLineage{}, 5, 1, data); err != nil {
		t.Fatalf("ShipEntry: %v", err)
	}
	if peer.State() != ReplicaHealthy {
		t.Fatalf("expected ReplicaHealthy after successful ship, got %s", peer.State())
	}

	deadline := time.Now().Add(2 * time.Second)
	var got []byte
	for time.Now().Before(deadline) {
		got, _ = replica.Read(5)
		if got != nil && got[0] == 0xAB {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("replica LBA 5 data mismatch: got [%02x %02x]", got[0], got[1])
	}
}

// TestReplicaPeer_ShipEntry_ConnFailure_MarksDegraded — the forward-
// carry CARRY-1 pin test from T4a-2. On ship error, peer must mark
// Degraded + (implicitly) Invalidate. Subsequent ShipEntry on the
// same peer is retained without touching the wire (health state is not
// the WAL egress owner; future recovery will replay from primary WAL).
//
// This closes catalogue §3.2.1 C4 (no-hard-stop / error-return
// handoff) from PARTIAL to DONE.
func TestReplicaPeer_ShipEntry_ConnFailure_MarksDegraded(t *testing.T) {
	// Reserve a port then release it → guaranteed unreachable. Ship
	// will lazy-dial and fail.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	_ = ln.Close()

	primary := storage.NewBlockStore(64, 4096)
	exec := transport.NewBlockExecutor(primary, deadAddr)
	target := ReplicaTarget{
		ReplicaID:       "r-dead",
		DataAddr:        deadAddr,
		ControlAddr:     deadAddr,
		Epoch:           3,
		EndpointVersion: 1,
	}
	peer, err := NewReplicaPeer(target, exec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	if peer.State() != ReplicaHealthy {
		t.Fatalf("expected ReplicaHealthy at construction, got %s", peer.State())
	}

	data := make([]byte, 4096)
	err = peer.ShipEntry(context.Background(), transport.RecoveryLineage{}, 0, 1, data)
	if err == nil {
		t.Fatal("expected ShipEntry error against unreachable replica, got nil")
	}
	if !strings.Contains(err.Error(), "dial") && !strings.Contains(err.Error(), "write") {
		t.Fatalf("expected dial/write error, got: %v", err)
	}

	// Peer must have transitioned Healthy → Degraded.
	if peer.State() != ReplicaDegraded {
		t.Fatalf("expected ReplicaDegraded after ship failure, got %s", peer.State())
	}

	// Subsequent ShipEntry is retained without touching the wire. This
	// is the single-egress shape: Degraded remains a health fact, not a
	// second WAL-routing decision.
	err = peer.ShipEntry(context.Background(), transport.RecoveryLineage{}, 1, 2, data)
	if err != nil {
		t.Fatalf("Degraded peer live write should be retained for recovery replay, got error: %v", err)
	}
}

type peerRecordingSessionSink struct {
	started chan struct{}
	release chan struct{}
	ended   chan struct{}

	mu      sync.Mutex
	entries int
}

func newPeerRecordingSessionSink() *peerRecordingSessionSink {
	return &peerRecordingSessionSink{
		started: make(chan struct{}),
		release: make(chan struct{}),
		ended:   make(chan struct{}),
	}
}

func (s *peerRecordingSessionSink) StartSession(fromLSN uint64) error {
	_ = fromLSN
	close(s.started)
	return nil
}

func (s *peerRecordingSessionSink) DrainBacklog(ctx context.Context) error {
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *peerRecordingSessionSink) EndSession() {
	select {
	case <-s.ended:
	default:
		close(s.ended)
	}
}

func (s *peerRecordingSessionSink) NotifyAppend(lba uint32, lsn uint64, data []byte) error {
	_, _, _ = lba, lsn, data
	s.mu.Lock()
	s.entries++
	s.mu.Unlock()
	return nil
}

func (s *peerRecordingSessionSink) Entries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries
}

// TestReplicaPeer_ShipEntry_DegradedWithActiveSession_RoutesSessionLane
// pins the production call path for the global single-emitter rule.
// Hardware #5 showed that executor-level gates are too low: degraded
// peers used to reject before BlockExecutor.Ship was reached. Active
// recovery sessions must get first refusal even when the peer's coarse
// state is Degraded.
func TestReplicaPeer_ShipEntry_DegradedWithActiveSession_RoutesSessionLane(t *testing.T) {
	primary := storage.NewBlockStore(8, 4096)
	coord := recovery.NewPeerShipCoordinator()
	replicaID := "r-peer-session"
	exec := transport.NewBlockExecutorWithDualLane(primary, "127.0.0.1:1", "127.0.0.1:2", coord, recovery.ReplicaID(replicaID))
	target := ReplicaTarget{
		ReplicaID:       replicaID,
		DataAddr:        "127.0.0.1:1",
		ControlAddr:     "127.0.0.1:1",
		Epoch:           3,
		EndpointVersion: 1,
	}
	peer, err := NewReplicaPeer(target, exec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	peer.SetState(ReplicaDegraded)

	sessionClient, sessionServer := net.Pipe()
	t.Cleanup(func() {
		_ = sessionClient.Close()
		_ = sessionServer.Close()
	})
	go func() {
		_, _ = io.Copy(io.Discard, sessionServer)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sink := newPeerRecordingSessionSink()
	bridge, _, ok := exec.DualLanePrimaryBridge()
	if !ok {
		t.Fatal("missing dual-lane bridge")
	}
	if err := bridge.StartRebuildSessionWithSink(ctx, sessionClient, recovery.ReplicaID(replicaID), 77, 10, 10, sink); err != nil {
		t.Fatalf("start active session: %v", err)
	}
	select {
	case <-sink.started:
	case <-time.After(2 * time.Second):
		t.Fatal("session sink did not start")
	}

	data := make([]byte, 4096)
	if err := peer.ShipEntry(context.Background(), transport.RecoveryLineage{}, 4, 11, data); err != nil {
		t.Fatalf("ShipEntry on degraded peer with active session: %v", err)
	}
	if got := sink.Entries(); got != 1 {
		t.Fatalf("ShipEntry routed %d entries to active session; want 1", got)
	}

	cancel()
	_ = sessionClient.Close()
	_ = sessionServer.Close()
	close(sink.release)
}

func TestReplicaPeer_RefreshLiveShipSessionAfter_RotatesPastRecoverySession(t *testing.T) {
	peer, _, _ := setupPeerWithRealReplica(t)
	oldSessionID := peer.sessionID
	if oldSessionID >= 500 {
		t.Fatalf("precondition: live session already beyond recovery marker: %d", oldSessionID)
	}
	if !peer.executor.HasSession(oldSessionID) {
		t.Fatalf("precondition: old live session %d not registered", oldSessionID)
	}

	if err := peer.RefreshLiveShipSessionAfter(500, 6001, "test recovery completed"); err != nil {
		t.Fatalf("RefreshLiveShipSessionAfter: %v", err)
	}
	if peer.sessionID <= 500 {
		t.Fatalf("new live sessionID=%d want > recovery session 500", peer.sessionID)
	}
	if peer.lineage.SessionID != peer.sessionID {
		t.Fatalf("lineage SessionID=%d does not match peer sessionID=%d", peer.lineage.SessionID, peer.sessionID)
	}
	if peer.lineage.TargetLSN != liveShipTargetLSN {
		t.Fatalf("lineage TargetLSN=%d want live sentinel %d", peer.lineage.TargetLSN, liveShipTargetLSN)
	}
	if peer.executor.HasSession(oldSessionID) {
		t.Fatalf("old live session %d still registered after refresh", oldSessionID)
	}
	if !peer.executor.HasSession(peer.sessionID) {
		t.Fatalf("new live session %d not registered", peer.sessionID)
	}
}

func TestReplicaPeer_PostRecoveryFirstLiveWrite_UsesRefreshedSession(t *testing.T) {
	peer, replica, _ := setupPeerWithRealReplica(t)
	oldSessionID := peer.sessionID

	first := bytes.Repeat([]byte{0x11}, 4096)
	if err := peer.ShipEntry(context.Background(), transport.RecoveryLineage{}, 2, 1, first); err != nil {
		t.Fatalf("pre-recovery ShipEntry: %v", err)
	}
	waitReplicaBlock(t, replica, 2, first)

	recoverySessionID := oldSessionID + 500
	recoveryLineage := transport.RecoveryLineage{
		SessionID:       recoverySessionID,
		Epoch:           peer.target.Epoch,
		EndpointVersion: peer.target.EndpointVersion,
		TargetLSN:       6001,
	}
	if err := peer.executor.FenceSync(peer.target.ReplicaID, recoveryLineage); err != nil {
		t.Fatalf("establish recovery lineage: %v", err)
	}

	if err := peer.RefreshLiveShipSessionAfter(recoveryLineage.SessionID, recoveryLineage.TargetLSN, "test recovery completed"); err != nil {
		t.Fatalf("RefreshLiveShipSessionAfter: %v", err)
	}
	if peer.executor.HasSession(oldSessionID) {
		t.Fatalf("old live session %d still registered after refresh", oldSessionID)
	}

	after := bytes.Repeat([]byte{0x22}, 4096)
	if err := peer.ShipEntry(context.Background(), transport.RecoveryLineage{}, 3, 6002, after); err != nil {
		t.Fatalf("post-recovery ShipEntry: %v", err)
	}
	waitReplicaBlock(t, replica, 3, after)
}

// TestReplicaPeer_Invalidate_MovesToDegraded — direct call to
// Invalidate (outside ShipEntry path) also moves Healthy → Degraded.
// Exercises the authority-driven path (ReplicationVolume would call
// this on explicit peer-down from master updates).
func TestReplicaPeer_Invalidate_MovesToDegraded(t *testing.T) {
	peer, _, _ := setupPeerWithRealReplica(t)
	if peer.State() != ReplicaHealthy {
		t.Fatalf("precondition: expected Healthy, got %s", peer.State())
	}
	peer.Invalidate("authority said so")
	if peer.State() != ReplicaDegraded {
		t.Fatalf("expected Degraded after Invalidate, got %s", peer.State())
	}
	// Second Invalidate is a no-op state-wise (already Degraded).
	peer.Invalidate("still said so")
	if peer.State() != ReplicaDegraded {
		t.Fatalf("state should remain Degraded, got %s", peer.State())
	}
}

// TestReplicaPeer_Close_Idempotent — Close twice does not panic and
// the second call returns nil. Post-Close, ShipEntry and Invalidate
// are no-ops (reject / ignore).
func TestReplicaPeer_Close_Idempotent(t *testing.T) {
	peer, _, _ := setupPeerWithRealReplica(t)

	if err := peer.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := peer.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// ShipEntry on closed peer returns error.
	data := make([]byte, 4096)
	err := peer.ShipEntry(context.Background(), transport.RecoveryLineage{}, 0, 1, data)
	if err == nil {
		t.Fatal("ShipEntry on closed peer should error")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected 'closed' error, got: %v", err)
	}

	// Invalidate on closed peer is a silent no-op.
	peer.Invalidate("post-close")
	// No state assertion — closed is closed. No panic == pass.
}

// TestReplicaPeer_NewReplicaPeer_RejectsNilExecutor — defensive check;
// a nil executor cannot participate in the session lifecycle.
func TestReplicaPeer_NewReplicaPeer_RejectsNilExecutor(t *testing.T) {
	target := ReplicaTarget{
		ReplicaID:       "r-x",
		Epoch:           1,
		EndpointVersion: 1,
	}
	_, err := NewReplicaPeer(target, nil)
	if err == nil {
		t.Fatal("expected error on nil executor")
	}
}

// TestReplicaPeer_NewReplicaPeer_RejectsZeroAuthority — Epoch=0 or
// EndpointVersion=0 would produce a lineage the replica listener
// rejects (acceptMutationLineage requires nonzero). Catch it at
// construction, not at first ship.
func TestReplicaPeer_NewReplicaPeer_RejectsZeroAuthority(t *testing.T) {
	primary := storage.NewBlockStore(64, 4096)
	exec := transport.NewBlockExecutor(primary, "127.0.0.1:0")

	cases := []struct {
		name   string
		target ReplicaTarget
	}{
		{"ZeroEpoch", ReplicaTarget{ReplicaID: "r", Epoch: 0, EndpointVersion: 1}},
		{"ZeroEndpointVersion", ReplicaTarget{ReplicaID: "r", Epoch: 1, EndpointVersion: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReplicaPeer(tc.target, exec)
			if err == nil {
				t.Fatal("expected error on zero authority field")
			}
		})
	}
}

// --- T4b-3 wrapper tests (5 per QA spec) ---

// TestReplicaPeer_Barrier_Happy — round-trip through a real replica
// listener. Peer is Healthy before and after; ack carries the peer's
// registered lineage byte-exact.
func TestReplicaPeer_Barrier_Happy(t *testing.T) {
	peer, _, _ := setupPeerWithRealReplica(t)
	if peer.State() != ReplicaHealthy {
		t.Fatalf("precondition: expected Healthy, got %s", peer.State())
	}

	ack, err := peer.Barrier(context.Background(), 42)
	if err != nil {
		t.Fatalf("Barrier: %v", err)
	}
	if !ack.Success {
		t.Fatal("ack.Success=false")
	}
	// Echo must match the peer's registered lineage (not the caller's
	// targetLSN — that's coordinator-layer only).
	if ack.Lineage != peer.lineage {
		t.Fatalf("ack.Lineage %+v != peer.lineage %+v", ack.Lineage, peer.lineage)
	}
	if peer.State() != ReplicaHealthy {
		t.Fatalf("Barrier success must leave peer Healthy, got %s", peer.State())
	}
}

// TestReplicaPeer_Barrier_ErrorPropagates_MarksDegraded — the
// INV-REPL-BARRIER-FAILURE-DEGRADES-PEER pin. Transport-level
// barrier failure must flow through to peer.Invalidate +
// state=Degraded (V2-faithful per §0-B locality; preserves V2's
// WALShipper-internal markDegraded contract at the V3 peer wrapper
// layer).
func TestReplicaPeer_Barrier_ErrorPropagates_MarksDegraded(t *testing.T) {
	// Reserve a port then release it → guaranteed unreachable.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	_ = ln.Close()

	primary := storage.NewBlockStore(64, 4096)
	exec := transport.NewBlockExecutor(primary, deadAddr)
	target := ReplicaTarget{
		ReplicaID:       "r-dead-barrier",
		DataAddr:        deadAddr,
		ControlAddr:     deadAddr,
		Epoch:           4,
		EndpointVersion: 2,
	}
	peer, err := NewReplicaPeer(target, exec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	if peer.State() != ReplicaHealthy {
		t.Fatalf("precondition: Healthy, got %s", peer.State())
	}

	ack, err := peer.Barrier(context.Background(), 100)
	if err == nil {
		t.Fatal("Barrier against unreachable replica should error")
	}
	if ack.Success {
		t.Fatal("ack.Success should be false on error path")
	}
	if peer.State() != ReplicaDegraded {
		t.Fatalf("expected ReplicaDegraded after Barrier failure, got %s", peer.State())
	}
	// Error shape: should contain "barrier" and the underlying
	// transport / dial failure signal.
	if !strings.Contains(err.Error(), "barrier") {
		t.Fatalf("expected 'barrier' in error, got: %v", err)
	}
}

// TestReplicaPeer_Fence_Happy — fence exchange at caller-supplied
// lineage against a real replica. Peer stays Healthy on success.
func TestReplicaPeer_Fence_Happy(t *testing.T) {
	peer, _, _ := setupPeerWithRealReplica(t)

	// Use the peer's registered lineage so the replica's
	// acceptMutationLineage rule passes (same-authority +
	// same TargetLSN).
	fenceLineage := peer.lineage

	if err := peer.Fence(context.Background(), fenceLineage); err != nil {
		t.Fatalf("Fence: %v", err)
	}
	if peer.State() != ReplicaHealthy {
		t.Fatalf("Fence success must leave peer Healthy, got %s", peer.State())
	}
}

// TestReplicaPeer_Barrier_AfterClose_Errors — post-Close Barrier
// returns error without panic. No state mutation (peer is already
// Unknown after Close).
func TestReplicaPeer_Barrier_AfterClose_Errors(t *testing.T) {
	peer, _, _ := setupPeerWithRealReplica(t)
	if err := peer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := peer.Barrier(context.Background(), 1)
	if err == nil {
		t.Fatal("Barrier on closed peer must error")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected 'closed' in error, got: %v", err)
	}
}

// TestReplicaPeer_Fence_AfterClose_Errors — post-Close Fence returns
// error without panic.
func TestReplicaPeer_Fence_AfterClose_Errors(t *testing.T) {
	peer, _, _ := setupPeerWithRealReplica(t)
	fenceLineage := peer.lineage
	if err := peer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := peer.Fence(context.Background(), fenceLineage)
	if err == nil {
		t.Fatal("Fence on closed peer must error")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected 'closed' in error, got: %v", err)
	}
}
