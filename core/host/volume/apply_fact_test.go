package volume

// T4a-6 follow-up: host-level tests for the install-or-refuse
// ordering fix. Exercises Host.applyFact directly (extracted from
// streamOnce) with stub adapter + stub ReplicationVolume so the
// ordering and failure-skip semantics can be asserted without a
// real gRPC stream or a real transport listener.
//
// Covers QA finding #1 (ordering + fail-closed) and QA finding #3
// (real coverage of the T4a-5 host seam beyond just the
// decodeReplicaTargets field-copy).

import (
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// newDiscardLogger returns a logger that discards all output. Keeps
// the stub-host tests quiet.
func newDiscardLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

// --- Stubs ---

// orderingRecorder logs the sequence of collaborator method calls so
// tests can assert "UpdateReplicaSet happened before OnAssignment".
type orderingRecorder struct {
	mu       sync.Mutex
	sequence []string
}

func (o *orderingRecorder) record(event string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.sequence = append(o.sequence, event)
}

func (o *orderingRecorder) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, len(o.sequence))
	copy(out, o.sequence)
	return out
}

type stubAdapter struct {
	rec       *orderingRecorder
	callCount atomic.Int64
	lastInfo  adapter.AssignmentInfo
}

func (s *stubAdapter) OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog {
	s.callCount.Add(1)
	s.lastInfo = info
	s.rec.record("OnAssignment")
	return adapter.ApplyLog{}
}

type stubReplication struct {
	rec            *orderingRecorder
	callCount      atomic.Int64
	lastGeneration uint64
	lastTargetIDs  []string
	returnErr      error
}

func (s *stubReplication) UpdateReplicaSet(generation uint64, targets []replication.ReplicaTarget) error {
	s.callCount.Add(1)
	s.lastGeneration = generation
	s.lastTargetIDs = nil
	for _, t := range targets {
		s.lastTargetIDs = append(s.lastTargetIDs, t.ReplicaID)
	}
	s.rec.record("UpdateReplicaSet")
	return s.returnErr
}

// newStubbedHost builds a Host shell suitable for driving applyFact
// directly. Avoids grpc.NewClient + adapter.NewVolumeReplicaAdapter,
// which want real network / executor wiring.
func newStubbedHost(t *testing.T, volumeID, replicaID string, rec *orderingRecorder) (*Host, *stubAdapter, *stubReplication) {
	t.Helper()
	a := &stubAdapter{rec: rec}
	r := &stubReplication{rec: rec}
	h := &Host{
		cfg: Config{
			VolumeID:  volumeID,
			ReplicaID: replicaID,
		},
		log:         newDiscardLogger(),
		adpt:        a,
		replication: r,
	}
	return h, a, r
}

// --- Test 1: install-or-refuse ordering ---

// TestHost_ApplyFact_Self_InstallsReplicaSetBeforeAssignment — the
// primary QA finding #1 pin. For a self-replica fact,
// UpdateReplicaSet MUST run before OnAssignment so the adapter's
// projection cannot flip Healthy before peers are installed.
func TestHost_ApplyFact_Self_InstallsReplicaSetBeforeAssignment(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r1", rec)

	fact := &control.AssignmentFact{
		VolumeId:        "vol1",
		ReplicaId:       "r1", // self
		Epoch:           5,
		EndpointVersion: 2,
		DataAddr:        "127.0.0.1:1000",
		CtrlAddr:        "127.0.0.1:1001",
		Peers: []*control.ReplicaDescriptor{{
			ReplicaId:       "r2",
			Epoch:           5,
			EndpointVersion: 2,
			DataAddr:        "127.0.0.1:2000",
			CtrlAddr:        "127.0.0.1:2001",
		}},
		PeerSetGeneration: 42,
	}

	h.applyFact(fact)

	// Both collaborators called exactly once.
	if got := r.callCount.Load(); got != 1 {
		t.Fatalf("UpdateReplicaSet calls: got %d want 1", got)
	}
	if got := a.callCount.Load(); got != 1 {
		t.Fatalf("OnAssignment calls: got %d want 1", got)
	}

	// Ordering: UpdateReplicaSet BEFORE OnAssignment.
	seq := rec.snapshot()
	if len(seq) != 2 {
		t.Fatalf("sequence length=%d want 2: %v", len(seq), seq)
	}
	if seq[0] != "UpdateReplicaSet" || seq[1] != "OnAssignment" {
		t.Fatalf("wrong ordering: got %v, want [UpdateReplicaSet OnAssignment]", seq)
	}

	// Replication received the right payload.
	if r.lastGeneration != 42 {
		t.Fatalf("replication generation: got %d want 42", r.lastGeneration)
	}
	if len(r.lastTargetIDs) != 1 || r.lastTargetIDs[0] != "r2" {
		t.Fatalf("replication targets: got %v want [r2]", r.lastTargetIDs)
	}

	// Adapter received the right identity.
	if a.lastInfo.Epoch != 5 || a.lastInfo.EndpointVersion != 2 {
		t.Fatalf("adapter identity: got (epoch=%d, ev=%d) want (5, 2)",
			a.lastInfo.Epoch, a.lastInfo.EndpointVersion)
	}
}

// --- Test 2: fail-closed on UpdateReplicaSet failure ---

// TestHost_ApplyFact_Self_UpdateReplicaSetFailure_SkipsOnAssignment —
// QA finding #1 part 2: if UpdateReplicaSet fails for any reason,
// OnAssignment MUST be skipped. Adapter projection stays at its
// prior (not-Healthy) state; StorageBackend keeps writes blocked
// until master stream replay redelivers a fact that installs.
func TestHost_ApplyFact_Self_UpdateReplicaSetFailure_SkipsOnAssignment(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r1", rec)
	r.returnErr = errors.New("simulated UpdateReplicaSet failure")

	fact := &control.AssignmentFact{
		VolumeId:          "vol1",
		ReplicaId:         "r1",
		Epoch:             3,
		EndpointVersion:   1,
		DataAddr:          "127.0.0.1:1000",
		CtrlAddr:          "127.0.0.1:1001",
		PeerSetGeneration: 7,
	}

	h.applyFact(fact)

	// UpdateReplicaSet was called (once).
	if got := r.callCount.Load(); got != 1 {
		t.Fatalf("UpdateReplicaSet calls: got %d want 1", got)
	}
	// OnAssignment was NOT called — fail-closed.
	if got := a.callCount.Load(); got != 0 {
		t.Fatalf("fail-closed violated: OnAssignment calls=%d want 0", got)
	}
	// Sequence has exactly one entry, UpdateReplicaSet.
	seq := rec.snapshot()
	if len(seq) != 1 || seq[0] != "UpdateReplicaSet" {
		t.Fatalf("sequence: got %v, want [UpdateReplicaSet]", seq)
	}
}

func TestHost_ApplyFact_Self_LocalReadinessBlocked_SkipsReplicationAndAdapter(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r1", rec)
	readyCh := make(chan adapter.AssignmentInfo, 1)
	h.cfg.ReadyMarker = readyCh
	h.BlockLocalReadiness("durable recovery failed: wal_integrity_fault")

	fact := &control.AssignmentFact{
		VolumeId:          "vol1",
		ReplicaId:         "r1",
		Epoch:             2,
		EndpointVersion:   1,
		PeerSetGeneration: 7,
		Peers: []*control.ReplicaDescriptor{
			{ReplicaId: "r1", DataAddr: "d1", CtrlAddr: "c1", Epoch: 2, EndpointVersion: 1},
		},
	}

	h.applyFact(fact)

	if got := r.callCount.Load(); got != 0 {
		t.Fatalf("local readiness block must skip UpdateReplicaSet: got %d calls", got)
	}
	if got := a.callCount.Load(); got != 0 {
		t.Fatalf("local readiness block must skip OnAssignment: got %d calls", got)
	}
	if seq := rec.snapshot(); len(seq) != 0 {
		t.Fatalf("local readiness block sequence=%v want empty", seq)
	}
	select {
	case got := <-readyCh:
		t.Fatalf("local readiness block must not emit ready marker: %+v", got)
	default:
	}
}

func TestHost_ApplyFact_SupportingReplica_LocalReadinessBlocked_SkipsAdapter(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, _ := newStubbedHost(t, "vol1", "r2", rec)
	readyCh := make(chan adapter.AssignmentInfo, 1)
	h.cfg.ReadyMarker = readyCh
	h.BlockLocalReadiness("awaiting durable recovery")

	fact := &control.AssignmentFact{
		VolumeId:        "vol1",
		ReplicaId:       "r1",
		Epoch:           5,
		EndpointVersion: 2,
		Peers: []*control.ReplicaDescriptor{
			{ReplicaId: "r2", DataAddr: "d2", CtrlAddr: "c2", Epoch: 5, EndpointVersion: 2},
		},
	}

	h.applyFact(fact)

	if got := a.callCount.Load(); got != 0 {
		t.Fatalf("local readiness block must skip supporting OnAssignment: got %d calls", got)
	}
	if seq := rec.snapshot(); len(seq) != 0 {
		t.Fatalf("local readiness block sequence=%v want empty", seq)
	}
	select {
	case got := <-readyCh:
		t.Fatalf("local readiness block must not emit supporting ready marker: %+v", got)
	default:
	}
}

// --- Test 3: supersede branch unaffected ---

// TestHost_ApplyFact_Supersede_NeverCallsAdapterOrReplication —
// guards the supersede branch (fact names another replica). Neither
// OnAssignment nor UpdateReplicaSet should fire — only recordOtherLine.
func TestHost_ApplyFact_Supersede_NeverCallsAdapterOrReplication(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r1", rec)

	fact := &control.AssignmentFact{
		VolumeId:          "vol1",
		ReplicaId:         "r2", // not self
		Epoch:             9,
		EndpointVersion:   1,
		PeerSetGeneration: 100,
	}

	h.applyFact(fact)

	if a.callCount.Load() != 0 {
		t.Fatalf("supersede must not call adapter: got %d calls", a.callCount.Load())
	}
	if r.callCount.Load() != 0 {
		t.Fatalf("supersede must not call replication: got %d calls", r.callCount.Load())
	}
	if h.LastOtherLine() == nil {
		t.Fatal("supersede did not record other line")
	}
	if h.LastOtherLine().ReplicaId != "r2" {
		t.Fatalf("recorded wrong replica: got %s want r2", h.LastOtherLine().ReplicaId)
	}
}

func TestHost_ApplyFact_SupportingReplica_UsesSelfPeerDescriptorWithoutReplication(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r2", rec)
	readyCh := make(chan adapter.AssignmentInfo, 1)
	h.cfg.ReadyMarker = readyCh

	fact := &control.AssignmentFact{
		VolumeId:        "vol1",
		ReplicaId:       "r1", // active primary, not self
		Epoch:           5,
		EndpointVersion: 2,
		DataAddr:        "127.0.0.1:1000",
		CtrlAddr:        "127.0.0.1:1001",
		Peers: []*control.ReplicaDescriptor{{
			ReplicaId:       "r2",
			Epoch:           5,
			EndpointVersion: 2,
			DataAddr:        "127.0.0.1:2000",
			CtrlAddr:        "127.0.0.1:2001",
		}},
		PeerSetGeneration: 100,
	}

	h.applyFact(fact)

	if got := a.callCount.Load(); got != 1 {
		t.Fatalf("supporting replica should apply self peer lineage once: got %d calls", got)
	}
	if got := r.callCount.Load(); got != 0 {
		t.Fatalf("supporting replica must not install primary fan-out peer set: got %d calls", got)
	}
	if h.LastOtherLine() == nil || h.LastOtherLine().ReplicaId != "r1" {
		t.Fatalf("supporting replica did not record active primary line: %+v", h.LastOtherLine())
	}
	if a.lastInfo.VolumeID != "vol1" || a.lastInfo.ReplicaID != "r2" {
		t.Fatalf("adapter identity: got volume=%q replica=%q want vol1/r2", a.lastInfo.VolumeID, a.lastInfo.ReplicaID)
	}
	if a.lastInfo.Epoch != 5 || a.lastInfo.EndpointVersion != 2 {
		t.Fatalf("adapter lineage: got epoch=%d ev=%d want 5/2", a.lastInfo.Epoch, a.lastInfo.EndpointVersion)
	}
	if a.lastInfo.DataAddr != "127.0.0.1:2000" || a.lastInfo.CtrlAddr != "127.0.0.1:2001" {
		t.Fatalf("adapter addrs: got data=%q ctrl=%q", a.lastInfo.DataAddr, a.lastInfo.CtrlAddr)
	}
	seq := rec.snapshot()
	if len(seq) != 1 || seq[0] != "OnAssignment" {
		t.Fatalf("supporting replica sequence: got %v want [OnAssignment]", seq)
	}
	select {
	case got := <-readyCh:
		if got.ReplicaID != "r2" || got.Epoch != 5 || got.EndpointVersion != 2 {
			t.Fatalf("ready marker identity: %+v", got)
		}
	default:
		t.Fatal("supporting replica did not emit ready marker for durable lineage latch")
	}

	primaryFact := &control.AssignmentFact{
		VolumeId:          "vol1",
		ReplicaId:         "r2",
		Epoch:             6,
		EndpointVersion:   3,
		DataAddr:          "127.0.0.1:3000",
		CtrlAddr:          "127.0.0.1:3001",
		PeerSetGeneration: 101,
	}

	h.applyFact(primaryFact)

	if got := a.callCount.Load(); got != 2 {
		t.Fatalf("promotion after supporting assignment should apply adapter again: got %d calls", got)
	}
	if got := r.callCount.Load(); got != 1 {
		t.Fatalf("promotion after supporting assignment should install primary peer set: got %d calls", got)
	}
	select {
	case got := <-readyCh:
		if got.ReplicaID != "r2" || got.Epoch != 6 || got.EndpointVersion != 3 {
			t.Fatalf("promoted ready marker identity: %+v", got)
		}
	default:
		t.Fatal("supporting marker must not consume primary ready marker")
	}
}

func TestHost_ApplyFact_SupportingReplica_InvalidSelfPeerDescriptorStaysSuperseded(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r2", rec)

	fact := &control.AssignmentFact{
		VolumeId:        "vol1",
		ReplicaId:       "r1",
		Epoch:           5,
		EndpointVersion: 2,
		Peers: []*control.ReplicaDescriptor{{
			ReplicaId:       "r2",
			Epoch:           5,
			EndpointVersion: 2,
			DataAddr:        "", // invalid: do not invent local identity
			CtrlAddr:        "127.0.0.1:2001",
		}},
	}

	h.applyFact(fact)

	if got := a.callCount.Load(); got != 0 {
		t.Fatalf("invalid supporting peer descriptor must not call adapter: got %d calls", got)
	}
	if got := r.callCount.Load(); got != 0 {
		t.Fatalf("invalid supporting peer descriptor must not call replication: got %d calls", got)
	}
	if h.LastOtherLine() == nil || h.LastOtherLine().ReplicaId != "r1" {
		t.Fatalf("invalid supporting peer descriptor should still record active primary line: %+v", h.LastOtherLine())
	}
}

func TestHost_ApplyFact_SupportingReplica_MismatchedSelfPeerLineageStaysSuperseded(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r2", rec)

	fact := &control.AssignmentFact{
		VolumeId:        "vol1",
		ReplicaId:       "r1",
		Epoch:           5,
		EndpointVersion: 2,
		Peers: []*control.ReplicaDescriptor{{
			ReplicaId:       "r2",
			Epoch:           5,
			EndpointVersion: 3, // newer than the active primary line: reject
			DataAddr:        "127.0.0.1:2000",
			CtrlAddr:        "127.0.0.1:2001",
		}},
	}

	h.applyFact(fact)

	if got := a.callCount.Load(); got != 0 {
		t.Fatalf("mismatched supporting peer lineage must not call adapter: got %d calls", got)
	}
	if got := r.callCount.Load(); got != 0 {
		t.Fatalf("mismatched supporting peer lineage must not call replication: got %d calls", got)
	}
	if h.LastOtherLine() == nil || h.LastOtherLine().ReplicaId != "r1" {
		t.Fatalf("mismatched supporting peer lineage should still record active primary line: %+v", h.LastOtherLine())
	}
}

func TestHost_ApplyFact_SupportingReplica_ReportsReplicaReadyButNotFrontendPrimary(t *testing.T) {
	exec := NewHealthyPathExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)
	h := &Host{
		cfg: Config{
			VolumeID:  "vol1",
			ReplicaID: "r2",
		},
		log:  newDiscardLogger(),
		adpt: a,
	}
	h.view = NewAdapterProjectionView(a, "vol1", "r2", h)
	s := NewStatusServer(h.view)

	h.applyFact(&control.AssignmentFact{
		VolumeId:        "vol1",
		ReplicaId:       "r1",
		Epoch:           7,
		EndpointVersion: 3,
		Peers: []*control.ReplicaDescriptor{{
			ReplicaId:       "r2",
			Epoch:           7,
			EndpointVersion: 3,
			DataAddr:        "127.0.0.1:2000",
			CtrlAddr:        "127.0.0.1:2001",
		}},
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a.Projection().Mode == engine.ModeHealthy {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := a.Projection().Mode; got != engine.ModeHealthy {
		t.Fatalf("supporting adapter mode=%s want %s; commands=%v", got, engine.ModeHealthy, exec.Commands())
	}

	body := s.statusProjection()
	if body.AuthorityRole != AuthorityRoleUnknown {
		t.Fatalf("supporting authority role=%q want %q", body.AuthorityRole, AuthorityRoleUnknown)
	}
	if body.ReplicationRole != ReplicationRoleReady {
		t.Fatalf("supporting replication role=%q want %q", body.ReplicationRole, ReplicationRoleReady)
	}
	if body.FrontendPrimaryReady {
		t.Fatalf("supporting replica_ready must not become frontend primary: %+v", body)
	}
	report := h.buildReport()
	if len(report.GetSlots()) != 1 || !report.GetSlots()[0].GetReadyForPrimary() {
		t.Fatalf("supporting replica did not advertise ReadyForPrimary after engine readiness: %+v", report.GetSlots())
	}
}

// --- Test 4: observer-only mode (replication == nil) ---

// TestHost_ApplyFact_ObserverOnly_NoReplicationCallsOnAssignment —
// when ReplicationVolume is nil (T0 / observer-only), apply flows
// straight to OnAssignment; the fact's peer list + generation are
// ignored on this host.
func TestHost_ApplyFact_ObserverOnly_NoReplicationCallsOnAssignment(t *testing.T) {
	rec := &orderingRecorder{}
	a := &stubAdapter{rec: rec}
	h := &Host{
		cfg: Config{
			VolumeID:  "vol1",
			ReplicaID: "r1",
		},
		log:         newDiscardLogger(),
		adpt:        a,
		replication: nil, // observer-only
	}

	fact := &control.AssignmentFact{
		VolumeId:          "vol1",
		ReplicaId:         "r1",
		Epoch:             1,
		EndpointVersion:   1,
		PeerSetGeneration: 1,
		Peers: []*control.ReplicaDescriptor{{
			ReplicaId: "r99",
		}},
	}

	h.applyFact(fact)

	if a.callCount.Load() != 1 {
		t.Fatalf("observer-only must call adapter: got %d", a.callCount.Load())
	}
	seq := rec.snapshot()
	if len(seq) != 1 || seq[0] != "OnAssignment" {
		t.Fatalf("observer-only sequence: got %v, want [OnAssignment]", seq)
	}
}

// --- Test 5: auxiliary field-copy test for decodeReplicaTargets ---

// TestDecodeReplicaTargets_FieldCopy — small auxiliary pin verifying
// the host-only decoder is a pure field copy (matches
// decodeAssignmentFact discipline). Not the main T4a-5 coverage
// (applyFact tests above own that), but fence against future drift.
func TestDecodeReplicaTargets_FieldCopy(t *testing.T) {
	fact := &control.AssignmentFact{
		Peers: []*control.ReplicaDescriptor{
			{
				ReplicaId:       "peer-alpha",
				Epoch:           11,
				EndpointVersion: 22,
				DataAddr:        "10.0.0.1:5000",
				CtrlAddr:        "10.0.0.1:5001",
			},
			{
				ReplicaId:       "peer-beta",
				Epoch:           12,
				EndpointVersion: 23,
				DataAddr:        "10.0.0.2:5000",
				CtrlAddr:        "10.0.0.2:5001",
			},
		},
		PeerSetGeneration: 99,
	}

	targets, gen := decodeReplicaTargets(fact)

	if gen != 99 {
		t.Fatalf("generation: got %d want 99", gen)
	}
	if len(targets) != 2 {
		t.Fatalf("targets length: got %d want 2", len(targets))
	}
	expect := []replication.ReplicaTarget{
		{ReplicaID: "peer-alpha", Epoch: 11, EndpointVersion: 22, DataAddr: "10.0.0.1:5000", ControlAddr: "10.0.0.1:5001"},
		{ReplicaID: "peer-beta", Epoch: 12, EndpointVersion: 23, DataAddr: "10.0.0.2:5000", ControlAddr: "10.0.0.2:5001"},
	}
	for i, want := range expect {
		got := targets[i]
		if got != want {
			t.Fatalf("target[%d]: got %+v, want %+v", i, got, want)
		}
	}

	// Empty-peers case.
	factEmpty := &control.AssignmentFact{PeerSetGeneration: 5}
	tgts, g := decodeReplicaTargets(factEmpty)
	if len(tgts) != 0 {
		t.Fatalf("empty peers: got %d targets want 0", len(tgts))
	}
	if g != 5 {
		t.Fatalf("empty peers generation: got %d want 5", g)
	}
}
