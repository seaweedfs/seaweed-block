package authority

import (
	"context"
	"sync"
	"time"
)

// ============================================================
// P14 S4 — Observation Host
//
// Owns the ObservationStore, wires push-ingest endpoints,
// runs reactive snapshot rebuild, and feeds synthesized
// ClusterSnapshot values into the TopologyController.
//
// Institution logic lives here, not in cmd/sparrow. The cmd
// layer is only allowed to construct, start, and stop the host.
//
// Default operation is build-on-change (sketch §11): every
// successful Ingest fires a non-blocking signal that wakes the
// rebuild goroutine. The goroutine drains the signal, takes one
// store snapshot, runs BuildSnapshot, and hands the
// ClusterSnapshot to the controller's SubmitClusterSnapshot.
// Pending / Unsupported volumes are surfaced via the last
// build's SupportabilityReport — NOT by mutating the controller.
// ============================================================

// ControllerSink is the narrow sink interface the host uses to
// feed synthesized observations into the topology controller.
// Kept as an interface so the host does not depend on the
// concrete TopologyController type and so tests can provide a
// fake.
//
// The sink receives BOTH the supported snapshot AND the
// supportability report in one atomic call. This is intentional:
// the controller must hear pending / unsupported transitions
// even when the supported-volumes set shrinks to zero. An
// earlier draft suppressed submission on empty snapshots, which
// left stale desired state live in the controller after a
// supportability collapse — an architect-blocker finding.
//
// This interface also keeps the boundary-guard check
// deterministic (sketch §16a): observation code only calls
// read-only authority surfaces and this single controller
// intake surface, which is a cluster-state submission, not an
// authority mint.
type ControllerSink interface {
	SubmitObservedState(snap ClusterSnapshot, report SupportabilityReport) error
}

// ObservationHost is the institution host for P14 S4.
type ObservationHost struct {
	store    *ObservationStore
	topology AcceptedTopology
	sink     ControllerSink
	reader   AuthorityBasisReader

	mu         sync.Mutex
	lastBuild  *BuildResult
	sinkErrors []error

	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	gcTick   time.Duration
	gcRetain time.Duration
}

// ObservationHostConfig pins host-wide knobs. GCInterval is
// optional; zero means no periodic GC sweep.
//
// Reader is a read-only authority view (*Publisher satisfies it).
// It is used by BuildSnapshot to stamp the per-volume current
// authority basis into supported VolumeTopologySnapshots so the
// controller's stale-resistance check can succeed on subsequent
// feeds. Reader may be nil for early-boot tests that do not yet
// have a publisher wired up; in production it must be set.
type ObservationHostConfig struct {
	Freshness      FreshnessConfig
	Topology       AcceptedTopology
	Sink           ControllerSink
	Reader         AuthorityBasisReader
	Now            func() time.Time
	GCInterval     time.Duration
	GCRetainWindow time.Duration
}

// NewObservationHost constructs a host. It does NOT start the
// rebuild goroutine — call Start for that.
func NewObservationHost(cfg ObservationHostConfig) *ObservationHost {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	store := NewObservationStore(cfg.Freshness, now)
	gcRetain := cfg.GCRetainWindow
	if gcRetain <= 0 {
		// Reasonable default: four times the freshness window.
		// Long enough that an expired observation stays visible
		// across several diagnosis cycles; short enough to bound
		// memory.
		gcRetain = cfg.Freshness.withDefaults().FreshnessWindow * 4
	}
	h := &ObservationHost{
		store:    store,
		topology: cfg.Topology,
		sink:     cfg.Sink,
		reader:   cfg.Reader,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		gcTick:   cfg.GCInterval,
		gcRetain: gcRetain,
	}
	store.SetOnMutation(h.signalRebuild)
	return h
}

// Store returns the underlying store. Exposed for tests and for
// downstream diagnosis; the host does not expect external code
// to manipulate it beyond Ingest.
func (h *ObservationHost) Store() *ObservationStore {
	return h.store
}

// SetNowForTest forwards a test-controllable clock to the
// underlying ObservationStore. Tests use this to advance the
// freshness window deterministically (e.g., to exercise
// PCDD-DEAD-PEER detection). Production code MUST NOT call this.
//
// Mirrors authority.TopologyController.SetNowForTest — identifier
// name is auditable; a boundary-guard test enforces that
// SetNowForTest appears only in _test.go files.
func (h *ObservationHost) SetNowForTest(now func() time.Time) {
	h.store.SetNowForTest(now)
}

// Ingest is the primary push entry point. Source adapters call
// this with a translated Observation (typically via
// HeartbeatToObservation). Ingest delegates to the store and
// returns its error, if any.
func (h *ObservationHost) Ingest(obs Observation) error {
	return h.store.Ingest(obs)
}

// IngestHeartbeat is a convenience entry for sources that still
// speak the V2-shaped wire. It translates and then calls Ingest.
func (h *ObservationHost) IngestHeartbeat(msg HeartbeatMessage) error {
	obs, err := HeartbeatToObservation(msg)
	if err != nil {
		return err
	}
	return h.Ingest(obs)
}

// LastBuild returns the most recent BuildResult produced by the
// rebuild loop, if any. Safe for concurrent readers; returns a
// value copy.
func (h *ObservationHost) LastBuild() (BuildResult, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastBuild == nil {
		return BuildResult{}, false
	}
	return *h.lastBuild, true
}

// signalRebuild is the mutation callback registered with the
// store. It must be non-blocking: a pending wake is coalesced
// with the new mutation, so build-on-change stays reactive
// without piling up goroutines (sketch §11 coalescing
// allowance).
func (h *ObservationHost) signalRebuild() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

// Start launches the rebuild goroutine. The goroutine runs until
// Stop is called or ctx is cancelled, whichever happens first.
// Returns immediately.
func (h *ObservationHost) Start(ctx context.Context) {
	go h.run(ctx)
}

// Stop requests the rebuild goroutine to exit and waits for it.
// Idempotent.
func (h *ObservationHost) Stop() {
	select {
	case <-h.stop:
		// already stopped
	default:
		close(h.stop)
	}
	<-h.done
}

func (h *ObservationHost) run(ctx context.Context) {
	defer close(h.done)
	var gcC <-chan time.Time
	if h.gcTick > 0 {
		ticker := time.NewTicker(h.gcTick)
		defer ticker.Stop()
		gcC = ticker.C
	}

	// First build on start — so consumers see an initial empty
	// snapshot rather than waiting for the first ingest.
	h.rebuildOnce()

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.stop:
			return
		case <-h.wake:
			h.rebuildOnce()
		case <-gcC:
			h.store.GC(h.gcRetain)
			// GC may have set the mutation flag; that is OK — the
			// next iteration will pick up the wake.
		}
	}
}

// rebuildOnce takes one store snapshot, runs BuildSnapshot, and
// pushes the result (supported snapshot + supportability report)
// to the sink atomically.
//
// Unlike an earlier draft that suppressed submission when no
// supported volumes existed, this method ALWAYS submits if the
// sink is set and the build produced something the controller
// should hear — supported volumes, pending transitions, or
// unsupported transitions. A build with no supported volumes
// AND no report entries is a genuine no-op (e.g. start-up
// before any ingest) and is skipped.
func (h *ObservationHost) rebuildOnce() {
	snap := h.store.Snapshot()
	result := BuildSnapshot(snap, h.topology, h.reader)

	h.mu.Lock()
	h.lastBuild = &result
	h.mu.Unlock()

	if h.sink == nil {
		return
	}
	hasSupported := len(result.Snapshot.Volumes) > 0
	hasReport := len(result.Report.Pending) > 0 || len(result.Report.Unsupported) > 0
	if !hasSupported && !hasReport {
		return
	}
	if err := h.sink.SubmitObservedState(result.Snapshot, result.Report); err != nil {
		h.mu.Lock()
		h.sinkErrors = append(h.sinkErrors, err)
		h.mu.Unlock()
	}
}
