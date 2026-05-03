package replication

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ProbeLoop is the per-volume primary-side degraded-peer probe loop.
// It iterates over peers in ReplicaDegraded state at a bounded
// interval, calls a host-injected probe function on each, and applies
// per-peer cooldown (with backoff) so flapping peers do not cause a
// retrigger storm.
//
// Layering: this loop is runtime / wiring. It owns the timer,
// iteration discipline, cooldown progression, and in-flight guard.
// It does NOT own the recovery FSM (engine), the probe transport
// (BlockExecutor), or the peer state machine (ReplicaPeer). A
// degraded peer's transition back to Healthy / CatchingUp /
// NeedsRebuild is driven by the engine on probe-result ingest, NOT
// by this loop.
//
// Authority bound: the loop's peer source is a function that
// snapshots the current ReplicationVolume.peers map under lock. It
// does NOT consult master, does NOT enumerate network-discoverable
// addresses. Only master-admitted peers are eligible.
//
// Backoff: per-peer cooldown starts at base (5 s), doubles on
// consecutive probe failures up to a cap (60 s), and resets to base
// on first success.
//
// Pinned by:
//   - INV-G5-5C-PRIMARY-RECOVERY-AUTHORITY-BOUNDED
//   - INV-G5-5C-SINGLE-INFLIGHT-PER-PEER
//   - INV-G5-5C-RECOVERY-BACKOFF
type ProbeLoop struct {
	cfg        ProbeLoopConfig
	peersFn    PeerSourceFn
	probeFn    ProbeFn
	cooldownFn CooldownFn // per-peer cooldown gate
	resultFn   ResultFn   // per-peer probe outcome — drives cooldown progression

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}

	state atomic.Uint32 // probeLoopState
}

// ProbeLoopConfig holds the loop's tuning knobs. Defaults: 5 s
// interval, 1 max concurrent.
type ProbeLoopConfig struct {
	// Interval between iterations. Must be > 0; 0 disables the loop
	// (Start returns without spawning a goroutine).
	Interval time.Duration

	// MaxConcurrent caps the number of probe calls in flight in a
	// single iteration. Currently hard-bound to 1; values > 1 are
	// rejected at construction so we never silently accept a knob
	// the runtime ignores. Future scaling will revisit this
	// constraint with explicit binding.
	MaxConcurrent int

	// CooldownBase is the per-peer cooldown after a probe attempt.
	// Default 5 s. On consecutive failures the cooldown doubles up
	// to CooldownCap; on success it resets to CooldownBase.
	CooldownBase time.Duration

	// CooldownCap caps the backoff. Default 60 s.
	CooldownCap time.Duration
}

// DefaultProbeLoopConfig returns the default probe-loop config.
func DefaultProbeLoopConfig() ProbeLoopConfig {
	return ProbeLoopConfig{
		Interval:      5 * time.Second,
		MaxConcurrent: 1,
		CooldownBase:  5 * time.Second,
		CooldownCap:   60 * time.Second,
	}
}

// PeerSourceFn returns a snapshot of the current peer set. Called once
// per iteration. Implementation MUST acquire the appropriate lock on
// ReplicationVolume so the slice is internally consistent. The loop
// does not mutate the returned slice.
type PeerSourceFn func() []*ReplicaPeer

// ProbeFn is invoked by the loop for each eligible peer. Returns nil
// on success (probe completed and feed-back-to-engine succeeded),
// non-nil on any failure path. The loop forwards the result to
// ResultFn for cooldown progression; ProbeFn itself MUST NOT mutate
// peer state — recovery FSM is the engine's job via OnProbeResult.
type ProbeFn func(ctx context.Context, peer *ReplicaPeer) error

// CooldownFn gates whether a peer is currently eligible for a probe
// attempt. Called once per peer per iteration. Returns true if the
// peer should be probed; false if it is in cooldown or has an
// in-flight session. Implementation tracks per-peer cooldown
// progression based on ResultFn callbacks from prior iterations.
//
// Default implementation (provided by ReplicaPeer in batch 2):
// time-based cooldown stored on the peer itself.
type CooldownFn func(peer *ReplicaPeer) bool

// ResultFn is invoked once per probe attempt with the outcome. The
// loop uses this to advance cooldown / backoff state on the peer
// (engine-side recovery decisions are NOT this seam — those flow
// from inside probeFn via OnProbeResult to the adapter).
//
// err == nil signals a successful probe completion; non-nil err
// indicates a probe failure that should drive backoff (cooldown
// doubling up to cap). The receiver MUST be cheap and non-blocking;
// the loop calls it synchronously after probeFn returns.
type ResultFn func(peer *ReplicaPeer, err error)

// probeLoopState is the loop's lifecycle state.
type probeLoopState uint32

const (
	probeLoopStateNew probeLoopState = iota
	probeLoopStateRunning
	probeLoopStateStopped
)

// NewProbeLoop constructs a probe loop with the given config + seams.
// Returns an error if the config is invalid (e.g., zero interval).
//
// Called by: ReplicationVolume at construction time when a host
// configures probe-loop wiring. ReplicationVolume retains the
// returned *ProbeLoop and calls Start / Stop in its lifecycle.
// Owns: nothing yet — Start spawns the goroutine.
// Borrows: peersFn + probeFn + cooldownFn — caller retains.
func NewProbeLoop(cfg ProbeLoopConfig, peersFn PeerSourceFn, probeFn ProbeFn, cooldownFn CooldownFn, resultFn ResultFn) (*ProbeLoop, error) {
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("replication: ProbeLoop: interval must be > 0 (got %v)", cfg.Interval)
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1
	}
	// MaxConcurrent=1 only. Reject silently-accepted knobs that the
	// runtime ignores.
	if cfg.MaxConcurrent != 1 {
		return nil, fmt.Errorf(
			"replication: ProbeLoop: MaxConcurrent=%d not supported (must be 1)",
			cfg.MaxConcurrent)
	}
	if cfg.CooldownBase <= 0 {
		cfg.CooldownBase = 5 * time.Second
	}
	if cfg.CooldownCap <= 0 {
		cfg.CooldownCap = 60 * time.Second
	}
	if cfg.CooldownCap < cfg.CooldownBase {
		cfg.CooldownCap = cfg.CooldownBase
	}
	if peersFn == nil {
		return nil, fmt.Errorf("replication: ProbeLoop: peersFn is nil")
	}
	if probeFn == nil {
		return nil, fmt.Errorf("replication: ProbeLoop: probeFn is nil")
	}
	if cooldownFn == nil {
		return nil, fmt.Errorf("replication: ProbeLoop: cooldownFn is nil")
	}
	if resultFn == nil {
		return nil, fmt.Errorf("replication: ProbeLoop: resultFn is nil")
	}
	return &ProbeLoop{
		cfg:        cfg,
		peersFn:    peersFn,
		probeFn:    probeFn,
		cooldownFn: cooldownFn,
		resultFn:   resultFn,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}, nil
}

// Start begins the loop. Idempotent — second and later calls are
// no-ops. Returns nil on success. Safe to call before Stop; safe to
// call after Stop (returns nil but does not restart — once stopped,
// the loop is terminal).
//
// CP4B-2 lesson 1: Stop-before-Start MUST NOT deadlock. We use sync.Once
// for both Start and Stop so neither blocks on the other.
//
// Called by: ReplicationVolume when a primary role is admitted and
// probe-loop wiring is configured.
// Owns: spawns the loop goroutine (one per ProbeLoop instance).
// Borrows: nothing.
func (l *ProbeLoop) Start() error {
	if l == nil {
		return nil
	}
	var startErr error
	l.startOnce.Do(func() {
		// If already stopped before Start, skip the goroutine entirely.
		// The doneCh stays open until first Start; if Stop already ran,
		// it closed doneCh as well so subsequent waits return immediately.
		if l.state.Load() == uint32(probeLoopStateStopped) {
			return
		}
		l.state.Store(uint32(probeLoopStateRunning))
		go l.run()
	})
	return startErr
}

// Stop signals the loop to terminate and waits for the goroutine to
// exit. Idempotent. Safe to call before Start (returns immediately).
// Safe to call from any goroutine.
//
// CP4B-2 lesson 1: Stop must not deadlock when called before Start.
// We close stopCh and doneCh atomically via sync.Once.
//
// Called by: ReplicationVolume.Close.
// Owns: closes stopCh; waits on doneCh.
// Borrows: nothing.
func (l *ProbeLoop) Stop() {
	if l == nil {
		return
	}
	l.stopOnce.Do(func() {
		close(l.stopCh)
		// If the loop was never started, doneCh would never close on
		// its own; close it here so any future external wait returns.
		// We detect "never started" by observing state remained New.
		if l.state.Load() == uint32(probeLoopStateNew) {
			l.state.Store(uint32(probeLoopStateStopped))
			close(l.doneCh)
			return
		}
		l.state.Store(uint32(probeLoopStateStopped))
	})
	// Wait outside the Once so concurrent Stop callers all see the goroutine exit.
	<-l.doneCh
}

// run is the loop body. Spawned by Start; exits when stopCh is closed.
func (l *ProbeLoop) run() {
	defer close(l.doneCh)

	ticker := time.NewTicker(l.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			l.tick()
		}
	}
}

// tick runs one iteration of the loop: snapshot peers, filter to
// degraded + cooldown-eligible, call probeFn on each up to
// MaxConcurrent, do not block forever on any single probe.
//
// CP4B-2 lesson 3: callback panic must NOT crash the goroutine. We
// wrap each probeFn call in a recover.
func (l *ProbeLoop) tick() {
	peers := l.peersFn()
	if len(peers) == 0 {
		return
	}

	// Sequential dispatch (MaxConcurrent=1). Parallel dispatch is a
	// future optimization.
	for _, peer := range peers {
		select {
		case <-l.stopCh:
			return
		default:
		}

		// Authority-bounded (§1.E): peer was sourced from
		// ReplicationVolume.peers (master-admitted). No additional
		// authority check needed here.

		// Single in-flight + cooldown gate (§1.G #3 + #7).
		// cooldownFn is the canonical gate; ReplicaPeer ProbeIfDegraded
		// wraps the state-check + cooldown atomically (added in step 2).
		if !l.cooldownFn(peer) {
			continue
		}

		l.dispatchProbe(peer)
	}
}

// dispatchProbe invokes probeFn on a single peer, with panic recovery
// per CP4B-2 lesson 3 (callback panic must not crash the loop). On
// return — whether nil error, non-nil error, or panic — invokes
// resultFn so cooldown progression is always advanced.
func (l *ProbeLoop) dispatchProbe(peer *ReplicaPeer) {
	// Best-effort context for the probe call. Tied to stopCh so a
	// long-running probe is unblocked by Stop.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-l.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	var probeErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("replication: ProbeLoop probeFn panic peer=%s: %v",
					peer.target.ReplicaID, r)
				probeErr = fmt.Errorf("probeFn panic: %v", r)
			}
		}()
		probeErr = l.probeFn(ctx, peer)
	}()

	if probeErr != nil {
		log.Printf("replication: ProbeLoop probe failed peer=%s: %v",
			peer.target.ReplicaID, probeErr)
	}

	// Forward outcome to cooldown/backoff seam, with panic-safe wrap
	// (CP4B-2 lesson 3 again — defensive on the result callback too).
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("replication: ProbeLoop resultFn panic peer=%s: %v",
					peer.target.ReplicaID, r)
			}
		}()
		l.resultFn(peer, probeErr)
	}()
}
