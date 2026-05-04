package replication

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
)

// DurabilityMode selects the per-volume durability semantic applied
// by SyncLocalAndReplicas. V3 mirrors the V2 BlockVol.DurabilityMode
// three-way choice; the enum values are zero-safe (DurabilityBestEffort
// is the zero value, same as V2).
type DurabilityMode int

const (
	// DurabilityBestEffort accepts local fsync as the completion
	// signal. Replica barrier failures are translated to
	// peer.Invalidate + Degraded (done inside peer.Barrier) but do
	// NOT fail the Sync call.
	DurabilityBestEffort DurabilityMode = iota

	// DurabilitySyncQuorum requires a majority of nodes (primary +
	// replicas) to be durable. rf = len(peers) + 1; quorum = rf/2 + 1.
	// The primary counts as 1 when its local fsync succeeds; a failed
	// local fsync drops the primary's contribution to 0, short-
	// circuiting the mode arithmetic.
	DurabilitySyncQuorum

	// DurabilitySyncAll requires EVERY configured replica's barrier
	// to succeed. Any peer failure fails the Sync call.
	DurabilitySyncAll
)

func (m DurabilityMode) String() string {
	switch m {
	case DurabilityBestEffort:
		return "best_effort"
	case DurabilitySyncQuorum:
		return "sync_quorum"
	case DurabilitySyncAll:
		return "sync_all"
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

// ErrDurabilityBarrierFailed is returned by SyncLocalAndReplicas in
// DurabilitySyncAll mode when any peer barrier fails. Mirrors V2
// blockerr.ErrDurabilityBarrierFailed.
var ErrDurabilityBarrierFailed = errors.New("replication: sync_all durability barrier failed")

// ErrDurabilityQuorumLost is returned by SyncLocalAndReplicas in
// DurabilitySyncQuorum mode when fewer than quorum nodes are
// durable. Mirrors V2 blockerr.ErrDurabilityQuorumLost.
var ErrDurabilityQuorumLost = errors.New("replication: sync_quorum quorum not met")

// DurabilityResult is the per-Sync outcome reported by
// SyncLocalAndReplicas. DurableNodes counts the primary (1 iff local
// fsync succeeded) plus successfully-acked peer barriers. Quorum is
// the mode-derived required count. FailedPeers lists the replica IDs
// whose barrier errored (captured regardless of mode — every mode
// observes per-peer failures via peer.Barrier's internal
// Invalidate). Err is non-nil iff mode arithmetic declared failure.
type DurabilityResult struct {
	DurableNodes int
	Quorum       int
	FailedPeers  []string
	Err          error
}

// DurabilityCoordinator fans out local fsync + per-peer barrier
// calls in parallel and applies mode-dependent arithmetic to the
// result. Stateless; reusable across volumes. Matches V2
// MakeDistributedSync at the architectural level; V2 bound the
// closure to a specific volume at construction, V3 takes peers as a
// call-time argument so mode-change per call is natural.
type DurabilityCoordinator struct {
	// No config fields in T4b-4 MVP. V2 had none either.
}

// NewDurabilityCoordinator returns a reusable coordinator.
//
// Called by: Host / Provider composition root at volume lifecycle
// start (T4b-5 wiring); tests directly.
// Owns: nothing (stateless).
// Borrows: nothing.
func NewDurabilityCoordinator() *DurabilityCoordinator {
	return &DurabilityCoordinator{}
}

// SyncLocalAndReplicas runs localSync and a per-peer barrier in
// parallel, then applies mode-dependent arithmetic to decide the
// overall durability outcome. Returns the DurabilityResult and an
// error that mirrors result.Err for caller convenience.
//
// Execution model:
//
//  1. Zero-peers fast-path: if len(peers) == 0, localSync runs
//     serially; no goroutines spawned; no parallel machinery.
//     Primary is the only durable node. Pins
//     INV-REPL-ZERO-PEER-NO-SPAWN (V2 dist_group_commit.go:19-22
//     equivalent).
//
//  2. Parallel path: len(peers) >= 1. One goroutine runs localSync;
//     len(peers) goroutines each run peer.Barrier(ctx, targetLSN).
//     Flat wg.Add(1 + len(peers)) — no nested WaitGroup (V2's
//     two-level Add(2) + BarrierAll internal Add(n) nesting is
//     layout-only; see G-1 concern #11).
//
//  3. Local-fsync short-circuit: if localSync returned an error,
//     that error is surfaced verbatim and mode arithmetic is
//     skipped. Primary is the "1" in durableNodes = 1 + successes;
//     if it failed, checking quorum/sync-all is meaningless (G-1
//     concern #7).
//
//  4. Mode arithmetic (EvaluateBarrierAcks) otherwise runs over the
//     per-peer error vector. Per-peer peer-state mutation
//     (Invalidate + Degraded) already happened inside peer.Barrier
//     — see INV-REPL-BARRIER-FAILURE-DEGRADES-PEER pinned at T4b-3.
//
// Called by: ReplicationVolume.Sync (T4b-5).
// Owns: parallel fan-out; DurabilityResult construction; FailedPeers
// slice assembly.
// Borrows: localSync closure from caller; peers slice from caller
// (peer pointers retained only for the call duration).
func (d *DurabilityCoordinator) SyncLocalAndReplicas(
	ctx context.Context,
	mode DurabilityMode,
	targetLSN uint64,
	localSync func(ctx context.Context) (uint64, error),
	peers []*ReplicaPeer,
) (DurabilityResult, error) {
	rf := len(peers) + 1
	quorum := rf/2 + 1

	// Zero-peers fast-path (G-1 concern #9 / INV-REPL-ZERO-PEER-NO-SPAWN).
	if len(peers) == 0 {
		if _, err := localSync(ctx); err != nil {
			return DurabilityResult{DurableNodes: 0, Quorum: quorum, Err: err}, err
		}
		return DurabilityResult{DurableNodes: 1, Quorum: quorum}, nil
	}

	var (
		localErr error
		errs     = make([]error, len(peers))
		wg       sync.WaitGroup
	)
	wg.Add(1 + len(peers))
	go func() {
		defer wg.Done()
		_, localErr = localSync(ctx)
	}()
	for i, p := range peers {
		go func(idx int, peer *ReplicaPeer) {
			defer wg.Done()
			// peer.Barrier owns the error → Invalidate translation
			// per INV-REPL-BARRIER-FAILURE-DEGRADES-PEER. Coordinator
			// sees only ack + error and uses the error vector for
			// mode arithmetic.
			_, err := peer.Barrier(ctx, targetLSN)
			errs[idx] = err
		}(i, p)
	}
	wg.Wait()

	// Local-fsync short-circuit (G-1 concern #7 / V2 lines 46-48).
	if localErr != nil {
		// Collect FailedPeers anyway so diagnostic log of failed
		// peers survives even when local fsync is the primary cause.
		failed := collectFailedPeerIDs(peers, errs)
		return DurabilityResult{
			DurableNodes: 0,
			Quorum:       quorum,
			FailedPeers:  failed,
			Err:          localErr,
		}, localErr
	}

	// Mode arithmetic.
	result, err := d.EvaluateBarrierAcks(mode, rf, true, errs)
	result.FailedPeers = collectFailedPeerIDs(peers, errs)
	// Log per-peer failures for operator visibility. V2 had
	// vol.degradeReplica(err) as a log line; V3 logs here with peer
	// IDs so operators can grep on failure scope.
	for i, e := range errs {
		if e != nil {
			log.Printf("replication: durability barrier failed peer=%s lsn=%d err=%v",
				peers[i].Target().ReplicaID, targetLSN, e)
		}
	}
	return result, err
}

// EvaluateBarrierAcks applies mode arithmetic to a pre-collected
// per-peer error vector + local fsync success flag. Exposed as a
// pure function (no I/O) for unit testing and reuse.
//
// Called by: SyncLocalAndReplicas internally; tests directly.
// Owns: the arithmetic decision tree.
// Borrows: perPeerErrors slice — caller retains.
func (d *DurabilityCoordinator) EvaluateBarrierAcks(
	mode DurabilityMode,
	rf int,
	localSyncSucceeded bool,
	perPeerErrors []error,
) (DurabilityResult, error) {
	quorum := rf/2 + 1

	failCount := 0
	for _, err := range perPeerErrors {
		if err != nil {
			failCount++
		}
	}
	primaryDurable := 0
	if localSyncSucceeded {
		primaryDurable = 1
	}
	durableNodes := primaryDurable + (len(perPeerErrors) - failCount)

	result := DurabilityResult{
		DurableNodes: durableNodes,
		Quorum:       quorum,
	}

	switch mode {
	case DurabilitySyncAll:
		if failCount > 0 {
			result.Err = fmt.Errorf("%w: %d of %d barriers failed",
				ErrDurabilityBarrierFailed, failCount, len(perPeerErrors))
		}
	case DurabilitySyncQuorum:
		if durableNodes < quorum {
			result.Err = fmt.Errorf("%w: %d durable of %d needed",
				ErrDurabilityQuorumLost, durableNodes, quorum)
		}
	default:
		// DurabilityBestEffort — V2 dist_group_commit.go:80-82
		// fall-through. Per-peer peer.Invalidate already happened
		// inside peer.Barrier (T4b-3); nothing further here.
	}
	return result, result.Err
}

// collectFailedPeerIDs builds the FailedPeers slice for
// DurabilityResult. Side-effect-free; reads peer IDs without locking
// since Target() is read-only on the peer struct.
func collectFailedPeerIDs(peers []*ReplicaPeer, errs []error) []string {
	failed := []string{}
	for i, e := range errs {
		if e != nil {
			failed = append(failed, peers[i].Target().ReplicaID)
		}
	}
	return failed
}

