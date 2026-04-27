// Package volume probe-loop wiring (G5-5C).
//
// This file holds the *production* glue between the replication
// layer's degraded-peer probe loop (core/replication/probe_loop.go)
// and the engine-side recovery decision path (core/engine via
// core/adapter). The probe loop itself owns timing, cooldown, and
// in-flight gating; this wiring owns "what happens when the loop
// decides to dispatch a probe to peer X":
//
//   1. Read the peer's Target outside p.mu (peer.Target() is
//      read-only after construction — no lock needed).
//   2. Mint a fresh probe sessionID via replication.MintProbeSessionID.
//   3. Call peer.Executor().Probe(...) to perform the wire probe.
//   4. Forward the resulting adapter.ProbeResult into the per-volume
//      VolumeReplicaAdapter via OnProbeResult — that is the engine's
//      official ingress for probe facts (NormalizeProbe →
//      ProbeSucceeded/Failed + RecoveryFactsObserved).
//
// Lock-order discipline (architect 2026-04-27 #2): the probe call at
// step 3 happens with peer.mu RELEASED (the probe loop's
// dispatchProbe enforces this — ProbeIfDegraded returns under
// peer.mu, the loop releases the lock before calling probeFn).
// adapter.OnProbeResult acquires its own internal locks; it MUST
// NOT call back into peer.mu or volume.mu.

package volume

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/replication"
)

// ProductionProbeFn returns a replication.ProbeFn suitable for
// installation via ReplicationVolume.ConfigureProbeLoop on a host
// running the production binary. The returned function:
//
//   - mints a fresh probe sessionID via replication.MintProbeSessionID;
//   - dials peer.Executor().Probe with the peer's current Target
//     fields (DataAddr / CtrlAddr / Epoch / EndpointVersion);
//   - forwards the raw adapter.ProbeResult into adapter.OnProbeResult
//     so the engine's NormalizeProbe path drives the recovery
//     decision (probe → R/S/H → decide → catch-up / rebuild / none);
//   - returns a non-nil error iff the probe failed to complete (so
//     the probe loop's ResultFn can advance per-peer backoff).
//     A successful probe with FailReason set still returns nil here
//     — the *transport* completed; the *engine* will classify the
//     failure semantically via the OnProbeResult ingress and there
//     is no engine async result mixed into the probe transport
//     return value (architect Batch #5 self-check #3).
//
// The returned probeFn is safe to share across all peers of the
// same volume; ReplicaID identification flows from the *ReplicaPeer
// argument the loop passes in.
//
// Called by: cmd/blockvolume main wire, after the host's adapter
// has been constructed and the ReplicationVolume has been built.
// Owns: nothing (pure closure over the supplied adapter).
// Borrows: a *adapter.VolumeReplicaAdapter pointer that the caller
// retains; lifetime must outlive the probe loop's lifetime.
func ProductionProbeFn(adpt *adapter.VolumeReplicaAdapter) replication.ProbeFn {
	if adpt == nil {
		// Fail-closed shim: if no adapter is wired, return a probeFn
		// that ALWAYS reports failure (so cooldown progresses + log
		// is loud). Callers should not pass nil; this guard exists to
		// keep the probe-loop subsystem from panicking if the
		// composition root is misconfigured.
		return func(_ context.Context, peer *replication.ReplicaPeer) error {
			return fmt.Errorf("ProductionProbeFn: nil adapter wired (peer=%s)", peer.Target().ReplicaID)
		}
	}
	return func(ctx context.Context, peer *replication.ReplicaPeer) error {
		// (1) Read target outside peer.mu — Target is immutable after
		// peer construction (lineage bumps create new *ReplicaPeer
		// instances per UpdateReplicaSet teardown-and-recreate).
		t := peer.Target()
		if t.Epoch == 0 || t.EndpointVersion == 0 {
			return fmt.Errorf("ProductionProbeFn: peer %s has zero lineage (epoch=%d, EV=%d)",
				t.ReplicaID, t.Epoch, t.EndpointVersion)
		}
		// (2) Mint a fresh probe sessionID. This is distinct from the
		// peer's live-ship sessionID and from the adapter's own
		// probe sessionID counter (used for engine-emitted
		// ProbeReplica commands).
		sessionID := replication.MintProbeSessionID()

		// (3) Wire-level probe (executor.Probe handles the dial +
		// PROBE_REQ/PROBE_RESP exchange + boundary collection).
		// Performed with peer.mu RELEASED — peer.ProbeIfDegraded
		// returned under peer.mu but the loop's dispatchProbe has
		// since released it.
		exec := peer.Executor()
		if exec == nil {
			return fmt.Errorf("ProductionProbeFn: peer %s has nil executor", t.ReplicaID)
		}

		// Honour ctx by short-circuiting if already cancelled. The
		// transport-layer Probe has its own dial timeout (2s) +
		// per-call deadline (3s); we don't push ctx into it because
		// that's a transport API change. If the probe loop is in
		// shutdown mid-probe, the at-most-5s wait is acceptable for
		// G5-5C v0.5; future scaling can plumb ctx through.
		if err := ctx.Err(); err != nil {
			return err
		}

		result := exec.Probe(t.ReplicaID, t.DataAddr, t.ControlAddr, sessionID, t.Epoch, t.EndpointVersion)

		// (4) Forward to adapter — engine ingress for probe facts.
		// adapter.OnProbeResult is non-blocking (returns ApplyLog
		// after Apply runs synchronously). Any panic is recovered by
		// the probe loop's dispatchProbe (CP4B-2 lesson 3).
		// Always feed the adapter, even for failure ProbeResults —
		// engine's NormalizeProbe distinguishes Success path
		// (ProbeSucceeded + RecoveryFactsObserved) from failure path
		// (ProbeFailed) and stale-event guards in apply.go reject
		// any sessionID/epoch mismatch.
		adpt.OnProbeResult(result)

		// Architect Batch #5 self-check #3 binding: backoff advances
		// on TRANSPORT failure (return non-nil error). Engine's
		// *semantic* classification of a successful wire probe (e.g.,
		// "wire OK but R<S, rebuild needed") flows separately
		// through OnProbeResult and does NOT extend the loop's
		// cooldown — mixing those signals would slow recovery
		// dispatch. So:
		//
		//   result.Success == true  → return nil (transport OK;
		//                               cooldown resets to Base on
		//                               next OnProbeAttempt call).
		//   result.Success == false → return non-nil err (transport
		//                               failed; cooldown doubles up
		//                               to Cap to throttle retries
		//                               against an unreachable peer).
		if !result.Success {
			return fmt.Errorf("probe transport failed peer=%s: %s",
				t.ReplicaID, result.FailReason)
		}
		return nil
	}
}
