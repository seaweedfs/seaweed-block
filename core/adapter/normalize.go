// Package adapter connects the V3 mini engine to real runtime observations
// and command execution. It is the boundary where runtime facts become
// semantic events and semantic commands become runtime actions.
//
// The adapter has exactly four jobs:
//   1. Gather runtime observations
//   2. Normalize them into engine events
//   3. Execute engine commands
//   4. Feed back session-close truth
//
// The adapter must NOT:
//   - Invent identity truth
//   - Decide recovery class from convenience state
//   - Declare terminal success from progress/ack
//   - Use projection as a hidden control channel
package adapter

import (
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// --- Runtime observations (raw facts from transport/heartbeat) ---

// AssignmentInfo is a raw assignment from the master heartbeat.
type AssignmentInfo struct {
	VolumeID        string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	DataAddr        string
	CtrlAddr        string
}

// ProbeResult is the outcome of a transport probe.
type ProbeResult struct {
	ReplicaID       string
	Success         bool
	EndpointVersion uint64
	TransportEpoch  uint64
	FailReason      string

	// Recovery boundaries from the probe handshake.
	ReplicaFlushedLSN uint64 // R
	PrimaryTailLSN    uint64 // S
	PrimaryHeadLSN    uint64 // H
}

// SessionCloseResult is the outcome of a completed/failed recovery session.
type SessionCloseResult struct {
	ReplicaID   string
	SessionID   uint64
	Success     bool
	AchievedLSN uint64
	FailReason  string
}

// --- Normalization: raw facts → engine events ---

// NormalizeAssignment converts a master assignment into engine events.
// This is the identity truth ingress path.
func NormalizeAssignment(a AssignmentInfo) engine.Event {
	return engine.AssignmentObserved{
		VolumeID:        a.VolumeID,
		ReplicaID:       a.ReplicaID,
		Epoch:           a.Epoch,
		EndpointVersion: a.EndpointVersion,
		DataAddr:        a.DataAddr,
		CtrlAddr:        a.CtrlAddr,
	}
}

// NormalizeProbe converts a probe result into engine events.
// A successful probe produces up to 2 events:
//   1. ProbeSucceeded (reachability)
//   2. RecoveryFactsObserved (R/S/H boundaries)
// A failed probe produces 1 event:
//   1. ProbeFailed
//
// The adapter NEVER decides recovery class here. It only reports facts.
// The engine's decide() function uses R/S/H to classify.
func NormalizeProbe(p ProbeResult) []engine.Event {
	if !p.Success {
		return []engine.Event{
			engine.ProbeFailed{
				ReplicaID: p.ReplicaID,
				Reason:    p.FailReason,
			},
		}
	}

	events := []engine.Event{
		engine.ProbeSucceeded{
			ReplicaID:       p.ReplicaID,
			EndpointVersion: p.EndpointVersion,
			TransportEpoch:  p.TransportEpoch,
		},
	}

	// If R/S/H boundaries are available, report them as recovery facts.
	// The engine decides whether this means catch_up, rebuild, or none.
	if p.PrimaryHeadLSN > 0 {
		events = append(events, engine.RecoveryFactsObserved{
			ReplicaID: p.ReplicaID,
			R:         p.ReplicaFlushedLSN,
			S:         p.PrimaryTailLSN,
			H:         p.PrimaryHeadLSN,
		})
	}

	return events
}

// NormalizeSessionClose converts a session close result into an engine event.
// This is one of only two terminal truth paths.
func NormalizeSessionClose(r SessionCloseResult) engine.Event {
	if r.Success {
		return engine.SessionClosedCompleted{
			ReplicaID:   r.ReplicaID,
			SessionID:   r.SessionID,
			AchievedLSN: r.AchievedLSN,
		}
	}
	return engine.SessionClosedFailed{
		ReplicaID: r.ReplicaID,
		SessionID: r.SessionID,
		Reason:    r.FailReason,
	}
}

// NormalizeSessionPrepared creates a SessionPrepared event when the
// adapter has set up a recovery session in response to a Start* command.
func NormalizeSessionPrepared(replicaID string, sessionID uint64, kind engine.SessionKind, targetLSN uint64) engine.Event {
	return engine.SessionPrepared{
		ReplicaID: replicaID,
		SessionID: sessionID,
		Kind:      kind,
		TargetLSN: targetLSN,
	}
}

// NormalizeSessionStarted creates a SessionStarted event when
// the session begins executing.
func NormalizeSessionStarted(replicaID string, sessionID uint64) engine.Event {
	return engine.SessionStarted{
		ReplicaID: replicaID,
		SessionID: sessionID,
	}
}
