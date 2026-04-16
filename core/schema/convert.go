package schema

import (
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

// ToEngineEvent converts a schema EventInput to a typed engine Event.
func ToEngineEvent(ei EventInput) (engine.Event, error) {
	switch ei.Kind {
	case "AssignmentObserved":
		return engine.AssignmentObserved{
			VolumeID:        ei.VolumeID,
			ReplicaID:       ei.ReplicaID,
			Epoch:           ei.Epoch,
			EndpointVersion: ei.EndpointVersion,
			DataAddr:        ei.DataAddr,
			CtrlAddr:        ei.CtrlAddr,
		}, nil

	case "EndpointObserved":
		return engine.EndpointObserved{
			ReplicaID:       ei.ReplicaID,
			EndpointVersion: ei.EndpointVersion,
			DataAddr:        ei.DataAddr,
			CtrlAddr:        ei.CtrlAddr,
		}, nil

	case "ReplicaRemoved":
		return engine.ReplicaRemoved{
			ReplicaID: ei.ReplicaID,
			Reason:    ei.Reason,
		}, nil

	case "ProbeSucceeded":
		return engine.ProbeSucceeded{
			ReplicaID:       ei.ReplicaID,
			EndpointVersion: ei.EndpointVersion,
			TransportEpoch:  ei.TransportEpoch,
		}, nil

	case "ProbeFailed":
		return engine.ProbeFailed{
			ReplicaID:       ei.ReplicaID,
			EndpointVersion: ei.EndpointVersion,
			TransportEpoch:  ei.TransportEpoch,
			Reason:          ei.Reason,
		}, nil

	case "RecoveryFactsObserved":
		return engine.RecoveryFactsObserved{
			ReplicaID:       ei.ReplicaID,
			EndpointVersion: ei.EndpointVersion,
			TransportEpoch:  ei.TransportEpoch,
			R:               ei.R,
			S:               ei.S,
			H:               ei.H,
		}, nil

	case "SessionPrepared":
		return engine.SessionPrepared{
			ReplicaID: ei.ReplicaID,
			SessionID: ei.SessionID,
			Kind:      engine.SessionKind(ei.SessionKind),
			TargetLSN: ei.TargetLSN,
		}, nil

	case "SessionStarted":
		return engine.SessionStarted{
			ReplicaID: ei.ReplicaID,
			SessionID: ei.SessionID,
		}, nil

	case "SessionProgressObserved":
		return engine.SessionProgressObserved{
			ReplicaID:   ei.ReplicaID,
			SessionID:   ei.SessionID,
			AchievedLSN: ei.AchievedLSN,
		}, nil

	case "SessionClosedCompleted":
		return engine.SessionClosedCompleted{
			ReplicaID:   ei.ReplicaID,
			SessionID:   ei.SessionID,
			AchievedLSN: ei.AchievedLSN,
		}, nil

	case "SessionClosedFailed":
		return engine.SessionClosedFailed{
			ReplicaID: ei.ReplicaID,
			SessionID: ei.SessionID,
			Reason:    ei.Reason,
		}, nil

	case "SessionInvalidated":
		return engine.SessionInvalidated{
			ReplicaID: ei.ReplicaID,
			SessionID: ei.SessionID,
			Reason:    ei.Reason,
		}, nil

	default:
		return nil, fmt.Errorf("schema: unknown event kind %q", ei.Kind)
	}
}
