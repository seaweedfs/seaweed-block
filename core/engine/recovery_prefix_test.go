package engine

import "testing"

func TestHealthyPrefixRequiresCompletedRecoveryAndPostCloseAck(t *testing.T) {
	st := helperPrime(t, 1, 1, 100)
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 8, Kind: SessionCatchUp, FrontierHint: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 8})
	result := Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: 8, AchievedLSN: 100})
	for _, command := range result.Commands {
		if _, ok := command.(PublishHealthy); ok {
			t.Fatal("recovery close certified before post-close ack")
		}
	}
	result = Apply(st, DurableAckObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, DurableLSN: 120, PrimaryTailLSN: 1, PrimaryHeadLSN: 120})
	for _, command := range result.Commands {
		if healthy, ok := command.(PublishHealthy); ok {
			if healthy.CertifiedPrefix != 100 || healthy.RecoverySessionID != 8 || healthy.Epoch != 1 || healthy.EndpointVersion != 1 {
				t.Fatalf("healthy publication lost exact completed target: %+v", healthy)
			}
			return
		}
	}
	t.Fatal("post-close ack did not publish certified recovery prefix")
}

func TestHealthyProbeDoesNotCertifyRecoveryPrefix(t *testing.T) {
	st := helperPrime(t, 100, 1, 100)
	result := Apply(st, FenceCompleted{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1})
	found := false
	for _, command := range result.Commands {
		if healthy, ok := command.(PublishHealthy); ok {
			found = true
			if healthy.CertifiedPrefix != 0 || healthy.RecoverySessionID != 0 {
				t.Fatalf("plain healthy probe invented a prefix certificate: %+v", healthy)
			}
		}
	}
	if !found {
		t.Fatal("control did not reach healthy publication")
	}
}
