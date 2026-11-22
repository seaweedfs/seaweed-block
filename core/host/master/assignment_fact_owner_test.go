package master

import (
	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/bootstrap"
	"testing"
)

// INV-AUTH-HOLDER-IDENTITY-001 / TERM-HOLDER-001: fault injection at the
// stream encoder, not a claim that publisher ordering can emit an unissued
// line. The captured E2 publication is available while its journal is not.
func TestAssignmentFactKeepsCapturedMoveOwnerWithoutReadableJournal(t *testing.T) {
	h := &Host{boot: &bootstrap.DurableAuthorityBootstrap{Publisher: authority.NewPublisher(authority.NewStaticDirective(nil))}, topo: authority.AcceptedTopology{Volumes: []authority.VolumeExpected{{VolumeID: "v", Slots: []authority.ExpectedSlot{{ReplicaID: "r1", ServerID: "s1"}}}}}}
	for _, owner := range []string{"s2", ""} {
		t.Run("owner="+owner, func(t *testing.T) {
			info := adapter.AssignmentInfo{VolumeID: "v", ReplicaID: "r1", Epoch: 2, EndpointVersion: 1, ServerID: owner, OwnerFromMove: true}
			fact := (&services{host: h}).assignmentFact(info)
			if fact.ServerId != owner {
				t.Fatalf("captured owner %q relabelled %q from placement while journal unavailable", owner, fact.ServerId)
			}
		})
	}
	initial := adapter.AssignmentInfo{VolumeID: "v", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	if got := (&services{host: h}).assignmentFact(initial).ServerId; got != "s1" {
		t.Fatalf("move-less initial placement owner: %q", got)
	}
}
