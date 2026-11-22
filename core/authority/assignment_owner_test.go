package authority

import (
	"github.com/seaweedfs/seaweed-block/core/adapter"
	"testing"
)

// INV-AUTH-HOLDER-IDENTITY-001: an unissued move cannot authorize a
// placement fallback, while a published move captures its own target.
func TestAssignmentOwnerReservedAndIssuedCapture(t *testing.T) {
	p := NewPublisher(nil)
	state := &WorkspaceState{Moves: []WorkspaceMoveRecord{{NewReplicaID: "r1", NewEpoch: 2, NewHost: "s2"}}}
	for _, issued := range []bool{false, true} {
		state.Moves[0].Issued = issued
		info := adapter.AssignmentInfo{VolumeID: "v", ReplicaID: "r1", Epoch: 2, EndpointVersion: 1}
		p.captureAssignmentOwner(&info, state)
		want := ""
		if issued {
			want = "s2"
		}
		if !info.OwnerFromMove || info.ServerID != want {
			t.Fatalf("issued=%t: captured %+v", issued, info)
		}
	}
}
