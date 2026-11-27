package transport

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Each cause of a refused mutation lineage has its own reason, so a refusal
// log line says which one it was: a primary listener, an epoch below the
// floor, no role yet, a lineage below the held one, and the same lineage
// with another frontier hint. An admitted lineage has none.
func TestMutationRefusalNamesItsCause(t *testing.T) {
	held := RecoveryLineage{SessionID: 5, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}
	for _, c := range []struct {
		name     string
		setup    func(r *ReplicaListener)
		incoming RecoveryLineage
		want     string
	}{
		{"primary", func(r *ReplicaListener) { r.primary = true }, RecoveryLineage{SessionID: 6, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}, "is a primary"},
		{"below the floor", func(r *ReplicaListener) { r.epochFloor = 3 }, RecoveryLineage{SessionID: 6, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}, "below the floor 3"},
		{"no role", func(r *ReplicaListener) { r.requireRole, r.assigned = true, false }, RecoveryLineage{SessionID: 6, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}, "no role assigned"},
		{"below the held lineage", func(r *ReplicaListener) { r.activeLineage = held }, RecoveryLineage{SessionID: 1, Epoch: 2, EndpointVersion: 1, TargetLSN: 66}, "below the held lineage session=5"},
		{"same lineage, another hint", func(r *ReplicaListener) { r.activeLineage = held }, RecoveryLineage{SessionID: 5, Epoch: 2, EndpointVersion: 1, TargetLSN: 66}, "another frontier hint"},
		{"admitted", func(r *ReplicaListener) { r.activeLineage = held }, RecoveryLineage{SessionID: 6, Epoch: 2, EndpointVersion: 1, TargetLSN: 66}, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			l, err := NewReplicaListener("127.0.0.1:0", storage.NewBlockStore(8, 4096))
			if err != nil {
				t.Fatal(err)
			}
			defer l.Stop()
			l.mu.Lock()
			c.setup(l)
			why := l.mutationLineageRefusalLocked(c.incoming)
			l.mu.Unlock()
			if c.want == "" {
				if why != "" {
					t.Fatalf("refused %q; want admitted", why)
				}
				return
			}
			if !strings.Contains(why, c.want) {
				t.Fatalf("reason %q; want it to name %q", why, c.want)
			}
		})
	}
}
