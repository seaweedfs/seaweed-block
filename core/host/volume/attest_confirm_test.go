package volume

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type confirmRecorder struct {
	required  bool
	confirmed []map[string]storage.StoreID
}

func (c *confirmRecorder) UpdateReplicaSet(uint64, []replication.ReplicaTarget) error { return nil }
func (c *confirmRecorder) RequireAttestedStores()                                     { c.required = true }
func (c *confirmRecorder) ConfirmAttested(m map[string]storage.StoreID) {
	c.confirmed = append(c.confirmed, m)
}

// A host that attests turns the gate on when its replication slot is
// installed, and passes the stores of an attestation the master
// acknowledged (and only those) to the slot.
func TestHostConfirmsOnlyAnAcknowledgedAttestation(t *testing.T) {
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "s1", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true})
	if err != nil {
		t.Fatal(err)
	}
	rec := &confirmRecorder{}
	h.replicationMu.Lock()
	h.setReplicationLocked(rec)
	h.replicationMu.Unlock()
	if !rec.required {
		t.Fatal("installing the slot did not require attested stores")
	}
	h.confirmAttestation(nil) // a heartbeat without an attestation confirms nothing
	s2 := storage.StoreID{2}
	h.confirmAttestation(&control.StoreAttestation{VolumeId: "v1", ReplicaId: "r1", Epoch: 3, Stores: []*control.AttestedStore{
		{ReplicaId: "r1", StoreId: []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		{ReplicaId: "r2", StoreId: s2[:]},
		{ReplicaId: "bad", StoreId: []byte{1}},
	}})
	if len(rec.confirmed) != 1 || rec.confirmed[0]["r2"] != s2 || len(rec.confirmed[0]) != 2 {
		t.Fatalf("confirmed %v; want one confirmation with r1 and r2 (the malformed entry dropped)", rec.confirmed)
	}
}
