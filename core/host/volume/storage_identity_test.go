package volume

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storeident"
)

// Every heartbeat carries the durable root's file-system identity as raw
// facts, so the master can detect a root on its authority store's journal.
// An unconfigured identity reports zero (unknown).
func TestBuildReportCarriesStorageIdentity(t *testing.T) {
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "node-a", VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "127.0.0.1:9201", CtrlAddr: "127.0.0.1:9101",
		StorageIdentity: storeident.Identity{MachineID: "machine-1", Dev: 2049}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Close() }()
	r := h.buildReport()
	if r.GetMachineId() != "machine-1" || r.GetDurableRootDev() != 2049 {
		t.Fatalf("report identity = (%q, %d), want (machine-1, 2049)", r.GetMachineId(), r.GetDurableRootDev())
	}

	plain := newTestVolumeHost(t)
	defer func() { _ = plain.Close() }()
	if r := plain.buildReport(); r.GetMachineId() != "" || r.GetDurableRootDev() != 0 {
		t.Fatalf("unconfigured identity reported (%q, %d), want unknown", r.GetMachineId(), r.GetDurableRootDev())
	}
}
