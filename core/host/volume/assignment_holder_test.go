package volume

import (
	"strings"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// r2.34 AUTHORITY: replica r1 and loopback addresses are reused after a move.
// Field 9 is encoded on the wire so this regression also runs on the old schema.
func holderAssignment(t *testing.T, server string) *control.AssignmentFact {
	t.Helper()
	f := &control.AssignmentFact{VolumeId: "v1", ReplicaId: "r1", Epoch: 2, EndpointVersion: 1, DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304"}
	b, err := proto.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if server != "" {
		b = protowire.AppendTag(b, 9, protowire.BytesType)
		b = protowire.AppendString(b, server)
	}
	if err = proto.Unmarshal(b, f); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAssignmentHolderIdentityRestartedOldProcess(t *testing.T) {
	for _, server := range []string{"new-host", ""} {
		t.Run("server="+server, func(t *testing.T) {
			h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "old-host", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			// Forced move has kept r1; the restarted old process receives r1@2.
			// Endpoint-only matching is deliberately indistinguishable here.
			h.applyFact(holderAssignment(t, server))
			if p := h.realAdpt.Projection(); p.Epoch != 0 {
				t.Errorf("old process adopted foreign/missing server: %+v", p)
			}
			if h.LastOtherLine() == nil {
				t.Error("foreign primary line was not recorded")
			}
			for _, cmd := range h.t1exec.Commands() {
				if strings.HasPrefix(cmd, "PublishHealthy:") {
					t.Errorf("foreign line published healthy: %s", cmd)
				}
			}
			if h.view.Projection().Healthy {
				t.Error("foreign assignment made the frontend healthy")
			}
		})
	}
}

func TestAssignmentHolderIdentityMatchingServerStillAdopts(t *testing.T) {
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "new-host", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.applyFact(holderAssignment(t, "new-host"))
	deadline := time.Now().Add(time.Second)
	for !h.view.Projection().Healthy && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p := h.view.Projection(); !p.Healthy || p.Epoch != 2 {
		t.Fatalf("matching holder did not become healthy: %+v", p)
	}
}

// The old process may already have been healthy before the forced move. The
// foreign line must withdraw that projection even though r1 is unchanged.
func TestAssignmentHolderIdentityFencesPreviouslyHealthySameReplica(t *testing.T) {
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "old-host", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	own := holderAssignment(t, "old-host")
	own.Epoch = 1
	h.applyFact(own)
	deadline := time.Now().Add(time.Second)
	for !h.view.Projection().Healthy && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !h.view.Projection().Healthy {
		t.Fatal("old owner baseline not healthy")
	}
	h.applyFact(holderAssignment(t, "new-host"))
	if h.realAdpt.Projection().Epoch != 1 {
		t.Fatal("foreign epoch adopted")
	}
	if h.view.Projection().Healthy {
		t.Fatal("same-r1 foreign holder left the old frontend healthy")
	}
}

func TestAssignmentHolderIdentityRefusesCarriedEndpointMismatch(t *testing.T) {
	for _, endpoint := range []string{"data", "control"} {
		t.Run(endpoint, func(t *testing.T) {
			h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "new-host", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			f := holderAssignment(t, "new-host")
			if endpoint == "data" {
				f.DataAddr = "127.0.0.1:9991"
			} else {
				f.CtrlAddr = "127.0.0.1:9992"
			}
			h.applyFact(f)
			if h.realAdpt.Projection().Epoch != 0 || h.LastOtherLine() == nil {
				t.Fatal("mismatched endpoint was adopted or not recorded")
			}
		})
	}
}
