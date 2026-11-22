package volume

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
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
	stale := holderAssignment(t, "new-host")
	stale.Epoch = 0
	h.applyFact(stale)
	if h.view.Projection().Healthy {
		t.Fatal("stale foreign replay erased the newer fencing line")
	}
}

// Endpoints are routing data, not the holder identity (claude06 ruling).
func TestAssignmentHolderIdentityAdoptsAndWarnsCarriedEndpointMismatch(t *testing.T) {
	for _, endpoint := range []string{"data", "control"} {
		t.Run(endpoint, func(t *testing.T) {
			logs := &lockedBuffer{}
			h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "new-host", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true, Logger: log.New(logs, "", 0)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			f := holderAssignment(t, "new-host")
			if endpoint == "data" {
				f.DataAddr = "0.0.0.0:19303"
			} else {
				f.CtrlAddr = "localhost:19304"
			}
			h.BlockLocalReadiness("awaiting durable recovery")
			h.applyFact(f)
			h.ClearLocalReadinessBlock()
			h.replayLocalReadinessFactIfReady()
			deadline := time.Now().Add(time.Second)
			for !h.view.Projection().Healthy && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if p := h.view.Projection(); !p.Healthy || p.Epoch != 2 || h.LastOtherLine() != nil {
				t.Fatalf("own endpoint spelling fenced holder: %+v", p)
			}
			st := NewStatusServer(h.ProjectionView()).statusProjection()
			if len(st.Warnings) != 1 || !strings.Contains(st.Warnings[0], "master advertises data=") || !strings.Contains(st.Warnings[0], "this process listens on") {
				t.Fatalf("status warning missing: %+v", st.Warnings)
			}
			h.applyFact(f)
			if n := strings.Count(logs.String(), "master advertises data="); n != 1 {
				t.Fatalf("warning logged %d times, want once", n)
			}
			corrected := holderAssignment(t, "new-host")
			corrected.EndpointVersion = 2
			h.applyFact(corrected)
			if got := NewStatusServer(h.ProjectionView()).statusProjection().Warnings; len(got) != 0 {
				t.Fatalf("corrected endpoints retained warning: %v", got)
			}
		})
	}
}

type holderFenceCounter struct{ calls atomic.Uint32 }

func (f *holderFenceCounter) FenceAsPrimary(uint64) error { f.calls.Add(1); return nil }
func (f *holderFenceCounter) AdmitAsReplica(uint64) error { f.calls.Add(1); return nil }

// INV-AUTH-HOLDER-IDENTITY-001 / M3: a recorded foreign fact supersedes
// only an adopted own lineage and only when it is strictly newer.
func TestAssignmentHolderIdentityForeignStreamOrdering(t *testing.T) {
	for _, mode := range []string{"target-old-first", "target-own-endpoint-first", "served-newer-foreign", "served-newer-endpoint-version", "served-old-replay", "served-equal-replay", "never-assigned"} {
		t.Run(mode, func(t *testing.T) {
			h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "new-host", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			fencer := &holderFenceCounter{}
			if err := h.SetSelfFence(fencer); err != nil {
				t.Fatal(err)
			}
			own := holderAssignment(t, "new-host")
			foreign := holderAssignment(t, "old-host")
			foreign.Epoch = 1
			waitHealthy := func() {
				t.Helper()
				deadline := time.Now().Add(time.Second)
				for !h.view.Projection().Healthy && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if p := h.view.Projection(); !p.Healthy || p.Epoch != 2 {
					t.Fatalf("rightful target did not serve: %+v", p)
				}
			}
			switch mode {
			case "target-old-first":
				h.BlockLocalReadiness("awaiting durable recovery")
				h.applyFact(foreign)
				if fencer.calls.Load() != 0 || h.IsSuperseded("r1", 0, 0) {
					t.Fatal("unassigned target was fenced by the current old line")
				}
				h.applyFact(own)
				h.ClearLocalReadinessBlock()
				h.replayLocalReadinessFactIfReady()
				waitHealthy()
			case "target-own-endpoint-first":
				h.BlockLocalReadiness("awaiting durable recovery")
				h.applyFact(own)
				mismatch := holderAssignment(t, "new-host")
				mismatch.DataAddr = "127.0.0.1:9991"
				h.applyFact(mismatch)
				if fencer.calls.Load() != 0 {
					t.Fatal("early endpoint refusal fenced unassigned target")
				}
				h.ClearLocalReadinessBlock()
				h.replayLocalReadinessFactIfReady()
				waitHealthy()
			case "never-assigned":
				h.applyFact(foreign)
				foreign.Epoch = 2
				h.applyFact(foreign)
				if fencer.calls.Load() != 0 || h.realAdpt.Projection().Epoch != 0 || h.view.Projection().Healthy || h.IsSuperseded("r1", 0, 0) || h.LastOtherLine() == nil {
					t.Fatal("never-assigned process must record, wait, neither adopt nor fence")
				}
			default:
				h.applyFact(own)
				waitHealthy()
				before := fencer.calls.Load()
				if mode == "served-newer-foreign" {
					foreign.Epoch = 3
				}
				if mode == "served-equal-replay" {
					foreign.Epoch = 2
				}
				if mode == "served-newer-endpoint-version" {
					foreign.Epoch = 2
					foreign.EndpointVersion = 2
				}
				h.applyFact(foreign)
				if mode == "served-newer-foreign" || mode == "served-newer-endpoint-version" {
					if fencer.calls.Load() <= before || h.view.Projection().Healthy {
						t.Fatal("served holder was not fenced by newer foreign line")
					}
				} else if fencer.calls.Load() != before || !h.view.Projection().Healthy {
					t.Fatal("old foreign replay fenced a serving holder")
				}
			}
		})
	}
}

// INV-AUTH-HOLDER-IDENTITY-001: an old process cannot adopt a stale
// self-labelled EV1 after it has observed the forced move's foreign EV2.
func TestAssignmentHolderIdentityForcedMoveRefusesEveryDeliveredVersion(t *testing.T) {
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "old-host", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.BlockLocalReadiness("awaiting durable recovery")
	staleSelf := holderAssignment(t, "old-host")
	h.applyFact(staleSelf) // retained before the newer foreign delivery
	for _, ev := range []uint64{1, 2, 1, 3} {
		foreign := holderAssignment(t, "new-host")
		foreign.EndpointVersion = ev
		h.applyFact(foreign)
		if h.realAdpt.Projection().Epoch != 0 {
			t.Fatalf("old server adopted foreign EV%d", ev)
		}
	}
	// Covers the safety boundary even if an older delivery incorrectly
	// names the source: a known newer foreign line still takes precedence.
	h.ClearLocalReadinessBlock()
	h.replayLocalReadinessFactIfReady()
	if p := h.realAdpt.Projection(); p.Epoch != 0 {
		t.Fatalf("old server adopted stale EV1 after foreign newer line: %+v", p)
	}
	for _, cmd := range h.t1exec.Commands() {
		if strings.HasPrefix(cmd, "PublishHealthy:") {
			t.Fatalf("old server published healthy: %s", cmd)
		}
	}
	if h.view.Projection().Healthy {
		t.Fatal("old server served after forced move")
	}
}

// INV-AUTH-HOLDER-IDENTITY-001: a recorded foreign owner at this epoch
// cannot be overwritten by a self-labelled delivery, even at a higher EV.
func TestAssignmentHolderIdentitySameEpochCannotChangeServer(t *testing.T) {
	for _, ev := range []uint64{1, 2, 3} {
		t.Run(fmt.Sprint(ev), func(t *testing.T) {
			h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "old-host", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			foreign := holderAssignment(t, "new-host")
			foreign.EndpointVersion = 2
			h.applyFact(foreign)
			self := holderAssignment(t, "old-host")
			self.EndpointVersion = ev
			h.applyFact(self)
			if p := h.realAdpt.Projection(); p.Epoch != 0 {
				t.Fatalf("same epoch changed from recorded new-host to old-host at EV%d: %+v", ev, p)
			}
		})
	}
}
