package nvmerdma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
	master "github.com/seaweedfs/seaweed-block/core/host/master"
	volume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func reopenWait(t *testing.T, what string, fn func() bool) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for !fn() {
		if time.Now().After(end) {
			t.Fatal(what)
		}
		time.Sleep(time.Millisecond)
	}
}

// Real master lifecycle controller -> durable Publisher -> gRPC subscription
// -> real volume Host/adapter projection -> production memback provider ->
// the same providerBackend used by the RDMA/NBD target. No fake Backend or
// hand-set projection/epoch. This CPU control does not require RDMA hardware
// and does not claim memback durability or data replication across failover.
func TestProviderBackendReopensAfterRealMasterFailover(t *testing.T) {
	m, err := master.New(master.Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0", ControllerConfig: authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 2}})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	t.Cleanup(func() {
		ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		if e := m.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	if _, err = m.Lifecycle().Volumes.CreateVolume(lifecycle.VolumeSpec{VolumeID: "v1", SizeBytes: 64 * 4096, ReplicationFactor: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err = m.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "v1", DesiredRF: 2, Candidates: []lifecycle.PlacementCandidate{{VolumeID: "v1", ServerID: "s1", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}, {VolumeID: "v1", ServerID: "s2", ReplicaID: "r2", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
		t.Fatal(err)
	}
	h, err := volume.New(volume.Config{MasterAddr: m.Addr(), ServerID: "s1", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19101", CtrlAddr: "127.0.0.1:19102", HeartbeatInterval: time.Hour, EnableT1Readiness: true})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() {
		if e := h.Close(); e != nil {
			t.Error(e)
		}
	})
	transition := func(r1, r2 bool, want string, minEpoch uint64) {
		t.Helper()
		for i, ready := range []bool{r1, r2} {
			id := i + 1
			obs := authority.Observation{ServerID: fmt.Sprintf("s%d", id), ObservedAt: time.Now().UTC(), Server: authority.ServerFact{Reachable: ready, Eligible: ready}, Slots: []authority.SlotFact{{VolumeID: "v1", ReplicaID: fmt.Sprintf("r%d", id), DataAddr: fmt.Sprintf("127.0.0.1:19%d01", id), CtrlAddr: fmt.Sprintf("127.0.0.1:19%d02", id), Reachable: ready, Eligible: ready, ReadyForPrimary: ready, EvidenceScore: uint64(20 - i*10)}}}
			if err := m.ObservationHost().Ingest(obs); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := m.RunLifecycleProductTick(); err != nil {
			t.Fatal(err)
		}
		reopenWait(t, "authority did not fail over to "+want, func() bool {
			line, ok := m.Publisher().VolumeAuthorityLine("v1")
			return ok && line.ReplicaID == want && line.Epoch >= minEpoch
		})
		reopenWait(t, "host projection did not follow "+want, func() bool {
			p := h.ProjectionView().Projection()
			if want == "r1" {
				return p.Healthy && p.Epoch >= minEpoch
			}
			return !p.Healthy
		})
		line, _ := m.Publisher().VolumeAuthorityLine("v1")
		t.Logf("real publisher authority: %s epoch%d", line.ReplicaID, line.Epoch)
	}
	transition(true, true, "r1", 1)
	provider := memback.NewProvider(h.ProjectionView())
	adapters := map[string]*providerBackend{}
	for _, op := range []string{"read", "write", "sync", "demoted"} {
		b := &providerBackend{provider: provider, volumeID: "v1"}
		ctx, c := context.WithTimeout(context.Background(), time.Second)
		n, e := b.Write(ctx, 0, []byte("epoch-one"))
		c()
		if e != nil || n != 9 {
			t.Fatal("initial write", n, e)
		}
		adapters[op] = b
	}
	transition(false, true, "r2", 2)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	n, err := adapters["demoted"].Write(ctx, 0, []byte("forbidden"))
	cancel()
	if err == nil || n != 0 {
		t.Fatalf("demoted backend wrote bytes: %d %v", n, err)
	}
	if !errors.Is(err, frontend.ErrStalePrimary) && !errors.Is(err, frontend.ErrNotReady) {
		t.Fatal("unexpected refusal", err)
	}
	transition(true, false, "r1", 3)
	// Seed the NEW lineage using a fresh, real provider handle. Memback does not
	// retain old-lineage bytes; this is an availability/rebinding assertion.
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fresh, err := provider.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("new-primary")
	if n, err = fresh.Write(ctx, 0, want); err != nil || n != len(want) {
		t.Fatal(n, err)
	}
	for _, op := range []string{"read", "write", "sync"} {
		t.Run(op, func(t *testing.T) {
			b := adapters[op]
			switch op {
			case "read":
				got := make([]byte, len(want))
				n, e := b.Read(ctx, 0, got)
				if e != nil || n != len(got) || !bytes.Equal(got, want) {
					t.Fatalf("cached RDMA read failed after r1->r2->r1: n%d err%v bytes%q", n, e, got)
				}
			case "write":
				data := []byte("rebound-io!")
				n, e := b.Write(ctx, 0, data)
				if e != nil || n != len(data) {
					t.Fatalf("cached RDMA write failed after failback: n%d err%v", n, e)
				}
				got := make([]byte, len(data))
				n, e = fresh.Read(ctx, 0, got)
				if e != nil || n != len(data) || !bytes.Equal(got, data) {
					t.Fatal("new-lineage exact write bytes differ", n, e)
				}
			case "sync":
				if e := b.Sync(ctx); e != nil {
					t.Fatalf("cached RDMA sync failed after failback: %v", e)
				}
			}
		})
	}
}
