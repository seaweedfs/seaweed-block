package main

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A volume-delete refused with snapshot_has_dependents has already reserved
// the volume at the real master: workspace show then answers that the volume
// is being deleted (exit 1), and HEAD with its snapshots is unchanged. The
// control is a delete refused before any reservation (an established volume
// under a live lease): show still answers the volume normally.
func TestOpsChunkVolumeDeleteRefusedByDependentsLeavesTheVolumeReserved(t *testing.T) {
	for _, c := range []struct {
		name      string
		liveLease bool // an established volume whose line holds a live lease
		wantCode  string
		reserved  bool
	}{
		{"refused by dependents after the reservation", false, "snapshot_has_dependents", true},
		{"control: refused before the reservation", true, "not_admitted", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := master.Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
				WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }}
			h, stop := startDeleteMaster(t, cfg)
			defer stop()
			seedForcedVolumeLease(t, h, c.liveLease)
			if c.liveLease {
				attestStores(t, h)
			}
			store := newMemChunkStore()
			useMemChunkStore(t, store)
			v := cliVolume(t, store, "vol-f", "daily")
			tid, _ := layout.NewVolumeID()
			if code, out, errs := runChunk(cliArgs("restore", volArgs("source", v), volArgs("target", chunkstore.VolumeRef{Name: "t", ID: tid}), []string{"--snapshot", "daily"})...); code != 0 {
				t.Fatalf("restore: %d %q %q", code, out, errs)
			}
			cat, _ := chunkstore.NewBCatalog(store, store, defaultChunkPathCLI)
			head := func() layout.Head {
				hd, found, err := cat.ReadHead(context.Background(), v)
				if err != nil || !found {
					t.Fatalf("HEAD: %v %v", found, err)
				}
				return hd
			}
			before := head()
			code, out, stderr := runChunk(cliArgs("volume-delete", volArgs("volume", v), []string{"--master", h.Addr(), "--request-id", "del-vol-f"})...)
			if body := chunkErrorOut(t, out); code != chunkExitFailed || body.Code != c.wantCode {
				t.Fatalf("volume-delete: exit %d answer %+v stderr %s; want %s", code, body, stderr, c.wantCode)
			}
			if got := h.Lifecycle().Deletions.Reserved("vol-f"); got != c.reserved {
				t.Fatalf("reserved after the refused delete: %v; want %v", got, c.reserved)
			}
			if after := head(); !reflect.DeepEqual(after, before) || len(after.Snapshots["daily"].Dependents) != 1 {
				t.Fatalf("HEAD changed by the refused delete: before %+v, after %+v", before, after)
			}
			showCode, showOut, showErr := runCLI([]string{"ops", "workspace", "show", "--master", h.Addr(), "--volume", "vol-f"})
			t.Logf("show: exit %d\n%s%s", showCode, showOut, showErr)
			deleting := strings.Contains(showOut+showErr, "volume is being deleted")
			if c.reserved && (showCode != 1 || !deleting) {
				t.Fatalf("show after a delete refused by dependents: exit %d; want 1 with \"volume is being deleted\"", showCode)
			}
			if !c.reserved && (showCode != 0 || deleting || !strings.Contains(showOut, "volume: vol-f")) {
				t.Fatalf("show after a delete refused before the reservation: exit %d; want the volume shown normally", showCode)
			}
		})
	}
}

// attestStores sends vol-f's primary attestation of its write-ACK stores, as
// its heartbeat carries it in production: the volume's creation is over.
func attestStores(t *testing.T, h *master.Host) {
	t.Helper()
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots:            []*control.HeartbeatSlot{{VolumeId: "vol-f", ReplicaId: "r1", DataAddr: "127.0.0.1:19200", CtrlAddr: "127.0.0.1:19100"}},
		StoreAttestation: &control.StoreAttestation{VolumeId: "vol-f", ReplicaId: "r1", Epoch: 1, Stores: []*control.AttestedStore{{ReplicaId: "r1", StoreId: bytes.Repeat([]byte{7}, 16)}}}}); err != nil {
		t.Fatal(err)
	}
}
