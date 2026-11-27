package main

import (
	"bytes"
	"context"
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

// A delete of an established volume (its primary attested its write-ACK
// stores, so creation is over) whose line holds a live lease is refused by
// the real master's reservation before anything is persisted, and the
// command's JSON answer says so: code not_admitted and the "refused before
// persistence" sentence. HEAD stays live and no reservation is recorded.
func TestOpsChunkVolumeDeleteRefusalSaysNothingWasPersisted(t *testing.T) {
	cfg := master.Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }}
	h, stop := startDeleteMaster(t, cfg)
	defer stop()
	seedForcedVolumeLease(t, h, true)
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The primary's attestation of its stores ends creation, as its heartbeat
	// carries it in production.
	if _, err := control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots:            []*control.HeartbeatSlot{{VolumeId: "vol-f", ReplicaId: "r1", DataAddr: "127.0.0.1:19200", CtrlAddr: "127.0.0.1:19100"}},
		StoreAttestation: &control.StoreAttestation{VolumeId: "vol-f", ReplicaId: "r1", Epoch: 1, Stores: []*control.AttestedStore{{ReplicaId: "r1", StoreId: bytes.Repeat([]byte{7}, 16)}}}}); err != nil {
		t.Fatal(err)
	}
	store := newMemChunkStore()
	useMemChunkStore(t, store)
	v := cliVolume(t, store, "vol-f", "daily")
	code, out, stderr := runChunk(cliArgs("volume-delete", volArgs("volume", v), []string{"--master", h.Addr(), "--request-id", "live-lease"})...)
	body := chunkErrorOut(t, out)
	t.Logf("exit %d answer %+v", code, body)
	if code != chunkExitFailed || body.Code != "not_admitted" ||
		!strings.Contains(body.Message, "deletion refused before persistence") {
		t.Fatalf("volume-delete with a live lease: exit %d answer %+v stderr %s; want not_admitted saying it was refused before persistence", code, body, stderr)
	}
	if h.Lifecycle().Deletions.Reserved("vol-f") {
		t.Fatal("the refused delete recorded a reservation")
	}
	cat, _ := chunkstore.NewBCatalog(store, store, defaultChunkPathCLI)
	if hd, found, err := cat.ReadHead(context.Background(), v); err != nil || !found || hd.State != layout.StateLive {
		t.Fatalf("HEAD after the refused delete: %+v %v %v; want live", hd, found, err)
	}
}
