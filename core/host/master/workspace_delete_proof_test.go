package master

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The state with no exit: every store attested (stage 2) but the end of
// creation never recorded (the master read neither a ready answer nor a
// survivor's write since), while the primary may be serving acknowledged
// writes under its live lease. The delete keeps the live-lease refusal: no
// reservation stops that primary's grants.
func TestAStage2VolumeKeepsTheLiveLeaseRefusal(t *testing.T) {
	h, lc, _, _, _ := neverCreatedVolume(t, false)
	line := waitAuthorityLine(t, h.Publisher(), "v")
	published, _ := h.Publisher().PublishedAuthorityLine("v")
	server := h.workspaceLineOwner(published, "v", line.ReplicaID, line.Epoch)
	if err := h.ingestStoreAttestation(server, attestation("v", line.ReplicaID, line.Epoch, map[string]storage.StoreID{line.ReplicaID: storeOf(line.ReplicaID)})); err != nil {
		t.Fatal(err)
	}
	if waiting, _ := h.attest.awaited("v", h.replicaSlotsFor("v")); waiting != 0 || !h.attest.awaitingFirstReady("v") {
		t.Fatal("fixture: the volume is not in stage 2")
	}
	if h.inCreationPhase("v", line, nil) == "" {
		t.Fatal("fixture: the end of creation was recorded")
	}
	_, err := lc.ReserveVolumeDeletion(t.Context(), &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "del-stage2"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("delete of a stage-2 volume under a live lease: %v; want FailedPrecondition", err)
	}
	if h.attest.deleteWithoutAck("v") {
		t.Fatal("a stage-2 volume was marked for a delete without acknowledgement")
	}
}

// A delete without acknowledgement admitted in stage 1 freezes the store
// wait: stores attested after it never end the wait (the primary, whose
// assignment keeps awaiting them, never serves), also after a restart, and
// the delete finishes at its HEAD update.
func TestTheDeleteMarkFreezesTheStoreWait(t *testing.T) {
	cfg := Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir()}
	h := newBootGraceMasterCreating(t, cfg)
	a := h.VolumeDeleteAuthority("vol-rf3", "del-frozen")
	if err := a.Reserve(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !h.attest.deleteWithoutAck("vol-rf3") {
		t.Fatal("a stage-1 volume's delete was not admitted without acknowledgement")
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": testStoreID, "r2": testStoreID, "r3": testStoreID})); err != nil {
		t.Fatal(err)
	}
	if !inCreationWait(h) || h.attest.awaitingFirstReady("vol-rf3") {
		t.Fatal("the store wait ended after the delete mark")
	}
	h = restartCreatingMaster(t, h, cfg)
	defer closeTestMaster(t, h)
	if !inCreationWait(h) || !h.attest.deleteWithoutAck("vol-rf3") {
		t.Fatal("after a restart the frozen wait or the mark is lost")
	}
	if err := h.VolumeDeleteAuthority("vol-rf3", "del-frozen").NoLiveLease(t.Context()); err != nil {
		t.Fatalf("the frozen stage-1 delete's HEAD update: %v", err)
	}
}

// Past stage 1 nothing is marked: the ordinary delete applies.
func TestAStage2VolumeIsNotMarkedForADeleteWithoutAck(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{})
	defer closeTestMaster(t, h)
	attestAllStores(t, h)
	if err := h.VolumeDeleteAuthority("vol-rf3", "del-ordinary").Reserve(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.attest.deleteWithoutAck("vol-rf3") {
		t.Fatal("a stage-2 volume was marked for a delete without acknowledgement")
	}
}
