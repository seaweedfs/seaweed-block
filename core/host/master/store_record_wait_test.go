package master

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func factFor(h *Host, line authority.AuthorityBasis) (awaited, initial uint32, stores map[string][]byte) {
	fact := (&services{host: h}).assignmentFact(adapter.AssignmentInfo{VolumeID: "vol-rf3", ReplicaID: line.ReplicaID,
		Epoch: line.Epoch, EndpointVersion: line.EndpointVersion, DataAddr: line.DataAddr, CtrlAddr: line.CtrlAddr})
	stores = map[string][]byte{}
	for _, p := range fact.GetPeers() {
		stores[p.GetReplicaId()] = p.GetStoreId()
	}
	return fact.GetReplicaStoresAwaited(), fact.GetReplicaStoresInitial(), stores
}

// A new volume's first assignment waits for its replicas' attested stores:
// the fact counts the initial replicas with no recorded store, and its
// descriptors carry only stores the primary attested (never a replica's own
// word). Each recorded attestation wakes the assignment stream, so the
// primary receives the stores at the same generation; once every initial
// replica has one, nothing is awaited, for good (a later rebuild does not
// close the volume again; its replica just does not count).
func TestNewVolumeAssignmentWaitsForReplicaAttestations(t *testing.T) {
	dir := t.TempDir()
	h, err := New(Config{AuthorityStoreDir: dir, LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0",
		ControllerConfig: authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3}})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() { closeTestMaster(t, h) })
	line := rf3Primary(t, h)
	if awaited, initial, stores := factFor(h, line); awaited != 3 || initial != 3 || len(stores["r2"]) != 0 || len(stores["r3"]) != 0 {
		t.Fatalf("new volume: awaited %d of %d, stores %x; want 3 of 3 and no store in any descriptor", awaited, initial, stores)
	}
	s1, s2, t1 := storeID(0x11), storeID(0x22), storeID(0x33)
	wake := h.attest.watch("vol-rf3")
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": storeID(1), "r2": s1})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
	default:
		t.Fatal("a recorded attestation did not wake the assignment stream")
	}
	if awaited, _, stores := factFor(h, line); awaited != 1 || !bytes.Equal(stores["r2"], s1[:]) || len(stores["r3"]) != 0 {
		t.Fatalf("after r1+r2 attested: awaited %d, stores %x; want 1 (r3) and r2=%x", awaited, stores, s1)
	}
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": storeID(1), "r2": s1, "r3": t1})); err != nil {
		t.Fatal(err)
	}
	if awaited, _, stores := factFor(h, line); awaited != 0 || !bytes.Equal(stores["r3"], t1[:]) {
		t.Fatalf("all attested: awaited %d, stores %x; want 0 and r3=%x", awaited, stores, t1)
	}
	// r2 rebuilt (S2): it reconnects with a new store but the record (and the
	// descriptor) keeps S1 until the primary attests S2; the volume stays open.
	if _, _, stores := factFor(h, line); !bytes.Equal(stores["r2"], s1[:]) {
		t.Fatalf("r2 descriptor %x; want the attested %x", stores["r2"], s1)
	}
	if err := h.ingestStoreAttestation("node-b", attestation("vol-rf3", "r2", line.Epoch, map[string]storage.StoreID{"r2": s2})); err != nil {
		t.Fatal(err)
	}
	if awaited, _, stores := factFor(h, line); awaited != 0 || !bytes.Equal(stores["r2"], s1[:]) {
		t.Fatalf("after r2's own claim: awaited %d, r2 %x; want 0 and %x", awaited, stores["r2"], s1)
	}
	reloaded, err := loadStoreAttestations(dir)
	if err != nil {
		t.Fatal(err)
	}
	if waiting, initial := reloaded.awaited("vol-rf3", []string{"r1", "r2", "r3"}); waiting != 0 || initial != 0 {
		t.Fatalf("reloaded: %d of %d awaited; want the wait cleared", waiting, initial)
	}
}

// A volume bound before the record existed (an upgrade) waits for nothing:
// its frontend stays served; it is only not promoted until attested.
func TestUpgradedVolumeAwaitsNoAttestation(t *testing.T) {
	s, err := loadStoreAttestations(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if waiting, initial := s.awaited("v-old", []string{"r1", "r2"}); waiting != 0 || initial != 0 {
		t.Fatalf("no record: %d of %d", waiting, initial)
	}
	if err := s.open("v-new", []string{"r1", "r2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.open("v-new", []string{"r1", "r2", "r3"}); err != nil { // a second open changes nothing
		t.Fatal(err)
	}
	if waiting, initial := s.awaited("v-new", []string{"r1", "r2"}); waiting != 2 || initial != 2 {
		t.Fatalf("new: %d of %d awaited; want 2 of 2", waiting, initial)
	}
	if err := s.record(attestation("v-old", "r1", 3, map[string]storage.StoreID{"r2": storeID(2)})); err != nil {
		t.Fatal(err)
	}
	if err := s.open("v-old", []string{"r1", "r2"}); err != nil {
		t.Fatal(err)
	}
	if waiting, _ := s.awaited("v-old", []string{"r1", "r2"}); waiting != 0 {
		t.Fatalf("an attested volume opened again waits for %d", waiting)
	}
}

// An initial replica replaced during the creation wait (a new replica id in
// the placement) must not keep the volume waiting for the old id, which
// never attests: the wait follows the volume's current replicas.
func TestCreationWaitFollowsAReplacedInitialReplica(t *testing.T) {
	s, err := loadStoreAttestations(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.open("v", []string{"r1", "r2", "r3"}); err != nil {
		t.Fatal(err)
	}
	// r3 is replaced by r4 before it attests; the primary attests r1, r2, r4.
	if err := s.record(attestation("v", "r1", 1, map[string]storage.StoreID{"r1": storeID(1), "r2": storeID(2), "r4": storeID(4)})); err != nil {
		t.Fatal(err)
	}
	if waiting, initial := s.awaited("v", []string{"r1", "r2", "r4"}); waiting != 0 {
		t.Fatalf("after r3 was replaced by r4 and r1, r2, r4 attested: %d of %d awaited; want 0", waiting, initial)
	}
	// The wait is over for good: a later replacement (r5, no store yet) does
	// not close the volume again; its replica just does not count.
	if waiting, _ := s.awaited("v", []string{"r1", "r2", "r5"}); waiting != 0 {
		t.Fatalf("a replacement after the wait ended re-opened it: %d awaited", waiting)
	}
	// Before r4 attests, the wait names r4, not r3.
	s2, _ := loadStoreAttestations(t.TempDir())
	_ = s2.open("v", []string{"r1", "r2", "r3"})
	_ = s2.record(attestation("v", "r1", 1, map[string]storage.StoreID{"r1": storeID(1), "r2": storeID(2)}))
	if waiting, initial := s2.awaited("v", []string{"r1", "r2", "r4"}); waiting != 1 || initial != 3 {
		t.Fatalf("r4 not yet attested: %d of %d; want 1 of 3", waiting, initial)
	}
}
