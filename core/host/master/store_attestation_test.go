package master

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func attestation(vol, primary string, epoch uint64, stores map[string]storage.StoreID) *control.StoreAttestation {
	att := &control.StoreAttestation{VolumeId: vol, ReplicaId: primary, Epoch: epoch}
	for id, sid := range stores {
		att.Stores = append(att.Stores, &control.AttestedStore{ReplicaId: id, StoreId: append([]byte(nil), sid[:]...)})
	}
	return att
}

func storeID(b byte) storage.StoreID {
	var id storage.StoreID
	for i := range id {
		id[i] = b
	}
	return id
}

// rf3Primary publishes vol-rf3's first line (r1 on node-a) and returns it.
func rf3Primary(t *testing.T, h *Host) authority.AuthorityBasis {
	t.Helper()
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if line.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v", line)
	}
	return line
}

// The reference store of a replica is set only by the volume's primary
// line holder at that line's epoch; a replica's own claim, a stale epoch or
// another server is dropped; a replica the primary stops attesting keeps
// its last reference (it may hold the only copy of an acknowledged write),
// a new attestation after its rebuild replaces it; the record survives a
// master restart.
func TestStoreAttestationOnlyFromThePrimaryLine(t *testing.T) {
	dir := t.TempDir()
	h, err := New(Config{AuthorityStoreDir: dir, LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0",
		ControllerConfig: authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3}})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() { closeTestMaster(t, h) })
	line := rf3Primary(t, h)
	s1, s2, t1 := storeID(0x11), storeID(0x22), storeID(0x33)

	for _, c := range []struct {
		name, server, primary string
		epoch                 uint64
	}{
		{"a replica's own claim", "node-b", "r2", line.Epoch},
		{"the primary's replica id from another server", "node-b", "r1", line.Epoch},
		{"another epoch", "node-a", "r1", line.Epoch + 1},
	} {
		if err := h.ingestStoreAttestation(c.server, attestation("vol-rf3", c.primary, c.epoch, map[string]storage.StoreID{"r2": s2})); err != nil {
			t.Fatal(err)
		}
		if _, ok := h.AttestedStore("vol-rf3", "r2"); ok {
			t.Fatalf("%s set the reference", c.name)
		}
	}
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": storeID(1), "r2": s1, "r3": t1})); err != nil {
		t.Fatal(err)
	}
	if got, ok := h.AttestedStore("vol-rf3", "r2"); !ok || got != s1 {
		t.Fatalf("primary's attestation: r2 -> %x %t, want %x", got, ok, s1)
	}
	// r2 left the write-ACK set: its reference stays S1.
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": storeID(1), "r3": t1})); err != nil {
		t.Fatal(err)
	}
	if got, ok := h.AttestedStore("vol-rf3", "r2"); !ok || got != s1 {
		t.Fatalf("r2 dropped from the ack set: reference %x %t; want its last attested store %x kept", got, ok, s1)
	}
	// r2 rebuilt onto a new store S2 and rejoined: S2 replaces S1.
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": storeID(1), "r2": s2, "r3": t1})); err != nil {
		t.Fatal(err)
	}
	if got, ok := h.AttestedStore("vol-rf3", "r2"); !ok || got != s2 {
		t.Fatalf("r2 rebuilt and attested again: reference %x %t; want %x", got, ok, s2)
	}
	reloaded, err := loadStoreAttestations(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := reloaded.lookup("vol-rf3", "r3"); !ok || got != t1 {
		t.Fatalf("after restart: r3 -> %x %t, want %x", got, ok, t1)
	}
	if got, ok := reloaded.lookup("vol-rf3", "r2"); !ok || got != s2 {
		t.Fatalf("after restart: r2 -> %x %t, want %x", got, ok, s2)
	}
}

// The loss schedule: r2 counted toward write ACKs with store S1; its disk
// is replaced (S2, empty) and its own reports say S2; the primary dies. The
// probe of r2 shows S2 with a durable LSN as good as r3's, so only the
// store check tells them apart: r2 is not a survivor, r3 (attested T1,
// probed T1) is promoted. With the required frontier unknown, the RF3
// quorum rule needs two survivors, so nothing is promoted.
func TestPromotionRefusesAReplacedStoreThePrimaryNeverAttested(t *testing.T) {
	s1, s2, t1 := storeID(0x11), storeID(0x22), storeID(0x33)
	for _, c := range []struct {
		name          string
		requiredKnown bool
		r2Probed      storage.StoreID
		want          string // "" = no promotion
		wantNot       string
	}{
		{"replaced store, frontier known", true, s2, "r3", "r2"},
		{"replaced store, frontier unknown", false, s2, "", "r2"},
		{"control: the attested store", true, s1, "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
			t.Cleanup(func() { closeTestMaster(t, h) })
			line := rf3Primary(t, h)
			if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": storeID(1), "r2": s1, "r3": t1})); err != nil {
				t.Fatal(err)
			}
			// r2's node reports its new store: never the reference.
			if err := h.ingestStoreAttestation("node-b", attestation("vol-rf3", "r2", line.Epoch, map[string]storage.StoreID{"r2": s2})); err != nil {
				t.Fatal(err)
			}
			probed := map[string]storage.StoreID{"r2": c.r2Probed, "r3": t1}
			plans := []lifecycle.BlockVolumeWorkloadPlan{{VolumeID: "vol-rf3", Protocol: "iscsi"}}
			routes := map[string]string{}
			for i, id := range []string{"r2", "r3"} {
				id, sid := id, probed[id]
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/status":
						_ = json.NewEncoder(w).Encode(volume.StatusProjection{Projection: frontend.Projection{VolumeID: "vol-rf3", ReplicaID: id}, ReplicationRole: "replica_ready"})
					case "/status/durable":
						_ = json.NewEncoder(w).Encode(struct{ Volumes []durable.VolumeStatus }{[]durable.VolumeStatus{{VolumeID: "vol-rf3", ReplicaID: id, FrontierKnown: true, DurableLSN: 100, StoreID: hex.EncodeToString(sid[:])}}})
					default:
						http.NotFound(w, r)
					}
				}))
				t.Cleanup(srv.Close)
				plans[0].Replicas = append(plans[0].Replicas, lifecycle.BlockVolumeReplicaWorkload{ReplicaID: id, ISCSIListenPort: 3260 + i})
				routes[[]string{"127.0.0.1:23260", "127.0.0.1:23261"}[i]] = srv.URL
			}
			cfg := WorkloadPlanPromotionProbeConfig{AckProfile: "sync-quorum", Attested: h.AttestedStore,
				HTTPClient: &http.Client{Transport: promotionTraceRoutes(routes), Timeout: time.Second}}
			if c.requiredKnown {
				cfg.RequiredLSNByVolume = map[string]uint64{"vol-rf3": 50}
			}
			h.SetPromotionEvidenceProvider(NewWorkloadPlanPromotionEvidenceProvider(plans, cfg))
			ingestRF3ObservationForServersWithReady(t, h, "vol-rf3", "node-a", "node-b", "node-c", false, false, true, true, true, true)
			if _, err := h.RunLifecycleProductTick(); err != nil {
				t.Fatal(err)
			}
			issued := line
			deadline := time.Now().Add(2 * time.Second)
			for issued.Epoch <= line.Epoch && time.Now().Before(deadline) {
				issued, _ = h.Publisher().VolumeAuthorityLine("vol-rf3")
				time.Sleep(10 * time.Millisecond)
			}
			promoted := ""
			if issued.Epoch > line.Epoch {
				promoted = issued.ReplicaID
			}
			if c.wantNot != "" && promoted == c.wantNot {
				t.Fatalf("promoted %s, whose probed store %x is not the attested %x", promoted, c.r2Probed, s1)
			}
			if c.want != "" && promoted != c.want {
				t.Fatalf("promoted %q, want %s", promoted, c.want)
			}
			if c.name == "control: the attested store" && promoted != "r2" && promoted != "r3" {
				t.Fatalf("control: promoted %q, want a survivor", promoted)
			}
			if c.want == "" && c.wantNot != "" && promoted != "" {
				t.Fatalf("promoted %s with one attested survivor and an unknown frontier", promoted)
			}
		})
	}
}

// A provider without the attestation lookup refuses every candidate.
func TestPromotionProbeWithoutAttestationIsNeverReady(t *testing.T) {
	p := &workloadPlanPromotionEvidenceProvider{}
	if p.storeAttested("v", "r2", storeID(1)) {
		t.Fatal("no attestation lookup: a candidate was taken as attested")
	}
	p.attested = func(string, string) (storage.StoreID, bool) { return storageZero(), true }
	if p.storeAttested("v", "r2", storage.StoreID{}) {
		t.Fatal("an unknown probed store was taken as attested")
	}
}

func storageZero() storage.StoreID { return storage.StoreID{} }

// testStoreID is the store every probe fixture reports, and attestEvery
// attests it for every replica: fixtures about other rules hold the store
// check true.
var testStoreID = storeID(0x5a)

var testStoreHex = hex.EncodeToString(testStoreID[:])

func attestEvery(id storage.StoreID) func(string, string) (storage.StoreID, bool) {
	return func(string, string) (storage.StoreID, bool) { return id, true }
}

// The record merges: a replica an attestation does not name keeps its last
// reference (through a reload); one it names gets the new store; a lower
// epoch changes nothing.
func TestStoreAttestationsKeepADroppedReplicasReference(t *testing.T) {
	dir := t.TempDir()
	s, err := loadStoreAttestations(dir)
	if err != nil {
		t.Fatal(err)
	}
	s1, s2, t1 := storeID(0x11), storeID(0x22), storeID(0x33)
	for _, att := range []*control.StoreAttestation{
		attestation("v", "r1", 5, map[string]storage.StoreID{"r2": s1, "r3": t1}),
		attestation("v", "r1", 5, map[string]storage.StoreID{"r3": t1}),            // r2 dropped
		attestation("v", "r1", 4, map[string]storage.StoreID{"r3": storeID(0x44)}), // deposed primary
	} {
		if err := s.record(att); err != nil {
			t.Fatal(err)
		}
	}
	if got, ok := s.lookup("v", "r2"); !ok || got != s1 {
		t.Fatalf("dropped r2: %x %t; want %x kept", got, ok, s1)
	}
	if got, _ := s.lookup("v", "r3"); got != t1 {
		t.Fatalf("r3 after a lower-epoch report: %x; want %x", got, t1)
	}
	if err := s.record(attestation("v", "r1", 6, map[string]storage.StoreID{"r2": s2})); err != nil {
		t.Fatal(err)
	}
	reloaded, err := loadStoreAttestations(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := reloaded.lookup("v", "r2"); got != s2 {
		t.Fatalf("r2 after its rebuild, reloaded: %x; want %x", got, s2)
	}
	if got, _ := reloaded.lookup("v", "r3"); got != t1 {
		t.Fatalf("r3 not named at epoch 6, reloaded: %x; want %x kept", got, t1)
	}
}
