package lifecycle

import (
	"reflect"
	"testing"
	"time"
)

func TestNodeInventoryStore_RegisterNodePersistsCapacityAndReplicas(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenNodeInventoryStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	seen := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	reg := NodeRegistration{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9100",
		Labels:   map[string]string{"rack": "r1"},
		Pools: []StoragePool{{
			PoolID:     "pool-a",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 29,
			BlockSize:  4096,
			Labels:     map[string]string{"media": "ssd"},
		}},
		Replicas: []ReplicaInventory{{
			VolumeID:   "vol-a",
			ReplicaID:  "r2",
			StoreUUID:  "store-1",
			SizeBytes:  1 << 20,
			DurableLSN: 42,
			State:      "existing",
		}},
		SeenAt: seen,
	}
	if _, err := s.RegisterNode(reg); err != nil {
		t.Fatalf("register: %v", err)
	}

	reopened, err := OpenNodeInventoryStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, ok := reopened.GetNode("node-a")
	if !ok {
		t.Fatal("reopened store missing node")
	}
	if !reflect.DeepEqual(got, reg) {
		t.Fatalf("node record mismatch:\ngot  %+v\nwant %+v", got, reg)
	}
}

func TestNodeInventoryStore_RegisterNodeReplacesObservation(t *testing.T) {
	s, err := OpenNodeInventoryStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first := NodeRegistration{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9100",
		Pools: []StoragePool{{
			PoolID:     "pool-a",
			TotalBytes: 100,
			FreeBytes:  50,
			BlockSize:  4096,
		}},
		SeenAt: time.Unix(1, 0).UTC(),
	}
	if _, err := s.RegisterNode(first); err != nil {
		t.Fatalf("first register: %v", err)
	}
	second := first
	second.Pools[0].FreeBytes = 25
	second.SeenAt = time.Unix(2, 0).UTC()
	if _, err := s.RegisterNode(second); err != nil {
		t.Fatalf("second register: %v", err)
	}
	got, ok := s.GetNode("node-a")
	if !ok {
		t.Fatal("missing node")
	}
	if got.Pools[0].FreeBytes != 25 {
		t.Fatalf("free_bytes=%d want 25", got.Pools[0].FreeBytes)
	}
	if !got.SeenAt.Equal(second.SeenAt) {
		t.Fatalf("seen_at=%s want %s", got.SeenAt, second.SeenAt)
	}
}

func TestNodeInventoryStore_ReplicaInventoryDoesNotImplyReady(t *testing.T) {
	s, err := OpenNodeInventoryStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	reg := NodeRegistration{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9100",
		Replicas: []ReplicaInventory{{
			VolumeID:   "vol-a",
			ReplicaID:  "r2",
			StoreUUID:  "store-1",
			SizeBytes:  1 << 20,
			DurableLSN: 100,
			State:      "existing",
		}},
	}
	if _, err := s.RegisterNode(reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	got, ok := s.GetNode("node-a")
	if !ok {
		t.Fatal("missing node")
	}
	if got.Replicas[0].State != "existing" {
		t.Fatalf("state=%q want existing", got.Replicas[0].State)
	}
	typ := reflect.TypeOf(got.Replicas[0])
	if _, ok := typ.FieldByName("Ready"); ok {
		t.Fatal("replica inventory must not carry Ready; controller/recovery owns readiness")
	}
	if _, ok := typ.FieldByName("Primary"); ok {
		t.Fatal("replica inventory must not carry Primary; publisher owns authority")
	}
}

func TestNodeInventoryStore_BlankCapacityIsInventoryOnly(t *testing.T) {
	s, err := OpenNodeInventoryStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.RegisterNode(NodeRegistration{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9100",
		Pools: []StoragePool{{
			PoolID:     "pool-a",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	got, ok := s.GetNode("node-a")
	if !ok {
		t.Fatal("missing node")
	}
	if len(got.Pools) != 1 || len(got.Replicas) != 0 {
		t.Fatalf("blank node inventory got pools=%d replicas=%d", len(got.Pools), len(got.Replicas))
	}
	if _, ok := reflect.TypeOf(got).FieldByName("Assignment"); ok {
		t.Fatal("node registration must not carry assignment")
	}
}

func TestNodeInventoryStore_RemoveNodeIsIdempotent(t *testing.T) {
	s, err := OpenNodeInventoryStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.RegisterNode(NodeRegistration{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9100",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := s.RemoveNode("node-a"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := s.RemoveNode("node-a"); err != nil {
		t.Fatalf("idempotent remove: %v", err)
	}
	if _, ok := s.GetNode("node-a"); ok {
		t.Fatal("removed node still present")
	}
}
