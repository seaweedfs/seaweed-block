package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	ErrInvalidNodeRegistration = errors.New("lifecycle: invalid node registration")
	ErrNodeNotFound            = errors.New("lifecycle: node not found")
)

// StoragePool reports allocatable local capacity. It is placement input,
// not a create decision.
type StoragePool struct {
	PoolID     string            `json:"pool_id"`
	TotalBytes uint64            `json:"total_bytes"`
	FreeBytes  uint64            `json:"free_bytes"`
	BlockSize  uint64            `json:"block_size"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// ReplicaInventory reports durable local replica identity discovered on a
// node. It is controller evidence only; it does not imply replica_ready.
type ReplicaInventory struct {
	VolumeID   string `json:"volume_id"`
	ReplicaID  string `json:"replica_id"`
	StoreUUID  string `json:"store_uuid"`
	SizeBytes  uint64 `json:"size_bytes"`
	DurableLSN uint64 `json:"durable_lsn"`
	State      string `json:"state"`
}

// NodeRegistration is the durable node/inventory fact produced by node
// registration or heartbeat ingestion. It deliberately has no authority
// fields.
type NodeRegistration struct {
	ServerID string `json:"server_id"`
	// Addr is legacy node-level address. DataAddr/CtrlAddr are the
	// explicit block data/control endpoints used by G9F verification.
	Addr     string             `json:"addr,omitempty"`
	DataAddr string             `json:"data_addr,omitempty"`
	CtrlAddr string             `json:"ctrl_addr,omitempty"`
	Labels   map[string]string  `json:"labels,omitempty"`
	Pools    []StoragePool      `json:"pools,omitempty"`
	Replicas []ReplicaInventory `json:"replicas,omitempty"`
	SeenAt   time.Time          `json:"seen_at"`
}

// NodeInventoryStore persists one JSON registration record per server.
type NodeInventoryStore struct {
	mu      sync.Mutex
	dir     string
	records map[string]NodeRegistration
}

// OpenNodeInventoryStore opens or creates a node inventory store.
func OpenNodeInventoryStore(dir string) (*NodeInventoryStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty dir", ErrInvalidNodeRegistration)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lifecycle: mkdir node inventory %q: %w", dir, err)
	}
	s := &NodeInventoryStore{
		dir:     dir,
		records: make(map[string]NodeRegistration),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// RegisterNode records the latest observed inventory for a server.
// Repeating registration replaces observation fields, but still does not
// publish authority or readiness.
func (s *NodeInventoryStore) RegisterNode(reg NodeRegistration) (NodeRegistration, error) {
	if err := validateNodeRegistration(reg); err != nil {
		return NodeRegistration{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if reg.SeenAt.IsZero() {
		reg.SeenAt = time.Now().UTC()
	}
	reg = normalizeNodeRegistration(reg)
	if err := s.putLocked(reg); err != nil {
		return NodeRegistration{}, err
	}
	s.records[reg.ServerID] = reg
	return reg, nil
}

// RemoveNode deletes one registration record. Missing nodes are a
// successful no-op so cleanup is retry-safe.
func (s *NodeInventoryStore) RemoveNode(serverID string) error {
	if err := validateServerID(serverID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, serverID)
	if err := os.Remove(s.path(serverID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("lifecycle: remove node %s: %w", serverID, err)
	}
	return nil
}

// GetNode returns a copy of one registration record.
func (s *NodeInventoryStore) GetNode(serverID string) (NodeRegistration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[serverID]
	return copyNodeRegistration(rec), ok
}

// ListNodes returns registration records ordered by ServerID.
func (s *NodeInventoryStore) ListNodes() []NodeRegistration {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.records))
	for k := range s.records {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]NodeRegistration, 0, len(keys))
	for _, k := range keys {
		out = append(out, copyNodeRegistration(s.records[k]))
	}
	return out
}

func (s *NodeInventoryStore) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("lifecycle: readdir node inventory %q: %w", s.dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(s.dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("lifecycle: read node %q: %w", path, err)
		}
		var rec NodeRegistration
		if err := json.Unmarshal(raw, &rec); err != nil {
			return fmt.Errorf("lifecycle: parse node %q: %w", path, err)
		}
		if err := validateNodeRegistration(rec); err != nil {
			return fmt.Errorf("lifecycle: invalid node record %q: %w", path, err)
		}
		if filepath.Base(s.path(rec.ServerID)) != entry.Name() {
			return fmt.Errorf("lifecycle: node filename %q does not match server_id %q", entry.Name(), rec.ServerID)
		}
		s.records[rec.ServerID] = normalizeNodeRegistration(rec)
	}
	return nil
}

func (s *NodeInventoryStore) putLocked(rec NodeRegistration) error {
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("lifecycle: marshal node %s: %w", rec.ServerID, err)
	}
	path := s.path(rec.ServerID)
	tmp, err := os.CreateTemp(s.dir, rec.ServerID+".*.tmp")
	if err != nil {
		return fmt.Errorf("lifecycle: temp node %s: %w", rec.ServerID, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("lifecycle: write node temp %s: %w", rec.ServerID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("lifecycle: close node temp %s: %w", rec.ServerID, err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("lifecycle: replace node %s: %w", rec.ServerID, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("lifecycle: rename node %s: %w", rec.ServerID, err)
	}
	return nil
}

func (s *NodeInventoryStore) path(serverID string) string {
	return filepath.Join(s.dir, serverID+".json")
}

func validateNodeRegistration(reg NodeRegistration) error {
	if err := validateServerID(reg.ServerID); err != nil {
		return err
	}
	if reg.Addr == "" && (reg.DataAddr == "" || reg.CtrlAddr == "") {
		return fmt.Errorf("%w: empty address; provide addr or data_addr+ctrl_addr", ErrInvalidNodeRegistration)
	}
	for _, pool := range reg.Pools {
		if pool.PoolID == "" {
			return fmt.Errorf("%w: empty pool id", ErrInvalidNodeRegistration)
		}
		if pool.BlockSize == 0 {
			return fmt.Errorf("%w: pool %s block_size must be > 0", ErrInvalidNodeRegistration, pool.PoolID)
		}
		if pool.FreeBytes > pool.TotalBytes {
			return fmt.Errorf("%w: pool %s free_bytes exceeds total_bytes", ErrInvalidNodeRegistration, pool.PoolID)
		}
	}
	for _, replica := range reg.Replicas {
		if err := validateReplicaInventory(replica); err != nil {
			return err
		}
	}
	return nil
}

func validateReplicaInventory(replica ReplicaInventory) error {
	if err := validateVolumeID(replica.VolumeID); err != nil {
		return err
	}
	if replica.ReplicaID == "" {
		return fmt.Errorf("%w: empty replica id", ErrInvalidNodeRegistration)
	}
	if replica.StoreUUID == "" {
		return fmt.Errorf("%w: empty store uuid", ErrInvalidNodeRegistration)
	}
	if replica.SizeBytes == 0 {
		return fmt.Errorf("%w: replica %s/%s size_bytes must be > 0", ErrInvalidNodeRegistration, replica.VolumeID, replica.ReplicaID)
	}
	if replica.State == "" {
		return fmt.Errorf("%w: replica %s/%s empty state", ErrInvalidNodeRegistration, replica.VolumeID, replica.ReplicaID)
	}
	return nil
}

func validateServerID(serverID string) error {
	if serverID == "" {
		return fmt.Errorf("%w: empty server id", ErrInvalidNodeRegistration)
	}
	if !volumeIDPattern.MatchString(serverID) {
		return fmt.Errorf("%w: invalid server id %q", ErrInvalidNodeRegistration, serverID)
	}
	return nil
}

func normalizeNodeRegistration(reg NodeRegistration) NodeRegistration {
	reg.Labels = copyStringMap(reg.Labels)
	for i := range reg.Pools {
		reg.Pools[i].Labels = copyStringMap(reg.Pools[i].Labels)
	}
	return reg
}

func copyNodeRegistration(reg NodeRegistration) NodeRegistration {
	out := reg
	out.Labels = copyStringMap(reg.Labels)
	out.Pools = append([]StoragePool(nil), reg.Pools...)
	for i := range out.Pools {
		out.Pools[i].Labels = copyStringMap(out.Pools[i].Labels)
	}
	out.Replicas = append([]ReplicaInventory(nil), reg.Replicas...)
	return out
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
