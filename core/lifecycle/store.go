// Package lifecycle owns product-shaped volume lifecycle intent.
//
// It deliberately stops before authority publication:
// create/attach/delete records are desired product state, not
// AssignmentInfo and not epoch/endpoint-version truth. Later placement
// and publisher slices consume this state through a planner seam.
package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/geometry"
)

var (
	ErrInvalidVolumeSpec = errors.New("lifecycle: invalid volume spec")
	ErrVolumeConflict    = errors.New("lifecycle: volume already exists with different spec")
	ErrVolumeNotFound    = errors.New("lifecycle: volume not found")
	ErrAlreadyAttached   = errors.New("lifecycle: volume already attached to another node")
	ErrAttachedElsewhere = errors.New("lifecycle: volume attached to another node")
)

var volumeIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// VolumeSpec is the product-level desired volume shape. It carries no
// authority lineage and no placement decision.
type VolumeSpec struct {
	VolumeID   string `json:"volume_id"`
	LayoutID   string `json:"layout_id,omitempty"`
	SizeBytes  uint64 `json:"size_bytes"`
	BlockSize  uint32 `json:"block_size"`
	ChunkBytes uint64 `json:"chunk_bytes"`
	// A restore request is bound to one source snapshot before the target's
	// layout is created. These fields never change on a replay.
	SourceVolume      string `json:"source_volume,omitempty"`
	SourceLayoutID    string `json:"source_layout_id,omitempty"`
	SourceSnapshot    string `json:"source_snapshot,omitempty"`
	CreateRequestID   string `json:"create_request_id,omitempty"`
	ReplicationFactor int    `json:"replication_factor"`
	Protocol          string `json:"protocol,omitempty"`
	FrontendTransport string `json:"frontend_transport,omitempty"`
	PVCName           string `json:"pvc_name,omitempty"`
	PVCNamespace      string `json:"pvc_namespace,omitempty"`
	PVCUID            string `json:"pvc_uid,omitempty"`
	PVName            string `json:"pv_name,omitempty"`
}

// VolumeRecord is the durable lifecycle state for one volume.
type VolumeRecord struct {
	Spec       VolumeSpec `json:"spec"`
	AttachedTo string     `json:"attached_to,omitempty"`
	// StoreReady is a one-way placement hint, set only after the matching
	// VOLUME and unclaimed HEAD have been created in the filer. Start still
	// checks those records independently before serving.
	StoreReady bool `json:"store_ready,omitempty"`
}

// FileStore persists one JSON record per volume ID.
type FileStore struct {
	mu      sync.Mutex
	dir     string
	records map[string]VolumeRecord
	// snap mirrors records as an immutable copy, republished at every change
	// of records, for lock-free renewal-path reads.
	snap atomic.Pointer[map[string]VolumeRecord]
}

// publishLocked republishes the immutable snapshot of records. Caller holds
// mu or owns s.
func (s *FileStore) publishLocked() {
	m := make(map[string]VolumeRecord, len(s.records))
	for k, r := range s.records {
		m[k] = r
	}
	s.snap.Store(&m)
}

// GetVolumeSnapshot is GetVolume without the store mutex, for the lease
// renewal path: it reads the last published records, never waiting on a
// write in flight.
func (s *FileStore) GetVolumeSnapshot(volumeID string) (VolumeRecord, bool) {
	m := s.snap.Load()
	if m == nil {
		return VolumeRecord{}, false
	}
	rec, ok := (*m)[volumeID]
	return rec, ok
}

// OpenFileStore opens or creates a lifecycle intent store.
func OpenFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty dir", ErrInvalidVolumeSpec)
	}
	dir, err := filepath.Abs(filepath.Clean(dir))
	if err != nil {
		return nil, fmt.Errorf("lifecycle: resolve store path: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lifecycle: mkdir %q: %w", dir, err)
	}
	// INV-LIFECYCLE-STORE-DURABLE-001: existence does not prove durability.
	// Every open syncs the full ancestor chain, also after a failed first open.
	for parent := filepath.Dir(dir); ; parent = filepath.Dir(parent) {
		if err := syncBindingParent(parent); err != nil {
			log.Printf("lifecycle: could not sync %s (%v): the store directory entry may not survive a crash right after its creation", parent, err)
		}
		if parent == filepath.Dir(parent) {
			break
		}
	}

	s := &FileStore{
		dir:     dir,
		records: make(map[string]VolumeRecord),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	s.publishLocked()
	return s, nil
}

// CreateVolume records desired volume state. Repeating the same spec is
// idempotent; changing a previously-created spec is rejected.
func (s *FileStore) CreateVolume(spec VolumeSpec) (VolumeRecord, error) {
	spec = normalizeVolumeSpec(spec)
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[spec.VolumeID]; ok {
		if existing.Spec.SourceVolume != spec.SourceVolume || existing.Spec.SourceLayoutID != spec.SourceLayoutID || existing.Spec.SourceSnapshot != spec.SourceSnapshot || existing.Spec.CreateRequestID != spec.CreateRequestID {
			return VolumeRecord{}, ErrVolumeConflict
		}
		if err := validateSpec(spec); err != nil {
			return VolumeRecord{}, err
		}
		if !volumeSpecsCompatible(existing.Spec, spec) {
			return VolumeRecord{}, ErrVolumeConflict
		}
		merged := existing
		merged.Spec = mergeVolumeSpecMetadata(existing.Spec, spec)
		if merged.Spec != existing.Spec {
			if err := s.putLocked(merged); err != nil {
				return VolumeRecord{}, err
			}
			s.records[spec.VolumeID] = merged
			s.publishLocked()
			return merged, nil
		}
		return existing, nil
	}
	if err := geometry.ValidateNewVolumeSize(spec.SizeBytes); err != nil {
		return VolumeRecord{}, fmt.Errorf("%w: %w", ErrInvalidVolumeSpec, err)
	}
	if err := validateSpec(spec); err != nil {
		return VolumeRecord{}, err
	}
	// A previous create may have renamed its complete record but lost the
	// directory-sync result. Settle that identity instead of minting another.
	if raw, err := os.ReadFile(s.path(spec.VolumeID)); err == nil {
		var rec VolumeRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return VolumeRecord{}, err
		}
		if !volumeSpecsCompatible(rec.Spec, spec) {
			return VolumeRecord{}, ErrVolumeConflict
		}
		if _, err := layout.ParseVolumeID(rec.Spec.LayoutID); err != nil {
			return VolumeRecord{}, fmt.Errorf("%w: existing layout identity: %v; recreate", ErrInvalidVolumeSpec, err)
		}
		rec.Spec = mergeVolumeSpecMetadata(rec.Spec, spec)
		if err := s.putLocked(rec); err != nil {
			return VolumeRecord{}, err
		}
		s.records[spec.VolumeID] = rec
		s.publishLocked()
		return rec, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return VolumeRecord{}, err
	}
	if spec.LayoutID != "" {
		return VolumeRecord{}, fmt.Errorf("%w: layout_id is minted by the master", ErrInvalidVolumeSpec)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		return VolumeRecord{}, err
	}
	spec.LayoutID = id.String()
	rec := VolumeRecord{Spec: spec}
	if err := s.putLocked(rec); err != nil {
		return VolumeRecord{}, err
	}
	s.records[spec.VolumeID] = rec
	s.publishLocked()
	return rec, nil
}

// MarkStoreReady publishes the durable placement hint after the filer has
// accepted both exclusive creates. A retry of the same create may repeat it;
// another layout identity may not mark this record ready.
func (s *FileStore) MarkStoreReady(volumeID, layoutID string) (VolumeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	if !ok {
		return VolumeRecord{}, ErrVolumeNotFound
	}
	if rec.Spec.LayoutID != layoutID {
		return VolumeRecord{}, ErrVolumeConflict
	}
	if rec.StoreReady {
		return rec, nil
	}
	rec.StoreReady = true
	if err := s.putLocked(rec); err != nil {
		return VolumeRecord{}, err
	}
	s.records[volumeID] = rec
	s.publishLocked()
	return rec, nil
}

func volumeSpecsCompatible(a, b VolumeSpec) bool {
	a = normalizeVolumeSpec(a)
	b = normalizeVolumeSpec(b)
	return a.VolumeID == b.VolumeID &&
		(b.LayoutID == "" || a.LayoutID == b.LayoutID) &&
		a.SizeBytes == b.SizeBytes &&
		a.BlockSize == b.BlockSize &&
		a.ChunkBytes == b.ChunkBytes &&
		a.SourceVolume == b.SourceVolume &&
		a.SourceLayoutID == b.SourceLayoutID &&
		a.SourceSnapshot == b.SourceSnapshot &&
		a.CreateRequestID == b.CreateRequestID &&
		a.ReplicationFactor == b.ReplicationFactor &&
		a.Protocol == b.Protocol &&
		a.FrontendTransport == b.FrontendTransport
}

func mergeVolumeSpecMetadata(existing, incoming VolumeSpec) VolumeSpec {
	out := existing
	if out.PVCName == "" {
		out.PVCName = incoming.PVCName
	}
	if out.PVCNamespace == "" {
		out.PVCNamespace = incoming.PVCNamespace
	}
	if out.PVCUID == "" {
		out.PVCUID = incoming.PVCUID
	}
	if out.PVName == "" {
		out.PVName = incoming.PVName
	}
	return out
}

// DeleteVolume removes a desired volume record. Missing volumes are a
// successful no-op so delete is safe to retry.
func (s *FileStore) DeleteVolume(volumeID string) error {
	if err := validateVolumeID(volumeID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, volumeID)
	s.publishLocked()
	if err := os.Remove(s.path(volumeID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("lifecycle: remove %s: %w", volumeID, err)
	}
	return nil
}

// AttachVolume records a publish/attach intent. Attaching to the same node
// is idempotent; attaching elsewhere requires a detach first.
func (s *FileStore) AttachVolume(volumeID, nodeID string) (VolumeRecord, error) {
	if err := validateVolumeID(volumeID); err != nil {
		return VolumeRecord{}, err
	}
	if nodeID == "" {
		return VolumeRecord{}, fmt.Errorf("%w: empty node id", ErrInvalidVolumeSpec)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	if !ok {
		return VolumeRecord{}, ErrVolumeNotFound
	}
	if rec.AttachedTo != "" && rec.AttachedTo != nodeID {
		return VolumeRecord{}, ErrAlreadyAttached
	}
	rec.AttachedTo = nodeID
	if err := s.putLocked(rec); err != nil {
		return VolumeRecord{}, err
	}
	s.records[volumeID] = rec
	s.publishLocked()
	return rec, nil
}

// DetachVolume clears an attach intent. Repeating the same detach is
// idempotent; detaching a volume attached elsewhere is rejected.
func (s *FileStore) DetachVolume(volumeID, nodeID string) (VolumeRecord, error) {
	if err := validateVolumeID(volumeID); err != nil {
		return VolumeRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	if !ok {
		return VolumeRecord{}, ErrVolumeNotFound
	}
	if rec.AttachedTo != "" && nodeID != "" && rec.AttachedTo != nodeID {
		return VolumeRecord{}, ErrAttachedElsewhere
	}
	rec.AttachedTo = ""
	if err := s.putLocked(rec); err != nil {
		return VolumeRecord{}, err
	}
	s.records[volumeID] = rec
	s.publishLocked()
	return rec, nil
}

// GetVolume returns a copy of a volume record.
func (s *FileStore) GetVolume(volumeID string) (VolumeRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	return rec, ok
}

// ListVolumes returns records ordered by VolumeID.
func (s *FileStore) ListVolumes() []VolumeRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.records))
	for k := range s.records {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]VolumeRecord, 0, len(keys))
	for _, k := range keys {
		out = append(out, s.records[k])
	}
	return out
}

func (s *FileStore) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("lifecycle: readdir %q: %w", s.dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(s.dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("lifecycle: read %q: %w", path, err)
		}
		var rec VolumeRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return fmt.Errorf("lifecycle: parse %q: %w", path, err)
		}
		if err := validateSpec(rec.Spec); err != nil {
			return fmt.Errorf("lifecycle: invalid record %q: %w", path, err)
		}
		rec.Spec = normalizeVolumeSpec(rec.Spec)
		if filepath.Base(s.path(rec.Spec.VolumeID)) != entry.Name() {
			return fmt.Errorf("lifecycle: record filename %q does not match volume_id %q", entry.Name(), rec.Spec.VolumeID)
		}
		s.records[rec.Spec.VolumeID] = rec
	}
	return nil
}

func (s *FileStore) putLocked(rec VolumeRecord) error {
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("lifecycle: marshal %s: %w", rec.Spec.VolumeID, err)
	}
	return writeRecordDurably(s.dir, rec.Spec.VolumeID+".json", raw)
}

func (s *FileStore) path(volumeID string) string {
	return filepath.Join(s.dir, volumeID+".json")
}

func validateSpec(spec VolumeSpec) error {
	spec = normalizeVolumeSpec(spec)
	if err := validateVolumeID(spec.VolumeID); err != nil {
		return err
	}
	if spec.SourceVolume != "" || spec.SourceLayoutID != "" || spec.SourceSnapshot != "" || spec.CreateRequestID != "" {
		if err := validateVolumeID(spec.SourceVolume); err != nil || spec.SourceSnapshot == "" || spec.CreateRequestID == "" || spec.SourceVolume == spec.VolumeID {
			return fmt.Errorf("%w: restore needs a distinct source volume, snapshot and request id", ErrInvalidVolumeSpec)
		}
		if _, err := layout.ParseVolumeID(spec.SourceLayoutID); err != nil {
			return fmt.Errorf("%w: restore needs the source layout id: %v", ErrInvalidVolumeSpec, err)
		}
		if err := layout.CheckSnapshot(spec.SourceSnapshot, layout.SnapshotMeta{}); err != nil {
			return fmt.Errorf("%w: restore snapshot: %v", ErrInvalidVolumeSpec, err)
		}
		if len(spec.CreateRequestID) > 128 {
			return fmt.Errorf("%w: restore request id is over 128 bytes", ErrInvalidVolumeSpec)
		}
	}
	if spec.LayoutID != "" {
		if _, err := layout.ParseVolumeID(spec.LayoutID); err != nil {
			return fmt.Errorf("%w: layout_id: %v", ErrInvalidVolumeSpec, err)
		}
	}
	if spec.SizeBytes == 0 {
		return fmt.Errorf("%w: size_bytes must be > 0", ErrInvalidVolumeSpec)
	}
	if err := (layout.Geometry{SizeBytes: spec.SizeBytes, BlockSize: spec.BlockSize, ChunkBytes: spec.ChunkBytes}).Check(); err != nil {
		return fmt.Errorf("%w: geometry: %v", ErrInvalidVolumeSpec, err)
	}
	if spec.BlockSize != 4096 || spec.ChunkBytes != 4<<20 {
		return fmt.Errorf("%w: v1 accepts only block_size=4096 and chunk_bytes=4194304", ErrInvalidVolumeSpec)
	}
	if spec.ReplicationFactor <= 0 {
		return fmt.Errorf("%w: replication_factor must be > 0", ErrInvalidVolumeSpec)
	}
	switch spec.Protocol {
	case "iscsi":
		if spec.FrontendTransport != "" {
			return fmt.Errorf("%w: frontend_transport is valid only for nvme", ErrInvalidVolumeSpec)
		}
	case "nvme":
		if spec.FrontendTransport != "tcp" && spec.FrontendTransport != "rdma" {
			return fmt.Errorf("%w: nvme frontend_transport must be tcp or rdma", ErrInvalidVolumeSpec)
		}
	default:
		return fmt.Errorf("%w: protocol must be iscsi or nvme", ErrInvalidVolumeSpec)
	}
	return nil
}

func normalizeVolumeSpec(spec VolumeSpec) VolumeSpec {
	if spec.BlockSize == 0 {
		spec.BlockSize = 4096
	}
	if spec.ChunkBytes == 0 {
		spec.ChunkBytes = 4 << 20
	}
	if spec.Protocol == "" {
		spec.Protocol = "iscsi"
	}
	if spec.Protocol == "nvme" && spec.FrontendTransport == "" {
		spec.FrontendTransport = "tcp"
	}
	return spec
}

func validateVolumeID(volumeID string) error {
	if volumeID == "" {
		return fmt.Errorf("%w: empty volume id", ErrInvalidVolumeSpec)
	}
	if !volumeIDPattern.MatchString(volumeID) {
		return fmt.Errorf("%w: invalid volume id %q", ErrInvalidVolumeSpec, volumeID)
	}
	return nil
}
