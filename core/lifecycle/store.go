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
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
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
	VolumeID          string `json:"volume_id"`
	SizeBytes         uint64 `json:"size_bytes"`
	ReplicationFactor int    `json:"replication_factor"`
	PVCName           string `json:"pvc_name,omitempty"`
	PVCNamespace      string `json:"pvc_namespace,omitempty"`
	PVCUID            string `json:"pvc_uid,omitempty"`
	PVName            string `json:"pv_name,omitempty"`
}

// VolumeRecord is the durable lifecycle state for one volume.
type VolumeRecord struct {
	Spec       VolumeSpec `json:"spec"`
	AttachedTo string     `json:"attached_to,omitempty"`
}

// FileStore persists one JSON record per volume ID.
type FileStore struct {
	mu      sync.Mutex
	dir     string
	records map[string]VolumeRecord
}

// OpenFileStore opens or creates a lifecycle intent store.
func OpenFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty dir", ErrInvalidVolumeSpec)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lifecycle: mkdir %q: %w", dir, err)
	}
	s := &FileStore{
		dir:     dir,
		records: make(map[string]VolumeRecord),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// CreateVolume records desired volume state. Repeating the same spec is
// idempotent; changing a previously-created spec is rejected.
func (s *FileStore) CreateVolume(spec VolumeSpec) (VolumeRecord, error) {
	if err := validateSpec(spec); err != nil {
		return VolumeRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[spec.VolumeID]; ok {
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
			return merged, nil
		}
		return existing, nil
	}
	rec := VolumeRecord{Spec: spec}
	if err := s.putLocked(rec); err != nil {
		return VolumeRecord{}, err
	}
	s.records[spec.VolumeID] = rec
	return rec, nil
}

func volumeSpecsCompatible(a, b VolumeSpec) bool {
	return a.VolumeID == b.VolumeID &&
		a.SizeBytes == b.SizeBytes &&
		a.ReplicationFactor == b.ReplicationFactor
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
	path := s.path(rec.Spec.VolumeID)
	tmp, err := os.CreateTemp(s.dir, rec.Spec.VolumeID+".*.tmp")
	if err != nil {
		return fmt.Errorf("lifecycle: temp %s: %w", rec.Spec.VolumeID, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("lifecycle: write temp %s: %w", rec.Spec.VolumeID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("lifecycle: close temp %s: %w", rec.Spec.VolumeID, err)
	}
	// Windows os.Rename does not replace existing files. Remove first;
	// lifecycle records are intent metadata, and the in-memory state is
	// updated only after this write succeeds.
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("lifecycle: replace %s: %w", rec.Spec.VolumeID, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("lifecycle: rename %s: %w", rec.Spec.VolumeID, err)
	}
	return nil
}

func (s *FileStore) path(volumeID string) string {
	return filepath.Join(s.dir, volumeID+".json")
}

func validateSpec(spec VolumeSpec) error {
	if err := validateVolumeID(spec.VolumeID); err != nil {
		return err
	}
	if spec.SizeBytes == 0 {
		return fmt.Errorf("%w: size_bytes must be > 0", ErrInvalidVolumeSpec)
	}
	if spec.ReplicationFactor <= 0 {
		return fmt.Errorf("%w: replication_factor must be > 0", ErrInvalidVolumeSpec)
	}
	return nil
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
