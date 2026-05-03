package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var ErrInsufficientPlacementCandidates = errors.New("lifecycle: insufficient placement candidates")

// PlacementSlotIntent is the controller-facing desired slot generated from a
// placement plan. It is still not authority: no epoch, no endpoint version,
// no readiness.
type PlacementSlotIntent struct {
	ServerID  string `json:"server_id"`
	PoolID    string `json:"pool_id,omitempty"`
	ReplicaID string `json:"replica_id,omitempty"`
	Source    string `json:"source"`
}

// PlacementIntent is durable controller input for one volume.
type PlacementIntent struct {
	VolumeID  string                `json:"volume_id"`
	DesiredRF int                   `json:"desired_rf"`
	Slots     []PlacementSlotIntent `json:"slots"`
}

// PlacementIntentStore persists one placement intent per volume.
type PlacementIntentStore struct {
	mu      sync.Mutex
	dir     string
	records map[string]PlacementIntent
}

// OpenPlacementIntentStore opens or creates a placement intent store.
func OpenPlacementIntentStore(dir string) (*PlacementIntentStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty dir", ErrInvalidVolumeSpec)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lifecycle: mkdir placement intent %q: %w", dir, err)
	}
	s := &PlacementIntentStore{
		dir:     dir,
		records: make(map[string]PlacementIntent),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// ApplyPlan records placement intent from a sufficient plan. Insufficient
// plans are rejected rather than persisted as partial authority-shaped state.
func (s *PlacementIntentStore) ApplyPlan(plan PlacementPlan) (PlacementIntent, error) {
	if !plan.EnoughCandidates() {
		return PlacementIntent{}, ErrInsufficientPlacementCandidates
	}
	intent := PlacementIntent{
		VolumeID:  plan.VolumeID,
		DesiredRF: plan.DesiredRF,
		Slots:     make([]PlacementSlotIntent, 0, plan.DesiredRF),
	}
	for _, candidate := range plan.Candidates[:plan.DesiredRF] {
		intent.Slots = append(intent.Slots, PlacementSlotIntent{
			ServerID:  candidate.ServerID,
			PoolID:    candidate.PoolID,
			ReplicaID: candidate.ReplicaID,
			Source:    candidate.Source,
		})
	}
	if err := validatePlacementIntent(intent); err != nil {
		return PlacementIntent{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.putLocked(intent); err != nil {
		return PlacementIntent{}, err
	}
	s.records[intent.VolumeID] = intent
	return copyPlacementIntent(intent), nil
}

// DeletePlacement removes one placement intent. Missing intents are a
// successful no-op so reconciliation cleanup is retry-safe.
func (s *PlacementIntentStore) DeletePlacement(volumeID string) error {
	if err := validateVolumeID(volumeID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, volumeID)
	if err := os.Remove(s.path(volumeID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("lifecycle: remove placement intent %s: %w", volumeID, err)
	}
	return nil
}

// GetPlacement returns a copy of one placement intent.
func (s *PlacementIntentStore) GetPlacement(volumeID string) (PlacementIntent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	return copyPlacementIntent(rec), ok
}

// ListPlacements returns intents ordered by VolumeID.
func (s *PlacementIntentStore) ListPlacements() []PlacementIntent {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.records))
	for k := range s.records {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]PlacementIntent, 0, len(keys))
	for _, k := range keys {
		out = append(out, copyPlacementIntent(s.records[k]))
	}
	return out
}

func (s *PlacementIntentStore) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("lifecycle: readdir placement intent %q: %w", s.dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(s.dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("lifecycle: read placement intent %q: %w", path, err)
		}
		var rec PlacementIntent
		if err := json.Unmarshal(raw, &rec); err != nil {
			return fmt.Errorf("lifecycle: parse placement intent %q: %w", path, err)
		}
		if err := validatePlacementIntent(rec); err != nil {
			return fmt.Errorf("lifecycle: invalid placement intent %q: %w", path, err)
		}
		if filepath.Base(s.path(rec.VolumeID)) != entry.Name() {
			return fmt.Errorf("lifecycle: placement filename %q does not match volume_id %q", entry.Name(), rec.VolumeID)
		}
		s.records[rec.VolumeID] = rec
	}
	return nil
}

func (s *PlacementIntentStore) putLocked(rec PlacementIntent) error {
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("lifecycle: marshal placement intent %s: %w", rec.VolumeID, err)
	}
	path := s.path(rec.VolumeID)
	tmp, err := os.CreateTemp(s.dir, rec.VolumeID+".*.tmp")
	if err != nil {
		return fmt.Errorf("lifecycle: temp placement intent %s: %w", rec.VolumeID, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("lifecycle: write placement intent temp %s: %w", rec.VolumeID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("lifecycle: close placement intent temp %s: %w", rec.VolumeID, err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("lifecycle: replace placement intent %s: %w", rec.VolumeID, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("lifecycle: rename placement intent %s: %w", rec.VolumeID, err)
	}
	return nil
}

func (s *PlacementIntentStore) path(volumeID string) string {
	return filepath.Join(s.dir, volumeID+".json")
}

func validatePlacementIntent(intent PlacementIntent) error {
	if err := validateVolumeID(intent.VolumeID); err != nil {
		return err
	}
	if intent.DesiredRF <= 0 {
		return fmt.Errorf("%w: desired_rf must be > 0", ErrInvalidVolumeSpec)
	}
	if len(intent.Slots) != intent.DesiredRF {
		return fmt.Errorf("%w: slot count %d != desired_rf %d", ErrInvalidVolumeSpec, len(intent.Slots), intent.DesiredRF)
	}
	seenServers := make(map[string]bool)
	for _, slot := range intent.Slots {
		if err := validateServerID(slot.ServerID); err != nil {
			return err
		}
		if seenServers[slot.ServerID] {
			return fmt.Errorf("%w: duplicate server %s", ErrInvalidVolumeSpec, slot.ServerID)
		}
		seenServers[slot.ServerID] = true
		switch slot.Source {
		case PlacementSourceBlankPool:
			if slot.PoolID == "" {
				return fmt.Errorf("%w: blank-pool slot on %s missing pool id", ErrInvalidVolumeSpec, slot.ServerID)
			}
			if slot.ReplicaID != "" {
				return fmt.Errorf("%w: blank-pool slot on %s must not preassign replica id", ErrInvalidVolumeSpec, slot.ServerID)
			}
		case PlacementSourceExistingReplica:
			if slot.ReplicaID == "" {
				return fmt.Errorf("%w: existing-replica slot on %s missing replica id", ErrInvalidVolumeSpec, slot.ServerID)
			}
		default:
			return fmt.Errorf("%w: unknown placement source %q", ErrInvalidVolumeSpec, slot.Source)
		}
	}
	return nil
}

func copyPlacementIntent(in PlacementIntent) PlacementIntent {
	out := in
	out.Slots = append([]PlacementSlotIntent(nil), in.Slots...)
	return out
}
