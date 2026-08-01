package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

const restoreMarkerVersion = 1

const (
	RestoreStatePending   = "pending"
	RestoreStateApplying  = "applying"
	RestoreStateApplied   = "applied"
	RestoreStateActivated = "activated"
)

type RestoreTargetConfig struct {
	MarkerPath      string
	TargetDataPath  string
	SnapshotID      string
	TargetVolumeID  string
	TargetReplicaID string
}

type RestoreMarker struct {
	Version         int     `json:"version"`
	State           string  `json:"state"`
	SnapshotID      string  `json:"snapshot_id"`
	TargetVolumeID  string  `json:"target_volume_id"`
	TargetReplicaID string  `json:"target_replica_id"`
	Snapshot        *Record `json:"snapshot,omitempty"`
	RestoredBlocks  uint64  `json:"restored_blocks,omitempty"`
	RestoredBytes   uint64  `json:"restored_bytes,omitempty"`
	TargetFrontier  uint64  `json:"target_frontier,omitempty"`
}

type RestoreApplyResult struct {
	State          string
	RestoredBlocks uint64
	RestoredBytes  uint64
	TargetFrontier uint64
	AlreadyApplied bool
}

// RestoreTarget owns the durable publication fence for one new replica. It
// never grants authority itself: Activate only records that the local bytes
// passed verification and then invokes the caller's readiness callback.
type RestoreTarget struct {
	mu         sync.Mutex
	markerPath string
	marker     RestoreMarker
}

// OpenRestoreTarget creates or recovers the marker before the target storage
// file is opened. A pre-existing data file without a marker is ambiguous and
// therefore rejected rather than reused as a restore destination.
func OpenRestoreTarget(cfg RestoreTargetConfig) (*RestoreTarget, error) {
	if cfg.MarkerPath == "" || cfg.TargetDataPath == "" || cfg.SnapshotID == "" || cfg.TargetVolumeID == "" || cfg.TargetReplicaID == "" {
		return nil, fmt.Errorf("%w: restore marker, data path, snapshot, volume, and replica are required", ErrInvalidRequest)
	}
	if filepath.Clean(cfg.MarkerPath) == filepath.Clean(cfg.TargetDataPath) {
		return nil, fmt.Errorf("%w: restore marker and target data paths must differ", ErrInvalidRequest)
	}
	t := &RestoreTarget{markerPath: cfg.MarkerPath}
	raw, err := os.ReadFile(cfg.MarkerPath)
	if err == nil {
		if err := json.Unmarshal(raw, &t.marker); err != nil {
			return nil, fmt.Errorf("%w: parse restore marker: %v", ErrRestoreUnsafe, err)
		}
		if err := validateRestoreMarker(t.marker, cfg); err != nil {
			return nil, err
		}
		exists, err := restorePathExists(cfg.TargetDataPath)
		if err != nil {
			return nil, err
		}
		if (t.marker.State == RestoreStateApplied || t.marker.State == RestoreStateActivated) && !exists {
			return nil, fmt.Errorf("%w: %s marker has no target data", ErrRestoreUnsafe, t.marker.State)
		}
		return t, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("snapshot: read restore marker: %w", err)
	}
	exists, statErr := restorePathExists(cfg.TargetDataPath)
	if statErr != nil {
		return nil, statErr
	}
	if exists {
		return nil, fmt.Errorf("%w: target data exists without restore marker", ErrRestoreUnsafe)
	}
	t.marker = RestoreMarker{
		Version:         restoreMarkerVersion,
		State:           RestoreStatePending,
		SnapshotID:      cfg.SnapshotID,
		TargetVolumeID:  cfg.TargetVolumeID,
		TargetReplicaID: cfg.TargetReplicaID,
	}
	if err := t.persistLocked(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *RestoreTarget) Marker() RestoreMarker {
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneRestoreMarker(t.marker)
}

func (t *RestoreTarget) Apply(ctx context.Context, r io.Reader, rec Record, target storage.LogicalStorage) (RestoreApplyResult, error) {
	if r == nil || target == nil {
		return RestoreApplyResult{}, fmt.Errorf("%w: restore stream and target storage are required", ErrInvalidRequest)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if rec.SnapshotID != t.marker.SnapshotID || validateRecord(rec) != nil {
		return RestoreApplyResult{}, fmt.Errorf("%w: snapshot record does not match target", ErrRestoreConflict)
	}
	if t.marker.Snapshot != nil && !sameRestoreRecord(*t.marker.Snapshot, rec) {
		return RestoreApplyResult{}, fmt.Errorf("%w: target is already bound to another archive", ErrRestoreConflict)
	}
	if target.NumBlocks() != rec.NumBlocks || target.BlockSize() != rec.BlockSize {
		return RestoreApplyResult{}, fmt.Errorf("%w: target geometry blocks=%d/%d block_size=%d/%d", ErrRestoreConflict, target.NumBlocks(), rec.NumBlocks, target.BlockSize(), rec.BlockSize)
	}
	if t.marker.State == RestoreStateApplied || t.marker.State == RestoreStateActivated {
		if _, err := ApplyArchiveStream(ctx, r, rec, func(uint32, []byte) error { return nil }); err != nil {
			return RestoreApplyResult{}, err
		}
		return restoreApplyResult(t.marker, true), nil
	}
	if t.marker.State != RestoreStatePending && t.marker.State != RestoreStateApplying {
		return RestoreApplyResult{}, fmt.Errorf("%w: invalid apply state %q", ErrRestoreUnsafe, t.marker.State)
	}

	bound := rec
	t.marker.Snapshot = &bound
	t.marker.State = RestoreStateApplying
	t.marker.RestoredBlocks = 0
	t.marker.RestoredBytes = 0
	t.marker.TargetFrontier = 0
	if err := t.persistLocked(); err != nil {
		return RestoreApplyResult{}, err
	}
	var restoredBlocks, restoredBytes uint64
	_, err := ApplyArchiveStream(ctx, r, rec, func(lba uint32, data []byte) error {
		if _, err := target.Write(lba, data); err != nil {
			return fmt.Errorf("snapshot: apply target LBA %d: %w", lba, err)
		}
		restoredBlocks++
		restoredBytes += uint64(len(data))
		return nil
	})
	if err != nil {
		return RestoreApplyResult{}, t.resetPendingLocked(err)
	}
	frontier, err := target.Sync()
	if err != nil {
		return RestoreApplyResult{}, t.resetPendingLocked(fmt.Errorf("snapshot: sync restore target: %w", err))
	}
	if restoredBlocks != rec.RecordCount || restoredBytes != rec.DataBytes {
		return RestoreApplyResult{}, t.resetPendingLocked(fmt.Errorf("%w: applied counters do not match catalog", ErrArchiveCorrupt))
	}
	t.marker.State = RestoreStateApplied
	t.marker.RestoredBlocks = restoredBlocks
	t.marker.RestoredBytes = restoredBytes
	t.marker.TargetFrontier = frontier
	if err := t.persistLocked(); err != nil {
		return RestoreApplyResult{}, err
	}
	return restoreApplyResult(t.marker, false), nil
}

// Activate durably records publication eligibility before releasing local
// readiness. The callback is deliberately retried when an activated marker is
// reopened, so a process crash between those two operations remains safe.
func (t *RestoreTarget) Activate(releaseReadiness func() error) error {
	if releaseReadiness == nil {
		return fmt.Errorf("%w: readiness callback is required", ErrInvalidRequest)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.marker.State != RestoreStateApplied && t.marker.State != RestoreStateActivated {
		return fmt.Errorf("%w: state=%s", ErrRestoreNotApplied, t.marker.State)
	}
	if t.marker.State == RestoreStateApplied {
		t.marker.State = RestoreStateActivated
		if err := t.persistLocked(); err != nil {
			return err
		}
	}
	return releaseReadiness()
}

func (t *RestoreTarget) resetPendingLocked(cause error) error {
	t.marker.State = RestoreStatePending
	t.marker.RestoredBlocks = 0
	t.marker.RestoredBytes = 0
	t.marker.TargetFrontier = 0
	if err := t.persistLocked(); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (t *RestoreTarget) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(t.markerPath), 0o755); err != nil {
		return fmt.Errorf("snapshot: mkdir restore marker: %w", err)
	}
	raw, err := json.MarshalIndent(t.marker, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: marshal restore marker: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(t.markerPath), ".tmp-restore-marker-*")
	if err != nil {
		return fmt.Errorf("snapshot: create restore marker temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("snapshot: chmod restore marker: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("snapshot: write restore marker: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("snapshot: fsync restore marker: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("snapshot: close restore marker: %w", err)
	}
	if err := os.Rename(tmpPath, t.markerPath); err != nil {
		return fmt.Errorf("snapshot: publish restore marker: %w", err)
	}
	return syncDir(filepath.Dir(t.markerPath))
}

func validateRestoreMarker(marker RestoreMarker, cfg RestoreTargetConfig) error {
	if marker.Version != restoreMarkerVersion || marker.SnapshotID != cfg.SnapshotID || marker.TargetVolumeID != cfg.TargetVolumeID || marker.TargetReplicaID != cfg.TargetReplicaID {
		return fmt.Errorf("%w: restore marker identity mismatch", ErrRestoreConflict)
	}
	switch marker.State {
	case RestoreStatePending, RestoreStateApplying:
	case RestoreStateApplied, RestoreStateActivated:
		if marker.Snapshot == nil || validateRecord(*marker.Snapshot) != nil || marker.RestoredBlocks != marker.Snapshot.RecordCount || marker.RestoredBytes != marker.Snapshot.DataBytes {
			return fmt.Errorf("%w: invalid %s marker", ErrRestoreUnsafe, marker.State)
		}
	default:
		return fmt.Errorf("%w: invalid restore marker state %q", ErrRestoreUnsafe, marker.State)
	}
	if marker.Snapshot != nil {
		if validateRecord(*marker.Snapshot) != nil {
			return fmt.Errorf("%w: marker contains an invalid snapshot record", ErrRestoreUnsafe)
		}
		if marker.Snapshot.SnapshotID != marker.SnapshotID {
			return fmt.Errorf("%w: marker snapshot mismatch", ErrRestoreConflict)
		}
	}
	return nil
}

func sameRestoreRecord(a, b Record) bool {
	return a.SnapshotID == b.SnapshotID && a.SourceVolumeID == b.SourceVolumeID && a.Frontier == b.Frontier && a.SizeBytes == b.SizeBytes && a.NumBlocks == b.NumBlocks && a.BlockSize == b.BlockSize && a.RecordCount == b.RecordCount && a.DataBytes == b.DataBytes && a.ArchiveBytes == b.ArchiveBytes && a.ArchiveSHA256 == b.ArchiveSHA256
}

func restoreApplyResult(marker RestoreMarker, already bool) RestoreApplyResult {
	return RestoreApplyResult{State: marker.State, RestoredBlocks: marker.RestoredBlocks, RestoredBytes: marker.RestoredBytes, TargetFrontier: marker.TargetFrontier, AlreadyApplied: already}
}

func cloneRestoreMarker(marker RestoreMarker) RestoreMarker {
	if marker.Snapshot != nil {
		rec := *marker.Snapshot
		marker.Snapshot = &rec
	}
	return marker
}

func restorePathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("snapshot: stat restore target: %w", err)
}
