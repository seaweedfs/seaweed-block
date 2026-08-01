package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

const restoreMarkerVersion = 2

const (
	RestoreStatePending        = "pending"
	RestoreStateApplying       = "applying"
	RestoreStateApplied        = "applied"
	RestoreStateActivated      = "activated"
	RestoreStateIntegrityFault = "integrity_fault"
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
	TargetDataPath  string  `json:"target_data_path"`
	TargetStoreKind string  `json:"target_store_kind,omitempty"`
	TargetStorageID string  `json:"target_storage_id,omitempty"`
	TargetNumBlocks uint32  `json:"target_num_blocks,omitempty"`
	TargetBlockSize int     `json:"target_block_size,omitempty"`
	Snapshot        *Record `json:"snapshot,omitempty"`
	RestoredBlocks  uint64  `json:"restored_blocks,omitempty"`
	RestoredBytes   uint64  `json:"restored_bytes,omitempty"`
	TargetFrontier  uint64  `json:"target_frontier,omitempty"`
}

// PrepareStorage durably reserves the target path for one backend and
// geometry before that backend creates its file. This closes the crash window
// between store creation and BindStorage recording the generated StoreID.
func (t *RestoreTarget) PrepareStorage(kind string, numBlocks uint32, blockSize int) error {
	if !safeRestoreStorageKind(kind) || numBlocks == 0 || blockSize <= 0 {
		return fmt.Errorf("%w: restore storage kind and geometry are required", ErrInvalidRequest)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	legacyUpgrade := false
	if t.marker.TargetStoreKind == "" && t.marker.TargetStorageID != "" {
		legacyKind, ok := restoreStorageKindFromID(t.marker.TargetStorageID)
		if !ok {
			return fmt.Errorf("%w: restore marker has an unknown storage identity", ErrRestoreUnsafe)
		}
		t.marker.TargetStoreKind = legacyKind
		legacyUpgrade = true
	}
	if t.marker.State != RestoreStatePending {
		if t.marker.TargetStoreKind == kind && t.marker.TargetNumBlocks == numBlocks && t.marker.TargetBlockSize == blockSize {
			if legacyUpgrade {
				return t.persistLocked()
			}
			return nil
		}
		return fmt.Errorf("%w: restore storage intent conflicts with state %q", ErrRestoreConflict, t.marker.State)
	}
	if t.marker.TargetStoreKind != "" {
		if t.marker.TargetStoreKind != kind || t.marker.TargetNumBlocks != numBlocks || t.marker.TargetBlockSize != blockSize {
			return fmt.Errorf("%w: restore storage intent changed", ErrRestoreConflict)
		}
		if legacyUpgrade {
			return t.persistLocked()
		}
		return nil
	}
	exists, err := restorePathExists(t.marker.TargetDataPath)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%w: target data exists before durable storage intent", ErrRestoreUnsafe)
	}
	t.marker.TargetStoreKind = kind
	t.marker.TargetNumBlocks = numBlocks
	t.marker.TargetBlockSize = blockSize
	return t.persistLocked()
}

type RestoreApplyResult struct {
	State           string
	TargetStorageID string
	TargetNumBlocks uint32
	TargetBlockSize int
	RestoredBlocks  uint64
	RestoredBytes   uint64
	TargetFrontier  uint64
	AlreadyApplied  bool
}

// RestoreTarget owns the durable publication fence for one new replica. It
// never grants authority itself: Activate only records that the local bytes
// passed verification and then invokes the caller's readiness callback.
type RestoreTarget struct {
	mu           sync.Mutex
	activationMu sync.Mutex
	markerPath   string
	marker       RestoreMarker
	storage      storage.LogicalStorage
}

// OpenRestoreTarget creates or recovers the marker before the target storage
// file is opened. A pre-existing data file without a marker is ambiguous and
// therefore rejected rather than reused as a restore destination.
func OpenRestoreTarget(cfg RestoreTargetConfig) (*RestoreTarget, error) {
	if cfg.MarkerPath == "" || cfg.TargetDataPath == "" || cfg.SnapshotID == "" || cfg.TargetVolumeID == "" || cfg.TargetReplicaID == "" {
		return nil, fmt.Errorf("%w: restore marker, data path, snapshot, volume, and replica are required", ErrInvalidRequest)
	}
	markerPath, err := canonicalRestorePath(cfg.MarkerPath)
	if err != nil {
		return nil, err
	}
	targetDataPath, err := canonicalRestorePath(cfg.TargetDataPath)
	if err != nil {
		return nil, err
	}
	if markerPath == targetDataPath {
		return nil, fmt.Errorf("%w: restore marker and target data paths must differ", ErrInvalidRequest)
	}
	cfg.MarkerPath = markerPath
	cfg.TargetDataPath = targetDataPath
	t := &RestoreTarget{markerPath: markerPath}
	marker, exists, err := LoadRestoreMarker(markerPath)
	if err != nil {
		return nil, err
	}
	if exists {
		t.marker = marker
		if err := validateRestoreMarker(t.marker, cfg); err != nil {
			return nil, err
		}
		exists, err := restorePathExists(cfg.TargetDataPath)
		if err != nil {
			return nil, err
		}
		if (t.marker.State == RestoreStateApplied || t.marker.State == RestoreStateActivated || t.marker.State == RestoreStateIntegrityFault) && !exists {
			return nil, fmt.Errorf("%w: %s marker has no target data", ErrRestoreUnsafe, t.marker.State)
		}
		return t, nil
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
		TargetDataPath:  targetDataPath,
	}
	if err := t.persistLocked(); err != nil {
		return nil, err
	}
	return t, nil
}

// BindStorage binds the restore fence to one file-backed store before any
// archive bytes can be applied. Reopens must present the same canonical path,
// persistent store identity, and geometry recorded in the marker.
func (t *RestoreTarget) BindStorage(target storage.LogicalStorage) error {
	if target == nil {
		return fmt.Errorf("%w: target storage is required", ErrInvalidRequest)
	}
	provider, ok := target.(storage.DurableStorageIdentityProvider)
	if !ok {
		return fmt.Errorf("%w: target storage has no durable identity", ErrRestoreUnsafe)
	}
	identity := provider.DurableStorageIdentity()
	path, err := canonicalRestorePath(identity.Path)
	if err != nil || identity.StoreID == "" {
		return fmt.Errorf("%w: invalid target storage identity", ErrRestoreUnsafe)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if path != t.marker.TargetDataPath {
		return fmt.Errorf("%w: target storage path mismatch", ErrRestoreConflict)
	}
	if t.marker.TargetStoreKind == "" {
		return fmt.Errorf("%w: target storage was created without a durable intent", ErrRestoreUnsafe)
	}
	if !restoreStorageIDMatchesKind(identity.StoreID, t.marker.TargetStoreKind) || t.marker.TargetNumBlocks != target.NumBlocks() || t.marker.TargetBlockSize != target.BlockSize() {
		return fmt.Errorf("%w: target storage does not match its durable intent", ErrRestoreConflict)
	}
	if t.marker.TargetStorageID == "" {
		if t.marker.State != RestoreStatePending && t.marker.State != RestoreStateApplying {
			return fmt.Errorf("%w: %s marker has no storage identity", ErrRestoreUnsafe, t.marker.State)
		}
		t.marker.TargetStorageID = identity.StoreID
		if err := t.persistLocked(); err != nil {
			return err
		}
	} else if t.marker.TargetStorageID != identity.StoreID || t.marker.TargetNumBlocks != target.NumBlocks() || t.marker.TargetBlockSize != target.BlockSize() {
		return fmt.Errorf("%w: target storage identity or geometry mismatch", ErrRestoreConflict)
	}
	t.storage = target
	return nil
}

// LoadRestoreMarker inspects a durable restore fence without changing it.
// Callers use this to resume a restore even if its launch flag was lost.
func LoadRestoreMarker(path string) (RestoreMarker, bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return RestoreMarker{}, false, nil
	}
	if err != nil {
		return RestoreMarker{}, false, fmt.Errorf("snapshot: read restore marker: %w", err)
	}
	var marker RestoreMarker
	if err := json.Unmarshal(raw, &marker); err != nil {
		return RestoreMarker{}, false, fmt.Errorf("%w: parse restore marker: %v", ErrRestoreUnsafe, err)
	}
	return marker, true, nil
}

func (t *RestoreTarget) Marker() RestoreMarker {
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneRestoreMarker(t.marker)
}

func (t *RestoreTarget) Apply(ctx context.Context, r io.Reader, rec Record) (RestoreApplyResult, error) {
	if r == nil {
		return RestoreApplyResult{}, fmt.Errorf("%w: restore stream is required", ErrInvalidRequest)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.verifyStorageLocked(); err != nil {
		return RestoreApplyResult{}, err
	}
	if rec.SnapshotID != t.marker.SnapshotID || validateRecord(rec) != nil {
		return RestoreApplyResult{}, fmt.Errorf("%w: snapshot record does not match target", ErrRestoreConflict)
	}
	if t.marker.Snapshot != nil && !sameRestoreRecord(*t.marker.Snapshot, rec) {
		return RestoreApplyResult{}, fmt.Errorf("%w: target is already bound to another archive", ErrRestoreConflict)
	}
	if t.storage.NumBlocks() != rec.NumBlocks || t.storage.BlockSize() != rec.BlockSize {
		return RestoreApplyResult{}, fmt.Errorf("%w: target geometry blocks=%d/%d block_size=%d/%d", ErrRestoreConflict, t.storage.NumBlocks(), rec.NumBlocks, t.storage.BlockSize(), rec.BlockSize)
	}
	if t.marker.State == RestoreStateApplied || t.marker.State == RestoreStateActivated {
		stagedPath, err := stageVerifiedArchive(ctx, r, rec, filepath.Dir(t.markerPath))
		if err != nil {
			return RestoreApplyResult{}, err
		}
		defer os.Remove(stagedPath)
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
	stagedPath, err := stageVerifiedArchive(ctx, r, rec, filepath.Dir(t.markerPath))
	if err != nil {
		return RestoreApplyResult{}, t.resetPendingLocked(err)
	}
	defer os.Remove(stagedPath)
	var restoredBlocks, restoredBytes uint64
	staged, err := os.Open(stagedPath)
	if err != nil {
		return RestoreApplyResult{}, t.resetPendingLocked(fmt.Errorf("snapshot: reopen staged archive: %w", err))
	}
	_, applyErr := ApplyArchiveStream(ctx, staged, rec, func(lba uint32, data []byte) error {
		if _, err := t.storage.Write(lba, data); err != nil {
			return fmt.Errorf("snapshot: apply target LBA %d: %w", lba, err)
		}
		restoredBlocks++
		restoredBytes += uint64(len(data))
		return nil
	})
	closeErr := staged.Close()
	if applyErr != nil || closeErr != nil {
		return RestoreApplyResult{}, t.resetPendingLocked(errors.Join(applyErr, closeErr))
	}
	frontier, err := t.storage.Sync()
	if err != nil {
		return RestoreApplyResult{}, t.resetPendingLocked(fmt.Errorf("snapshot: sync restore target: %w", err))
	}
	if restoredBlocks != rec.RecordCount || restoredBytes != rec.DataBytes {
		return RestoreApplyResult{}, t.resetPendingLocked(fmt.Errorf("%w: applied counters do not match catalog", ErrArchiveCorrupt))
	}
	if err := verifyRestoredArchive(ctx, stagedPath, rec, t.storage); err != nil {
		if errors.Is(err, ErrArchiveCorrupt) {
			return RestoreApplyResult{}, t.failIntegrityLocked(err, restoredBlocks, restoredBytes, frontier)
		}
		return RestoreApplyResult{}, t.resetPendingLocked(err)
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

func verifyRestoredArchive(ctx context.Context, archivePath string, rec Record, target storage.LogicalStorage) error {
	archive, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("snapshot: reopen staged archive for target verification: %w", err)
	}
	defer archive.Close()
	nextLBA := uint32(0)
	zero := make([]byte, rec.BlockSize)
	verifyZeroRange := func(end uint32) error {
		for nextLBA < end {
			if err := ctx.Err(); err != nil {
				return err
			}
			got, err := target.Read(nextLBA)
			if err != nil {
				return fmt.Errorf("snapshot: verify target LBA %d: %w", nextLBA, err)
			}
			if !bytes.Equal(got, zero) {
				return fmt.Errorf("%w: restored target sparse LBA %d is non-zero", ErrArchiveCorrupt, nextLBA)
			}
			nextLBA++
		}
		return nil
	}
	_, err = ApplyArchiveStream(ctx, archive, rec, func(lba uint32, want []byte) error {
		if err := verifyZeroRange(lba); err != nil {
			return err
		}
		got, err := target.Read(lba)
		if err != nil {
			return fmt.Errorf("snapshot: verify target LBA %d: %w", lba, err)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("%w: restored target LBA %d does not match archive", ErrArchiveCorrupt, lba)
		}
		nextLBA = lba + 1
		return nil
	})
	if err != nil {
		return err
	}
	return verifyZeroRange(rec.NumBlocks)
}

// Activate durably records publication eligibility before releasing local
// readiness. The callback is deliberately retried when an activated marker is
// reopened, so a process crash between those two operations remains safe.
func (t *RestoreTarget) Activate(releaseReadiness func() error) error {
	if releaseReadiness == nil {
		return fmt.Errorf("%w: readiness callback is required", ErrInvalidRequest)
	}
	t.activationMu.Lock()
	defer t.activationMu.Unlock()
	t.mu.Lock()
	if err := t.verifyStorageLocked(); err != nil {
		t.mu.Unlock()
		return err
	}
	if t.marker.State != RestoreStateApplied && t.marker.State != RestoreStateActivated {
		state := t.marker.State
		t.mu.Unlock()
		return fmt.Errorf("%w: state=%s", ErrRestoreNotApplied, state)
	}
	if t.marker.State == RestoreStateApplied {
		t.marker.State = RestoreStateActivated
		if err := t.persistLocked(); err != nil {
			t.mu.Unlock()
			return err
		}
	}
	t.mu.Unlock()
	return releaseReadiness()
}

func (t *RestoreTarget) verifyStorageLocked() error {
	if t.storage == nil {
		return fmt.Errorf("%w: target storage is not bound", ErrRestoreUnsafe)
	}
	provider, ok := t.storage.(storage.DurableStorageIdentityProvider)
	if !ok {
		return fmt.Errorf("%w: target storage has no durable identity", ErrRestoreUnsafe)
	}
	identity := provider.DurableStorageIdentity()
	path, err := canonicalRestorePath(identity.Path)
	if err != nil || path != t.marker.TargetDataPath || identity.StoreID == "" || identity.StoreID != t.marker.TargetStorageID || t.storage.NumBlocks() != t.marker.TargetNumBlocks || t.storage.BlockSize() != t.marker.TargetBlockSize {
		return fmt.Errorf("%w: bound target storage identity changed", ErrRestoreUnsafe)
	}
	return nil
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

func (t *RestoreTarget) failIntegrityLocked(cause error, restoredBlocks, restoredBytes, frontier uint64) error {
	t.marker.State = RestoreStateIntegrityFault
	t.marker.RestoredBlocks = restoredBlocks
	t.marker.RestoredBytes = restoredBytes
	t.marker.TargetFrontier = frontier
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
	if marker.Version != restoreMarkerVersion || marker.SnapshotID != cfg.SnapshotID || marker.TargetVolumeID != cfg.TargetVolumeID || marker.TargetReplicaID != cfg.TargetReplicaID || marker.TargetDataPath != cfg.TargetDataPath {
		return fmt.Errorf("%w: restore marker identity mismatch", ErrRestoreConflict)
	}
	if marker.TargetStoreKind == "" && marker.TargetStorageID != "" {
		legacyKind, ok := restoreStorageKindFromID(marker.TargetStorageID)
		if !ok || marker.TargetNumBlocks == 0 || marker.TargetBlockSize <= 0 {
			return fmt.Errorf("%w: incomplete target storage identity", ErrRestoreUnsafe)
		}
		marker.TargetStoreKind = legacyKind
	}
	if marker.TargetStoreKind == "" {
		if marker.TargetStorageID != "" || marker.TargetNumBlocks != 0 || marker.TargetBlockSize != 0 {
			return fmt.Errorf("%w: incomplete target storage intent", ErrRestoreUnsafe)
		}
	} else if !safeRestoreStorageKind(marker.TargetStoreKind) || marker.TargetNumBlocks == 0 || marker.TargetBlockSize <= 0 || (marker.TargetStorageID != "" && !restoreStorageIDMatchesKind(marker.TargetStorageID, marker.TargetStoreKind)) {
		return fmt.Errorf("%w: incomplete target storage geometry", ErrRestoreUnsafe)
	}
	switch marker.State {
	case RestoreStatePending:
	case RestoreStateApplying:
		if marker.TargetStorageID == "" || marker.Snapshot == nil {
			return fmt.Errorf("%w: invalid applying marker", ErrRestoreUnsafe)
		}
	case RestoreStateApplied, RestoreStateActivated, RestoreStateIntegrityFault:
		if marker.TargetStorageID == "" || marker.Snapshot == nil || validateRecord(*marker.Snapshot) != nil || marker.RestoredBlocks != marker.Snapshot.RecordCount || marker.RestoredBytes != marker.Snapshot.DataBytes {
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

func safeRestoreStorageKind(kind string) bool {
	return kind != "" && kind != "." && kind != ".." && restoreDiscardIdentityPattern.MatchString(kind)
}

func restoreStorageKindFromID(storeID string) (string, bool) {
	kind, _, found := strings.Cut(storeID, ":")
	if !found {
		kind = storeID
	}
	return kind, safeRestoreStorageKind(kind)
}

func restoreStorageIDMatchesKind(storeID, kind string) bool {
	actual, ok := restoreStorageKindFromID(storeID)
	return ok && actual == kind
}

func sameRestoreRecord(a, b Record) bool {
	return a.SnapshotID == b.SnapshotID && a.SourceVolumeID == b.SourceVolumeID && a.Frontier == b.Frontier && a.SizeBytes == b.SizeBytes && a.NumBlocks == b.NumBlocks && a.BlockSize == b.BlockSize && a.RecordCount == b.RecordCount && a.DataBytes == b.DataBytes && a.ArchiveBytes == b.ArchiveBytes && a.ArchiveSHA256 == b.ArchiveSHA256
}

func restoreApplyResult(marker RestoreMarker, already bool) RestoreApplyResult {
	return RestoreApplyResult{
		State:           marker.State,
		TargetStorageID: marker.TargetStorageID,
		TargetNumBlocks: marker.TargetNumBlocks,
		TargetBlockSize: marker.TargetBlockSize,
		RestoredBlocks:  marker.RestoredBlocks,
		RestoredBytes:   marker.RestoredBytes,
		TargetFrontier:  marker.TargetFrontier,
		AlreadyApplied:  already,
	}
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

func canonicalRestorePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("snapshot: resolve restore path: %w", err)
	}
	abs = filepath.Clean(abs)
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("snapshot: resolve restore path: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err == nil {
		return filepath.Join(parent, filepath.Base(abs)), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("snapshot: resolve restore parent: %w", err)
	}
	return abs, nil
}

func stageVerifiedArchive(ctx context.Context, r io.Reader, rec Record, dir string) (path string, retErr error) {
	tmp, err := os.CreateTemp(dir, ".tmp-restore-archive-*")
	if err != nil {
		return "", fmt.Errorf("snapshot: create restore staging file: %w", err)
	}
	path = tmp.Name()
	defer func() {
		if retErr != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return "", fmt.Errorf("snapshot: chmod restore staging file: %w", err)
	}
	written, err := io.Copy(tmp, io.LimitReader(contextReader{ctx: ctx, r: r}, rec.ArchiveBytes+1))
	if err != nil {
		return "", fmt.Errorf("snapshot: stage restore archive: %w", err)
	}
	if written != rec.ArchiveBytes {
		return "", fmt.Errorf("%w: streamed archive bytes got %d want %d", ErrArchiveCorrupt, written, rec.ArchiveBytes)
	}
	if err := tmp.Sync(); err != nil {
		return "", fmt.Errorf("snapshot: fsync restore staging file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("snapshot: close restore staging file: %w", err)
	}
	digest, err := digestFile(path)
	if err != nil {
		return "", err
	}
	if digest != rec.ArchiveSHA256 {
		return "", fmt.Errorf("%w: digest got %s want %s", ErrArchiveCorrupt, digest, rec.ArchiveSHA256)
	}
	verified, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("snapshot: open restore staging file: %w", err)
	}
	_, verifyErr := ApplyArchiveStream(ctx, verified, rec, nil)
	closeErr := verified.Close()
	if verifyErr != nil || closeErr != nil {
		return "", errors.Join(verifyErr, closeErr)
	}
	return path, nil
}
