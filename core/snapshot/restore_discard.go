package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/parallelwal"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

var restoreDiscardIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

const (
	restoreDiscardReceiptVersion = 1
	restoreDiscardDataRemoved    = "data_removed"
	restoreDiscardComplete       = "discarded"
)

type restoreDiscardReceipt struct {
	Version         int    `json:"version"`
	State           string `json:"state"`
	OperationID     string `json:"operation_id"`
	SnapshotID      string `json:"snapshot_id"`
	TargetVolumeID  string `json:"target_volume_id"`
	TargetReplicaID string `json:"target_replica_id"`
}

// RestoreDiscardRequest identifies one offline restore target. The caller must
// stop the owning workload and fence authority before invoking this primitive.
type RestoreDiscardRequest struct {
	RootPath        string
	OperationID     string
	SnapshotID      string
	TargetVolumeID  string
	TargetReplicaID string
	AllowActivated  bool
}

// RestoreDiscardResult is terminal local evidence: both paths were confirmed
// absent before the call returned successfully.
type RestoreDiscardResult struct {
	OperationID      string `json:"operation_id"`
	SnapshotID       string `json:"snapshot_id"`
	TargetVolumeID   string `json:"target_volume_id"`
	TargetReplicaID  string `json:"target_replica_id"`
	MarkerRemoved    bool   `json:"marker_removed"`
	DataRemoved      bool   `json:"data_removed"`
	AlreadyDiscarded bool   `json:"already_discarded"`
}

// DiscardRestoreTarget removes one verified restore target while it is
// offline. Data is removed before the marker so a crash remains retryable;
// data without a marker is never deleted because its identity is ambiguous.
func DiscardRestoreTarget(req RestoreDiscardRequest) (RestoreDiscardResult, error) {
	if req.RootPath == "" || !safeRestoreDiscardIdentity(req.OperationID) || !safeRestoreDiscardIdentity(req.SnapshotID) || !safeRestoreDiscardIdentity(req.TargetVolumeID) || !safeRestoreDiscardIdentity(req.TargetReplicaID) {
		return RestoreDiscardResult{}, fmt.Errorf("%w: discard root and safe operation, snapshot, volume, and replica identities are required", ErrInvalidRequest)
	}
	root, err := filepath.Abs(req.RootPath)
	if err != nil {
		return RestoreDiscardResult{}, fmt.Errorf("snapshot: resolve discard root: %w", err)
	}
	root = filepath.Clean(root)
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return RestoreDiscardResult{}, fmt.Errorf("snapshot: inspect discard root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return RestoreDiscardResult{}, fmt.Errorf("%w: discard root must be a real directory", ErrRestoreUnsafe)
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil || filepath.Clean(resolvedRoot) != root {
		return RestoreDiscardResult{}, fmt.Errorf("%w: discard root has an unsafe path", ErrRestoreUnsafe)
	}

	markerPath := filepath.Join(root, req.TargetVolumeID+".restore.json")
	dataPath := filepath.Join(root, req.TargetVolumeID+".bin")
	receiptPath := filepath.Join(root, req.TargetVolumeID+".restore-discard.json")
	markerExists, err := regularRestoreDiscardPath(markerPath)
	if err != nil {
		return RestoreDiscardResult{}, err
	}
	dataExists, err := regularRestoreDiscardPath(dataPath)
	if err != nil {
		return RestoreDiscardResult{}, err
	}
	result := RestoreDiscardResult{
		OperationID:     req.OperationID,
		SnapshotID:      req.SnapshotID,
		TargetVolumeID:  req.TargetVolumeID,
		TargetReplicaID: req.TargetReplicaID,
	}
	receiptExists, err := regularRestoreDiscardPath(receiptPath)
	if err != nil {
		return RestoreDiscardResult{}, err
	}
	if !markerExists {
		if dataExists {
			return RestoreDiscardResult{}, fmt.Errorf("%w: target data exists without restore marker", ErrRestoreUnsafe)
		}
		if !receiptExists {
			return RestoreDiscardResult{}, fmt.Errorf("%w: restore target is absent without a durable discard receipt", ErrRestoreUnsafe)
		}
		receipt, err := loadRestoreDiscardReceipt(receiptPath)
		if err != nil || !sameRestoreDiscardReceiptIdentity(receipt, req) || (receipt.State != restoreDiscardDataRemoved && receipt.State != restoreDiscardComplete) {
			return RestoreDiscardResult{}, fmt.Errorf("%w: discard receipt does not match the requested target", ErrRestoreUnsafe)
		}
		if receipt.State != restoreDiscardComplete {
			receipt.State = restoreDiscardComplete
			if err := persistRestoreDiscardReceipt(receiptPath, receipt); err != nil {
				return RestoreDiscardResult{}, err
			}
		} else if err := syncDir(root); err != nil {
			return RestoreDiscardResult{}, err
		}
		result.MarkerRemoved = true
		result.DataRemoved = true
		result.AlreadyDiscarded = true
		return result, nil
	}

	marker, _, err := LoadRestoreMarker(markerPath)
	if err != nil {
		return RestoreDiscardResult{}, err
	}
	if err := validateRestoreMarker(marker, RestoreTargetConfig{
		MarkerPath:      markerPath,
		TargetDataPath:  dataPath,
		SnapshotID:      req.SnapshotID,
		TargetVolumeID:  req.TargetVolumeID,
		TargetReplicaID: req.TargetReplicaID,
	}); err != nil {
		return RestoreDiscardResult{}, err
	}
	if marker.State == RestoreStateActivated && !req.AllowActivated {
		return RestoreDiscardResult{}, fmt.Errorf("%w: activated restore target requires an explicit offline override", ErrRestoreUnsafe)
	}
	if dataExists {
		if marker.TargetStoreKind == "" {
			return RestoreDiscardResult{}, fmt.Errorf("%w: restore marker does not identify the target data store intent", ErrRestoreUnsafe)
		}
		identity, numBlocks, blockSize, err := inspectRestoreDiscardStorage(dataPath, marker.TargetStoreKind)
		if err != nil || (marker.TargetStorageID != "" && identity.StoreID != marker.TargetStorageID) || numBlocks != marker.TargetNumBlocks || blockSize != marker.TargetBlockSize {
			return RestoreDiscardResult{}, fmt.Errorf("%w: restore target storage identity or geometry mismatch", ErrRestoreUnsafe)
		}
		if err := os.Remove(dataPath); err != nil {
			return RestoreDiscardResult{}, fmt.Errorf("snapshot: remove restore target data: %w", err)
		}
		if err := syncDir(root); err != nil {
			return RestoreDiscardResult{}, err
		}
	}
	result.DataRemoved = true
	receipt := restoreDiscardReceipt{
		Version: restoreDiscardReceiptVersion, State: restoreDiscardDataRemoved,
		OperationID: req.OperationID, SnapshotID: req.SnapshotID, TargetVolumeID: req.TargetVolumeID, TargetReplicaID: req.TargetReplicaID,
	}
	if err := persistRestoreDiscardReceipt(receiptPath, receipt); err != nil {
		return RestoreDiscardResult{}, err
	}
	if err := os.Remove(markerPath); err != nil {
		return RestoreDiscardResult{}, fmt.Errorf("snapshot: remove restore target marker: %w", err)
	}
	if err := syncDir(root); err != nil {
		return RestoreDiscardResult{}, err
	}
	result.MarkerRemoved = true
	receipt.State = restoreDiscardComplete
	if err := persistRestoreDiscardReceipt(receiptPath, receipt); err != nil {
		return RestoreDiscardResult{}, err
	}
	return result, nil
}

func inspectRestoreDiscardStorage(path, storeKind string) (storage.DurableStorageIdentity, uint32, int, error) {
	switch storeKind {
	case "walstore":
		return storage.InspectWALStoreIdentity(path)
	case "smartwal":
		layout, err := smartwal.InspectLayout(path)
		return storage.DurableStorageIdentity{Path: path, StoreID: layout.StoreID}, layout.NumBlocks, int(layout.BlockSize), err
	case "parallelwal":
		return parallelwal.InspectStoreIdentity(path)
	default:
		return storage.DurableStorageIdentity{}, 0, 0, fmt.Errorf("snapshot: unknown restore target store identity")
	}
}

func loadRestoreDiscardReceipt(path string) (restoreDiscardReceipt, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return restoreDiscardReceipt{}, fmt.Errorf("snapshot: read discard receipt: %w", err)
	}
	var receipt restoreDiscardReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil || receipt.Version != restoreDiscardReceiptVersion {
		return restoreDiscardReceipt{}, fmt.Errorf("%w: invalid discard receipt", ErrRestoreUnsafe)
	}
	return receipt, nil
}

func persistRestoreDiscardReceipt(path string, receipt restoreDiscardReceipt) error {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("snapshot: marshal discard receipt: %w", err)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-restore-discard-*")
	if err != nil {
		return fmt.Errorf("snapshot: create discard receipt: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("snapshot: chmod discard receipt: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("snapshot: write discard receipt: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("snapshot: fsync discard receipt: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("snapshot: close discard receipt: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("snapshot: publish discard receipt: %w", err)
	}
	return syncDir(dir)
}

func sameRestoreDiscardReceiptIdentity(receipt restoreDiscardReceipt, req RestoreDiscardRequest) bool {
	return receipt.OperationID == req.OperationID && receipt.SnapshotID == req.SnapshotID && receipt.TargetVolumeID == req.TargetVolumeID && receipt.TargetReplicaID == req.TargetReplicaID
}

func regularRestoreDiscardPath(path string) (bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("snapshot: inspect restore target path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("%w: restore target path must be a regular file", ErrRestoreUnsafe)
	}
	return true, nil
}

func safeRestoreDiscardIdentity(value string) bool {
	return value != "" && value != "." && value != ".." && restoreDiscardIdentityPattern.MatchString(value)
}
