package snapshot

import (
	"context"
	"fmt"
	"sort"
)

type RestoreReplicaTarget struct {
	VolumeID        string
	ReplicaID       string
	RuntimeEndpoint string
	TargetStorageID string
	TargetNumBlocks uint32
	TargetBlockSize int
	RestoreState    string
}

type RestorePlan struct {
	Targets         []RestoreReplicaTarget
	AlreadyComplete bool
}

type RestoreTargetResolver interface {
	ResolveSnapshotRestoreTargets(ctx context.Context, targetVolumeID string, snapshot Record) (RestorePlan, error)
	CompleteSnapshotRestore(ctx context.Context, targetVolumeID, snapshotID string, expectedTargets []RestoreReplicaTarget) error
}

type RestoreRuntime interface {
	Apply(ctx context.Context, req RuntimeRestoreRequest, source ArchiveStreamer) (RestoreApplyResult, error)
	Activate(ctx context.Context, req RuntimeRestoreRequest) (RestoreMarker, error)
}

type RestoreOperationResult struct {
	SnapshotID      string
	TargetVolumeID  string
	ReplicaCount    int
	AlreadyComplete bool
}

func (c *Coordinator) ConfigureRestore(resolver RestoreTargetResolver, runtime RestoreRuntime) error {
	if resolver == nil || runtime == nil {
		return fmt.Errorf("snapshot: restore coordinator requires resolver and runtime")
	}
	c.restoreResolver = resolver
	c.restoreRuntime = runtime
	return nil
}

func (c *Coordinator) Restore(ctx context.Context, snapshotID, targetVolumeID string) (RestoreOperationResult, error) {
	if c == nil || c.restoreResolver == nil || c.restoreRuntime == nil {
		return RestoreOperationResult{}, fmt.Errorf("%w: restore coordinator is not configured", ErrRestoreNotReady)
	}
	if snapshotID == "" || targetVolumeID == "" {
		return RestoreOperationResult{}, fmt.Errorf("%w: snapshot and target volume are required", ErrInvalidRequest)
	}
	rec, release, err := c.manager.beginRead(snapshotID)
	if err != nil {
		return RestoreOperationResult{}, err
	}
	defer release()
	if targetVolumeID == rec.SourceVolumeID {
		return RestoreOperationResult{}, fmt.Errorf("%w: restore target must be a new volume", ErrInvalidRequest)
	}
	plan, err := c.restoreResolver.ResolveSnapshotRestoreTargets(ctx, targetVolumeID, rec)
	if err != nil {
		return RestoreOperationResult{}, fmt.Errorf("%w: %v", ErrRestoreNotReady, err)
	}
	if plan.AlreadyComplete {
		return RestoreOperationResult{SnapshotID: snapshotID, TargetVolumeID: targetVolumeID, AlreadyComplete: true}, nil
	}
	targets, err := validateRestoreTargets(targetVolumeID, plan.Targets)
	if err != nil {
		return RestoreOperationResult{}, err
	}
	applyResults := make(map[string]RestoreApplyResult, len(targets))
	for _, target := range targets {
		result, err := c.restoreRuntime.Apply(ctx, runtimeRestoreRequest(rec, target), c.manager)
		if err != nil {
			return RestoreOperationResult{}, fmt.Errorf("snapshot: apply replica %s: %w", target.ReplicaID, err)
		}
		if !validRestoreApplyEvidence(result, target, rec) {
			return RestoreOperationResult{}, fmt.Errorf("%w: replica %s returned invalid apply evidence", ErrRestoreUnsafe, target.ReplicaID)
		}
		applyResults[target.ReplicaID] = result
	}
	refreshed, err := c.restoreResolver.ResolveSnapshotRestoreTargets(ctx, targetVolumeID, rec)
	if err != nil {
		return RestoreOperationResult{}, fmt.Errorf("%w: target placement changed after apply", ErrRestoreNotReady)
	}
	if refreshed.AlreadyComplete {
		return RestoreOperationResult{SnapshotID: snapshotID, TargetVolumeID: targetVolumeID, ReplicaCount: len(targets), AlreadyComplete: true}, nil
	}
	refreshedTargets, err := validateRestoreTargets(targetVolumeID, refreshed.Targets)
	if err != nil || !sameRestoreTargets(targets, refreshedTargets) {
		return RestoreOperationResult{}, fmt.Errorf("%w: target placement changed after apply", ErrRestoreNotReady)
	}
	for _, target := range targets {
		marker, err := c.restoreRuntime.Activate(ctx, runtimeRestoreRequest(rec, target))
		if err != nil {
			return RestoreOperationResult{}, fmt.Errorf("snapshot: activate replica %s: %w", target.ReplicaID, err)
		}
		if !validRestoreActivationEvidence(marker, applyResults[target.ReplicaID], target, rec) {
			return RestoreOperationResult{}, fmt.Errorf("%w: replica %s returned invalid activation evidence", ErrRestoreUnsafe, target.ReplicaID)
		}
	}
	if err := c.restoreResolver.CompleteSnapshotRestore(ctx, targetVolumeID, snapshotID, targets); err != nil {
		return RestoreOperationResult{}, fmt.Errorf("snapshot: complete restore authority gate: %w", err)
	}
	return RestoreOperationResult{SnapshotID: snapshotID, TargetVolumeID: targetVolumeID, ReplicaCount: len(targets)}, nil
}

func validateRestoreTargets(targetVolumeID string, targets []RestoreReplicaTarget) ([]RestoreReplicaTarget, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: no restore replicas", ErrRestoreNotReady)
	}
	out := append([]RestoreReplicaTarget(nil), targets...)
	sort.Slice(out, func(i, j int) bool { return out[i].ReplicaID < out[j].ReplicaID })
	for i, target := range out {
		if target.VolumeID != targetVolumeID || target.ReplicaID == "" || ValidateRuntimeEndpoint(target.RuntimeEndpoint) != nil || target.TargetStorageID == "" || target.TargetNumBlocks == 0 || target.TargetBlockSize <= 0 || !validRestoreTargetState(target.RestoreState) {
			return nil, fmt.Errorf("%w: invalid restore replica target", ErrRestoreNotReady)
		}
		if i > 0 && target.ReplicaID == out[i-1].ReplicaID {
			return nil, fmt.Errorf("%w: duplicate restore replica %s", ErrRestoreNotReady, target.ReplicaID)
		}
	}
	return out, nil
}

func validRestoreApplyEvidence(result RestoreApplyResult, target RestoreReplicaTarget, rec Record) bool {
	return (result.State == RestoreStateApplied || result.State == RestoreStateActivated) &&
		result.TargetStorageID == target.TargetStorageID &&
		result.TargetNumBlocks == target.TargetNumBlocks &&
		result.TargetBlockSize == target.TargetBlockSize &&
		result.TargetNumBlocks == rec.NumBlocks &&
		result.TargetBlockSize == rec.BlockSize &&
		result.RestoredBlocks == rec.RecordCount &&
		result.RestoredBytes == rec.DataBytes
}

func validRestoreActivationEvidence(marker RestoreMarker, applied RestoreApplyResult, target RestoreReplicaTarget, rec Record) bool {
	return marker.State == RestoreStateActivated &&
		marker.SnapshotID == rec.SnapshotID &&
		marker.TargetVolumeID == target.VolumeID &&
		marker.TargetReplicaID == target.ReplicaID &&
		marker.TargetStorageID == applied.TargetStorageID &&
		marker.TargetNumBlocks == applied.TargetNumBlocks &&
		marker.TargetBlockSize == applied.TargetBlockSize &&
		marker.RestoredBlocks == applied.RestoredBlocks &&
		marker.RestoredBytes == applied.RestoredBytes &&
		marker.TargetFrontier == applied.TargetFrontier &&
		marker.Snapshot != nil && sameRestoreRecord(*marker.Snapshot, rec)
}

func sameRestoreTargets(a, b []RestoreReplicaTarget) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameRestoreTargetIdentity(a[i], b[i]) {
			return false
		}
	}
	return true
}

func sameRestoreTargetIdentity(a, b RestoreReplicaTarget) bool {
	return a.VolumeID == b.VolumeID &&
		a.ReplicaID == b.ReplicaID &&
		a.RuntimeEndpoint == b.RuntimeEndpoint &&
		a.TargetStorageID == b.TargetStorageID &&
		a.TargetNumBlocks == b.TargetNumBlocks &&
		a.TargetBlockSize == b.TargetBlockSize
}

func validRestoreTargetState(state string) bool {
	switch state {
	case RestoreStatePending, RestoreStateApplying, RestoreStateApplied, RestoreStateActivated:
		return true
	default:
		return false
	}
}

func runtimeRestoreRequest(rec Record, target RestoreReplicaTarget) RuntimeRestoreRequest {
	return RuntimeRestoreRequest{
		Endpoint:        target.RuntimeEndpoint,
		Snapshot:        rec,
		TargetVolumeID:  target.VolumeID,
		TargetReplicaID: target.ReplicaID,
		TargetStorageID: target.TargetStorageID,
		TargetNumBlocks: target.TargetNumBlocks,
		TargetBlockSize: target.TargetBlockSize,
	}
}
