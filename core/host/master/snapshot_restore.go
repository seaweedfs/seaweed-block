package master

import (
	"context"
	"fmt"
	"sort"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
)

func (h *Host) ResolveSnapshotRestoreTargets(ctx context.Context, targetVolumeID string, rec snapshot.Record) (snapshot.RestorePlan, error) {
	if h == nil {
		return snapshot.RestorePlan{}, snapshot.ErrRestoreNotReady
	}
	h.lifecycleProductMu.Lock()
	defer h.lifecycleProductMu.Unlock()
	return h.resolveSnapshotRestoreTargets(ctx, targetVolumeID, rec)
}

func (h *Host) resolveSnapshotRestoreTargets(ctx context.Context, targetVolumeID string, rec snapshot.Record) (snapshot.RestorePlan, error) {
	if err := ctx.Err(); err != nil {
		return snapshot.RestorePlan{}, err
	}
	if h == nil || h.lifecycle == nil || h.lifecycle.Volumes == nil || h.lifecycle.Placements == nil || h.obs == nil || h.Publisher() == nil {
		return snapshot.RestorePlan{}, snapshot.ErrRestoreNotReady
	}
	volume, ok := h.lifecycle.Volumes.GetVolume(targetVolumeID)
	if !ok || volume.Spec.SourceSnapshotID != rec.SnapshotID || volume.Spec.SizeBytes != rec.SizeBytes {
		return snapshot.RestorePlan{}, fmt.Errorf("target lifecycle intent does not match snapshot")
	}
	if volume.RestoreState == lifecycle.VolumeRestoreComplete {
		return snapshot.RestorePlan{AlreadyComplete: true}, nil
	}
	if volume.RestoreState != lifecycle.VolumeRestorePending {
		return snapshot.RestorePlan{}, fmt.Errorf("target restore state is %q", volume.RestoreState)
	}
	if line, ok := h.Publisher().VolumeAuthorityLine(targetVolumeID); ok && line.Assigned {
		return snapshot.RestorePlan{}, fmt.Errorf("%w: target already has authority", snapshot.ErrRestoreUnsafe)
	}
	placement, ok := h.lifecycle.Placements.GetPlacement(targetVolumeID)
	if !ok || placement.DesiredRF != volume.Spec.ReplicationFactor || len(placement.Slots) != volume.Spec.ReplicationFactor {
		return snapshot.RestorePlan{}, fmt.Errorf("target placement is incomplete")
	}
	plan := snapshot.RestorePlan{Targets: make([]snapshot.RestoreReplicaTarget, 0, len(placement.Slots))}
	for _, expected := range placement.Slots {
		if expected.Source != lifecycle.PlacementSourceExistingReplica || expected.ReplicaID == "" || expected.ServerID == "" {
			return snapshot.RestorePlan{}, fmt.Errorf("target placement has an unmaterialized replica")
		}
		slot, ok := h.obs.Store().SlotFact(targetVolumeID, expected.ReplicaID)
		target, ok := snapshotRestoreTargetFromFacts(targetVolumeID, expected.ServerID, expected.ReplicaID, rec.SnapshotID, ok, slot)
		if !ok {
			return snapshot.RestorePlan{}, fmt.Errorf("target replica %s has no fresh matching restore runtime", expected.ReplicaID)
		}
		plan.Targets = append(plan.Targets, target)
	}
	return plan, nil
}

func (h *Host) CompleteSnapshotRestore(ctx context.Context, targetVolumeID, snapshotID string, expectedTargets []snapshot.RestoreReplicaTarget) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if h == nil || h.lifecycle == nil || h.lifecycle.Volumes == nil {
		return snapshot.ErrRestoreNotReady
	}
	h.lifecycleProductMu.Lock()
	defer h.lifecycleProductMu.Unlock()
	volume, ok := h.lifecycle.Volumes.GetVolume(targetVolumeID)
	if !ok {
		return snapshot.ErrRestoreNotReady
	}
	plan, err := h.resolveSnapshotRestoreTargets(ctx, targetVolumeID, snapshot.Record{SnapshotID: snapshotID, SizeBytes: volume.Spec.SizeBytes})
	if err != nil {
		return fmt.Errorf("%w: target placement changed before authority gate", snapshot.ErrRestoreNotReady)
	}
	if plan.AlreadyComplete {
		return nil
	}
	if !sameSnapshotRestoreTargets(plan.Targets, expectedTargets) {
		return fmt.Errorf("%w: target placement changed before authority gate", snapshot.ErrRestoreNotReady)
	}
	_, err = h.lifecycle.Volumes.MarkRestoreComplete(targetVolumeID, snapshotID)
	return err
}

func sameSnapshotRestoreTargets(a, b []snapshot.RestoreReplicaTarget) bool {
	if len(a) != len(b) {
		return false
	}
	left := append([]snapshot.RestoreReplicaTarget(nil), a...)
	right := append([]snapshot.RestoreReplicaTarget(nil), b...)
	sort.Slice(left, func(i, j int) bool { return left[i].ReplicaID < left[j].ReplicaID })
	sort.Slice(right, func(i, j int) bool { return right[i].ReplicaID < right[j].ReplicaID })
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func snapshotRestoreTargetFromFacts(volumeID, expectedServerID, expectedReplicaID, snapshotID string, hasSlot bool, slot authority.SlotFact) (snapshot.RestoreReplicaTarget, bool) {
	if volumeID == "" || expectedServerID == "" || expectedReplicaID == "" || snapshotID == "" || !hasSlot {
		return snapshot.RestoreReplicaTarget{}, false
	}
	if slot.VolumeID != volumeID || slot.ReplicaID != expectedReplicaID || slot.ReportingServerID != expectedServerID || !slot.Reachable || slot.Withdrawn || slot.DataAddr == "" || slot.SnapshotRuntimeEndpoint == "" {
		return snapshot.RestoreReplicaTarget{}, false
	}
	if snapshot.ValidateRuntimeEndpoint(slot.SnapshotRuntimeEndpoint) != nil || !snapshotEndpointMatchesDataHost(slot.SnapshotRuntimeEndpoint, slot.DataAddr) {
		return snapshot.RestoreReplicaTarget{}, false
	}
	restore := slot.SnapshotRestore
	if restore.SnapshotID != snapshotID || restore.StorageID == "" || restore.NumBlocks == 0 || restore.BlockSize == 0 || !validSnapshotRestoreObservationState(restore.State) {
		return snapshot.RestoreReplicaTarget{}, false
	}
	return snapshot.RestoreReplicaTarget{
		VolumeID:        volumeID,
		ReplicaID:       expectedReplicaID,
		RuntimeEndpoint: slot.SnapshotRuntimeEndpoint,
		TargetStorageID: restore.StorageID,
		TargetNumBlocks: restore.NumBlocks,
		TargetBlockSize: int(restore.BlockSize),
	}, true
}

func validSnapshotRestoreObservationState(state string) bool {
	switch state {
	case snapshot.RestoreStatePending, snapshot.RestoreStateApplying, snapshot.RestoreStateApplied, snapshot.RestoreStateActivated:
		return true
	default:
		return false
	}
}

var _ snapshot.RestoreTargetResolver = (*Host)(nil)
