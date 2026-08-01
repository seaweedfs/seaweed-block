package csi

import (
	"context"
	"time"
)

const SnapshotStateReady = "ready"

type SnapshotSpec struct {
	SnapshotID     string
	Name           string
	SourceVolumeID string
	CreatedAt      time.Time
	State          string
	SizeBytes      uint64
}

// SnapshotProvisioner is the CSI-facing client for the dedicated,
// authenticated blockmaster snapshot service.
type SnapshotProvisioner interface {
	CreateSnapshot(ctx context.Context, name, sourceVolumeID string) (SnapshotSpec, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	GetSnapshot(ctx context.Context, snapshotID string) (SnapshotSpec, error)
	ListSnapshots(ctx context.Context, sourceVolumeID string) ([]SnapshotSpec, error)
	RestoreSnapshot(ctx context.Context, snapshotID, targetVolumeID string) error
}
