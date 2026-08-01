package csi

import (
	"context"
	"fmt"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/metadata"
)

type ControlSnapshotProvisioner struct {
	client control.SnapshotServiceClient
	token  string
}

func NewControlSnapshotProvisioner(client control.SnapshotServiceClient, token string) (*ControlSnapshotProvisioner, error) {
	if client == nil || token == "" {
		return nil, fmt.Errorf("csi: snapshot client and token are required")
	}
	return &ControlSnapshotProvisioner{client: client, token: token}, nil
}

func (p *ControlSnapshotProvisioner) CreateSnapshot(ctx context.Context, name, sourceVolumeID string) (SnapshotSpec, error) {
	record, err := p.client.CreateSnapshot(p.authorized(ctx), &control.CreateSnapshotRequest{Name: name, SourceVolumeId: sourceVolumeID})
	if err != nil {
		return SnapshotSpec{}, err
	}
	return snapshotSpecFromWire(record)
}

func (p *ControlSnapshotProvisioner) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	_, err := p.client.DeleteSnapshot(p.authorized(ctx), &control.DeleteSnapshotRequest{SnapshotId: snapshotID})
	return err
}

func (p *ControlSnapshotProvisioner) GetSnapshot(ctx context.Context, snapshotID string) (SnapshotSpec, error) {
	record, err := p.client.GetSnapshot(p.authorized(ctx), &control.GetSnapshotRequest{SnapshotId: snapshotID})
	if err != nil {
		return SnapshotSpec{}, err
	}
	spec, err := snapshotSpecFromWire(record)
	if err != nil {
		return SnapshotSpec{}, err
	}
	if spec.SnapshotID != snapshotID {
		return SnapshotSpec{}, fmt.Errorf("csi: get snapshot response identity mismatch")
	}
	return spec, nil
}

func (p *ControlSnapshotProvisioner) ListSnapshots(ctx context.Context, sourceVolumeID string) ([]SnapshotSpec, error) {
	response, err := p.client.ListSnapshots(p.authorized(ctx), &control.ListSnapshotsRequest{SourceVolumeId: sourceVolumeID})
	if err != nil {
		return nil, err
	}
	out := make([]SnapshotSpec, 0, len(response.GetSnapshots()))
	for _, record := range response.GetSnapshots() {
		item, err := snapshotSpecFromWire(record)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func (p *ControlSnapshotProvisioner) RestoreSnapshot(ctx context.Context, snapshotID, targetVolumeID string) error {
	response, err := p.client.RestoreSnapshot(p.authorized(ctx), &control.RestoreSnapshotRequest{SnapshotId: snapshotID, TargetVolumeId: targetVolumeID})
	if err != nil {
		return err
	}
	if response.GetSnapshotId() != snapshotID || response.GetTargetVolumeId() != targetVolumeID {
		return fmt.Errorf("csi: restore response identity mismatch")
	}
	return nil
}

func (p *ControlSnapshotProvisioner) authorized(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+p.token)
}

func snapshotSpecFromWire(record *control.SnapshotRecord) (SnapshotSpec, error) {
	if record == nil || record.GetSnapshotId() == "" || record.GetSourceVolumeId() == "" || record.GetSizeBytes() == 0 || record.GetCreatedAt() == nil {
		return SnapshotSpec{}, fmt.Errorf("csi: snapshot service returned an incomplete record")
	}
	if err := record.GetCreatedAt().CheckValid(); err != nil {
		return SnapshotSpec{}, fmt.Errorf("csi: snapshot service returned an invalid creation time: %w", err)
	}
	return SnapshotSpec{
		SnapshotID:     record.GetSnapshotId(),
		Name:           record.GetName(),
		SourceVolumeID: record.GetSourceVolumeId(),
		CreatedAt:      record.GetCreatedAt().AsTime(),
		State:          record.GetState(),
		SizeBytes:      record.GetSizeBytes(),
	}, nil
}

var _ SnapshotProvisioner = (*ControlSnapshotProvisioner)(nil)
