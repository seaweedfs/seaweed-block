package volume

import (
	"context"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ScheduledNewest returns the provider's newest-scheduled-snapshot
// callback (durable.ProviderConfig.ChunkScheduledNewest) over cat: the slot
// of the volume's newest published scheduled snapshot. A failure carries
// the code the control endpoint would report.
func ScheduledNewest(cat *chunkstore.Catalog) func(context.Context, string) (time.Time, error) {
	return func(ctx context.Context, volumeID string) (time.Time, error) {
		t, err := cat.NewestScheduledSlot(ctx, volumeID)
		if err != nil {
			_, code, _ := chunkErrorCode(err)
			return time.Time{}, &durable.SnapshotFailure{Code: code, Err: err}
		}
		return t, nil
	}
}

// ScheduledSnapshot returns the provider's scheduled-snapshot callback
// (durable.ProviderConfig.ChunkScheduledSnapshot) over cat. Each attempt
// is one named snapshot of class "scheduled" for the slot starting at at,
// named and requested by that slot (chunkstore.ScheduledSnapshotName), and
// labelled with keep (the count retention keeps), sealed through the same
// path as an explicit named snapshot: the request is resolved first, then
// one cut, then the record of that cut's HEAD. A failure carries the code
// the control endpoint would report.
func ScheduledSnapshot(cat *chunkstore.Catalog, keep int) func(context.Context, string, time.Time, func(context.Context) (storage.ChunkHead, error)) error {
	labels := map[string]string{chunkstore.LabelScheduleKeep: strconv.Itoa(keep)}
	return func(ctx context.Context, volumeID string, at time.Time, cut func(context.Context) (storage.ChunkHead, error)) error {
		name := chunkstore.ScheduledSnapshotName(at)
		sreq := chunkstore.SnapshotRequest{Name: name, RequestID: name, Labels: labels,
			CreatedBy: "blockvolume-schedule", Class: chunkstore.SnapshotClassScheduled}
		if _, err := sealSnapshot(ctx, cat, volumeID, sreq, cut); err != nil {
			_, code, _ := chunkErrorCode(err)
			return &durable.SnapshotFailure{Code: code, Err: err}
		}
		return nil
	}
}
