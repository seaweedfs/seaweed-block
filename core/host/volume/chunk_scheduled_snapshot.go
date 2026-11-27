package volume

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/retain"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ScheduledSnapshotName binds retries of one scheduled attempt to one name.
func ScheduledSnapshotName(at time.Time) string {
	return "sched-" + at.UTC().Format("20060102T150405Z")
}

// ScheduledSnapshot uses the same snapshot commit as an explicit snapshot;
// it never follows an ordinary cut with a second catalog write. A volume
// already at the snapshot limit refuses the name before any cut; the
// scheduled pass then still publishes the image by an ordinary cut, so
// the schedule keeps HEAD current, and records the refusal (status
// lastFailure snapshot_limit, then snapshot_overdue).
func ScheduledSnapshot(snapshot func(context.Context, string, string, layout.SnapshotMeta) (chunkstore.SnapshotInfo, error), keep int) func(context.Context, string, time.Time, func(context.Context) (storage.ChunkHead, error)) error {
	return func(ctx context.Context, volumeID string, at time.Time, cut func(context.Context) (storage.ChunkHead, error)) error {
		meta := layout.SnapshotMeta{CreatedNs: at.UnixNano(), Class: layout.ClassScheduled, Labels: map[string]string{retain.LabelScheduleKeep: strconv.Itoa(keep)}}
		if _, err := snapshot(ctx, volumeID, ScheduledSnapshotName(at), meta); err != nil {
			_, code, _ := chunkErrorCode(err)
			if errors.Is(err, layout.ErrTooManySnapshots) && cut != nil {
				if _, cutErr := cut(ctx); cutErr != nil {
					err = errors.Join(err, cutErr)
				}
			}
			return &durable.SnapshotFailure{Code: code, Err: err}
		}
		return nil
	}
}
