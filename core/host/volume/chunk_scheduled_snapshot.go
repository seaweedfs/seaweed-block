package volume

import (
	"context"
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
// it never follows an ordinary cut with a second catalog write.
func ScheduledSnapshot(snapshot func(context.Context, string, string, layout.SnapshotMeta) (chunkstore.SnapshotInfo, error), keep int) func(context.Context, string, time.Time, func(context.Context) (storage.ChunkHead, error)) error {
	return func(ctx context.Context, volumeID string, at time.Time, _ func(context.Context) (storage.ChunkHead, error)) error {
		meta := layout.SnapshotMeta{CreatedNs: at.UnixNano(), Class: layout.ClassScheduled, Labels: map[string]string{retain.LabelScheduleKeep: strconv.Itoa(keep)}}
		if _, err := snapshot(ctx, volumeID, ScheduledSnapshotName(at), meta); err != nil {
			_, code, _ := chunkErrorCode(err)
			return &durable.SnapshotFailure{Code: code, Err: err}
		}
		return nil
	}
}
