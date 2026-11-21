package storage

import "github.com/seaweedfs/seaweed-block/core/iolatency"

// DurabilityLatencyStatus separates request queue residence from physical file
// sync attempts. A shared sync has one sample, not one sample per waiter.
type DurabilityLatencyStatus struct {
	WALAppend       iolatency.Distribution // WAL append latency
	GroupCommitWait iolatency.Distribution // time a sync request waited in the group commit queue
	FileSync        iolatency.Distribution // physical file syncs (one sample per sync, however many waiters)
}

func (s *WALStore) DurabilityLatency() DurabilityLatencyStatus {
	return DurabilityLatencyStatus{s.instr.appendLatency.Snapshot(), s.committer.waitLatency.Snapshot(), s.fileSyncLatency.Snapshot()}
}
