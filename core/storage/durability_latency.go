package storage

import "github.com/seaweedfs/seaweed-block/core/iolatency"

// DurabilityLatencyStatus separates request queue residence from physical file
// sync attempts. A shared sync has one sample, not one sample per waiter.
type DurabilityLatencyStatus struct {
	WALAppend       iolatency.Distribution
	GroupCommitWait iolatency.Distribution
	FileSync        iolatency.Distribution
}

func (s *WALStore) DurabilityLatency() DurabilityLatencyStatus {
	return DurabilityLatencyStatus{s.instr.appendLatency.Snapshot(), s.committer.waitLatency.Snapshot(), s.fileSyncLatency.Snapshot()}
}
