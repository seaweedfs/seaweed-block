package durable

import (
	"sync/atomic"
	"time"
)

// WriteProfileStatus is a read-only diagnostic snapshot for write-path gates.
// It is not a performance API; callers use it to prove which product-owned
// path observed the write and roughly where time accumulated.
type WriteProfileStatus struct {
	TargetWriteOps              uint64
	TargetWriteBytes            uint64
	TargetWriteDurationNanos    uint64
	BackendWriteOps             uint64
	BackendWriteBytes           uint64
	BackendWriteDurationNanos   uint64
	BackendStorageWriteCalls    uint64
	BackendStorageWriteBlocks   uint64
	BackendStorageBatchCalls    uint64
	BackendStorageBatchBlocks   uint64
	WALCopyOps                  uint64
	WALCopyBytes                uint64
	WALCopyDurationNanos        uint64
	WALEncodeOps                uint64
	WALEncodeBytes              uint64
	WALEncodeDurationNanos      uint64
	WALChecksumOps              uint64
	WALChecksumBytes            uint64
	WALChecksumDurationNanos    uint64
	WALAppendOps                uint64
	WALAppendBytes              uint64
	WALAppendDurationNanos      uint64
	DirtyMapUpdateOps           uint64
	DirtyMapUpdateDurationNanos uint64
	BackendSyncOps              uint64
	BackendSyncDurationNanos    uint64
}

type writeProfile struct {
	targetWriteOps            atomic.Uint64
	targetWriteBytes          atomic.Uint64
	targetWriteDurationNanos  atomic.Uint64
	backendWriteOps           atomic.Uint64
	backendWriteBytes         atomic.Uint64
	backendWriteDurationNanos atomic.Uint64
	backendStorageWriteCalls  atomic.Uint64
	backendStorageWriteBlocks atomic.Uint64
	backendStorageBatchCalls  atomic.Uint64
	backendStorageBatchBlocks atomic.Uint64
	backendSyncOps            atomic.Uint64
	backendSyncDurationNanos  atomic.Uint64
}

func (p *writeProfile) recordTargetWrite(bytes int, d time.Duration) {
	if bytes <= 0 {
		return
	}
	p.targetWriteOps.Add(1)
	p.targetWriteBytes.Add(uint64(bytes))
	p.targetWriteDurationNanos.Add(durationNanos(d))
}

func (p *writeProfile) recordBackendWrite(bytes int, d time.Duration) {
	if bytes <= 0 {
		return
	}
	p.backendWriteOps.Add(1)
	p.backendWriteBytes.Add(uint64(bytes))
	p.backendWriteDurationNanos.Add(durationNanos(d))
}

func (p *writeProfile) recordBackendStorageWrite(blocks int, batched bool) {
	if blocks <= 0 {
		return
	}
	p.backendStorageWriteCalls.Add(1)
	p.backendStorageWriteBlocks.Add(uint64(blocks))
	if batched {
		p.backendStorageBatchCalls.Add(1)
		p.backendStorageBatchBlocks.Add(uint64(blocks))
	}
}

func (p *writeProfile) recordBackendSync(d time.Duration) {
	p.backendSyncOps.Add(1)
	p.backendSyncDurationNanos.Add(durationNanos(d))
}

func durationNanos(d time.Duration) uint64 {
	if d <= 0 {
		return 1
	}
	return uint64(d.Nanoseconds())
}

func (p *writeProfile) snapshot() WriteProfileStatus {
	if p == nil {
		return WriteProfileStatus{}
	}
	return WriteProfileStatus{
		TargetWriteOps:            p.targetWriteOps.Load(),
		TargetWriteBytes:          p.targetWriteBytes.Load(),
		TargetWriteDurationNanos:  p.targetWriteDurationNanos.Load(),
		BackendWriteOps:           p.backendWriteOps.Load(),
		BackendWriteBytes:         p.backendWriteBytes.Load(),
		BackendWriteDurationNanos: p.backendWriteDurationNanos.Load(),
		BackendStorageWriteCalls:  p.backendStorageWriteCalls.Load(),
		BackendStorageWriteBlocks: p.backendStorageWriteBlocks.Load(),
		BackendStorageBatchCalls:  p.backendStorageBatchCalls.Load(),
		BackendStorageBatchBlocks: p.backendStorageBatchBlocks.Load(),
		BackendSyncOps:            p.backendSyncOps.Load(),
		BackendSyncDurationNanos:  p.backendSyncDurationNanos.Load(),
	}
}
