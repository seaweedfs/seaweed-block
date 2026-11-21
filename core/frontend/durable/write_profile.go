package durable

import (
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/iolatency"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// WriteProfileStatus is a read-only diagnostic snapshot for write-path gates.
// It is not a performance API; callers use it to prove which product-owned
// path observed the write and roughly where time accumulated.
type WriteProfileStatus struct {
	DurabilityLatency           DurabilityLatencyStatus // latency distributions of writes, flushes and syncs
	TargetWriteOps              uint64                  // writes the protocol target handed to the volume
	TargetWriteBytes            uint64                  // their bytes
	TargetWriteRequestMaxBytes  uint64                  // the largest of them
	TargetWriteDurationNanos    uint64                  // their total time at the target
	BackendWriteRequestOps      uint64                  // write requests that entered the durable backend
	BackendWriteRequestBytes    uint64                  // their bytes
	BackendWriteRequestMaxBytes uint64                  // the largest of them
	BackendWriteOps             uint64                  // backend writes completed
	BackendWriteBytes           uint64                  // their bytes
	BackendWriteDurationNanos   uint64                  // their total time
	BackendStorageWriteCalls    uint64                  // single-write calls into the store
	BackendStorageWriteBlocks   uint64                  // blocks they wrote
	BackendStorageBatchCalls    uint64                  // batch-write calls into the store
	BackendStorageBatchBlocks   uint64                  // blocks they wrote
	BackendFullBlockBatchCalls  uint64                  // batch calls made only of whole blocks
	BackendFullBlockBatchBlocks uint64                  // blocks they wrote
	BackendFullBlockBatchMax    uint64                  // the largest such batch, in blocks
	WALCopyOps                  uint64                  // WAL record payload copies
	WALCopyBytes                uint64                  // their bytes
	WALCopyDurationNanos        uint64                  // their total time
	WALEncodeOps                uint64                  // WAL record encodes
	WALEncodeBytes              uint64                  // their bytes
	WALEncodeDurationNanos      uint64                  // their total time
	WALChecksumOps              uint64                  // WAL record checksums
	WALChecksumBytes            uint64                  // their bytes
	WALChecksumDurationNanos    uint64                  // their total time
	WALAppendOps                uint64                  // WAL appends
	WALAppendBytes              uint64                  // their bytes
	WALAppendDurationNanos      uint64                  // their total time
	WALAppendLockWaitOps        uint64                  // appends that waited for the WAL append lock
	WALAppendLockWaitNanos      uint64                  // their total wait
	WriteCommitLockWaitOps      uint64                  // writes that waited for the commit lock
	WriteCommitLockWaitNanos    uint64                  // their total wait
	WALAppendWriteAtCalls       uint64                  // file WriteAt calls made by WAL appends
	WALAppendWriteAtBytes       uint64                  // their bytes
	WALAppendWriteAtMaxBytes    uint64                  // the largest of them
	WALAppendWrapCount          uint64                  // times the WAL append wrapped to the start of the log
	WALAppendPaddingBytes       uint64                  // padding bytes written at those wraps
	DirtyMapUpdateOps           uint64                  // dirty-map updates after writes
	DirtyMapUpdateDurationNanos uint64                  // their total time
	BackendSyncOps              uint64                  // backend syncs (flush, FUA, explicit sync)
	BackendSyncDurationNanos    uint64                  // their total time
}

type DurabilityLatencyStatus struct {
	Frontend    iolatency.RequestStatus          // per-request latency at the frontend, by stage
	BackendSync iolatency.Distribution           // latency of backend syncs
	WAL         *storage.DurabilityLatencyStatus `json:",omitempty"` // the WAL's own sync latency, where the store reports it
}

type writeProfile struct {
	requests                  iolatency.Requests
	syncLatency               iolatency.Histogram
	targetWriteOps            atomic.Uint64
	targetWriteBytes          atomic.Uint64
	targetWriteRequestMax     atomic.Uint64
	targetWriteDurationNanos  atomic.Uint64
	backendWriteRequestOps    atomic.Uint64
	backendWriteRequestBytes  atomic.Uint64
	backendWriteRequestMax    atomic.Uint64
	backendWriteOps           atomic.Uint64
	backendWriteBytes         atomic.Uint64
	backendWriteDurationNanos atomic.Uint64
	backendStorageWriteCalls  atomic.Uint64
	backendStorageWriteBlocks atomic.Uint64
	backendStorageBatchCalls  atomic.Uint64
	backendStorageBatchBlocks atomic.Uint64
	backendFullBatchCalls     atomic.Uint64
	backendFullBatchBlocks    atomic.Uint64
	backendFullBatchMax       atomic.Uint64
	backendSyncOps            atomic.Uint64
	backendSyncDurationNanos  atomic.Uint64
}

func (p *writeProfile) recordTargetWrite(bytes int, d time.Duration) {
	if bytes <= 0 {
		return
	}
	p.targetWriteOps.Add(1)
	v := uint64(bytes)
	p.targetWriteBytes.Add(v)
	updateMax(&p.targetWriteRequestMax, v)
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

func (p *writeProfile) recordBackendWriteRequest(bytes int) {
	if bytes <= 0 {
		return
	}
	v := uint64(bytes)
	p.backendWriteRequestOps.Add(1)
	p.backendWriteRequestBytes.Add(v)
	updateMax(&p.backendWriteRequestMax, v)
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

func (p *writeProfile) recordBackendFullBlockBatch(blocks int) {
	if blocks <= 0 {
		return
	}
	v := uint64(blocks)
	p.backendFullBatchCalls.Add(1)
	p.backendFullBatchBlocks.Add(v)
	updateMax(&p.backendFullBatchMax, v)
}

func (p *writeProfile) recordBackendSync(d time.Duration) {
	p.backendSyncOps.Add(1)
	p.backendSyncDurationNanos.Add(durationNanos(d))
}

func updateMax(dst *atomic.Uint64, v uint64) {
	for {
		old := dst.Load()
		if v <= old || dst.CompareAndSwap(old, v) {
			return
		}
	}
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
		DurabilityLatency:           DurabilityLatencyStatus{Frontend: p.requests.Snapshot(), BackendSync: p.syncLatency.Snapshot()},
		TargetWriteOps:              p.targetWriteOps.Load(),
		TargetWriteBytes:            p.targetWriteBytes.Load(),
		TargetWriteRequestMaxBytes:  p.targetWriteRequestMax.Load(),
		TargetWriteDurationNanos:    p.targetWriteDurationNanos.Load(),
		BackendWriteRequestOps:      p.backendWriteRequestOps.Load(),
		BackendWriteRequestBytes:    p.backendWriteRequestBytes.Load(),
		BackendWriteRequestMaxBytes: p.backendWriteRequestMax.Load(),
		BackendWriteOps:             p.backendWriteOps.Load(),
		BackendWriteBytes:           p.backendWriteBytes.Load(),
		BackendWriteDurationNanos:   p.backendWriteDurationNanos.Load(),
		BackendStorageWriteCalls:    p.backendStorageWriteCalls.Load(),
		BackendStorageWriteBlocks:   p.backendStorageWriteBlocks.Load(),
		BackendStorageBatchCalls:    p.backendStorageBatchCalls.Load(),
		BackendStorageBatchBlocks:   p.backendStorageBatchBlocks.Load(),
		BackendFullBlockBatchCalls:  p.backendFullBatchCalls.Load(),
		BackendFullBlockBatchBlocks: p.backendFullBatchBlocks.Load(),
		BackendFullBlockBatchMax:    p.backendFullBatchMax.Load(),
		BackendSyncOps:              p.backendSyncOps.Load(),
		BackendSyncDurationNanos:    p.backendSyncDurationNanos.Load(),
	}
}
