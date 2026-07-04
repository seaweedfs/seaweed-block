package storage

import (
	"sync/atomic"
	"time"
)

// WriteInstrumentationStatus is a read-only snapshot of storage-internal write
// costs. It is diagnostic evidence only; it must not affect write semantics.
type WriteInstrumentationStatus struct {
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
}

// WriteInstrumented is an optional LogicalStorage extension used by durable
// status/report gates to expose storage-internal write cost evidence.
type WriteInstrumented interface {
	WriteInstrumentation() WriteInstrumentationStatus
}

type writeInstrumentation struct {
	walCopyOps                  atomic.Uint64
	walCopyBytes                atomic.Uint64
	walCopyDurationNanos        atomic.Uint64
	walEncodeOps                atomic.Uint64
	walEncodeBytes              atomic.Uint64
	walEncodeDurationNanos      atomic.Uint64
	walChecksumOps              atomic.Uint64
	walChecksumBytes            atomic.Uint64
	walChecksumDurationNanos    atomic.Uint64
	walAppendOps                atomic.Uint64
	walAppendBytes              atomic.Uint64
	walAppendDurationNanos      atomic.Uint64
	dirtyMapUpdateOps           atomic.Uint64
	dirtyMapUpdateDurationNanos atomic.Uint64
}

func storageDurationNanos(d time.Duration) uint64 {
	if d <= 0 {
		return 1
	}
	return uint64(d.Nanoseconds())
}

func (i *writeInstrumentation) recordWALCopy(bytes int, d time.Duration) {
	if i == nil || bytes <= 0 {
		return
	}
	i.walCopyOps.Add(1)
	i.walCopyBytes.Add(uint64(bytes))
	i.walCopyDurationNanos.Add(storageDurationNanos(d))
}

func (i *writeInstrumentation) recordWALEncode(bytes int, d time.Duration) {
	if i == nil || bytes <= 0 {
		return
	}
	i.walEncodeOps.Add(1)
	i.walEncodeBytes.Add(uint64(bytes))
	i.walEncodeDurationNanos.Add(storageDurationNanos(d))
}

func (i *writeInstrumentation) recordWALChecksum(bytes int, d time.Duration) {
	if i == nil || bytes <= 0 {
		return
	}
	i.walChecksumOps.Add(1)
	i.walChecksumBytes.Add(uint64(bytes))
	i.walChecksumDurationNanos.Add(storageDurationNanos(d))
}

func (i *writeInstrumentation) recordWALAppend(bytes int, d time.Duration) {
	if i == nil || bytes <= 0 {
		return
	}
	i.walAppendOps.Add(1)
	i.walAppendBytes.Add(uint64(bytes))
	i.walAppendDurationNanos.Add(storageDurationNanos(d))
}

func (i *writeInstrumentation) recordDirtyMapUpdate(ops int, d time.Duration) {
	if i == nil || ops <= 0 {
		return
	}
	i.dirtyMapUpdateOps.Add(uint64(ops))
	i.dirtyMapUpdateDurationNanos.Add(storageDurationNanos(d))
}

func (i *writeInstrumentation) snapshot() WriteInstrumentationStatus {
	if i == nil {
		return WriteInstrumentationStatus{}
	}
	return WriteInstrumentationStatus{
		WALCopyOps:                  i.walCopyOps.Load(),
		WALCopyBytes:                i.walCopyBytes.Load(),
		WALCopyDurationNanos:        i.walCopyDurationNanos.Load(),
		WALEncodeOps:                i.walEncodeOps.Load(),
		WALEncodeBytes:              i.walEncodeBytes.Load(),
		WALEncodeDurationNanos:      i.walEncodeDurationNanos.Load(),
		WALChecksumOps:              i.walChecksumOps.Load(),
		WALChecksumBytes:            i.walChecksumBytes.Load(),
		WALChecksumDurationNanos:    i.walChecksumDurationNanos.Load(),
		WALAppendOps:                i.walAppendOps.Load(),
		WALAppendBytes:              i.walAppendBytes.Load(),
		WALAppendDurationNanos:      i.walAppendDurationNanos.Load(),
		DirtyMapUpdateOps:           i.dirtyMapUpdateOps.Load(),
		DirtyMapUpdateDurationNanos: i.dirtyMapUpdateDurationNanos.Load(),
	}
}
