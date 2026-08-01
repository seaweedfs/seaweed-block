package nvme

import (
	"sync/atomic"
	"time"
)

// Stats is a snapshot of target-level NVMe/TCP activity.
//
// It is intentionally transport-focused, not a performance API. The first use
// is lab evidence: after a real Linux initiator run, we need to know whether
// writes arrived inline in the capsule or through R2T/H2CData. Phase timings
// are cumulative per-operation attribution evidence; concurrent phases can
// overlap in wall time and do not affect request handling. Capsule receive
// includes socket wait and decode. R2T collection includes target send,
// initiator response, and H2CData receive.
type Stats struct {
	SessionsAccepted uint64 `json:"sessions_accepted"`
	AdminConnects    uint64 `json:"admin_connects"`
	IOConnects       uint64 `json:"io_connects"`

	ReadCommands  uint64 `json:"read_commands"`
	WriteCommands uint64 `json:"write_commands"`
	FlushCommands uint64 `json:"flush_commands"`

	InlineWriteCommands uint64 `json:"inline_write_commands"`
	InlineWriteBytes    uint64 `json:"inline_write_bytes"`
	R2TWriteCommands    uint64 `json:"r2t_write_commands"`
	R2TWriteBytes       uint64 `json:"r2t_write_bytes"`

	H2CDataPDUs  uint64 `json:"h2c_data_pdus"`
	H2CDataBytes uint64 `json:"h2c_data_bytes"`
	C2HDataPDUs  uint64 `json:"c2h_data_pdus"`
	C2HDataBytes uint64 `json:"c2h_data_bytes"`

	WriteCapsuleReceiveParseOps   uint64 `json:"write_capsule_receive_parse_ops"`
	WriteCapsuleReceiveParseNanos uint64 `json:"write_capsule_receive_parse_ns"`
	R2TDataCollectionOps          uint64 `json:"r2t_data_collection_ops"`
	R2TDataCollectionNanos        uint64 `json:"r2t_data_collection_ns"`
	WriteDispatchWaitOps          uint64 `json:"write_dispatch_wait_ops"`
	WriteDispatchWaitNanos        uint64 `json:"write_dispatch_wait_ns"`
	WriteHandlerOps               uint64 `json:"write_handler_ops"`
	WriteHandlerNanos             uint64 `json:"write_handler_ns"`
	WriteCompletionQueueWaitOps   uint64 `json:"write_completion_queue_wait_ops"`
	WriteCompletionQueueWaitNanos uint64 `json:"write_completion_queue_wait_ns"`
	WriteCompletionSendOps        uint64 `json:"write_completion_send_ops"`
	WriteCompletionSendNanos      uint64 `json:"write_completion_send_ns"`
}

type targetStats struct {
	sessionsAccepted atomic.Uint64
	adminConnects    atomic.Uint64
	ioConnects       atomic.Uint64

	readCommands  atomic.Uint64
	writeCommands atomic.Uint64
	flushCommands atomic.Uint64

	inlineWriteCommands atomic.Uint64
	inlineWriteBytes    atomic.Uint64
	r2tWriteCommands    atomic.Uint64
	r2tWriteBytes       atomic.Uint64

	h2cDataPDUs  atomic.Uint64
	h2cDataBytes atomic.Uint64
	c2hDataPDUs  atomic.Uint64
	c2hDataBytes atomic.Uint64

	writeCapsuleReceiveParseOps   atomic.Uint64
	writeCapsuleReceiveParseNanos atomic.Uint64
	r2tDataCollectionOps          atomic.Uint64
	r2tDataCollectionNanos        atomic.Uint64
	writeDispatchWaitOps          atomic.Uint64
	writeDispatchWaitNanos        atomic.Uint64
	writeHandlerOps               atomic.Uint64
	writeHandlerNanos             atomic.Uint64
	writeCompletionQueueWaitOps   atomic.Uint64
	writeCompletionQueueWaitNanos atomic.Uint64
	writeCompletionSendOps        atomic.Uint64
	writeCompletionSendNanos      atomic.Uint64
}

func (s *targetStats) snapshot() Stats {
	if s == nil {
		return Stats{}
	}
	return Stats{
		SessionsAccepted:              s.sessionsAccepted.Load(),
		AdminConnects:                 s.adminConnects.Load(),
		IOConnects:                    s.ioConnects.Load(),
		ReadCommands:                  s.readCommands.Load(),
		WriteCommands:                 s.writeCommands.Load(),
		FlushCommands:                 s.flushCommands.Load(),
		InlineWriteCommands:           s.inlineWriteCommands.Load(),
		InlineWriteBytes:              s.inlineWriteBytes.Load(),
		R2TWriteCommands:              s.r2tWriteCommands.Load(),
		R2TWriteBytes:                 s.r2tWriteBytes.Load(),
		H2CDataPDUs:                   s.h2cDataPDUs.Load(),
		H2CDataBytes:                  s.h2cDataBytes.Load(),
		C2HDataPDUs:                   s.c2hDataPDUs.Load(),
		C2HDataBytes:                  s.c2hDataBytes.Load(),
		WriteCapsuleReceiveParseOps:   s.writeCapsuleReceiveParseOps.Load(),
		WriteCapsuleReceiveParseNanos: s.writeCapsuleReceiveParseNanos.Load(),
		R2TDataCollectionOps:          s.r2tDataCollectionOps.Load(),
		R2TDataCollectionNanos:        s.r2tDataCollectionNanos.Load(),
		WriteDispatchWaitOps:          s.writeDispatchWaitOps.Load(),
		WriteDispatchWaitNanos:        s.writeDispatchWaitNanos.Load(),
		WriteHandlerOps:               s.writeHandlerOps.Load(),
		WriteHandlerNanos:             s.writeHandlerNanos.Load(),
		WriteCompletionQueueWaitOps:   s.writeCompletionQueueWaitOps.Load(),
		WriteCompletionQueueWaitNanos: s.writeCompletionQueueWaitNanos.Load(),
		WriteCompletionSendOps:        s.writeCompletionSendOps.Load(),
		WriteCompletionSendNanos:      s.writeCompletionSendNanos.Load(),
	}
}

func statsDurationNanos(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64(d.Nanoseconds())
}
