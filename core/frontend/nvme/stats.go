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
	SessionsAccepted uint64 `json:"sessions_accepted"` // TCP sessions accepted
	AdminConnects    uint64 `json:"admin_connects"`    // admin-queue Connect commands
	IOConnects       uint64 `json:"io_connects"`       // I/O-queue Connect commands

	ReadCommands  uint64 `json:"read_commands"`  // Read commands received
	WriteCommands uint64 `json:"write_commands"` // Write commands received
	FlushCommands uint64 `json:"flush_commands"` // Flush commands received

	InlineWriteCommands uint64 `json:"inline_write_commands"` // writes whose data came inline in the command capsule
	InlineWriteBytes    uint64 `json:"inline_write_bytes"`    // their data bytes
	R2TWriteCommands    uint64 `json:"r2t_write_commands"`    // writes whose data the target requested with R2T
	R2TWriteBytes       uint64 `json:"r2t_write_bytes"`       // their data bytes

	H2CDataPDUs  uint64 `json:"h2c_data_pdus"`  // host-to-controller data PDUs received
	H2CDataBytes uint64 `json:"h2c_data_bytes"` // their bytes
	C2HDataPDUs  uint64 `json:"c2h_data_pdus"`  // controller-to-host data PDUs sent (read data)
	C2HDataBytes uint64 `json:"c2h_data_bytes"` // their bytes

	// Write phase attribution: for each phase, the operations timed and
	// their total nanoseconds (overlapping phases can overlap in time).
	WriteCapsuleReceiveParseOps   uint64 `json:"write_capsule_receive_parse_ops"` // write capsules received and decoded (includes socket wait)
	WriteCapsuleReceiveParseNanos uint64 `json:"write_capsule_receive_parse_ns"`  // total nanoseconds of those operations
	R2TDataCollectionOps          uint64 `json:"r2t_data_collection_ops"`         // R2T data collections (R2T send to the last H2CData)
	R2TDataCollectionNanos        uint64 `json:"r2t_data_collection_ns"`          // total nanoseconds of those operations
	WriteDispatchWaitOps          uint64 `json:"write_dispatch_wait_ops"`         // waits for a write handler to take the command
	WriteDispatchWaitNanos        uint64 `json:"write_dispatch_wait_ns"`          // total nanoseconds of those operations
	WriteHandlerOps               uint64 `json:"write_handler_ops"`               // write handler runs (the backend write)
	WriteHandlerNanos             uint64 `json:"write_handler_ns"`                // total nanoseconds of those operations
	WriteCompletionQueueWaitOps   uint64 `json:"write_completion_queue_wait_ops"` // waits of a completion for the send queue
	WriteCompletionQueueWaitNanos uint64 `json:"write_completion_queue_wait_ns"`  // total nanoseconds of those operations
	WriteCompletionSendOps        uint64 `json:"write_completion_send_ops"`       // completion sends to the host
	WriteCompletionSendNanos      uint64 `json:"write_completion_send_ns"`        // total nanoseconds of those operations
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
