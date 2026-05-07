package nvme

import "sync/atomic"

// Stats is a snapshot of target-level NVMe/TCP activity.
//
// It is intentionally transport-focused, not a performance API. The first use
// is lab evidence: after a real Linux initiator run, we need to know whether
// writes arrived inline in the capsule or through R2T/H2CData.
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
}

func (s *targetStats) snapshot() Stats {
	if s == nil {
		return Stats{}
	}
	return Stats{
		SessionsAccepted:    s.sessionsAccepted.Load(),
		AdminConnects:       s.adminConnects.Load(),
		IOConnects:          s.ioConnects.Load(),
		ReadCommands:        s.readCommands.Load(),
		WriteCommands:       s.writeCommands.Load(),
		FlushCommands:       s.flushCommands.Load(),
		InlineWriteCommands: s.inlineWriteCommands.Load(),
		InlineWriteBytes:    s.inlineWriteBytes.Load(),
		R2TWriteCommands:    s.r2tWriteCommands.Load(),
		R2TWriteBytes:       s.r2tWriteBytes.Load(),
		H2CDataPDUs:         s.h2cDataPDUs.Load(),
		H2CDataBytes:        s.h2cDataBytes.Load(),
		C2HDataPDUs:         s.c2hDataPDUs.Load(),
		C2HDataBytes:        s.c2hDataBytes.Load(),
	}
}
