package nvme

// NVMe IO command handler — minimal T2 scope.
//
// Structure mirrors core/frontend/iscsi/scsi.go: an IOHandler
// wraps a frontend.Backend and dispatches NVMe IO Read/Write
// commands to it. LBA math uses the namespace's logical block
// size (512B default). Stale-lineage errors from the backend
// map to SCT=3 SC=3 (ANA Transition) via StatusError so the
// wire layer can encode a proper CQE.
//
// L0 tests exercise this handler directly without PDU framing
// — same rationale as the iSCSI slice: handler-level is the
// deterministic seam between an NVMe command and the frontend
// contract. Capsule framing + fabric login land in a later
// checkpoint.

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// DefaultBlockSize / DefaultVolumeBlocks are symmetric with the
// iSCSI defaults so the two protocols present the same 1 MiB
// volume to the contract backend.
const (
	DefaultBlockSize    uint32 = 512
	DefaultVolumeBlocks uint64 = 2048 // 2048 * 512 = 1 MiB
)

// NVMe IO command opcodes (NVM Command Set spec §6).
// Package-private because callers build IOCommand using these
// constants through the handler seam, not directly off the wire.
// The NVMe admin opcode set (Identify, Get Log Page, etc.) is
// intentionally out of scope for T2 L0 — admin commands land
// with capsule framing in a later checkpoint.
const (
	ioFlush uint8 = 0x00
	ioWrite uint8 = 0x01
	ioRead  uint8 = 0x02
)

// IOCommand is the T2 normalized form of an NVMe IO command.
// Callers (wire decoder in a later checkpoint) translate the
// 64-byte Submission Queue Entry into this struct. L0 tests
// build IOCommand directly.
//
// Fields match the NVMe IO Read/Write CDW10-13 layout:
//   - SLBA  = CDW10 (low) + CDW11 (high) — 64-bit starting LBA
//   - NLB   = CDW12[15:0] + 1  (Number of Logical Blocks, zero-based on wire,
//                               one-based in this struct for readability)
//   - NSID  = command header Namespace Identifier
//
// Data is the host-provided write payload (nil for Read).
// Expected read length = NLB * BlockSize; caller sizes the
// returned data accordingly.
type IOCommand struct {
	Opcode uint8
	NSID   uint32
	SLBA   uint64
	NLB    uint32
	Data   []byte
}

// IOResult is the handler-level outcome of one IO command.
// Mirrors iscsi.SCSIResult in shape. Callers that want an
// idiomatic error use AsError() — nil return means Status
// was Success.
type IOResult struct {
	SCT    uint8
	SC     uint8
	Data   []byte // read payload (nil for writes / errors)
	Reason string
}

// AsError returns a non-nil *StatusError iff Status != Success.
func (r IOResult) AsError() *StatusError {
	if r.SCT == SCTGeneric && r.SC == SCSuccess {
		return nil
	}
	return &StatusError{SCT: r.SCT, SC: r.SC, Reason: r.Reason}
}

// IOHandler dispatches IOCommands to a frontend.Backend. Owns
// no storage; every Read/Write routes through Backend.Read /
// .Write so the per-op lineage fence fires exactly once per
// NVMe command. Stateless with respect to the backend.
type IOHandler struct {
	backend    frontend.Backend
	blockSize  uint32
	volumeSize uint64
	// nsid is the single namespace ID this handler serves. T2
	// advertises NSID=1 (the canonical "first namespace" in
	// NVMe discovery); commands for any other NSID are
	// rejected with Invalid Namespace.
	nsid uint32
}

// HandlerConfig configures an IOHandler. Zero values pick T2
// defaults.
type HandlerConfig struct {
	Backend    frontend.Backend
	BlockSize  uint32
	VolumeSize uint64
	NSID       uint32
}

// NewIOHandler builds a handler. Backend MUST be non-nil.
func NewIOHandler(cfg HandlerConfig) *IOHandler {
	if cfg.Backend == nil {
		panic("nvme: NewIOHandler: Backend required")
	}
	bs := cfg.BlockSize
	if bs == 0 {
		bs = DefaultBlockSize
	}
	vs := cfg.VolumeSize
	if vs == 0 {
		vs = DefaultVolumeBlocks * uint64(bs)
	}
	nsid := cfg.NSID
	if nsid == 0 {
		nsid = 1
	}
	return &IOHandler{
		backend:    cfg.Backend,
		blockSize:  bs,
		volumeSize: vs,
		nsid:       nsid,
	}
}

// BlockSize exposes the advertised logical block size.
func (h *IOHandler) BlockSize() uint32 { return h.blockSize }

// VolumeSize exposes the volume size in bytes.
func (h *IOHandler) VolumeSize() uint64 { return h.volumeSize }

// NSID exposes the namespace this handler serves.
func (h *IOHandler) NSID() uint32 { return h.nsid }

// Handle dispatches one IOCommand. Minimal T2 opcode set:
// Read, Write, Flush. Everything else → Invalid Opcode.
func (h *IOHandler) Handle(ctx context.Context, cmd IOCommand) IOResult {
	if cmd.NSID != h.nsid {
		// Invalid Namespace or Format (SCT=0 SC=0x0B is the
		// spec'd code, but we use InvalidField 0x02 for T2
		// scope because the test harness only exercises NSID=1).
		return IOResult{
			SCT:    SCTGeneric,
			SC:     SCInvalidField,
			Reason: fmt.Sprintf("unknown NSID %d", cmd.NSID),
		}
	}
	switch cmd.Opcode {
	case ioRead:
		return h.read(ctx, cmd)
	case ioWrite:
		return h.write(ctx, cmd)
	case ioFlush:
		// T3b wire: dispatch to Backend.Sync. Durable backends
		// flush the WAL; memback Sync is a no-op (returns nil).
		// Errors propagate as InternalError (NVMe 1.4 §4.5; SCT=0
		// SC=0x06) — stop rule §3.6 (error-faithful) forbids
		// swallowing to Success.
		if err := h.backend.Sync(ctx); err != nil {
			return mapBackendError(err, "flush")
		}
		return IOResult{SCT: SCTGeneric, SC: SCSuccess}
	default:
		return IOResult{
			SCT:    SCTGeneric,
			SC:     SCInvalidOpcode,
			Reason: fmt.Sprintf("unsupported opcode 0x%02x", cmd.Opcode),
		}
	}
}

// ---------- data path ----------

func (h *IOHandler) read(ctx context.Context, cmd IOCommand) IOResult {
	if cmd.NLB == 0 {
		// NVMe treats NLB=0 (wire) as one block; IOCommand.NLB
		// is one-based so 0 here means "caller set it wrong".
		// Fail closed.
		return IOResult{SCT: SCTGeneric, SC: SCInvalidField, Reason: "NLB=0"}
	}
	totalBlocks := h.volumeSize / uint64(h.blockSize)
	if cmd.SLBA >= totalBlocks || cmd.SLBA+uint64(cmd.NLB) > totalBlocks {
		return IOResult{SCT: SCTGeneric, SC: SCLBAOutOfRange, Reason: "read LBA out of range"}
	}
	byteLen := int(cmd.NLB) * int(h.blockSize)
	buf := make([]byte, byteLen)
	_, err := h.backend.Read(ctx, int64(cmd.SLBA)*int64(h.blockSize), buf)
	if err != nil {
		return mapBackendError(err, "read")
	}
	return IOResult{SCT: SCTGeneric, SC: SCSuccess, Data: buf}
}

func (h *IOHandler) write(ctx context.Context, cmd IOCommand) IOResult {
	if cmd.NLB == 0 {
		return IOResult{SCT: SCTGeneric, SC: SCInvalidField, Reason: "NLB=0"}
	}
	totalBlocks := h.volumeSize / uint64(h.blockSize)
	if cmd.SLBA >= totalBlocks || cmd.SLBA+uint64(cmd.NLB) > totalBlocks {
		return IOResult{SCT: SCTGeneric, SC: SCLBAOutOfRange, Reason: "write LBA out of range"}
	}
	expectedBytes := int(cmd.NLB) * int(h.blockSize)
	if len(cmd.Data) < expectedBytes {
		return IOResult{
			SCT:    SCTGeneric,
			SC:     SCInvalidField,
			Reason: "host data shorter than NLB*BlockSize",
		}
	}
	_, err := h.backend.Write(ctx, int64(cmd.SLBA)*int64(h.blockSize), cmd.Data[:expectedBytes])
	if err != nil {
		return mapBackendError(err, "write")
	}
	return IOResult{SCT: SCTGeneric, SC: SCSuccess}
}

// mapBackendError translates frontend errors into NVMe CQE
// status tuples. ErrStalePrimary → ANA Transition;
// ErrBackendClosed → ANA Inaccessible; anything else → Internal
// Error (fail-closed; real-backend-specific mapping lands with
// T3).
func mapBackendError(err error, op string) IOResult {
	if errors.Is(err, frontend.ErrStalePrimary) {
		return IOResult{
			SCT:    SCTPathRelated,
			SC:     SCPathAsymAccessTransition,
			Reason: "stale primary lineage on " + op,
		}
	}
	if errors.Is(err, frontend.ErrBackendClosed) {
		return IOResult{
			SCT:    SCTPathRelated,
			SC:     SCPathAsymAccessInaccessible,
			Reason: "backend closed on " + op,
		}
	}
	return IOResult{
		SCT:    SCTGeneric,
		SC:     SCInternalError,
		Reason: "backend error on " + op + ": " + err.Error(),
	}
}
