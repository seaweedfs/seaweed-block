package iscsi

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// SCSI opcodes (SBC-4 / SPC-5). Only the subset T2 needs for a
// basic attach + write + read round-trip is declared here; the
// rest are rejected as ILLEGAL REQUEST until later tracks add
// them.
const (
	ScsiTestUnitReady  uint8 = 0x00
	ScsiRequestSense   uint8 = 0x03
	ScsiInquiry        uint8 = 0x12
	ScsiReadCapacity10 uint8 = 0x25
	ScsiRead10         uint8 = 0x28
	ScsiWrite10        uint8 = 0x2a
	ScsiReportLuns     uint8 = 0xa0
)

// Fixed block size for the T2 contract backend. Real volumes
// will negotiate; memback and the recording backend are byte-
// addressable but expose a 512-byte block to the initiator for
// READ(10)/WRITE(10) LBA math. 512 is the SCSI default and
// matches Linux initiator expectations.
const DefaultBlockSize uint32 = 512

// DefaultVolumeBlocks is the number of blocks the contract
// backend reports to INQUIRY / READ CAPACITY. 1 MiB is enough
// for T2's short round-trip payloads without committing us to
// a durability claim (sketch §5 non-claim).
const DefaultVolumeBlocks uint64 = 2048 // 2048 * 512 = 1 MiB

// SCSIResult is the handler-level outcome of one SCSI command.
// Carries the full wire-visible tuple a SCSI Response PDU would
// expose, so L0 unit tests can assert without wire decoding.
//
// Callers that want a Go-idiomatic error value for non-success
// statuses should use (SCSIResult).AsError() — a nil return
// means Status == Good.
type SCSIResult struct {
	Status   uint8
	Data     []byte
	SenseKey uint8
	ASC      uint8
	ASCQ     uint8
	// Reason is diagnostic only; carried forward to SCSIError.
	Reason string
}

// AsError returns a non-nil *SCSIError iff r.Status != Good.
func (r SCSIResult) AsError() *SCSIError {
	if r.Status == StatusGood {
		return nil
	}
	return &SCSIError{
		Status:   r.Status,
		SenseKey: r.SenseKey,
		ASC:      r.ASC,
		ASCQ:     r.ASCQ,
		Reason:   r.Reason,
	}
}

// SCSIHandler dispatches SCSI CDBs to a frontend.Backend. The
// handler owns NO storage of its own; every READ/WRITE routes
// through Backend.Read/Write so the per-op lineage fence kicks
// in exactly once per SCSI command (test spec §4 T2.L0.i1).
//
// The handler is STATELESS with respect to the backend — each
// HandleCommand call takes context + cdb + dataOut and returns
// one SCSIResult. No caching, no reordering, no retry. Retries
// (if any) are an initiator / protocol concern, not this
// layer's.
type SCSIHandler struct {
	backend     frontend.Backend
	blockSize   uint32
	volumeSize  uint64 // bytes
	vendorID    string // 8 bytes
	productID   string // 16 bytes
	serialNo    string
}

// HandlerConfig configures an SCSIHandler. Zero values pick
// sensible defaults for T2 contract backends.
type HandlerConfig struct {
	Backend    frontend.Backend
	BlockSize  uint32
	VolumeSize uint64
	VendorID   string
	ProductID  string
	SerialNo   string
}

// NewSCSIHandler constructs a handler. Backend MUST be non-nil.
func NewSCSIHandler(cfg HandlerConfig) *SCSIHandler {
	if cfg.Backend == nil {
		panic("iscsi: NewSCSIHandler: Backend required")
	}
	bs := cfg.BlockSize
	if bs == 0 {
		bs = DefaultBlockSize
	}
	vs := cfg.VolumeSize
	if vs == 0 {
		vs = DefaultVolumeBlocks * uint64(bs)
	}
	vendor := cfg.VendorID
	if vendor == "" {
		vendor = "SeaweedF"
	}
	product := cfg.ProductID
	if product == "" {
		product = "BlockVol        "
	}
	serial := cfg.SerialNo
	if serial == "" {
		serial = "SWF00001"
	}
	return &SCSIHandler{
		backend:    cfg.Backend,
		blockSize:  bs,
		volumeSize: vs,
		vendorID:   vendor,
		productID:  product,
		serialNo:   serial,
	}
}

// BlockSize exposes the advertised logical block size for the
// frame layer (each WRITE(10) LBA translates to BlockSize bytes).
func (h *SCSIHandler) BlockSize() uint32 { return h.blockSize }

// VolumeSize exposes the advertised volume size in bytes.
func (h *SCSIHandler) VolumeSize() uint64 { return h.volumeSize }

// HandleCommand dispatches a CDB to the matching op-handler.
// The minimum op set for T2 L0 is declared above; every other
// opcode returns ILLEGAL REQUEST — sufficient for basic attach
// + round-trip + stale-path assertions. Expanding to the full
// SPC-5 surface lands with L2 (real initiator) in a later T2
// session.
func (h *SCSIHandler) HandleCommand(ctx context.Context, cdb [16]byte, dataOut []byte) SCSIResult {
	switch cdb[0] {
	case ScsiTestUnitReady:
		return SCSIResult{Status: StatusGood}
	case ScsiRequestSense:
		return h.requestSense(cdb)
	case ScsiInquiry:
		return h.inquiry(cdb)
	case ScsiReadCapacity10:
		return h.readCapacity10()
	case ScsiRead10:
		return h.read10(ctx, cdb)
	case ScsiWrite10:
		return h.write10(ctx, cdb, dataOut)
	case ScsiReportLuns:
		return h.reportLuns(cdb)
	default:
		return illegalRequest(ASCInvalidOpcode, 0x00, fmt.Sprintf("unsupported opcode 0x%02x", cdb[0]))
	}
}

// --- data path ---

func (h *SCSIHandler) read10(ctx context.Context, cdb [16]byte) SCSIResult {
	lba := uint64(binary.BigEndian.Uint32(cdb[2:6]))
	transferLen := uint32(binary.BigEndian.Uint16(cdb[7:9]))
	return h.doRead(ctx, lba, transferLen)
}

func (h *SCSIHandler) write10(ctx context.Context, cdb [16]byte, dataOut []byte) SCSIResult {
	lba := uint64(binary.BigEndian.Uint32(cdb[2:6]))
	transferLen := uint32(binary.BigEndian.Uint16(cdb[7:9]))
	return h.doWrite(ctx, lba, transferLen, dataOut)
}

func (h *SCSIHandler) doRead(ctx context.Context, lba uint64, transferLen uint32) SCSIResult {
	if transferLen == 0 {
		return SCSIResult{Status: StatusGood}
	}
	totalBlocks := h.volumeSize / uint64(h.blockSize)
	if lba >= totalBlocks || lba+uint64(transferLen) > totalBlocks {
		return illegalRequest(ASCLBAOutOfRange, 0x00, "read LBA out of range")
	}
	byteLen := int(transferLen) * int(h.blockSize)
	buf := make([]byte, byteLen)
	_, err := h.backend.Read(ctx, int64(lba)*int64(h.blockSize), buf)
	if err != nil {
		return mapBackendError(err, "read")
	}
	return SCSIResult{Status: StatusGood, Data: buf}
}

func (h *SCSIHandler) doWrite(ctx context.Context, lba uint64, transferLen uint32, dataOut []byte) SCSIResult {
	if transferLen == 0 {
		return SCSIResult{Status: StatusGood}
	}
	totalBlocks := h.volumeSize / uint64(h.blockSize)
	if lba >= totalBlocks || lba+uint64(transferLen) > totalBlocks {
		return illegalRequest(ASCLBAOutOfRange, 0x00, "write LBA out of range")
	}
	expectedBytes := int(transferLen) * int(h.blockSize)
	if len(dataOut) < expectedBytes {
		return illegalRequest(ASCInvalidFieldInCDB, 0x00, "dataOut shorter than expected")
	}
	_, err := h.backend.Write(ctx, int64(lba)*int64(h.blockSize), dataOut[:expectedBytes])
	if err != nil {
		return mapBackendError(err, "write")
	}
	return SCSIResult{Status: StatusGood}
}

// --- metadata path ---

func (h *SCSIHandler) requestSense(cdb [16]byte) SCSIResult {
	allocLen := cdb[4]
	if allocLen == 0 {
		allocLen = 18
	}
	data := buildSenseData(SenseNone, 0x00, 0x00)
	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: StatusGood, Data: data}
}

func (h *SCSIHandler) inquiry(cdb [16]byte) SCSIResult {
	evpd := cdb[1] & 0x01
	if evpd != 0 {
		// VPD pages are useful for real initiators but not for
		// the T2 L0 contract smoke. Reject until L2 coverage
		// lands.
		return illegalRequest(ASCInvalidFieldInCDB, 0x00, "VPD not supported in T2 scope")
	}
	allocLen := binary.BigEndian.Uint16(cdb[3:5])
	if allocLen == 0 {
		allocLen = 36
	}
	data := make([]byte, 36)
	data[0] = 0x00                  // Peripheral device type: direct-access block
	data[1] = 0x00                  // RMB=0
	data[2] = 0x06                  // Version: SPC-4
	data[3] = 0x02                  // Response data format
	data[4] = 31                    // Additional length (36-5)
	copy(data[8:16], padRight(h.vendorID, 8))
	copy(data[16:32], padRight(h.productID, 16))
	copy(data[32:36], "0001")
	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: StatusGood, Data: data}
}

func (h *SCSIHandler) readCapacity10() SCSIResult {
	totalBlocks := h.volumeSize / uint64(h.blockSize)
	data := make([]byte, 8)
	if totalBlocks > 0 {
		// Returns LBA of last block, not count.
		binary.BigEndian.PutUint32(data[0:4], uint32(totalBlocks-1))
	}
	binary.BigEndian.PutUint32(data[4:8], h.blockSize)
	return SCSIResult{Status: StatusGood, Data: data}
}

func (h *SCSIHandler) reportLuns(cdb [16]byte) SCSIResult {
	// T2 advertises a single LUN 0. 8-byte LUN list + 8-byte
	// header.
	data := make([]byte, 16)
	binary.BigEndian.PutUint32(data[0:4], 8) // list length
	// LUN 0 encoded as zero.
	return SCSIResult{Status: StatusGood, Data: data}
}

// --- helpers ---

// mapBackendError translates a frontend.Backend error into the
// SCSI status/sense tuple. ErrStalePrimary is the load-bearing
// case (test-spec §4 T2.L0.i2/i3): stale WRITE must not
// acknowledge success, stale READ must not return stale bytes.
// Both map to CHECK CONDITION + NOT READY + stale-lineage ASC.
//
// Other backend errors surface as CHECK CONDITION / MEDIUM
// ERROR — conservative fail-closed; real-backend-specific
// mapping lands with T3.
func mapBackendError(err error, op string) SCSIResult {
	if errors.Is(err, frontend.ErrStalePrimary) {
		return SCSIResult{
			Status:   StatusCheckCondition,
			SenseKey: SenseNotReady,
			ASC:      ASCStaleLineage,
			ASCQ:     ASCQStaleLineage,
			Reason:   "stale primary lineage on " + op,
		}
	}
	if errors.Is(err, frontend.ErrBackendClosed) {
		return SCSIResult{
			Status:   StatusCheckCondition,
			SenseKey: SenseNotReady,
			ASC:      ASCNotReady,
			ASCQ:     ASCNotReadyManualIntv,
			Reason:   "backend closed on " + op,
		}
	}
	return SCSIResult{
		Status:   StatusCheckCondition,
		SenseKey: SenseMediumError,
		ASC:      0x11, // Unrecovered read error (or generic medium error)
		ASCQ:     0x00,
		Reason:   "backend error on " + op + ": " + err.Error(),
	}
}

func illegalRequest(asc, ascq uint8, reason string) SCSIResult {
	return SCSIResult{
		Status:   StatusCheckCondition,
		SenseKey: SenseIllegalRequest,
		ASC:      asc,
		ASCQ:     ascq,
		Reason:   reason,
	}
}

// buildSenseData emits an 18-byte fixed-format sense buffer
// (SPC-5 §4.5.3). Used by REQUEST SENSE.
func buildSenseData(key, asc, ascq uint8) []byte {
	d := make([]byte, 18)
	d[0] = 0x70          // Current, fixed format
	d[2] = key & 0x0f    // Sense key
	d[7] = 10            // Additional length
	d[12] = asc
	d[13] = ascq
	return d
}

func padRight(s string, n int) []byte {
	b := []byte(s)
	if len(b) >= n {
		return b[:n]
	}
	out := make([]byte, n)
	copy(out, b)
	for i := len(b); i < n; i++ {
		out[i] = ' '
	}
	return out
}
