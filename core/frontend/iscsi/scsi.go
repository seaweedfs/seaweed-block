package iscsi

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// SCSI opcodes (SBC-4 / SPC-5). Extended in Batch 10.5 to cover
// the Linux/kernel + Windows-initiator OS probe surface per the
// locked port plan (v3-phase-15-t2-batch-10-5-port-plan.md §3.1).
// Opcodes not in this list still return ILLEGAL REQUEST.
const (
	ScsiTestUnitReady     uint8 = 0x00
	ScsiRequestSense      uint8 = 0x03
	ScsiInquiry           uint8 = 0x12
	ScsiModeSelect6       uint8 = 0x15
	ScsiModeSense6        uint8 = 0x1a
	ScsiStartStopUnit     uint8 = 0x1b
	ScsiReadCapacity10    uint8 = 0x25
	ScsiRead10            uint8 = 0x28
	ScsiWrite10           uint8 = 0x2a
	ScsiSyncCache10       uint8 = 0x35
	ScsiModeSelect10      uint8 = 0x55
	ScsiModeSense10       uint8 = 0x5a
	ScsiRead16            uint8 = 0x88
	ScsiWrite16           uint8 = 0x8a
	ScsiSyncCache16       uint8 = 0x91
	ScsiServiceActionIn16 uint8 = 0x9e
	ScsiReportLuns        uint8 = 0xa0
)

// Service Action codes for SERVICE ACTION IN(16) opcode 0x9e.
const (
	SaiReadCapacity16 uint8 = 0x10
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
	// Batch 10.5: MODE_SELECT + START_STOP_UNIT — trivial Good
	// stubs paired with MODE_SENSE (see port plan §3.1.a).
	case ScsiModeSelect6, ScsiModeSelect10:
		return SCSIResult{Status: StatusGood}
	case ScsiStartStopUnit:
		return SCSIResult{Status: StatusGood}
	// Batch 10.5: MODE_SENSE(6/10).
	case ScsiModeSense6:
		return h.modeSense6(cdb)
	case ScsiModeSense10:
		return h.modeSense10(cdb)
	case ScsiReadCapacity10:
		return h.readCapacity10()
	case ScsiRead10:
		return h.read10(ctx, cdb)
	case ScsiWrite10:
		return h.write10(ctx, cdb, dataOut)
	// Batch 10.5: SYNC_CACHE(10/16) — memback non-durable, no-op
	// Good. T3 owns real flush behavior when durable backend lands.
	case ScsiSyncCache10, ScsiSyncCache16:
		return SCSIResult{Status: StatusGood}
	// Batch 10.5: 16-byte data variants (64-bit LBA).
	case ScsiRead16:
		return h.read16(ctx, cdb)
	case ScsiWrite16:
		return h.write16(ctx, cdb, dataOut)
	case ScsiServiceActionIn16:
		sa := cdb[1] & 0x1f
		if sa == SaiReadCapacity16 {
			return h.readCapacity16(cdb)
		}
		return illegalRequest(ASCInvalidFieldInCDB, 0x00,
			fmt.Sprintf("unsupported SAI16 service action 0x%02x", sa))
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

// Batch 10.5: 16-byte data variants. 64-bit LBA (bytes 2–9) +
// 32-bit transferLen (bytes 10–13). Dispatches to the same
// doRead/doWrite paths as the 10-byte variants.

func (h *SCSIHandler) read16(ctx context.Context, cdb [16]byte) SCSIResult {
	lba := binary.BigEndian.Uint64(cdb[2:10])
	transferLen := binary.BigEndian.Uint32(cdb[10:14])
	return h.doRead(ctx, lba, transferLen)
}

func (h *SCSIHandler) write16(ctx context.Context, cdb [16]byte, dataOut []byte) SCSIResult {
	lba := binary.BigEndian.Uint64(cdb[2:10])
	transferLen := binary.BigEndian.Uint32(cdb[10:14])
	return h.doWrite(ctx, lba, transferLen, dataOut)
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

// Batch 10.5: Standard INQUIRY ported to V2's 96-byte tuned
// response (PM Medium finding 2026-04-22).
//
// Key V2 parity bytes we now emit:
//   byte 4  = 91     Additional length (96 - 5)
//   byte 7  = 0x02   CmdQue=1 — command queuing supported
//   bytes 8-15      Vendor ID, 8 bytes, space-padded
//   bytes 16-31     Product ID, 16 bytes, space-padded
//   bytes 32-35     Product revision "0001"
//   bytes 36-95     Reserved / zero
//
// Skip list compliance (ports plan §3.2):
//   byte 5 bit 4 (TPGS implicit) — NOT set (ALUA is skip-list)
//   byte 6 / other tuning bits   — zero (V2 sets some for
//     enclosure/multiPort/vendor-specific; T2 has no reason
//     to advertise them)
//
// NOTE: the prior 36-byte minimal response still worked for
// iscsiadm attach, but under-advertised CmdQue and forced the
// kernel to assume minimum SPC capabilities. V2 tuning avoids
// spurious SCSI layer log messages about command queuing
// behavior.
func (h *SCSIHandler) inquiry(cdb [16]byte) SCSIResult {
	evpd := cdb[1] & 0x01
	pageCode := cdb[2]
	allocLen := binary.BigEndian.Uint16(cdb[3:5])
	if allocLen == 0 {
		allocLen = 96
	}
	if evpd != 0 {
		return h.inquiryVPD(pageCode, allocLen)
	}
	data := make([]byte, 96)
	data[0] = 0x00 // Peripheral device type: direct-access block (SBC)
	data[1] = 0x00 // RMB=0 (not removable)
	data[2] = 0x06 // Version: SPC-4
	data[3] = 0x02 // Response data format (SPC-2+)
	data[4] = 91   // Additional length (96 - 5)
	// byte 5: SCCS / ACC / TPGS / 3PC — all zero in T2 (no ALUA)
	// byte 6: reserved / EncServ / VS / MultiP — zero
	data[7] = 0x02 // BQue=0, CmdQue=1
	copy(data[8:16], padRight(h.vendorID, 8))
	copy(data[16:32], padRight(h.productID, 16))
	copy(data[32:36], "0001")
	// bytes 36-95 reserved; V2 leaves zeros.
	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: StatusGood, Data: data}
}

// Batch 10.5: INQUIRY VPD. Approved pages: 0x00 (supported list),
// 0x80 (unit serial number), 0x83 (device identification, non-ALUA
// branch only). VPD 0xB0 / 0xB2 are explicitly NOT implemented
// and NOT advertised (port plan §3.3 N3 — advertised list must
// match implemented set or kernel logs errors on every probe).
func (h *SCSIHandler) inquiryVPD(pageCode uint8, allocLen uint16) SCSIResult {
	switch pageCode {
	case 0x00: // Supported VPD pages
		// Peripheral device type + page code + 2-byte page length
		// + one byte per supported page. List is EXACT match to
		// the pages the switch below actually implements.
		pages := []byte{0x00, 0x80, 0x83}
		data := make([]byte, 4+len(pages))
		data[0] = 0x00
		data[1] = 0x00
		binary.BigEndian.PutUint16(data[2:4], uint16(len(pages)))
		copy(data[4:], pages)
		if int(allocLen) < len(data) {
			data = data[:allocLen]
		}
		return SCSIResult{Status: StatusGood, Data: data}

	case 0x80: // Unit Serial Number
		serial := padRight(h.serialNo, 8)
		data := make([]byte, 4+len(serial))
		data[0] = 0x00
		data[1] = 0x80
		binary.BigEndian.PutUint16(data[2:4], uint16(len(serial)))
		copy(data[4:], serial)
		if int(allocLen) < len(data) {
			data = data[:allocLen]
		}
		return SCSIResult{Status: StatusGood, Data: data}

	case 0x83: // Device Identification (non-ALUA branch only)
		return h.inquiryVPD83(allocLen)

	default:
		return illegalRequest(ASCInvalidFieldInCDB, 0x00,
			fmt.Sprintf("VPD page 0x%02x not implemented in T2 scope", pageCode))
	}
}

// inquiryVPD83 emits ONE NAA-6 Registered Extended designator
// derived from the volume identity. V2 has an ALUA branch (TPG /
// RTP descriptors); that's skipped in Batch 10.5 (ALUA is
// skip-list). V2's fallback stub NAA was a hardcoded constant —
// unusable for T2 because all volumes would report the same
// unique ID and Linux multipath / udev would conflate them
// (port plan §3.3 N1). Instead we derive a per-volume NAA from
// sha256(VolumeID)[:7] prefixed with 0x60 (NAA-6).
func (h *SCSIHandler) inquiryVPD83(allocLen uint16) SCSIResult {
	// Designator 1: NAA-6 identifier (8 bytes). Derive from the
	// frontend.Backend's captured VolumeID — stable across the
	// backend's lifetime and the only authoritative identity
	// the handler has access to (no extra config threading).
	naa := naaFromVolumeID(h.backend.Identity().VolumeID)
	naaDesc := []byte{
		0x01, // code set = binary
		0x03, // PIV=0, association=00 (logical unit), type=3 (NAA)
		0x00, // reserved
		0x08, // identifier length = 8 bytes
	}
	naaDesc = append(naaDesc, naa[:]...)

	data := make([]byte, 4+len(naaDesc))
	data[0] = 0x00
	data[1] = 0x83
	binary.BigEndian.PutUint16(data[2:4], uint16(len(naaDesc)))
	copy(data[4:], naaDesc)
	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: StatusGood, Data: data}
}

// naaFromVolumeID builds an 8-byte NAA-6 identifier from the
// handler's VolumeID. The high nibble of byte 0 is pinned to 0x6
// (NAA-6 Registered Extended). The remaining 60 bits are
// sha256(VolumeID) truncated to fit. Deterministic (same VolumeID
// → same NAA across process restarts) and collision-free for
// realistic volume counts (T2 single-volume is trivially safe).
func naaFromVolumeID(volumeID string) [8]byte {
	sum := sha256.Sum256([]byte(volumeID))
	var out [8]byte
	copy(out[:], sum[:8])
	// Force the high nibble to 0x6 (NAA-6). Preserve the low
	// nibble of byte 0 from the hash.
	out[0] = 0x60 | (out[0] & 0x0f)
	return out
}

func (h *SCSIHandler) readCapacity10() SCSIResult {
	totalBlocks := h.volumeSize / uint64(h.blockSize)
	data := make([]byte, 8)
	if totalBlocks > 0 {
		// Returns LBA of last block, not count. If >2^32 blocks,
		// V2 returns 0xFFFFFFFF to signal "use READ_CAPACITY(16)";
		// we preserve that.
		if totalBlocks > 0xFFFFFFFF {
			binary.BigEndian.PutUint32(data[0:4], 0xFFFFFFFF)
		} else {
			binary.BigEndian.PutUint32(data[0:4], uint32(totalBlocks-1))
		}
	}
	binary.BigEndian.PutUint32(data[4:8], h.blockSize)
	return SCSIResult{Status: StatusGood, Data: data}
}

// Batch 10.5: READ_CAPACITY(16). 32-byte response with 64-bit
// last-LBA + block size + flags.
//
// LBPME (byte 14 bit 7): 0 in T2 — must stay aligned with
// implemented surface. Batch 10.5 explicitly does NOT port
// UNMAP / VPD 0xB2 (port plan §3.2 skip list). Advertising
// LBPME=1 would tell the initiator "logical block provisioning
// management enabled" while the backing discovery surfaces
// (VPD 0xB0/0xB2) + operations (UNMAP) are absent. V2 sets
// LBPME=1 because V2 also implements UNMAP + VPD 0xB2; the
// "V2 parity" wording in an earlier draft was wrong — what V2
// parity-demands is that advertised ≡ implemented. For T2
// that means LBPME=0 until UNMAP + VPD 0xB2 are in-scope.
// (PM High / architect Medium finding, 2026-04-22.)
func (h *SCSIHandler) readCapacity16(cdb [16]byte) SCSIResult {
	// Architect Medium finding (2026-04-22): do NOT force
	// allocLen up to the full 32-byte response size. The
	// initiator's allocation length is the authoritative
	// upper bound per SPC-5 §6.6; returning more than asked
	// for is a protocol violation. If the caller supplies
	// allocLen=0, SPC-5 §6.6 says "no data shall be transferred"
	// — we return an empty Good response in that case.
	allocLen := binary.BigEndian.Uint32(cdb[10:14])
	totalBlocks := h.volumeSize / uint64(h.blockSize)
	data := make([]byte, 32)
	if totalBlocks > 0 {
		binary.BigEndian.PutUint64(data[0:8], totalBlocks-1)
	}
	binary.BigEndian.PutUint32(data[8:12], h.blockSize)
	// LBPME + LBPRZ stay zero — aligned with skip list §3.2.
	if allocLen < uint32(len(data)) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: StatusGood, Data: data}
}

// Batch 10.5: MODE_SENSE(6) / MODE_SENSE(10). Build minimal mode
// page data for pages 0x08 (Caching) and 0x0A (Control); 0x3F
// returns all. Other pages produce an empty mode page set,
// matching V2. No mode_select persistence (MODE_SELECT is no-op
// Good). The header shape differs between 6-byte and 10-byte
// variants; page body is shared via buildModePages.

func (h *SCSIHandler) modeSense6(cdb [16]byte) SCSIResult {
	allocLen := cdb[4]
	if allocLen == 0 {
		allocLen = 4
	}
	pages := h.buildModePages(cdb[2] & 0x3f)
	data := make([]byte, 4+len(pages))
	data[0] = byte(3 + len(pages)) // Mode data length (everything after byte 0)
	// data[1..3]: medium type / device-specific / block descriptor length = 0
	copy(data[4:], pages)
	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: StatusGood, Data: data}
}

func (h *SCSIHandler) modeSense10(cdb [16]byte) SCSIResult {
	allocLen := binary.BigEndian.Uint16(cdb[7:9])
	if allocLen == 0 {
		allocLen = 8
	}
	pages := h.buildModePages(cdb[2] & 0x3f)
	data := make([]byte, 8+len(pages))
	binary.BigEndian.PutUint16(data[0:2], uint16(6+len(pages))) // Mode data length
	// data[2..7]: medium type / device-specific / LONGLBA flag / block descriptor length = 0
	copy(data[8:], pages)
	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: StatusGood, Data: data}
}

func (h *SCSIHandler) buildModePages(pageCode uint8) []byte {
	switch pageCode {
	case 0x08: // Caching
		return modePage08()
	case 0x0a: // Control
		return modePage0A()
	case 0x3f: // All pages
		var pages []byte
		pages = append(pages, modePage08()...)
		pages = append(pages, modePage0A()...)
		return pages
	default:
		return nil
	}
}

// modePage08 is the Caching mode page (SBC-4 §7.5.5). WCE=1 tells
// Windows/Linux the device supports a write cache — matches V2's
// Windows-tuned value. SYNC_CACHE is a no-op in T2 (memback is
// non-durable per port plan §3.3 N2); WCE=1 just means the
// kernel may send flushes more eagerly, which we accept.
func modePage08() []byte {
	page := make([]byte, 20)
	page[0] = 0x08 // page code
	page[1] = 18   // page length (20 - 2)
	page[2] = 0x04 // WCE=1, RCD=0
	return page
}

// modePage0A is the Control mode page (SPC-5 §8.4.8). Default
// zero values are fine for T2.
func modePage0A() []byte {
	page := make([]byte, 12)
	page[0] = 0x0a // page code
	page[1] = 10   // page length (12 - 2)
	return page
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
