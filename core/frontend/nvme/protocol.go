package nvme

// NVMe/TCP wire protocol primitives (NVMe-oF spec §3, NVMe TCP
// Transport spec §3). Adapted from
// weed/storage/blockvol/nvme/protocol.go — wire format is
// byte-for-byte the V2 shape so a future merge or shared client
// is straightforward.

import (
	"encoding/binary"
	"fmt"
)

// ---------- PDU type codes ----------

const (
	pduICReq       uint8 = 0x0 // Initialization Connection Request
	pduICResp      uint8 = 0x1 // Initialization Connection Response
	pduH2CTermReq  uint8 = 0x2 // Host-to-Controller Termination Request
	pduC2HTermReq  uint8 = 0x3 // Controller-to-Host Termination Request
	pduCapsuleCmd  uint8 = 0x4 // NVMe Capsule Command
	pduCapsuleResp uint8 = 0x5 // NVMe Capsule Response
	pduH2CData     uint8 = 0x6 // Host-to-Controller Data Transfer
	pduC2HData     uint8 = 0x7 // Controller-to-Host Data Transfer
	pduR2T         uint8 = 0x9 // Ready-to-Transfer
)

// ---------- Admin / Fabric command opcodes ----------

const (
	// NVM admin opcode subset. Batch 11a introduces Identify
	// (0x06). SetFeatures / GetFeatures / KeepAlive / AsyncEventRequest
	// land with Batch 11b per port plan §7.
	adminGetLogPage    uint8 = 0x02
	adminIdentify      uint8 = 0x06
	adminAbort         uint8 = 0x08
	adminSetFeatures   uint8 = 0x09
	adminGetFeatures   uint8 = 0x0A
	adminAsyncEvent    uint8 = 0x0C
	adminKeepAlive     uint8 = 0x18
	adminFabric        uint8 = 0x7F // Fabric-specific commands
)

// Fabric command types (FCType).
const (
	fcPropertySet uint8 = 0x00
	fcConnect     uint8 = 0x01
	fcPropertyGet uint8 = 0x04
	fcDisconnect  uint8 = 0x08
)

// Identify CNS values (Batch 11a).
const (
	cnsIdentifyNamespace  uint8 = 0x00
	cnsIdentifyController uint8 = 0x01
	cnsActiveNSList       uint8 = 0x02
	cnsNSDescriptorList   uint8 = 0x03
)

// Misc constants used by Identify builders. Only the versions
// we actually advertise are declared here; adding dead versions
// violates port-model discipline (port plan §5 stop-rule #4
// "Advertised ≡ implemented").
const (
	identifySize  = 4096       // Identify response size (fixed)
	nvmeVersion13 = 0x00010300 // NVMe 1.3 — what 11a's VS register advertises (D11)
)

// ---------- Sizes ----------

const (
	commonHeaderSize = 8
	maxHeaderSize    = 128

	capsuleCmdSize  = 64
	capsuleRespSize = 16
	c2hDataHdrSize  = 16
	h2cDataHdrSize  = 16
	r2tHdrSize      = 16
	icBodySize      = 120
	connectDataSize = 1024

	c2hFlagLast uint8 = 0x04 // C2HData last-PDU flag
)

// ---------- CommonHeader (8 bytes) ----------

// CommonHeader is the 8-byte preamble of every NVMe/TCP PDU.
type CommonHeader struct {
	Type         uint8
	Flags        uint8
	HeaderLength uint8
	DataOffset   uint8
	DataLength   uint32
}

func (h *CommonHeader) Marshal(buf []byte) {
	buf[0] = h.Type
	buf[1] = h.Flags
	buf[2] = h.HeaderLength
	buf[3] = h.DataOffset
	binary.LittleEndian.PutUint32(buf[4:], h.DataLength)
}

func (h *CommonHeader) Unmarshal(buf []byte) {
	h.Type = buf[0]
	h.Flags = buf[1]
	h.HeaderLength = buf[2]
	h.DataOffset = buf[3]
	h.DataLength = binary.LittleEndian.Uint32(buf[4:])
}

func (h *CommonHeader) String() string {
	return fmt.Sprintf("PDU{type=0x%x hlen=%d doff=%d dlen=%d}",
		h.Type, h.HeaderLength, h.DataOffset, h.DataLength)
}

// PDU is implemented by every PDU-specific header.
type PDU interface {
	Marshal([]byte)
	Unmarshal([]byte)
}

// ---------- ICRequest / ICResponse (120-byte body) ----------

type ICRequest struct {
	PDUFormatVersion uint16
	PDUDataAlignment uint8
	PDUDataDigest    uint8
	PDUMaxR2T        uint32
}

func (r *ICRequest) Marshal(buf []byte) {
	for i := range buf[:icBodySize] {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[0:], r.PDUFormatVersion)
	buf[2] = r.PDUDataAlignment
	buf[3] = r.PDUDataDigest
	binary.LittleEndian.PutUint32(buf[4:], r.PDUMaxR2T)
}

func (r *ICRequest) Unmarshal(buf []byte) {
	r.PDUFormatVersion = binary.LittleEndian.Uint16(buf[0:])
	r.PDUDataAlignment = buf[2]
	r.PDUDataDigest = buf[3]
	r.PDUMaxR2T = binary.LittleEndian.Uint32(buf[4:])
}

type ICResponse struct {
	PDUFormatVersion uint16
	PDUDataAlignment uint8
	PDUDataDigest    uint8
	MaxH2CDataLength uint32
}

func (r *ICResponse) Marshal(buf []byte) {
	for i := range buf[:icBodySize] {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[0:], r.PDUFormatVersion)
	buf[2] = r.PDUDataAlignment
	buf[3] = r.PDUDataDigest
	binary.LittleEndian.PutUint32(buf[4:], r.MaxH2CDataLength)
}

func (r *ICResponse) Unmarshal(buf []byte) {
	r.PDUFormatVersion = binary.LittleEndian.Uint16(buf[0:])
	r.PDUDataAlignment = buf[2]
	r.PDUDataDigest = buf[3]
	r.MaxH2CDataLength = binary.LittleEndian.Uint32(buf[4:])
}

// ---------- CapsuleCommand (64-byte specific header) ----------

type CapsuleCommand struct {
	OpCode uint8
	PRP    uint8
	CID    uint16
	FCType uint8 // Fabric command type (only when OpCode=0x7F)
	NSID   uint32
	DPTR   [16]byte
	D10    uint32
	D11    uint32
	D12    uint32
	D13    uint32
	D14    uint32
	D15    uint32
}

// Lba returns the starting LBA from D10:D11.
func (c *CapsuleCommand) Lba() uint64 { return uint64(c.D11)<<32 | uint64(c.D10) }

// LbaLength returns NLB (D12 low 16 bits, zero-based on wire,
// converted to one-based here for ergonomic use).
func (c *CapsuleCommand) LbaLength() uint32 { return c.D12&0xFFFF + 1 }

func (c *CapsuleCommand) Marshal(buf []byte) {
	for i := range buf[:capsuleCmdSize] {
		buf[i] = 0
	}
	buf[0] = c.OpCode
	buf[1] = c.PRP
	binary.LittleEndian.PutUint16(buf[2:], c.CID)
	if c.OpCode == adminFabric {
		buf[4] = c.FCType
	} else {
		binary.LittleEndian.PutUint32(buf[4:], c.NSID)
	}
	copy(buf[24:40], c.DPTR[:])
	binary.LittleEndian.PutUint32(buf[40:], c.D10)
	binary.LittleEndian.PutUint32(buf[44:], c.D11)
	binary.LittleEndian.PutUint32(buf[48:], c.D12)
	binary.LittleEndian.PutUint32(buf[52:], c.D13)
	binary.LittleEndian.PutUint32(buf[56:], c.D14)
	binary.LittleEndian.PutUint32(buf[60:], c.D15)
}

func (c *CapsuleCommand) Unmarshal(buf []byte) {
	c.OpCode = buf[0]
	c.PRP = buf[1]
	c.CID = binary.LittleEndian.Uint16(buf[2:])
	c.FCType = buf[4]
	c.NSID = binary.LittleEndian.Uint32(buf[4:])
	copy(c.DPTR[:], buf[24:40])
	c.D10 = binary.LittleEndian.Uint32(buf[40:])
	c.D11 = binary.LittleEndian.Uint32(buf[44:])
	c.D12 = binary.LittleEndian.Uint32(buf[48:])
	c.D13 = binary.LittleEndian.Uint32(buf[52:])
	c.D14 = binary.LittleEndian.Uint32(buf[56:])
	c.D15 = binary.LittleEndian.Uint32(buf[60:])
}

// ---------- CapsuleResponse (16-byte specific header / NVMe CQE) ----------

type CapsuleResponse struct {
	DW0     uint32
	DW1     uint32
	SQHD    uint16
	QueueID uint16
	CID     uint16
	Status  uint16
}

func (r *CapsuleResponse) Marshal(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:], r.DW0)
	binary.LittleEndian.PutUint32(buf[4:], r.DW1)
	binary.LittleEndian.PutUint16(buf[8:], r.SQHD)
	binary.LittleEndian.PutUint16(buf[10:], r.QueueID)
	binary.LittleEndian.PutUint16(buf[12:], r.CID)
	binary.LittleEndian.PutUint16(buf[14:], r.Status)
}

func (r *CapsuleResponse) Unmarshal(buf []byte) {
	r.DW0 = binary.LittleEndian.Uint32(buf[0:])
	r.DW1 = binary.LittleEndian.Uint32(buf[4:])
	r.SQHD = binary.LittleEndian.Uint16(buf[8:])
	r.QueueID = binary.LittleEndian.Uint16(buf[10:])
	r.CID = binary.LittleEndian.Uint16(buf[12:])
	r.Status = binary.LittleEndian.Uint16(buf[14:])
}

// ---------- C2HDataHeader / H2CDataHeader / R2THeader (16 bytes each) ----------

type C2HDataHeader struct {
	CCCID uint16
	DATAO uint32
	DATAL uint32
}

func (h *C2HDataHeader) Marshal(buf []byte) {
	for i := range buf[:c2hDataHdrSize] {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[0:], h.CCCID)
	binary.LittleEndian.PutUint32(buf[4:], h.DATAO)
	binary.LittleEndian.PutUint32(buf[8:], h.DATAL)
}

func (h *C2HDataHeader) Unmarshal(buf []byte) {
	h.CCCID = binary.LittleEndian.Uint16(buf[0:])
	h.DATAO = binary.LittleEndian.Uint32(buf[4:])
	h.DATAL = binary.LittleEndian.Uint32(buf[8:])
}

type R2THeader struct {
	CCCID uint16
	TAG   uint16
	DATAO uint32
	DATAL uint32
}

func (h *R2THeader) Marshal(buf []byte) {
	for i := range buf[:r2tHdrSize] {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[0:], h.CCCID)
	binary.LittleEndian.PutUint16(buf[2:], h.TAG)
	binary.LittleEndian.PutUint32(buf[4:], h.DATAO)
	binary.LittleEndian.PutUint32(buf[8:], h.DATAL)
}

func (h *R2THeader) Unmarshal(buf []byte) {
	h.CCCID = binary.LittleEndian.Uint16(buf[0:])
	h.TAG = binary.LittleEndian.Uint16(buf[2:])
	h.DATAO = binary.LittleEndian.Uint32(buf[4:])
	h.DATAL = binary.LittleEndian.Uint32(buf[8:])
}

type H2CDataHeader struct {
	CCCID uint16
	TAG   uint16
	DATAO uint32
	DATAL uint32
}

func (h *H2CDataHeader) Marshal(buf []byte) {
	for i := range buf[:h2cDataHdrSize] {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[0:], h.CCCID)
	binary.LittleEndian.PutUint16(buf[2:], h.TAG)
	binary.LittleEndian.PutUint32(buf[4:], h.DATAO)
	binary.LittleEndian.PutUint32(buf[8:], h.DATAL)
}

func (h *H2CDataHeader) Unmarshal(buf []byte) {
	h.CCCID = binary.LittleEndian.Uint16(buf[0:])
	h.TAG = binary.LittleEndian.Uint16(buf[2:])
	h.DATAO = binary.LittleEndian.Uint32(buf[4:])
	h.DATAL = binary.LittleEndian.Uint32(buf[8:])
}

// ---------- ConnectData (1024 bytes, payload of Fabric Connect) ----------

type ConnectData struct {
	HostID  [16]byte
	CNTLID  uint16
	SubNQN  string
	HostNQN string
}

func (d *ConnectData) Marshal(buf []byte) {
	for i := range buf[:connectDataSize] {
		buf[i] = 0
	}
	copy(buf[0:16], d.HostID[:])
	binary.LittleEndian.PutUint16(buf[16:], d.CNTLID)
	copyNQN(buf[256:512], d.SubNQN)
	copyNQN(buf[512:768], d.HostNQN)
}

func (d *ConnectData) Unmarshal(buf []byte) {
	copy(d.HostID[:], buf[0:16])
	d.CNTLID = binary.LittleEndian.Uint16(buf[16:])
	d.SubNQN = extractNQN(buf[256:512])
	d.HostNQN = extractNQN(buf[512:768])
}

func copyNQN(dst []byte, s string) {
	n := copy(dst, s)
	if n < len(dst) {
		dst[n] = 0
	}
}

func extractNQN(buf []byte) string {
	for i, b := range buf {
		if b == 0 {
			return string(buf[:i])
		}
	}
	return string(buf)
}

// ---------- Status word encoding helpers ----------

// MakeStatusField packs SCT + SC + DNR into the 16-bit CQE
// status field. The Phase Tag bit (bit 0) is NOT set here —
// that's a queue-level concern handled by the caller. T2's
// minimal session keeps Phase Tag at 0 throughout.
func MakeStatusField(sct, sc uint8, dnr bool) uint16 {
	w := uint16(sct&0x07)<<9 | uint16(sc)<<1
	if dnr {
		w |= 1 << 15
	}
	return w
}
