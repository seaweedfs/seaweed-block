package iscsi

// PDU framing for iSCSI (RFC 7143 §12).
//
// Adapted from weed/storage/blockvol/iscsi/pdu.go with ONE
// substantive change: the V2 file also declared SCSI status
// byte constants (SCSIStatusGood/CheckCond/Busy/ResvConflict)
// alongside the PDU-layer opcodes. Those SCSI-layer constants
// live in errors.go (StatusGood / StatusCheckCondition / ...)
// in the V3 port; this file is PDU-only. Otherwise the wire
// format is byte-for-byte the V2 shape and tests pin the
// identical layout.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// BHSLength — Basic Header Segment (RFC 7143 §12.1).
const BHSLength = 48

// Initiator opcodes (RFC 7143 §12.1.1).
const (
	OpNOPOut       uint8 = 0x00
	OpSCSICmd      uint8 = 0x01
	OpSCSITaskMgmt uint8 = 0x02
	OpLoginReq     uint8 = 0x03
	OpTextReq      uint8 = 0x04
	OpSCSIDataOut  uint8 = 0x05
	OpLogoutReq    uint8 = 0x06
	OpSNACKReq     uint8 = 0x0c
)

// Target opcodes (RFC 7143 §12.1.1).
const (
	OpNOPIn        uint8 = 0x20
	OpSCSIResp     uint8 = 0x21
	OpSCSITaskResp uint8 = 0x22
	OpLoginResp    uint8 = 0x23
	OpTextResp     uint8 = 0x24
	OpSCSIDataIn   uint8 = 0x25
	OpLogoutResp   uint8 = 0x26
	OpR2T          uint8 = 0x31
	OpAsyncMsg     uint8 = 0x32
	OpReject       uint8 = 0x3f
)

// BHS flag masks.
const (
	opcMask = 0x3f
	FlagI   = 0x40 // Immediate (byte 0)
	FlagF   = 0x80 // Final (byte 1)
	FlagR   = 0x40 // Read (byte 1, SCSI cmd)
	FlagW   = 0x20 // Write (byte 1, SCSI cmd)
	FlagC   = 0x40 // Continue (byte 1, login)
	FlagT   = 0x80 // Transit (byte 1, login)
	FlagS   = 0x01 // Status (byte 1, Data-In)
	FlagU   = 0x02 // Underflow (byte 1, SCSI Resp)
	FlagO   = 0x04 // Overflow (byte 1, SCSI Resp)
	FlagBiU = 0x08 // Bidi underflow
	FlagBiO = 0x10 // Bidi overflow
	FlagA   = 0x40 // Acknowledge (byte 1, Data-Out)
)

// Login stage constants (CSG/NSG, 2-bit fields in byte 1).
const (
	StageSecurityNeg uint8 = 0
	StageLoginOp     uint8 = 1
	StageFullFeature uint8 = 3
)

// iSCSI response byte values (RFC 7143 §11.4.2).
const ISCSIRespCompleted uint8 = 0x00

// MaxDataSegmentLength caps what we'll accept. The iSCSI data
// segment length is a 3-byte field (max 16 MiB - 1). 8 MiB is
// well above typical MaxRecvDataSegmentLength negotiation
// results, and bounds memory pressure from a hostile peer.
const MaxDataSegmentLength = 8 * 1024 * 1024

var (
	ErrPDUTruncated     = errors.New("iscsi: PDU truncated")
	ErrPDUTooLarge      = errors.New("iscsi: data segment exceeds maximum length")
	ErrInvalidAHSLength = errors.New("iscsi: invalid AHS length (not multiple of 4)")
	ErrUnknownOpcode    = errors.New("iscsi: unknown opcode")
)

// PDU represents a full iSCSI Protocol Data Unit.
type PDU struct {
	BHS         [BHSLength]byte
	AHS         []byte // Additional Header Segment (multiple of 4 bytes)
	DataSegment []byte // Data segment (padded to 4-byte boundary on wire)
}

// --- BHS field accessors ---

func (p *PDU) Opcode() uint8 { return p.BHS[0] & opcMask }

func (p *PDU) SetOpcode(op uint8) {
	p.BHS[0] = (p.BHS[0] & ^uint8(opcMask)) | (op & opcMask)
}

func (p *PDU) Immediate() bool { return p.BHS[0]&FlagI != 0 }

func (p *PDU) SetImmediate(v bool) {
	if v {
		p.BHS[0] |= FlagI
	} else {
		p.BHS[0] &^= FlagI
	}
}

func (p *PDU) OpSpecific1() uint8      { return p.BHS[1] }
func (p *PDU) SetOpSpecific1(v uint8)  { p.BHS[1] = v }
func (p *PDU) TotalAHSLength() uint8   { return p.BHS[4] }

func (p *PDU) DataSegmentLength() uint32 {
	return uint32(p.BHS[5])<<16 | uint32(p.BHS[6])<<8 | uint32(p.BHS[7])
}

func (p *PDU) SetDataSegmentLength(n uint32) {
	p.BHS[5] = byte(n >> 16)
	p.BHS[6] = byte(n >> 8)
	p.BHS[7] = byte(n)
}

func (p *PDU) LUN() uint64       { return binary.BigEndian.Uint64(p.BHS[8:16]) }
func (p *PDU) SetLUN(lun uint64) { binary.BigEndian.PutUint64(p.BHS[8:16], lun) }

func (p *PDU) InitiatorTaskTag() uint32       { return binary.BigEndian.Uint32(p.BHS[16:20]) }
func (p *PDU) SetInitiatorTaskTag(tag uint32) { binary.BigEndian.PutUint32(p.BHS[16:20], tag) }

func (p *PDU) Field32(offset int) uint32 {
	return binary.BigEndian.Uint32(p.BHS[offset : offset+4])
}
func (p *PDU) SetField32(offset int, v uint32) {
	binary.BigEndian.PutUint32(p.BHS[offset:offset+4], v)
}

// Named BHS field accessors.
func (p *PDU) TSIH() uint16      { return binary.BigEndian.Uint16(p.BHS[14:16]) }
func (p *PDU) SetTSIH(v uint16)  { binary.BigEndian.PutUint16(p.BHS[14:16], v) }

func (p *PDU) CmdSN() uint32      { return binary.BigEndian.Uint32(p.BHS[24:28]) }
func (p *PDU) SetCmdSN(v uint32)  { binary.BigEndian.PutUint32(p.BHS[24:28], v) }

func (p *PDU) ExpStatSN() uint32     { return binary.BigEndian.Uint32(p.BHS[28:32]) }
func (p *PDU) SetExpStatSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[28:32], v) }

func (p *PDU) StatSN() uint32     { return binary.BigEndian.Uint32(p.BHS[24:28]) }
func (p *PDU) SetStatSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[24:28], v) }

func (p *PDU) ExpCmdSN() uint32     { return binary.BigEndian.Uint32(p.BHS[28:32]) }
func (p *PDU) SetExpCmdSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[28:32], v) }

func (p *PDU) MaxCmdSN() uint32     { return binary.BigEndian.Uint32(p.BHS[32:36]) }
func (p *PDU) SetMaxCmdSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[32:36], v) }

func (p *PDU) DataSN() uint32     { return binary.BigEndian.Uint32(p.BHS[36:40]) }
func (p *PDU) SetDataSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[36:40], v) }

func (p *PDU) BufferOffset() uint32     { return binary.BigEndian.Uint32(p.BHS[40:44]) }
func (p *PDU) SetBufferOffset(v uint32) { binary.BigEndian.PutUint32(p.BHS[40:44], v) }

func (p *PDU) R2TSN() uint32     { return binary.BigEndian.Uint32(p.BHS[36:40]) }
func (p *PDU) SetR2TSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[36:40], v) }

func (p *PDU) DesiredDataLength() uint32     { return binary.BigEndian.Uint32(p.BHS[44:48]) }
func (p *PDU) SetDesiredDataLength(v uint32) { binary.BigEndian.PutUint32(p.BHS[44:48], v) }

func (p *PDU) ExpectedDataTransferLength() uint32 {
	return binary.BigEndian.Uint32(p.BHS[20:24])
}
func (p *PDU) SetExpectedDataTransferLength(v uint32) {
	binary.BigEndian.PutUint32(p.BHS[20:24], v)
}

func (p *PDU) TargetTransferTag() uint32     { return binary.BigEndian.Uint32(p.BHS[20:24]) }
func (p *PDU) SetTargetTransferTag(v uint32) { binary.BigEndian.PutUint32(p.BHS[20:24], v) }

func (p *PDU) ISID() [6]byte {
	var id [6]byte
	copy(id[:], p.BHS[8:14])
	return id
}

func (p *PDU) SetISID(id [6]byte) { copy(p.BHS[8:14], id[:]) }

func (p *PDU) CDB() [16]byte {
	var cdb [16]byte
	copy(cdb[:], p.BHS[32:48])
	return cdb
}

func (p *PDU) SetCDB(cdb [16]byte) { copy(p.BHS[32:48], cdb[:]) }

func (p *PDU) ResidualCount() uint32     { return binary.BigEndian.Uint32(p.BHS[44:48]) }
func (p *PDU) SetResidualCount(v uint32) { binary.BigEndian.PutUint32(p.BHS[44:48], v) }

// --- Wire I/O ---

// pad4 rounds n up to the next multiple of 4.
func pad4(n uint32) uint32 { return (n + 3) &^ 3 }

// ReadPDU reads a complete PDU from r.
func ReadPDU(r io.Reader) (*PDU, error) {
	p := &PDU{}

	if _, err := io.ReadFull(r, p.BHS[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, ErrPDUTruncated
		}
		return nil, err
	}

	ahsLen := uint32(p.TotalAHSLength()) * 4
	if ahsLen > 0 {
		p.AHS = make([]byte, ahsLen)
		if _, err := io.ReadFull(r, p.AHS); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil, ErrPDUTruncated
			}
			return nil, err
		}
	}

	dsLen := p.DataSegmentLength()
	if dsLen > MaxDataSegmentLength {
		return nil, fmt.Errorf("%w: %d bytes", ErrPDUTooLarge, dsLen)
	}

	if dsLen > 0 {
		paddedLen := pad4(dsLen)
		buf := make([]byte, paddedLen)
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil, ErrPDUTruncated
			}
			return nil, err
		}
		p.DataSegment = buf[:dsLen]
	}

	return p, nil
}

// WritePDU writes a complete PDU to w, with proper padding.
func WritePDU(w io.Writer, p *PDU) error {
	if len(p.AHS) > 0 {
		if len(p.AHS)%4 != 0 {
			return ErrInvalidAHSLength
		}
		p.BHS[4] = uint8(len(p.AHS) / 4)
	} else {
		p.BHS[4] = 0
	}
	p.SetDataSegmentLength(uint32(len(p.DataSegment)))

	if _, err := w.Write(p.BHS[:]); err != nil {
		return err
	}
	if len(p.AHS) > 0 {
		if _, err := w.Write(p.AHS); err != nil {
			return err
		}
	}
	if len(p.DataSegment) > 0 {
		if _, err := w.Write(p.DataSegment); err != nil {
			return err
		}
		padLen := pad4(uint32(len(p.DataSegment))) - uint32(len(p.DataSegment))
		if padLen > 0 {
			var pad [3]byte
			if _, err := w.Write(pad[:padLen]); err != nil {
				return err
			}
		}
	}
	return nil
}

// OpcodeName returns a human-readable name for the given opcode.
func OpcodeName(op uint8) string {
	switch op {
	case OpNOPOut:
		return "NOP-Out"
	case OpSCSICmd:
		return "SCSI-Command"
	case OpSCSITaskMgmt:
		return "SCSI-Task-Mgmt"
	case OpLoginReq:
		return "Login-Request"
	case OpTextReq:
		return "Text-Request"
	case OpSCSIDataOut:
		return "SCSI-Data-Out"
	case OpLogoutReq:
		return "Logout-Request"
	case OpSNACKReq:
		return "SNACK-Request"
	case OpNOPIn:
		return "NOP-In"
	case OpSCSIResp:
		return "SCSI-Response"
	case OpSCSITaskResp:
		return "SCSI-Task-Mgmt-Response"
	case OpLoginResp:
		return "Login-Response"
	case OpTextResp:
		return "Text-Response"
	case OpSCSIDataIn:
		return "SCSI-Data-In"
	case OpLogoutResp:
		return "Logout-Response"
	case OpR2T:
		return "R2T"
	case OpAsyncMsg:
		return "Async-Message"
	case OpReject:
		return "Reject"
	default:
		return fmt.Sprintf("Unknown(0x%02x)", op)
	}
}

// --- Login PDU helpers ---

func (p *PDU) LoginCSG() uint8 { return (p.BHS[1] >> 2) & 0x03 }
func (p *PDU) LoginNSG() uint8 { return p.BHS[1] & 0x03 }

func (p *PDU) SetLoginStages(csg, nsg uint8) {
	p.BHS[1] = (p.BHS[1] & 0xF0) | ((csg & 0x03) << 2) | (nsg & 0x03)
}

func (p *PDU) LoginTransit() bool { return p.BHS[1]&FlagT != 0 }

func (p *PDU) SetLoginTransit(v bool) {
	if v {
		p.BHS[1] |= FlagT
	} else {
		p.BHS[1] &^= FlagT
	}
}

func (p *PDU) LoginContinue() bool { return p.BHS[1]&FlagC != 0 }

func (p *PDU) LoginStatusClass() uint8  { return p.BHS[36] }
func (p *PDU) LoginStatusDetail() uint8 { return p.BHS[37] }

func (p *PDU) SetLoginStatus(class, detail uint8) {
	p.BHS[36] = class
	p.BHS[37] = detail
}

// SCSI Response PDU helpers.
func (p *PDU) SCSIResponse() uint8       { return p.BHS[2] }
func (p *PDU) SetSCSIResponse(v uint8)   { p.BHS[2] = v }
func (p *PDU) SCSIStatusByte() uint8     { return p.BHS[3] }
func (p *PDU) SetSCSIStatusByte(v uint8) { p.BHS[3] = v }
