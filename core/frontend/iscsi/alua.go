package iscsi

import "encoding/binary"

// ALUAState is the SPC asymmetric logical unit access state exposed to OS
// initiators. It is protocol reporting only; authority remains outside iSCSI.
type ALUAState uint8

const (
	ALUAActiveOptimized    ALUAState = 0x00
	ALUAActiveNonOptimized ALUAState = 0x01
	ALUAStandby            ALUAState = 0x02
	ALUAUnavailable        ALUAState = 0x0e
	ALUATransitioning      ALUAState = 0x0f
)

func (s ALUAState) String() string {
	switch s {
	case ALUAActiveOptimized:
		return "active_optimized"
	case ALUAActiveNonOptimized:
		return "active_non_optimized"
	case ALUAStandby:
		return "standby"
	case ALUAUnavailable:
		return "unavailable"
	case ALUATransitioning:
		return "transitioning"
	default:
		return "unknown"
	}
}

// ALUAProvider is an optional read-only state source for SCSIHandler. It must
// consume frontend facts only; it must not mint authority or mutate placement.
type ALUAProvider interface {
	ALUAState() ALUAState
	TargetPortGroupID() uint16
	RelativeTargetPortID() uint16
	DeviceNAA() [8]byte
}

func (h *SCSIHandler) aluaWriteReject() *SCSIResult {
	if h.alua == nil {
		return nil
	}
	switch h.alua.ALUAState() {
	case ALUAActiveOptimized, ALUAActiveNonOptimized:
		return nil
	default:
		r := SCSIResult{
			Status:   StatusCheckCondition,
			SenseKey: SenseNotReady,
			ASC:      ASCNotReady,
			ASCQ:     ASCQTargetPortStandby,
			Reason:   "ALUA path is not writable",
		}
		return &r
	}
}

func (h *SCSIHandler) maintenanceIn(cdb [16]byte) SCSIResult {
	sa := cdb[1] & 0x1f
	switch sa {
	case SaiReportTargetPortGroups:
		return h.reportTargetPortGroups(cdb)
	default:
		return illegalRequest(ASCInvalidOpcode, 0x00, "unsupported MAINTENANCE IN service action")
	}
}

func (h *SCSIHandler) reportTargetPortGroups(cdb [16]byte) SCSIResult {
	if h.alua == nil {
		return illegalRequest(ASCInvalidOpcode, 0x00, "REPORT TARGET PORT GROUPS requires ALUA provider")
	}
	allocLen := binary.BigEndian.Uint32(cdb[6:10])
	data := make([]byte, 16)

	// Layout per SPC-4 §6.27 Table 175. The 4-byte parameter list header
	// is followed by one Target Port Group descriptor (12 bytes), with
	// one Target Port descriptor inside it (4 bytes).
	//
	// data[0:4] : parameter list length = 12
	// data[4]   : descriptor byte 0 — PREF | reserved | AAS (bits 3:0)
	// data[5]   : descriptor byte 1 — T_SUP|O_SUP|LBD_SUP|U_SUP|S_SUP|AN_SUP|AO_SUP|Reserved
	// data[6:8] : descriptor bytes 2-3 — Target Port Group
	// data[8]   : descriptor byte 4 — Reserved
	// data[9]   : descriptor byte 5 — Status Code
	// data[10]  : descriptor byte 6 — Vendor Unique
	// data[11]  : descriptor byte 7 — Target Port Count (= 1)
	// data[12:16]: target port descriptor 0
	//   data[12:14] : obsolete
	//   data[14:16] : Relative Target Port Identifier
	binary.BigEndian.PutUint32(data[0:4], 12)
	data[4] = uint8(h.alua.ALUAState()) & 0x0f
	// Support flags: T_SUP (bit 7) | U_SUP (bit 4) | S_SUP (bit 3) | AN_SUP (bit 2) | AO_SUP (bit 1).
	// bit 0 is reserved and must be 0.
	data[5] = 0x9e
	binary.BigEndian.PutUint16(data[6:8], h.alua.TargetPortGroupID())
	data[11] = 0x01
	binary.BigEndian.PutUint16(data[14:16], h.alua.RelativeTargetPortID())

	if allocLen > 0 && allocLen < uint32(len(data)) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: StatusGood, Data: data}
}
