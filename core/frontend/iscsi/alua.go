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

	binary.BigEndian.PutUint32(data[0:4], 12) // return data length, excluding header
	data[4] = uint8(h.alua.ALUAState()) & 0x0f
	data[5] = 0x8f // A/O, A/NO, Standby, Unavailable, Transitioning supported.
	binary.BigEndian.PutUint16(data[8:10], h.alua.TargetPortGroupID())
	data[11] = 0x01 // one target port descriptor
	binary.BigEndian.PutUint16(data[14:16], h.alua.RelativeTargetPortID())

	if allocLen > 0 && allocLen < uint32(len(data)) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: StatusGood, Data: data}
}
