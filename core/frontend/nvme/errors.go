// Package nvme is the T2 NVMe/TCP target for the V3 frontend.
// Symmetric with core/frontend/iscsi: consumes authority truth
// through frontend.Provider, never mints assignments, never
// imports core/authority. See
// sw-block/design/v3-phase-15-t2-sketch.md §6.
package nvme

import "fmt"

// NVMe Status Code Types (SCT). Selected subset; full table
// lives in NVMe base spec §4.5.
const (
	SCTGeneric        uint8 = 0x0
	SCTCommandSpecific uint8 = 0x1
	SCTMediaDataInteg uint8 = 0x2
	SCTPathRelated    uint8 = 0x3
	SCTVendorSpecific uint8 = 0x7
)

// NVMe Status Codes within SCTGeneric.
const (
	SCSuccess              uint8 = 0x00
	SCInvalidOpcode        uint8 = 0x01
	SCInvalidField         uint8 = 0x02
	SCCommandIDConflict    uint8 = 0x03
	SCDataTransferError    uint8 = 0x04
	SCAbortedCmdFencing    uint8 = 0x05
	SCInternalError        uint8 = 0x06
	SCCommandAbort         uint8 = 0x07
	SCNamespaceNotReady    uint8 = 0x82
	SCLBAOutOfRange        uint8 = 0x80
	SCCapacityExceeded     uint8 = 0x81
)

// NVMe Status Codes within SCTPathRelated. These are the
// "ANA" (Asymmetric Namespace Access) family — the direct
// semantic match for a V3 authority move: a path that was
// serving is temporarily or persistently unavailable because
// the controller has transitioned state.
//
// We pick ANATransition (0x03) for stale-lineage because it
// corresponds to "the transition is in progress" — exactly
// what a V3 RefreshEndpoint / cross-replica reassign presents
// to the host. ANAInaccessible (0x02) is reserved for the
// closed-backend case so operators can tell the two fail
// modes apart in CQE logs (parity with iSCSI's 0x04/0x0A
// vs 0x04/0x00 distinction — see iscsi/errors.go).
const (
	SCPathInternalError      uint8 = 0x00
	SCPathAsymAccessPersistLoss uint8 = 0x01
	SCPathAsymAccessInaccessible uint8 = 0x02
	SCPathAsymAccessTransition   uint8 = 0x03
	SCPathControllerPathingErr uint8 = 0x60
	SCPathHostPathingErr     uint8 = 0x61
	SCPathCommandAbortHost   uint8 = 0x71
)

// StatusError is the Go error surface for non-success NVMe
// completion. Carries the SCT+SC tuple plus a diagnostic
// reason; the wire layer encodes these into the 16-bit CQE
// status field.
//
// Symmetric with iscsi.SCSIError in shape: L0 unit tests can
// assert the exact tuple without decoding a capsule.
type StatusError struct {
	SCT    uint8
	SC     uint8
	Reason string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("nvme: sct=0x%x sc=0x%02x reason=%s", e.SCT, e.SC, e.Reason)
}

// EncodeStatusField packs SCT + SC into the 16-bit NVMe CQE
// status field. Phase Tag / More / DNR bits are caller-owned
// and set at the wire layer (0 for T2 scope). Format per NVMe
// base spec §4.6.1:
//
//	bit  0        : Phase Tag
//	bits 1–8      : Status Code (SC)
//	bits 9–11     : Status Code Type (SCT)
//	bits 12–13    : CRD (reserved in T2)
//	bit  14       : More
//	bit  15       : DNR
func (e *StatusError) EncodeStatusField() uint16 {
	return uint16(e.SC)<<1 | uint16(e.SCT&0x7)<<9
}

// NewStaleLineage is the canonical constructor surfaced when
// the bound frontend.Backend reports ErrStalePrimary. ANA
// Transition (SCT=3, SC=3) semantically matches an in-progress
// authority move. Linux's nvme driver treats this as a path
// retry condition, which is the behavior we want once L2-OS
// lands.
func NewStaleLineage() *StatusError {
	return &StatusError{
		SCT:    SCTPathRelated,
		SC:     SCPathAsymAccessTransition,
		Reason: "stale primary lineage",
	}
}

// NewBackendClosed is the canonical constructor for
// ErrBackendClosed. Distinct from stale-lineage (Inaccessible
// vs Transition) so logs disambiguate.
func NewBackendClosed() *StatusError {
	return &StatusError{
		SCT:    SCTPathRelated,
		SC:     SCPathAsymAccessInaccessible,
		Reason: "backend closed",
	}
}

// NewInvalidField is used for malformed commands (unsupported
// opcode, LBA out of range, short data transfer, etc.). Caller
// picks the exact SC.
func NewInvalidField(sc uint8, reason string) *StatusError {
	return &StatusError{
		SCT:    SCTGeneric,
		SC:     sc,
		Reason: reason,
	}
}
