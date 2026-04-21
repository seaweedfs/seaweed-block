// Package iscsi is the T2 iSCSI target for the V3 frontend.
// It serves as the protocol adaptation between the iSCSI /
// SCSI wire and the T1 frontend.Backend contract. The package
// is a PURE consumer of authority truth — it never mints
// assignments and never imports core/authority. See
// sw-block/design/v3-phase-15-t2-sketch.md §3.
package iscsi

import "fmt"

// SCSI status byte values (SPC-5 §6). Only the subset we use
// directly is declared.
const (
	StatusGood           uint8 = 0x00
	StatusCheckCondition uint8 = 0x02
	StatusBusy           uint8 = 0x08
	StatusReservationConflict uint8 = 0x18
)

// Sense keys (SPC-5 §4.5.6).
const (
	SenseNone           uint8 = 0x00
	SenseRecoveredError uint8 = 0x01
	SenseNotReady       uint8 = 0x02
	SenseMediumError    uint8 = 0x03
	SenseHardwareError  uint8 = 0x04
	SenseIllegalRequest uint8 = 0x05
	SenseUnitAttention  uint8 = 0x06
	SenseAborted        uint8 = 0x0b
)

// ASC/ASCQ (selected — full tables live in spc-5 Annex F).
const (
	ASCInvalidOpcode       uint8 = 0x20
	ASCInvalidFieldInCDB   uint8 = 0x24
	ASCLBAOutOfRange       uint8 = 0x21
	ASCNotReady            uint8 = 0x04
	ASCNotReadyManualIntv  uint8 = 0x03

	// Stale primary: we choose "NOT READY / CAUSE NOT REPORTABLE"
	// (0x04/0x00) with a distinct sense key so initiators treat
	// the stale-lineage window as a transient not-ready rather
	// than a persistent hardware fault. The exact ASC is an
	// implementation choice per sketch §6 ("deterministic
	// mapping"); this file is the single source of truth.
	ASCStaleLineage     uint8 = 0x04
	ASCQStaleLineage    uint8 = 0x00
)

// SCSIError is the concrete error type returned up the protocol
// stack when a SCSI command cannot be completed successfully.
// It carries the full status + sense tuple an initiator would
// see on the wire, so L0 unit tests can assert the exact shape
// without decoding a PDU.
//
// Spec §4 (T2 test-spec): "stale WRITE returns CHECK CONDITION
// with non-empty sense key; stale READ returns CHECK CONDITION
// and does NOT return data to initiator".
type SCSIError struct {
	Status    uint8
	SenseKey  uint8
	ASC       uint8
	ASCQ      uint8
	Reason    string // free-form; diagnostic only
}

func (e *SCSIError) Error() string {
	return fmt.Sprintf("iscsi: status=0x%02x sense=0x%02x asc=0x%02x ascq=0x%02x reason=%s",
		e.Status, e.SenseKey, e.ASC, e.ASCQ, e.Reason)
}

// NewCheckConditionStaleLineage is the canonical constructor
// used by the SCSI dispatch layer when frontend.ErrStalePrimary
// surfaces from a backend Read/Write. Centralized so the exact
// status/sense mapping is a single symbol the test suite can
// assert against.
func NewCheckConditionStaleLineage() *SCSIError {
	return &SCSIError{
		Status:   StatusCheckCondition,
		SenseKey: SenseNotReady,
		ASC:      ASCStaleLineage,
		ASCQ:     ASCQStaleLineage,
		Reason:   "stale primary lineage",
	}
}

// NewCheckConditionIllegalRequest is used for malformed CDBs:
// negative offsets, LBA out of range, etc.
func NewCheckConditionIllegalRequest(asc, ascq uint8, reason string) *SCSIError {
	return &SCSIError{
		Status:   StatusCheckCondition,
		SenseKey: SenseIllegalRequest,
		ASC:      asc,
		ASCQ:     ascq,
		Reason:   reason,
	}
}
