package storage

import "errors"

// T4c-2 recovery contract — unified `RecoveryEntry` + `ScanLBAs`
// surface across substrate impls. Promoted from T4c-pre-A POC code
// (`walstore_recovery_poc.go` + `smartwal/recovery_poc.go`); see
// `feedback_g1_pre_code_review.md` for the discipline that produced
// it. Each substrate emits entries under its own sub-mode semantic
// (memo §13.0a, §5.1):
//
//   - walstore: `wal_replay` — V2-faithful per-LSN; 3 writes to the
//     same LBA produce 3 entries (no dedup); LSN labels the entry
//     contemporaneous with the write
//   - smartwal: `state_convergence` — per-LBA dedup; LSN is scan-time
//     (memo §13.6 round-34 invariant `INV-REPL-RECOVERY-STREAM-
//     LSN-IS-SCAN-TIME`)
//
// The catch-up sender consumes ScanLBAs through the LogicalStorage
// interface and MUST NOT depend on either sub-mode's specifics —
// both deliver state-convergent results at the replica when the
// stream completes.

// RecoveryMode labels the substrate sub-mode that served a recovery
// session. Surfaces in observability (memo §5.1 / §13.0a) so
// operators can tell at a glance whether a replica was caught up via
// V2-faithful per-LSN replay or via per-LBA state convergence.
type RecoveryMode string

const (
	// RecoveryModeWALReplay — walstore sub-mode. V2-faithful per-LSN
	// emission; 3 writes to LBA=L produce 3 entries.
	RecoveryModeWALReplay RecoveryMode = "wal_replay"

	// RecoveryModeStateConvergence — smartwal sub-mode. Per-LBA
	// last-writer-wins; 3 writes to LBA=L produce 1 entry.
	RecoveryModeStateConvergence RecoveryMode = "state_convergence"

	// RecoveryModeNone is returned by substrates that do not retain
	// a recovery window (e.g. in-memory BlockStore). Catch-up callers
	// MUST NOT use ScanLBAs against substrates returning this mode
	// for any LSN below H — the substrate has no way to satisfy the
	// retention contract.
	RecoveryModeNone RecoveryMode = "none"
)

// ErrWALRecycled is returned by `LogicalStorage.ScanLBAs` when the
// requested `fromLSN` is at or below the substrate's retention
// boundary. The caller MUST NOT retry the catch-up at the same
// fromLSN — the recovery class needs to escalate to rebuild (T5
// territory).
//
// Per memo §13.0a: every substrate raises the same sentinel at its
// own retention boundary; the inner-loop sender does not need to
// distinguish substrates to honor the escalation rule (memo §7.4 +
// G-1 §3 row #8 placement decision §4.3).
var ErrWALRecycled = errors.New("storage: WAL recycled past requested LSN")

// RecoveryEntry is one entry emitted by `LogicalStorage.ScanLBAs`.
// Carries enough information for the catch-up sender to write a
// wire frame to the replica.
//
// `LSN` semantics depend on the substrate's recovery mode (memo
// §13.6 round-34): walstore emits the entry's contemporaneous LSN;
// smartwal emits the scan-time LSN. Callers that need V2-faithful
// per-LSN ordering MUST use a walstore substrate; callers that need
// state convergence semantics get either substrate.
//
// Flags is the entry type marker (write / trim / etc.); same byte
// values as the V2 walstore entry types, propagated transparently
// so wire frames remain decodable by existing replica handlers.
type RecoveryEntry struct {
	LSN   uint64
	LBA   uint32
	Flags uint8
	Data  []byte
}

// Recovery entry flag values. Match V2 walstore byte semantics so
// wire frames stay backward-compatible with replica handlers.
const (
	RecoveryEntryWrite uint8 = 1
	RecoveryEntryTrim  uint8 = 2
)

// --- T4d-1 (round-43 + v0.3 boundary discipline): structured recovery
//     failure kind ---
//
// `core/engine` MUST NOT import `core/storage` (memory rule
// `feedback_engine_no_storage_import.md`). The substrate-facing kind
// lives here; transport extracts it via `errors.As` and maps it to
// the engine-owned `engine.RecoveryFailureKind` at the boundary.

// StorageRecoveryFailureKind classifies why a substrate recovery
// operation (today: ScanLBAs) failed. Substrate-facing only — this
// type does NOT cross into engine. Transport reads it (via errors.As
// against `*RecoveryFailure`) and maps to `engine.RecoveryFailureKind`.
type StorageRecoveryFailureKind int

const (
	// StorageRecoveryFailureUnknown — default zero value; treat as
	// generic substrate error (retryable per engine policy).
	StorageRecoveryFailureUnknown StorageRecoveryFailureKind = iota

	// StorageRecoveryFailureWALRecycled — fromLSN at or below the
	// substrate's retention boundary. Tier-class change; transport
	// maps to engine kind that triggers Rebuild escalation.
	StorageRecoveryFailureWALRecycled

	// StorageRecoveryFailureSubstrateIO — substrate IO error
	// (read failure, decode failure mid-scan, etc.). Retryable.
	StorageRecoveryFailureSubstrateIO
)

// String returns the human-readable kind name for diagnostics. NOT
// parsed by anyone — typed branching uses the int constants directly.
func (k StorageRecoveryFailureKind) String() string {
	switch k {
	case StorageRecoveryFailureWALRecycled:
		return "WALRecycled"
	case StorageRecoveryFailureSubstrateIO:
		return "SubstrateIO"
	default:
		return "Unknown"
	}
}

// RecoveryFailure is the typed-error envelope substrates return from
// `ScanLBAs` when the failure is classifiable. Transport extracts the
// kind via `errors.As(err, &target)` and maps it to engine kind.
//
// Implements `error` and `Unwrap` so existing `errors.Is(err,
// ErrWALRecycled)` keeps working during the migration.
type RecoveryFailure struct {
	Kind   StorageRecoveryFailureKind
	Cause  error  // wrapped underlying error for diagnostics
	Detail string // free-form additional context
}

// Error returns a human-readable message. NOT parsed.
func (f *RecoveryFailure) Error() string {
	if f.Cause != nil {
		if f.Detail != "" {
			return "storage: " + f.Kind.String() + ": " + f.Detail + ": " + f.Cause.Error()
		}
		return "storage: " + f.Kind.String() + ": " + f.Cause.Error()
	}
	if f.Detail != "" {
		return "storage: " + f.Kind.String() + ": " + f.Detail
	}
	return "storage: " + f.Kind.String()
}

// Unwrap supports `errors.Is(err, storage.ErrWALRecycled)` so
// pre-T4d-1 callers continue to work during the migration window.
func (f *RecoveryFailure) Unwrap() error { return f.Cause }

// NewWALRecycledFailure wraps a substrate-internal recycle error in
// the typed envelope. Substrate impls call this from ScanLBAs.
func NewWALRecycledFailure(cause error, detail string) *RecoveryFailure {
	if cause == nil {
		cause = ErrWALRecycled
	}
	return &RecoveryFailure{
		Kind:   StorageRecoveryFailureWALRecycled,
		Cause:  cause,
		Detail: detail,
	}
}

// NewSubstrateIOFailure wraps a substrate IO error in the typed
// envelope. Use when ScanLBAs failed mid-scan with a read/decode error.
func NewSubstrateIOFailure(cause error, detail string) *RecoveryFailure {
	return &RecoveryFailure{
		Kind:   StorageRecoveryFailureSubstrateIO,
		Cause:  cause,
		Detail: detail,
	}
}

// --- T4d-1 Option C hybrid: per-LBA applied-LSN exposure ---

// --- G7-redo priority 2.5: WAL recycle gate via pin floor ---

// RecycleFloorSource is implemented by external coordinators that
// gate WAL recycle advancement based on per-replica commitments.
// Substrates consult this on every checkpoint advance / WAL tail
// move to ensure entries below the floor are retained for replicas
// with active recover sessions.
//
// Today this is `core/recovery.PeerShipCoordinator`'s
// `MinPinAcrossActiveSessions() (uint64, bool)` — Go structural
// typing satisfies this interface automatically; no import cycle.
//
// Per docs/recovery-wiring-plan.md §6 + design doc
// docs/recovery-pin-floor-wire.md §5 retention inequality.
type RecycleFloorSource interface {
	// MinPinAcrossActiveSessions returns the LSN below which any
	// recycle is safe (across all active recover sessions) and a
	// flag that says whether any session is active.
	//
	// `(0, false)` means no active session — the substrate may
	// recycle freely per its retention policy.
	// `(floor, true)` means at least one session is active and the
	// substrate MUST NOT advance its recycle past `floor`.
	MinPinAcrossActiveSessions() (uint64, bool)
}

// RecycleFloorGate is an optional interface a LogicalStorage impl
// satisfies if it supports the pin-floor gate. Cmd / wiring code
// type-asserts on this and skips the wiring if the substrate does
// not implement it (BlockStore today; smartwal can opt in later).
//
// `walstore.WALStore` and `memorywal.Store` implement this.
type RecycleFloorGate interface {
	SetRecycleFloorSource(src RecycleFloorSource)
}

// ErrAppliedLSNsNotTracked is returned by `LogicalStorage.AppliedLSNs`
// when the substrate does not maintain per-LBA applied-LSN metadata
// (e.g., in-memory BlockStore). The replica recovery apply gate
// (T4d-2) falls back to a session-only in-memory map seeded from
// recovery/live applies during the session.
//
// Per architect kickoff §2.5 #1 (Option C hybrid): substrates that
// CAN expose per-LBA applied-LSN cleanly do so; substrates that can't
// MUST return this sentinel explicitly (NOT a panic, NOT silent
// degradation).
var ErrAppliedLSNsNotTracked = errors.New("storage: substrate does not track per-LBA applied LSN")
