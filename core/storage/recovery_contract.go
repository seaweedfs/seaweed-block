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
