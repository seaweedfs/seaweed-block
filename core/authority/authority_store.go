package authority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// ============================================================
// P14 S5 — Durable Authority Store Interface
//
// One record per VolumeID represents the current authoritative
// line for that volume. The store persists values the Publisher
// minted and reloads them at restart; it NEVER synthesizes
// Epoch / EndpointVersion on its own.
//
// See sw-block/design/v3-phase-14-s5-sketch.md.
// ============================================================

// DurableRecord is the per-volume current authority line that
// persists across restart. One record per VolumeID; the store
// holds exactly one at any time.
//
// Fields (pinned in sketch §5):
//
//   - VolumeID / ReplicaID / Epoch / EndpointVersion /
//     DataAddr / CtrlAddr: the AssignmentInfo content the
//     Publisher minted. Reloaded into Publisher state at boot.
//   - WriteSeq: monotonic per VolumeID, incremented by the
//     Publisher just before each write-through. Used for
//     reload sanity and as a future-proof ordering key for
//     non-single-file backends.
//
// Wall-clock is NOT a field here. Storing a wall-clock timestamp
// would invite someone to use it as a freshness/stale judgment —
// which the S5 architect constraint explicitly forbids (durable
// truth is authoritative until Publisher mints a new one).
type DurableRecord struct {
	VolumeID        string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	DataAddr        string
	CtrlAddr        string
	WriteSeq        uint64
}

// AuthorityStore is the narrow durable-authority interface. The
// load-bearing rule is: a store implementation may preserve and
// reload truth, but it may NEVER mint truth (sketch §3).
//
// Every write surface (Put) is a passthrough: the record is
// already minted by the Publisher. The store serializes, writes
// atomically, and fsyncs; it does not look at the record's
// Epoch / EndpointVersion values in any way that could decide
// authority.
type AuthorityStore interface {
	// Load returns every record currently in the store, skipping
	// records that failed integrity checks. Individual skipped
	// records do NOT cause Load to fail — per-volume fail-closed
	// (sketch §10). Only index-level failures (cannot enumerate
	// the store) return error.
	Load() ([]DurableRecord, error)

	// LoadWithSkips is Load with the per-record skip errors
	// exposed. Used by the bootstrap pre-check path so
	// index-level failures can be separated from per-record
	// corruption. Per-volume skips do NOT populate err; only
	// index-level failures do.
	LoadWithSkips() ([]DurableRecord, []error, error)

	// Put writes one record atomically. On success the record is
	// durable and readable by a future Load(). On failure the
	// caller is expected to roll back its in-memory state (see
	// Publisher.apply).
	Put(record DurableRecord) error

	// Close releases store resources. Idempotent.
	Close() error
}

// storeEnvelope is the on-disk wrapper around a DurableRecord.
// Keeping the checksum OUTSIDE the DurableRecord struct avoids a
// circular serialization (checksum of a struct that contains
// itself). The on-wire layout is:
//
//   {"v":1,"body":{...record...},"checksum":"<hex sha256>"}
//
// Checksum is over the canonical JSON serialization of Body.
type storeEnvelope struct {
	Version  int           `json:"v"`
	Body     DurableRecord `json:"body"`
	Checksum string        `json:"checksum"`
}

const storeEnvelopeVersion = 1

// ErrCorruptRecord indicates a single record failed integrity
// checks. Callers skip the record and continue — per-volume fail
// closed, not whole-process.
var ErrCorruptRecord = errors.New("authority_store: corrupt record")

// computeChecksum returns the SHA-256 hex over the canonical
// JSON serialization of a DurableRecord body. Deterministic —
// two runs with the same record produce the same sum.
func computeChecksum(r DurableRecord) string {
	body, err := json.Marshal(r)
	if err != nil {
		// json.Marshal on a struct with only plain fields cannot
		// fail in practice; if it ever does, we return the empty
		// string which will fail verification safely.
		return ""
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// encodeEnvelope serializes a record + checksum into the on-disk
// JSON envelope format.
func encodeEnvelope(r DurableRecord) ([]byte, error) {
	env := storeEnvelope{
		Version:  storeEnvelopeVersion,
		Body:     r,
		Checksum: computeChecksum(r),
	}
	return json.Marshal(env)
}

// decodeEnvelope parses an on-disk envelope and verifies the
// checksum. Returns ErrCorruptRecord if the envelope version is
// unknown, the JSON is malformed, or the checksum does not
// match the body.
//
// Also verifies the envelope's Body.VolumeID matches
// expectedVolumeID (sketch §8 rule: filename / VolumeID
// disagreement is corruption-tier, fail closed for that volume).
// Pass empty string to skip that check.
func decodeEnvelope(raw []byte, expectedVolumeID string) (DurableRecord, error) {
	var env storeEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return DurableRecord{}, fmt.Errorf("%w: decode: %v", ErrCorruptRecord, err)
	}
	if env.Version != storeEnvelopeVersion {
		return DurableRecord{}, fmt.Errorf("%w: envelope version %d unsupported", ErrCorruptRecord, env.Version)
	}
	want := computeChecksum(env.Body)
	if env.Checksum != want {
		return DurableRecord{}, fmt.Errorf("%w: checksum mismatch", ErrCorruptRecord)
	}
	if expectedVolumeID != "" && env.Body.VolumeID != expectedVolumeID {
		return DurableRecord{}, fmt.Errorf("%w: VolumeID mismatch: body=%q expected=%q",
			ErrCorruptRecord, env.Body.VolumeID, expectedVolumeID)
	}
	if env.Body.VolumeID == "" {
		return DurableRecord{}, fmt.Errorf("%w: empty VolumeID", ErrCorruptRecord)
	}
	return env.Body, nil
}
