package authority

import "time"

// ============================================================
// P14 S4 — Observation-Institution Unsupported Evidence
//
// Per-volume latest-wins evidence shape. Kept separate from S3's
// policy-owner UnsupportedEvidence (which is owner-scoped,
// current-fact only). See sw-block/design/v3-phase-14-s4-sketch.md
// §9.
//
// Pending and Unsupported are VISIBLY DISTINCT collections on
// BuildResult. A volume that is within the pending grace is NOT
// in Unsupported — if those collapsed, 14A review of "pending vs
// unsupported" would be harder for no benefit.
// ============================================================

// VolumeUnsupportedEvidence records why one volume could not
// enter the synthesized ClusterSnapshot as supported. It is
// latest-wins per (VolumeID).
//
// OffendingFacts is retained raw — the observation layer does not
// rank or filter them. That keeps the evidence usable by humans
// and by 14A review.
type VolumeUnsupportedEvidence struct {
	VolumeID         string
	SnapshotRevision uint64
	EvaluatedAt      time.Time
	Reasons          []string
	OffendingFacts   []Observation
	Note             string
}

// VolumePendingStatus records a volume that is within the
// pending grace. It is NOT an error — it is a transitional
// observation that has not yet earned unsupported evidence.
// Intentionally a different type from VolumeUnsupportedEvidence
// so consumers cannot accidentally blur the two.
type VolumePendingStatus struct {
	VolumeID       string
	EvaluatedAt    time.Time
	PendingReasons []string
	Note           string
}

// SupportabilityReport is the per-volume supportability
// breakdown for one build. Both maps are keyed by VolumeID.
// Volumes in Supported appear in the companion ClusterSnapshot;
// volumes in Pending / Unsupported do not.
type SupportabilityReport struct {
	Pending     map[string]VolumePendingStatus
	Unsupported map[string]VolumeUnsupportedEvidence
}

func newSupportabilityReport() *SupportabilityReport {
	return &SupportabilityReport{
		Pending:     map[string]VolumePendingStatus{},
		Unsupported: map[string]VolumeUnsupportedEvidence{},
	}
}

// BuildResult is the full output of one snapshot build. The
// ClusterSnapshot is the OUTPUT; supportability decisions were
// made from store inputs, not from this value. (One-way pipeline,
// sketch §12 + §16a.)
type BuildResult struct {
	Snapshot   ClusterSnapshot
	Report     SupportabilityReport
	BuiltAt    time.Time
	InputRev   uint64
}
