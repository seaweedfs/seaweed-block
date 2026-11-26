package authority

import (
	"time"
)

// ============================================================
// P14 S3 — Bounded Policy Owner
//
// Implements the first accepted S3 topology set (one volume, two
// replica slots on distinct servers, one current primary, one
// failover candidate) as a Directive-satisfying policy owner.
//
// The sketch (docs/sw-block/design/v3-phase-14-s3-sketch.md) pins:
//   - input model: PolicySnapshot + AuthorityBasis + SlotObservation
//   - freshness: snapshot slot is cap-1 overwrite-latest
//   - decision table: §7.1 / §7.2 / §7.3
//   - stale-resistance: re-check authority basis before emit
//   - unsupported-topology evidence: §8 "bounded no-op is allowed;
//     silent idle is not"
//
// Contract boundaries (inherit from S2):
//   * policy owner emits AssignmentAsk; it NEVER constructs
//     AssignmentInfo.
//   * authority.Publisher remains the sole minter of Epoch /
//     EndpointVersion.
//   * adapter.OnAssignment stays the one and only consumer ingress.
//   * No Remove / retirement intent in this slice.
// ============================================================

// AuthorityBasis is the identity + endpoint snapshot of whatever
// was last published for a (VolumeID, ReplicaID). It is what the
// policy owner compares against the snapshot's own Authority field
// to decide whether the snapshot is still current.
type AuthorityBasis struct {
	Assigned        bool
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	DataAddr        string
	CtrlAddr        string
	// Incarnation is the primary process the line was issued for ("" = not
	// known); see adapter.AssignmentInfo.Incarnation.
	Incarnation string
}

// AuthorityBasisReader exposes read-only views of the publisher's
// current authority state, used by the policy owner for stale-
// resistance and out-of-topology-authority checks before emit.
//
//   - LastAuthorityBasis returns the last published basis for a
//     specific (VolumeID, ReplicaID). Per-slot view.
//   - VolumeAuthorityLine returns the AuthorityBasis with the
//     highest Epoch across EVERY (VolumeID, *) key present in
//     publisher state. Per-volume view. This is what the policy
//     owner consults to detect out-of-topology authority that a
//     per-slot query would miss — e.g. publisher holds (v1, r99)
//     while the accepted topology set is {r1, r2}.
//
// Publisher satisfies both; tests provide a fake.
type AuthorityBasisReader interface {
	LastAuthorityBasis(volumeID, replicaID string) (AuthorityBasis, bool)
	VolumeAuthorityLine(volumeID string) (AuthorityBasis, bool)
}

// LastAuthorityBasis exposes the publisher's last published fact
// as an AuthorityBasis, satisfying AuthorityBasisReader.
func (p *Publisher) LastAuthorityBasis(volumeID, replicaID string) (AuthorityBasis, bool) {
	info, ok := p.LastPublished(volumeID, replicaID)
	if !ok {
		return AuthorityBasis{}, false
	}
	return AuthorityBasis{
		Assigned:        true,
		ReplicaID:       info.ReplicaID,
		Epoch:           info.Epoch,
		EndpointVersion: info.EndpointVersion,
		DataAddr:        info.DataAddr,
		CtrlAddr:        info.CtrlAddr,
		Incarnation:     info.Incarnation,
	}, true
}

// VolumeAuthorityLine returns the per-volume current authority
// line: the highest-Epoch AuthorityBasis across every
// (VolumeID, *) key currently present in the publisher state.
// Used by PolicyOwner to detect authority on a replica outside
// the snapshot's accepted topology set, which a per-slot
// LastAuthorityBasis query would miss.
//
// Returns (zero, false) when no publication exists for the volume.
func (p *Publisher) VolumeAuthorityLine(volumeID string) (AuthorityBasis, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.volumeLineLocked(volumeID)
}

// volumeLineLocked is VolumeAuthorityLine with p.mu held.
func (p *Publisher) volumeLineLocked(volumeID string) (AuthorityBasis, bool) {
	var best AuthorityBasis
	any := false
	for k, st := range p.state {
		if !st.present || k.volumeID != volumeID {
			continue
		}
		if !any || st.info.Epoch > best.Epoch {
			best = AuthorityBasis{
				Assigned:        true,
				ReplicaID:       st.info.ReplicaID,
				Epoch:           st.info.Epoch,
				EndpointVersion: st.info.EndpointVersion,
				DataAddr:        st.info.DataAddr,
				CtrlAddr:        st.info.CtrlAddr,
				Incarnation:     st.info.Incarnation,
			}
			any = true
		}
	}
	return best, any
}

// UnsupportedEvidence records the last time the policy owner
// declined to emit an ask because the snapshot — or the observed
// publisher authority state — was outside the accepted topology.
// It is the explicit evidence path required by the sketch §8:
// "bounded no-op is allowed; silent idle is not."
//
// Basis carries the OFFENDING authority basis as a structured
// artifact:
//
//   - snapshot-side unsupported (snap.Authority points outside
//     the accepted topology, or the snapshot violates the
//     distinct-servers invariant): Basis == snap.Authority.
//   - publisher-side unsupported (the publisher's current per-
//     volume authority line is on a replica outside the two
//     accepted slots): Basis == the publisher's current line,
//     not snap.Authority (which may be Assigned=false when the
//     collector missed the offending replica).
//
// Consumers of evidence should read Basis as the structured
// proof, not parse Reason.
type UnsupportedEvidence struct {
	SnapshotRevision uint64
	CollectedAt      time.Time
	Basis            AuthorityBasis
	Reason           string
}

// basisMatchesLine is the per-volume stale-resistance check.
// It compares snap.Authority against the publisher's current
// per-volume line on VolumeID / ReplicaID / Epoch / EndpointVersion
// (sketch §11 minimum compare), covering both "both assigned" and
// "both unassigned" as still-current cases. Any disagreement is
// treated as stale.
func basisMatchesLine(snap AuthorityBasis, line AuthorityBasis, hasLine bool) bool {
	if snap.Assigned {
		return hasLine &&
			line.ReplicaID == snap.ReplicaID &&
			line.Epoch == snap.Epoch &&
			line.EndpointVersion == snap.EndpointVersion
	}
	// snap saw no authority. Only still current if the publisher
	// also sees no authority for this volume.
	return !hasLine
}
