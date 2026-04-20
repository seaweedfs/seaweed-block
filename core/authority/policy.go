package authority

import (
	"context"
	"fmt"
	"sync"
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
}

// SlotObservation is one slot's policy-visible facts in the
// accepted two-slot topology. Every field is a bounded, explicitly
// owned observation supplied by the upstream collector. The policy
// owner NEVER infers these from engine internals.
type SlotObservation struct {
	ReplicaID       string
	ServerID        string
	DataAddr        string
	CtrlAddr        string
	Reachable       bool
	ReadyForPrimary bool
	Eligible        bool
	Withdrawn       bool
}

// PolicySnapshot is the one complete point-in-time view submitted
// to the policy owner for one volume. Later snapshots supersede
// earlier ones (overwrite-latest semantics). Fields from different
// revisions must never be combined into a synthetic decision basis.
type PolicySnapshot struct {
	VolumeID          string
	CollectedRevision uint64
	CollectedAt       time.Time
	Authority         AuthorityBasis
	PrimarySlot       SlotObservation // configured-preferred slot
	SecondarySlot     SlotObservation
}

// PolicyConfig carries the bounded per-owner configuration. The
// first slice only needs to know which volume this owner serves;
// slot preference is encoded in the snapshot's slot labeling
// (PrimarySlot is the configured-preferred slot by convention).
type PolicyConfig struct {
	VolumeID string
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

// decisionKind is the internal classification of what the decision
// table produced for a given snapshot.
type decisionKind int

const (
	decisionEmit        decisionKind = iota // an ask is ready to emit
	decisionNoAsk                           // current state is correct; wait for next snapshot
	decisionUnsupported                     // authority basis is outside accepted topology set
)

// PolicyOwner is the first S3 Directive implementation. It consumes
// PolicySnapshot values via SubmitSnapshot and produces
// AssignmentAsk values via Next, following the decision table in
// the sketch and the stale-resistance rule before each emit.
//
// One PolicyOwner serves one volume. Multi-volume support is
// deliberately out of scope for the first slice.
type PolicyOwner struct {
	config PolicyConfig
	reader AuthorityBasisReader

	// Snapshot ingress: cap-1 overwrite-latest slot + non-blocking
	// wake. Matches the authority delivery discipline.
	mu        sync.Mutex
	latest    PolicySnapshot
	hasLatest bool
	wake      chan struct{}

	// Unsupported-topology evidence (§8).
	lastUnsupported    UnsupportedEvidence
	hasLastUnsupported bool
}

// NewPolicyOwner creates a PolicyOwner bound to a single volume.
// reader is usually a *Publisher; tests may inject a fake.
func NewPolicyOwner(config PolicyConfig, reader AuthorityBasisReader) *PolicyOwner {
	return &PolicyOwner{
		config: config,
		reader: reader,
		wake:   make(chan struct{}, 1),
	}
}

// SubmitSnapshot hands the policy owner a complete point-in-time
// view. Newer snapshots overwrite unread older ones (§12 coalescing
// rule). Mismatched VolumeID is rejected because the first slice
// binds one owner to one volume.
func (o *PolicyOwner) SubmitSnapshot(snap PolicySnapshot) error {
	if snap.VolumeID != o.config.VolumeID {
		return fmt.Errorf("policy: snapshot VolumeID=%q, owner bound to %q",
			snap.VolumeID, o.config.VolumeID)
	}
	o.mu.Lock()
	o.latest = snap
	o.hasLatest = true
	o.mu.Unlock()
	// Non-blocking wake: if a signal is already pending, the waiter
	// will see our new snapshot when it checks the slot.
	select {
	case o.wake <- struct{}{}:
	default:
	}
	return nil
}

// LastUnsupported returns the most recent UnsupportedEvidence, if
// any. Tests and observability surfaces use this to verify that a
// no-ask-plus-evidence branch was taken intentionally rather than
// silently idling.
func (o *PolicyOwner) LastUnsupported() (UnsupportedEvidence, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.lastUnsupported, o.hasLastUnsupported
}

// Next satisfies Directive. It blocks until it can emit an ask
// under the decision table with four ordered guards:
//
//  1. topology-honesty on the snapshot itself: the two accepted
//     slots must be on distinct non-empty ServerIDs. If they
//     share a ServerID, the snapshot violates the accepted
//     topology invariant; record unsupported evidence and wait.
//  2. out-of-topology authority: if the publisher's current
//     per-volume authority line is on a replica not in the
//     snapshot's two slots, the snapshot cannot legitimately
//     emit Bind or Reassign on top of it. Record unsupported
//     evidence and wait.
//  3. stale-resistance: snap.Authority must match the publisher's
//     current per-volume line. If it does not (newer line exists,
//     or snap sees no authority while the publisher has one), the
//     candidate decision is dropped and the owner waits for a
//     newer snapshot.
//  4. decision table: emit the AssignmentAsk if decide() produced
//     one; otherwise continue.
//
// Unsupported-topology and no-ask outcomes record evidence and
// loop back to waiting.
func (o *PolicyOwner) Next(ctx context.Context) (AssignmentAsk, error) {
	for {
		snap, err := o.waitForSnapshot(ctx)
		if err != nil {
			return AssignmentAsk{}, err
		}

		// Guard 1: distinct-servers topology invariant. The
		// accepted topology set is "two replica slots on distinct
		// servers". Two cases violate the invariant:
		//   - either ServerID is empty (identity absent, cannot
		//     prove distinctness),
		//   - both ServerIDs are non-empty and equal.
		// Both are treated as unsupported with evidence. Anything
		// softer — for example "empty is permitted" — would let
		// the slice claim a topology invariant it cannot check.
		if snap.PrimarySlot.ServerID == "" ||
			snap.SecondarySlot.ServerID == "" ||
			snap.PrimarySlot.ServerID == snap.SecondarySlot.ServerID {
			o.recordUnsupported(snap, snap.Authority, "distinct-servers topology invariant violated (empty or matching ServerID)")
			continue
		}

		// Guard 2: snapshot-side authority in topology. If the
		// snapshot's own Authority field names a replica outside
		// the two accepted slots, the snapshot itself is
		// unsupported — record evidence and wait.
		if snap.Authority.Assigned && !snapshotIncludesReplica(snap, snap.Authority.ReplicaID) {
			o.recordUnsupported(snap, snap.Authority, "snapshot authority ReplicaID not in accepted topology: "+snap.Authority.ReplicaID)
			continue
		}

		// Guard 3: publisher-side out-of-topology authority. Even
		// if the snapshot's view is in-topology, the publisher may
		// hold authority on some other replica (split collector,
		// prior out-of-band Bind). Refuse to emit in that case.
		currentLine, hasLine := o.reader.VolumeAuthorityLine(snap.VolumeID)
		if hasLine && !snapshotIncludesReplica(snap, currentLine.ReplicaID) {
			o.recordUnsupported(snap, currentLine, "publisher authority on replica outside accepted topology: "+currentLine.ReplicaID)
			continue
		}

		// Guard 4: stale-resistance against the per-volume line.
		if !basisMatchesLine(snap.Authority, currentLine, hasLine) {
			continue
		}

		// Guard 4: decision table.
		ask, kind := o.decide(snap)
		switch kind {
		case decisionUnsupported:
			o.recordUnsupported(snap, snap.Authority, "authority ReplicaID not in accepted topology set")
			continue
		case decisionNoAsk:
			continue
		case decisionEmit:
			return ask, nil
		}
	}
}

// snapshotIncludesReplica reports whether replicaID matches either
// of the two slot replicas carried by the snapshot.
func snapshotIncludesReplica(snap PolicySnapshot, replicaID string) bool {
	return replicaID != "" &&
		(replicaID == snap.PrimarySlot.ReplicaID || replicaID == snap.SecondarySlot.ReplicaID)
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

// waitForSnapshot drains the overwrite-latest slot. Blocks until
// a snapshot is available or ctx is cancelled.
func (o *PolicyOwner) waitForSnapshot(ctx context.Context) (PolicySnapshot, error) {
	for {
		o.mu.Lock()
		if o.hasLatest {
			snap := o.latest
			o.hasLatest = false
			o.mu.Unlock()
			return snap, nil
		}
		o.mu.Unlock()
		select {
		case <-o.wake:
			// New snapshot may be available; loop to check.
		case <-ctx.Done():
			return PolicySnapshot{}, ctx.Err()
		}
	}
}

// recordUnsupported stores an evidence entry describing why this
// snapshot did not produce an ask. The offendingBasis argument is
// deliberately separate from snap.Authority: for publisher-side
// out-of-topology findings (guard 3) the real offender is the
// publisher's current per-volume authority line, not the snapshot's
// (often-unassigned) view. Snapshot-side findings pass
// snap.Authority as the offending basis.
func (o *PolicyOwner) recordUnsupported(snap PolicySnapshot, offendingBasis AuthorityBasis, reason string) {
	o.mu.Lock()
	o.lastUnsupported = UnsupportedEvidence{
		SnapshotRevision: snap.CollectedRevision,
		CollectedAt:      snap.CollectedAt,
		Basis:            offendingBasis,
		Reason:           reason,
	}
	o.hasLastUnsupported = true
	o.mu.Unlock()
}


// acceptable is the single predicate shared by the no-authority
// Bind path and the failover Reassign path (sketch §9). Writing it
// once avoids drift where failover becomes stricter or looser than
// initial bind.
func (o *PolicyOwner) acceptable(s SlotObservation) bool {
	if s.ReplicaID == "" {
		return false
	}
	return s.Reachable && s.ReadyForPrimary && s.Eligible && !s.Withdrawn
}

// decide implements the decision table. Returns (ask, decisionKind).
// For non-emit decisions, the returned ask is the zero value and
// must not be sent.
func (o *PolicyOwner) decide(snap PolicySnapshot) (AssignmentAsk, decisionKind) {
	// §7.1 — no published authority yet.
	if !snap.Authority.Assigned {
		primaryOK := o.acceptable(snap.PrimarySlot)
		secondaryOK := o.acceptable(snap.SecondarySlot)
		switch {
		case primaryOK:
			// Tie-break: configured-preferred slot. PrimarySlot is
			// the preferred slot by the snapshot's labeling
			// convention. If both are acceptable we still pick
			// primary; if only primary is acceptable we pick it.
			return o.bindAskFor(snap.PrimarySlot), decisionEmit
		case secondaryOK:
			return o.bindAskFor(snap.SecondarySlot), decisionEmit
		default:
			return AssignmentAsk{}, decisionNoAsk
		}
	}

	// Published authority exists. Locate the current slot.
	var current, other SlotObservation
	switch snap.Authority.ReplicaID {
	case snap.PrimarySlot.ReplicaID:
		current = snap.PrimarySlot
		other = snap.SecondarySlot
	case snap.SecondarySlot.ReplicaID:
		current = snap.SecondarySlot
		other = snap.PrimarySlot
	default:
		// §7.3 — authority points outside the accepted topology set.
		return AssignmentAsk{}, decisionUnsupported
	}

	// §7.2 — published authority matches one of the two slots.
	if o.acceptable(current) {
		// Endpoint refresh check: either DataAddr or CtrlAddr
		// differs from the published basis. The sketch §13.1 case 5
		// explicitly covers either axis changing.
		if current.DataAddr != snap.Authority.DataAddr ||
			current.CtrlAddr != snap.Authority.CtrlAddr {
			return o.refreshAskFor(current), decisionEmit
		}
		return AssignmentAsk{}, decisionNoAsk
	}

	// Current slot is not acceptable. Consider the other slot.
	if o.acceptable(other) {
		return o.reassignAskFor(other), decisionEmit
	}
	return AssignmentAsk{}, decisionNoAsk
}

// bindAskFor / refreshAskFor / reassignAskFor construct the three
// S3-admitted AssignmentAsk shapes. They are the ONLY places in
// this package that construct an AssignmentAsk — the decision
// table branches exclusively through them, which gives a clean
// single-place audit point for later phases.
func (o *PolicyOwner) bindAskFor(s SlotObservation) AssignmentAsk {
	return AssignmentAsk{
		VolumeID:  o.config.VolumeID,
		ReplicaID: s.ReplicaID,
		DataAddr:  s.DataAddr,
		CtrlAddr:  s.CtrlAddr,
		Intent:    IntentBind,
	}
}

func (o *PolicyOwner) refreshAskFor(s SlotObservation) AssignmentAsk {
	return AssignmentAsk{
		VolumeID:  o.config.VolumeID,
		ReplicaID: s.ReplicaID,
		DataAddr:  s.DataAddr,
		CtrlAddr:  s.CtrlAddr,
		Intent:    IntentRefreshEndpoint,
	}
}

func (o *PolicyOwner) reassignAskFor(s SlotObservation) AssignmentAsk {
	return AssignmentAsk{
		VolumeID:  o.config.VolumeID,
		ReplicaID: s.ReplicaID,
		DataAddr:  s.DataAddr,
		CtrlAddr:  s.CtrlAddr,
		Intent:    IntentReassign,
	}
}
