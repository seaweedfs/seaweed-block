// Package authority is the system-owned publication route for
// assignment truth. It owns the authorship of Epoch and
// EndpointVersion — never accepts them as input — and fans out
// authoritative AssignmentInfo values to subscribers keyed by
// (VolumeID, ReplicaID).
//
// Three boundaries hold:
//  1. The authority package owns publication of assignment truth.
//  2. The authority package does NOT own failover policy.
//  3. Engine and adapter remain consumers, not producers, of
//     assignment truth.
package authority

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// AskIntent is the only control knob a Directive has. The publisher
// uses it to decide which authoritative field to advance and by how
// much. The current publisher supports exactly three intents.
// Retirement is deliberately not part of this input surface yet.
type AskIntent int

const (
	// IntentBind: first-time binding for (VolumeID, ReplicaID).
	// Publisher mints Epoch=1, EndpointVersion=1. Rejected
	// (ErrBindAlreadyBound) if a prior publish already exists for the
	// key or for any other slot of the same volume: Bind is genesis
	// for the volume; another slot is reached by IntentReassign.
	IntentBind AskIntent = iota + 1

	// IntentRefreshEndpoint: same volume/replica, new transport
	// endpoints. Publisher keeps Epoch, bumps EndpointVersion.
	// No-op if addresses are unchanged. Rejected if no prior
	// publish exists.
	IntentRefreshEndpoint

	// IntentReassign: bump the authoritative epoch for an
	// already-bound key. Publisher increments Epoch and resets
	// EndpointVersion to 1. Rejected if no prior publish exists.
	IntentReassign
)

// String returns a stable name for diagnostics.
func (i AskIntent) String() string {
	switch i {
	case IntentBind:
		return "Bind"
	case IntentRefreshEndpoint:
		return "RefreshEndpoint"
	case IntentReassign:
		return "Reassign"
	default:
		return "Unknown"
	}
}

// AssignmentAsk is the narrow input a Directive emits. It is
// deliberately narrower than adapter.AssignmentInfo: it carries no
// Epoch and no EndpointVersion. Those are authored by the publisher.
type AssignmentAsk struct {
	VolumeID  string
	ReplicaID string
	DataAddr  string
	CtrlAddr  string
	Intent    AskIntent
	// Evidence is the kind and probed evidence a reassign carries to its
	// target (a bind is always a bootstrap); see adapter.PrimaryEvidence. It
	// is a probe fact about the target, never an authority fact.
	Evidence adapter.PrimaryEvidence
	// Incarnation is the target's reported process incarnation in the facts
	// the decision used ("" = not reported). A refresh whose incarnation
	// differs from the line's is a restarted primary: a new endpoint version
	// even at the same addresses.
	Incarnation string
	// ExpectCurrent, when set, makes a reassign or an endpoint refresh
	// conditional on the volume's line being exactly this replica at this
	// epoch (and, when given, at these addresses) when the publisher mints
	// (under its lock): an operator's move never lands on a line it did not
	// see, and a refresh decided against a line never rewrites the line a
	// move put in its place.
	ExpectCurrent *ExpectedLine
}

// ExpectedLine is the line a conditional reassign or refresh was decided
// against. DataAddr and CtrlAddr are checked only when set.
type ExpectedLine struct {
	ReplicaID string
	Epoch     uint64
	DataAddr  string
	CtrlAddr  string
}


// ErrExpectedLineMoved: a conditional reassign found the volume's line no
// longer the one it was decided against; the publisher mints nothing.
var ErrExpectedLineMoved = errors.New("authority: the volume's line moved since the move was decided")

// ErrReassignWithoutKind: a primary change was asked without a kind (no
// promotion-gate evidence, not a forced move); the publisher mints no line.
var ErrReassignWithoutKind = errors.New("authority: a reassign needs a primary kind (promotion evidence or a forced move)")

// ErrFenceSuperseded: a primary change decided on a fencing round at epoch
// e+1 arrived after the volume's line left e; the publisher mints nothing.
var ErrFenceSuperseded = errors.New("authority: the fencing round's epoch is no longer the next epoch of the line")

// ErrPromotionUnfenced: a promotion carried neither a fencing round's epoch
// nor the planned-move mark; the publisher mints nothing.
var ErrPromotionUnfenced = errors.New("authority: a promotion needs a fencing round's decision or a guarded planned move")

// promotionBasisHolds checks a promotion's basis against the epoch it is
// minted at; for a heal, the target must be the replica the line names now.
func promotionBasisHolds(ask AssignmentAsk, epoch uint64, state map[subKey]assignmentState, lineEpoch uint64) error {
	ev := ask.Evidence
	switch ev.Basis {
	case adapter.BasisFenced:
		if ev.FencedEpoch != epoch {
			return fmt.Errorf("fenced at %d, minted at %d", ev.FencedEpoch, epoch)
		}
	case adapter.BasisPlanned:
		if ev.FencedEpoch != 0 {
			return fmt.Errorf("a planned move carries no fenced epoch (%d)", ev.FencedEpoch)
		}
	case adapter.BasisHeal:
		if ev.FencedEpoch != epoch {
			return fmt.Errorf("heal fenced at %d, minted at %d", ev.FencedEpoch, epoch)
		}
		if cur, ok := state[subKey{volumeID: ask.VolumeID, replicaID: ask.ReplicaID}]; !ok || !cur.present || cur.info.Epoch != lineEpoch {
			return fmt.Errorf("a heal continues the line's own primary; %s is not it", ask.ReplicaID)
		}
	default:
		return fmt.Errorf("no basis")
	}
	return nil
}

// Directive is the input side of the authority route. A Directive
// emits one AssignmentAsk at a time. The publisher's Run loop calls
// Next repeatedly until ctx cancellation.
//
// The Directive interface is the seam where P14 S3's failover
// policy will later plug in. S2 ships only a trivial implementation:
// StaticDirective.
type Directive interface {
	Next(ctx context.Context) (AssignmentAsk, error)
}

// StaticDirective emits a pre-wired sequence of AssignmentAsk
// values, then blocks on ctx cancellation. It is the trivial S2
// implementation used for tests and the sparrow closure target.
//
// StaticDirective carries no failover policy, no eligibility logic,
// no trigger logic. It is pure "replay these requests".
type StaticDirective struct {
	mu   sync.Mutex
	asks []AssignmentAsk
	idx  int
}

// NewStaticDirective returns a Directive that emits asks in order.
// After the last ask, Next blocks until ctx cancellation.
func NewStaticDirective(asks []AssignmentAsk) *StaticDirective {
	cp := make([]AssignmentAsk, len(asks))
	copy(cp, asks)
	return &StaticDirective{asks: cp}
}

// Append adds an ask to the tail. Safe for tests to call while a
// publisher Run loop is consuming.
func (s *StaticDirective) Append(ask AssignmentAsk) {
	s.mu.Lock()
	s.asks = append(s.asks, ask)
	s.mu.Unlock()
}

// Next returns the next ask, or blocks until ctx is cancelled when
// the pre-wired sequence is exhausted. Returning ctx.Err() signals
// the publisher's Run to exit cleanly.
func (s *StaticDirective) Next(ctx context.Context) (AssignmentAsk, error) {
	for {
		s.mu.Lock()
		if s.idx < len(s.asks) {
			ask := s.asks[s.idx]
			s.idx++
			s.mu.Unlock()
			return ask, nil
		}
		s.mu.Unlock()

		// Exhausted. Block briefly and retry so Append can add more,
		// but honor ctx cancellation promptly.
		select {
		case <-ctx.Done():
			return AssignmentAsk{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Errors returned by the publisher for rejected asks.
var (
	ErrMissingVolumeID  = errors.New("authority: AssignmentAsk missing VolumeID")
	ErrMissingReplicaID = errors.New("authority: AssignmentAsk missing ReplicaID")
	ErrMissingDataAddr  = errors.New("authority: AssignmentAsk missing DataAddr")
	ErrMissingCtrlAddr  = errors.New("authority: AssignmentAsk missing CtrlAddr")
	ErrMissingIntent    = errors.New("authority: AssignmentAsk missing Intent")
	ErrUnknownIntent    = errors.New("authority: AssignmentAsk Intent not recognized")
	ErrBindAlreadyBound = errors.New("authority: Bind on already-bound key")
	ErrRefreshNotBound  = errors.New("authority: RefreshEndpoint on unbound key")
	ErrReassignNotBound = errors.New("authority: Reassign on unbound key")
)

func validateAsk(ask AssignmentAsk) error {
	if ask.VolumeID == "" {
		return ErrMissingVolumeID
	}
	if ask.ReplicaID == "" {
		return ErrMissingReplicaID
	}
	if ask.DataAddr == "" {
		return ErrMissingDataAddr
	}
	if ask.CtrlAddr == "" {
		return ErrMissingCtrlAddr
	}
	if ask.Intent == 0 {
		return ErrMissingIntent
	}
	if ask.Intent != IntentBind &&
		ask.Intent != IntentRefreshEndpoint &&
		ask.Intent != IntentReassign {
		return ErrUnknownIntent
	}
	return nil
}
