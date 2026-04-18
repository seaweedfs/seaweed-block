// Package authority is the system-owned publication route for
// assignment truth. It owns the authorship of Epoch and
// EndpointVersion — never accepts them as input — and fans out
// authoritative AssignmentInfo values to subscribers keyed by
// (VolumeID, ReplicaID).
//
// The design note is docs/p14-s2-design.md. Three boundaries hold:
//   1. S2 owns publication of authority truth.
//   2. S2 does NOT own failover policy.
//   3. Engine and adapter remain consumers, not producers, of
//      assignment truth.
package authority

import (
	"context"
	"errors"
	"sync"
	"time"
)

// AskIntent is the only control knob a Directive has. The publisher
// uses it to decide which authoritative field to advance and by how
// much. S2 supports exactly three intents. Retirement is deferred
// (see docs/p14-s2-design.md "What we are NOT building").
type AskIntent int

const (
	// IntentBind: first-time binding for (VolumeID, ReplicaID).
	// Publisher mints Epoch=1, EndpointVersion=1. Rejected if a
	// prior publish already exists for the key.
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
