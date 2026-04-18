package authority

import (
	"context"
	"log"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// subKey identifies a subscription / authoring slot by the full
// (VolumeID, ReplicaID) unit. Keying by ReplicaID alone would
// multiplex two different volumes that share a replica name — the
// wrong routing boundary for authority truth.
type subKey struct {
	volumeID  string
	replicaID string
}

// assignmentState holds the last authored AssignmentInfo for a
// (VolumeID, ReplicaID). Used for authoring decisions (what to
// advance next) and for late-subscriber catch-up via LastPublished.
type assignmentState struct {
	info    adapter.AssignmentInfo
	present bool
}

// Publisher is the system-owned authority for assignment truth.
// It is the sole place in production code that constructs an
// adapter.AssignmentInfo with Epoch > 0.
//
// The publisher's Run loop consumes AssignmentAsk values from a
// Directive, applies the S2 authoring rules, and fans out the
// resulting AssignmentInfo to subscribers keyed by (VolumeID,
// ReplicaID).
type Publisher struct {
	dir Directive

	mu    sync.Mutex
	state map[subKey]assignmentState
	subs  map[subKey][]chan adapter.AssignmentInfo
}

// NewPublisher creates a Publisher that consumes from dir.
func NewPublisher(dir Directive) *Publisher {
	return &Publisher{
		dir:   dir,
		state: make(map[subKey]assignmentState),
		subs:  make(map[subKey][]chan adapter.AssignmentInfo),
	}
}

// Subscribe returns a channel that delivers every AssignmentInfo
// authored for (volumeID, replicaID). Late subscribers whose key
// already has a last published fact receive it immediately on the
// returned channel. Close via Unsubscribe.
//
// The returned channel has a small buffer. If a subscriber drains
// too slowly, publications for that subscriber are dropped with a
// log entry — slow subscribers must not stall the authoring loop.
// LastPublished is available for catch-up.
func (p *Publisher) Subscribe(volumeID, replicaID string) <-chan adapter.AssignmentInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := subKey{volumeID: volumeID, replicaID: replicaID}
	ch := make(chan adapter.AssignmentInfo, 4)
	p.subs[k] = append(p.subs[k], ch)
	if st, ok := p.state[k]; ok && st.present {
		select {
		case ch <- st.info:
		default:
		}
	}
	return ch
}

// Unsubscribe closes every subscription channel registered for
// (volumeID, replicaID). Idempotent.
func (p *Publisher) Unsubscribe(volumeID, replicaID string) {
	p.mu.Lock()
	chs := p.subs[subKey{volumeID: volumeID, replicaID: replicaID}]
	delete(p.subs, subKey{volumeID: volumeID, replicaID: replicaID})
	p.mu.Unlock()
	for _, ch := range chs {
		close(ch)
	}
}

// LastPublished returns the most recent AssignmentInfo authored for
// (volumeID, replicaID), if any. Read-only.
func (p *Publisher) LastPublished(volumeID, replicaID string) (adapter.AssignmentInfo, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.state[subKey{volumeID: volumeID, replicaID: replicaID}]
	if !ok || !st.present {
		return adapter.AssignmentInfo{}, false
	}
	return st.info, true
}

// Run drives the publisher. Loops: Directive.Next → apply → fan out.
// Exits on ctx cancellation or when Directive.Next returns a
// non-nil error. Returns the terminating error.
//
// Run is the ONLY path in production code that produces an
// authoritative AssignmentInfo. It calls apply internally; apply is
// unexported and thus not an authoring verb available to callers.
func (p *Publisher) Run(ctx context.Context) error {
	for {
		ask, err := p.dir.Next(ctx)
		if err != nil {
			return err
		}
		if applyErr := p.apply(ask); applyErr != nil {
			log.Printf("authority: rejected ask %+v: %v", ask, applyErr)
			continue
		}
	}
}

// apply implements the S2 authoring rules. It is the ONLY place in
// production code that constructs an adapter.AssignmentInfo with a
// non-zero Epoch. The structural non-forgeability test in
// authority_test.go enforces this boundary at package level.
func (p *Publisher) apply(ask AssignmentAsk) error {
	if err := validateAsk(ask); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	k := subKey{volumeID: ask.VolumeID, replicaID: ask.ReplicaID}
	prev, had := p.state[k]

	var next adapter.AssignmentInfo
	switch ask.Intent {
	case IntentBind:
		if had && prev.present {
			return ErrBindAlreadyBound
		}
		next = adapter.AssignmentInfo{
			VolumeID:        ask.VolumeID,
			ReplicaID:       ask.ReplicaID,
			Epoch:           1,
			EndpointVersion: 1,
			DataAddr:        ask.DataAddr,
			CtrlAddr:        ask.CtrlAddr,
		}

	case IntentRefreshEndpoint:
		if !had || !prev.present {
			return ErrRefreshNotBound
		}
		if prev.info.DataAddr == ask.DataAddr && prev.info.CtrlAddr == ask.CtrlAddr {
			// Idempotent no-op. Do not fan out, do not log as
			// rejection — the directive is allowed to retry.
			return nil
		}
		next = prev.info
		next.EndpointVersion = prev.info.EndpointVersion + 1
		next.DataAddr = ask.DataAddr
		next.CtrlAddr = ask.CtrlAddr

	case IntentReassign:
		if !had || !prev.present {
			return ErrReassignNotBound
		}
		next = adapter.AssignmentInfo{
			VolumeID:        ask.VolumeID,
			ReplicaID:       ask.ReplicaID,
			Epoch:           prev.info.Epoch + 1,
			EndpointVersion: 1,
			DataAddr:        ask.DataAddr,
			CtrlAddr:        ask.CtrlAddr,
		}

	default:
		return ErrUnknownIntent
	}

	p.state[k] = assignmentState{info: next, present: true}
	for _, ch := range p.subs[k] {
		select {
		case ch <- next:
		default:
			log.Printf("authority: subscriber drop for %s/%s epoch=%d endpointVersion=%d",
				ask.VolumeID, ask.ReplicaID, next.Epoch, next.EndpointVersion)
		}
	}
	return nil
}

// AssignmentConsumer is the narrow interface the Bridge calls back
// into on delivery. adapter.VolumeReplicaAdapter satisfies it via
// OnAssignment. Declared here so tests can provide their own fake
// consumer without building a full adapter.
type AssignmentConsumer interface {
	OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog
}

// Bridge subscribes to (volumeID, replicaID) on pub and forwards
// every delivered AssignmentInfo to consumer.OnAssignment. Exits
// cleanly on ctx cancellation. The only state Bridge holds is "am I
// still the live subscriber"; it never decides anything.
//
// Bridge closes its subscription via Unsubscribe when it exits so
// the publisher's fan-out state stays tidy.
func Bridge(ctx context.Context, pub *Publisher, consumer AssignmentConsumer, volumeID, replicaID string) {
	ch := pub.Subscribe(volumeID, replicaID)
	defer pub.Unsubscribe(volumeID, replicaID)
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-ch:
			if !ok {
				return
			}
			consumer.OnAssignment(info)
		}
	}
}
