package authority

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

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

// subscription is a live, per-caller subscription to the authority
// stream. Each Subscribe returns exactly one subscription; cancel
// is isolated — one subscription ending does NOT affect others on
// the same (VolumeID, ReplicaID) key.
//
// Delivery is lossless for the current fact: the per-subscription
// channel has capacity 1 and the publisher overwrites a pending
// value with the latest one. A subscriber that was behind therefore
// always reads the LATEST authoritative state, never a stale one,
// and never loses the current fact silently. Intermediate states
// between the subscriber's last read and the next may be skipped
// — for authority truth that is correct (the engine cares about
// current identity, not history).
type subscription struct {
	id  uint64
	key subKey
	ch  chan adapter.AssignmentInfo
}

// Publisher is the system-owned authority for assignment truth.
// It is the sole place in production code that constructs an
// adapter.AssignmentInfo with Epoch > 0.
//
// The publisher's Run loop consumes AssignmentAsk values from a
// Directive, applies the S2 authoring rules, and fans out the
// resulting AssignmentInfo to per-subscription channels keyed by
// (VolumeID, ReplicaID).
type Publisher struct {
	dir Directive

	nextSubID atomic.Uint64

	mu    sync.Mutex
	state map[subKey]assignmentState
	subs  map[subKey][]*subscription
}

// NewPublisher creates a Publisher that consumes from dir.
func NewPublisher(dir Directive) *Publisher {
	return &Publisher{
		dir:   dir,
		state: make(map[subKey]assignmentState),
		subs:  make(map[subKey][]*subscription),
	}
}

// Subscribe registers a new subscriber for (volumeID, replicaID)
// and returns a receive channel plus a cancel function. Calling
// cancel closes this subscription's channel and removes it from
// the publisher's fan-out list — it does NOT affect any other
// subscription on the same key.
//
// If the publisher has already authored an AssignmentInfo for the
// key, the new subscriber receives it immediately as late-
// subscriber catch-up.
//
// cancel is idempotent and safe to call from any goroutine.
func (p *Publisher) Subscribe(volumeID, replicaID string) (<-chan adapter.AssignmentInfo, func()) {
	id := p.nextSubID.Add(1)
	sub := &subscription{
		id:  id,
		key: subKey{volumeID: volumeID, replicaID: replicaID},
		// Capacity 1 with overwrite-latest semantics on full. See
		// deliver() for the rationale.
		ch: make(chan adapter.AssignmentInfo, 1),
	}

	p.mu.Lock()
	p.subs[sub.key] = append(p.subs[sub.key], sub)
	st, hasState := p.state[sub.key]
	p.mu.Unlock()

	// Late-subscriber catch-up: deliver the last authored fact
	// immediately so a subscriber that attaches after authoring
	// does not miss identity.
	if hasState && st.present {
		sub.ch <- st.info // cap=1, fresh channel, always succeeds
	}

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			p.cancelSubscription(sub)
		})
	}
	return sub.ch, cancel
}

// cancelSubscription removes sub from the fan-out list and closes
// its channel. Other subscriptions on the same key are untouched.
//
// Safe to call concurrently with Run exit (which closes all
// remaining subs via closeAllSubscriptions). The close protocol:
// whoever finds the target still in the map owns the close; if
// cancel arrives after Run already removed the sub, cancel is a
// no-op. This avoids the double-close panic.
func (p *Publisher) cancelSubscription(target *subscription) {
	p.mu.Lock()
	list := p.subs[target.key]
	found := false
	for i, s := range list {
		if s == target {
			p.subs[target.key] = append(list[:i], list[i+1:]...)
			found = true
			break
		}
	}
	if found && len(p.subs[target.key]) == 0 {
		delete(p.subs, target.key)
	}
	p.mu.Unlock()
	if found {
		close(target.ch)
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
//
// On Run exit, all still-live subscription channels are closed so
// Bridges can observe end-of-stream cleanly.
func (p *Publisher) Run(ctx context.Context) error {
	defer p.closeAllSubscriptions()

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

// closeAllSubscriptions closes every live subscription. Called on
// Run exit so no Bridge is left waiting on a stream that will
// never produce again.
func (p *Publisher) closeAllSubscriptions() {
	p.mu.Lock()
	all := make([]*subscription, 0)
	for _, list := range p.subs {
		all = append(all, list...)
	}
	p.subs = make(map[subKey][]*subscription)
	p.mu.Unlock()
	for _, s := range all {
		close(s.ch)
	}
}

// apply implements the S2 authoring rules. It is the ONLY place in
// production code that constructs an adapter.AssignmentInfo with a
// non-zero Epoch. The structural non-forgeability test in
// authority_test.go enforces this boundary at repo scope.
func (p *Publisher) apply(ask AssignmentAsk) error {
	if err := validateAsk(ask); err != nil {
		return err
	}

	p.mu.Lock()
	k := subKey{volumeID: ask.VolumeID, replicaID: ask.ReplicaID}
	prev, had := p.state[k]

	var next adapter.AssignmentInfo
	switch ask.Intent {
	case IntentBind:
		if had && prev.present {
			p.mu.Unlock()
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
			p.mu.Unlock()
			return ErrRefreshNotBound
		}
		if prev.info.DataAddr == ask.DataAddr && prev.info.CtrlAddr == ask.CtrlAddr {
			// Idempotent no-op. Do not fan out, do not log as
			// rejection — the directive is allowed to retry.
			p.mu.Unlock()
			return nil
		}
		next = prev.info
		next.EndpointVersion = prev.info.EndpointVersion + 1
		next.DataAddr = ask.DataAddr
		next.CtrlAddr = ask.CtrlAddr

	case IntentReassign:
		if !had || !prev.present {
			p.mu.Unlock()
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
		p.mu.Unlock()
		return ErrUnknownIntent
	}

	p.state[k] = assignmentState{info: next, present: true}

	// Snapshot subscribers under lock, release the lock BEFORE
	// delivering. This prevents a slow consumer goroutine from
	// stalling the authoring loop, and avoids holding p.mu while
	// delivery does overwrite-latest on per-subscription channels.
	subs := make([]*subscription, len(p.subs[k]))
	copy(subs, p.subs[k])
	p.mu.Unlock()

	for _, s := range subs {
		deliverLatest(s.ch, next)
	}
	return nil
}

// deliverLatest performs a lossless-for-current-fact send onto a
// capacity-1 channel. If the channel is empty, the send succeeds
// directly. If the channel already holds a pending value (because
// the subscriber hasn't drained yet), the stale pending value is
// dropped and replaced with the latest one.
//
// The subscriber therefore always reads the LATEST authoritative
// state next, never a stale one, and never silently loses the
// current fact. For authority truth that is the correct semantic:
// the engine cares about current identity, not interstitial
// history between two publications.
//
// This replaces the earlier "drop on full with only a log line"
// behavior, which could leave a subscriber on stale truth until
// an unrelated future ask happened.
func deliverLatest(ch chan adapter.AssignmentInfo, info adapter.AssignmentInfo) {
	for {
		select {
		case ch <- info:
			return
		case <-ch:
			// Drained a stale pending value. Retry send. Because
			// only the publisher produces and the publisher's
			// authoring loop is sequential per (vid, rid), the
			// next iteration's send will either succeed or find
			// a concurrent consumer read — both cases converge.
		}
	}
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
// cleanly on ctx cancellation. Its own subscription cancel is the
// returned cancel func from Subscribe — Bridge does NOT call the
// old per-key Unsubscribe, so one Bridge exiting never disconnects
// other peers on the same authority stream.
func Bridge(ctx context.Context, pub *Publisher, consumer AssignmentConsumer, volumeID, replicaID string) {
	ch, cancel := pub.Subscribe(volumeID, replicaID)
	defer cancel()
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
