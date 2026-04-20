package authority

import (
	"context"
	"fmt"
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
//
// Delivery and close are guarded by a per-subscription mutex so
// the publisher never sends on a closed channel (panic) or loops
// on a closed channel's zero-receives (hang). A cancellation that
// races with an in-flight delivery sees one of two outcomes:
//   - delivery wins: it drain+sends under the lock, then close
//     runs next and closes the channel.
//   - close wins: close sets closed=true and closes the channel,
//     then delivery sees closed and returns without touching ch.
type subscription struct {
	id  uint64
	key subKey
	ch  chan adapter.AssignmentInfo

	mu     sync.Mutex
	closed bool
}

// deliver performs an overwrite-latest send to this subscription.
// Called by Publisher.apply outside pub.mu. Safe to call
// concurrently with close(): if the subscription is already
// closed, deliver is a no-op and never touches ch.
func (s *subscription) deliver(info adapter.AssignmentInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	// Drain any stale pending value (non-blocking: either the
	// buffer has one item, or it's empty). Then send the latest.
	// Cap=1 + known-empty-after-drain means the send is also
	// non-blocking. Consumer-side receives never need the lock.
	select {
	case <-s.ch:
	default:
	}
	s.ch <- info
}

// close closes the subscription's delivery channel exactly once.
// Idempotent. Concurrent deliver() calls observe closed=true and
// return without touching ch — so a cancel during in-flight
// delivery never produces a send-on-closed panic.
func (s *subscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
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

	// P14 S5 — durable authority store. Optional; nil means
	// in-memory only (existing behavior, used by tests and
	// scenarios that don't need restart recovery).
	//
	// When non-nil:
	//   - NewPublisher reloads state from store at construction.
	//   - apply() writes through under pub.mu on every
	//     successful mint. If Put fails, the in-memory state is
	//     rolled back and the mint returns an error.
	store AuthorityStore

	// writeSeq is the monotonic per-volume write-sequence used
	// when generating DurableRecords. Keyed by VolumeID (not
	// subKey) because S5 persists one record per volume, and
	// WriteSeq reflects the cumulative mint history for that
	// volume across all its slots. Lives on Publisher so it
	// survives across mints; starts at the max observed during
	// reload so new writes extend, not overwrite, durable
	// ordering.
	writeSeqByVolume map[string]uint64

	// loadErrs are any per-record skip errors encountered during
	// reload. Exposed via LoadErrors() for tests and operator
	// diagnosis. Per-volume fail-closed per sketch §10: these
	// records are simply absent from state and the volume
	// behaves as "never bound" until a new mint.
	loadErrs []error
}

// PublisherOption configures a Publisher at construction.
// Currently the only option is WithStore, but the variadic
// shape is chosen so later slices can add options without
// changing the constructor signature.
type PublisherOption func(*Publisher)

// WithStore wires the Publisher to a durable AuthorityStore.
// The constructor reloads state from the store and every
// successful apply() writes through synchronously before the
// in-memory state is committed.
//
// Passing nil (or omitting the option entirely) keeps the
// Publisher in-memory-only — the S1..S4 behavior.
func WithStore(store AuthorityStore) PublisherOption {
	return func(p *Publisher) {
		p.store = store
	}
}

// NewPublisher creates a Publisher that consumes from dir.
//
// When WithStore is supplied, the constructor performs a
// synchronous reload from the store before returning: it
// verifies each record, populates in-memory state, and records
// any per-record skip errors (ErrCorruptRecord) for later
// retrieval via LoadErrors(). Index-level reload failures
// (e.g. cannot list the store directory) are reported by
// panicking — the bootstrap contract is that such failures
// fail the whole boot, and callers would otherwise have to
// introduce a wide error-return path into a construction
// function that historically cannot fail.
//
// Callers that want non-panic reload-error handling can check
// store.Load() themselves before calling NewPublisher, or use
// the future-planned authority_bootstrap helper that composes
// the pieces.
func NewPublisher(dir Directive, opts ...PublisherOption) *Publisher {
	p := &Publisher{
		dir:              dir,
		state:            make(map[subKey]assignmentState),
		subs:             make(map[subKey][]*subscription),
		writeSeqByVolume: map[string]uint64{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	if p.store != nil {
		p.reloadFromStore()
	}
	return p
}

// reloadFromStore reads every durable record and populates the
// Publisher's in-memory state. Per-volume fail-closed: corrupt
// records are logged to p.loadErrs and their volumes are left
// "never bound" in memory. An index-level failure (store cannot
// be enumerated) panics — whole-process fail-closed per
// sketch §10.
func (p *Publisher) reloadFromStore() {
	records, skips, err := p.store.LoadWithSkips()
	if err != nil {
		panic(fmt.Sprintf("authority: index-level store load failure: %v", err))
	}
	for _, rec := range records {
		key := subKey{volumeID: rec.VolumeID, replicaID: rec.ReplicaID}
		p.state[key] = assignmentState{
			info: adapter.AssignmentInfo{
				VolumeID:        rec.VolumeID,
				ReplicaID:       rec.ReplicaID,
				Epoch:           rec.Epoch,
				EndpointVersion: rec.EndpointVersion,
				DataAddr:        rec.DataAddr,
				CtrlAddr:        rec.CtrlAddr,
			},
			present: true,
		}
		if rec.WriteSeq > p.writeSeqByVolume[rec.VolumeID] {
			p.writeSeqByVolume[rec.VolumeID] = rec.WriteSeq
		}
	}
	p.loadErrs = append(p.loadErrs, skips...)
}

// LoadErrors returns the per-record skip errors observed during
// the last reload. Each entry is an ErrCorruptRecord wrap.
// Used by restart tests and (in later phases) operator
// diagnosis surfaces.
func (p *Publisher) LoadErrors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]error, len(p.loadErrs))
	copy(out, p.loadErrs)
	return out
}

// Subscribe registers a new subscriber for (volumeID, replicaID)
// and returns a receive channel plus a cancel function. Calling
// cancel closes this subscription's channel and removes it from
// the publisher's fan-out list — it does NOT affect any other
// subscription on the same key.
//
// If the publisher has already authored an AssignmentInfo for the
// key, the new subscriber receives it immediately as late-
// subscriber catch-up — and the catch-up value is guaranteed to
// be the current authoritative state at the moment the subscriber
// becomes visible, because the catch-up send and the subs-map
// insert happen together under pub.mu. An apply() that authors a
// newer fact after the subscriber is visible will drain the stale
// catch-up value and replace it with the new fact via deliver().
//
// cancel is idempotent and safe to call from any goroutine.
func (p *Publisher) Subscribe(volumeID, replicaID string) (<-chan adapter.AssignmentInfo, func()) {
	id := p.nextSubID.Add(1)
	sub := &subscription{
		id:  id,
		key: subKey{volumeID: volumeID, replicaID: replicaID},
		// Capacity 1 with overwrite-latest semantics on full. See
		// subscription.deliver() for the rationale.
		ch: make(chan adapter.AssignmentInfo, 1),
	}

	// Subs-map insert and catch-up send happen atomically under
	// pub.mu. This closes the "catch-up arrives after a newer
	// apply()" race: any apply() contending for pub.mu must either
	// run before us (so the state we read IS the current authoritative
	// state), or after us (so it sees us in the subs map and will
	// drain our catch-up via deliver()). The catch-up send to
	// sub.ch is non-blocking because sub is fresh and only we can
	// see it — no other goroutine can have filled its cap=1 buffer.
	p.mu.Lock()
	p.subs[sub.key] = append(p.subs[sub.key], sub)
	if st, ok := p.state[sub.key]; ok && st.present {
		sub.ch <- st.info
	}
	p.mu.Unlock()

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
		target.close()
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
		s.close()
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
		// Per the P14 S3 "reassign semantic" boundary note
		// (sw-block/design/v3-phase-14-s3-reassign-semantic.md):
		// IntentReassign is a PER-VOLUME authority advance, not a
		// per-slot Epoch bump. The target ReplicaID may differ
		// from the currently-published replica for this volume
		// (this is what makes cross-slot failover semantically
		// valid in the first S3 accepted topology set). The new
		// Epoch is minted from the maximum Epoch across every
		// (VolumeID, *) key in publisher state, plus 1.
		//
		// IntentReassign still requires that *some* prior
		// publication exists for this VolumeID. First-time
		// establishment is the IntentBind path, not this one —
		// using Reassign to create a volume's very first binding
		// would confuse the per-volume monotonic line.
		maxEpoch := maxPublishedEpochForVolume(p.state, ask.VolumeID)
		if maxEpoch == 0 {
			p.mu.Unlock()
			return ErrReassignNotBound
		}
		next = adapter.AssignmentInfo{
			VolumeID:        ask.VolumeID,
			ReplicaID:       ask.ReplicaID,
			Epoch:           maxEpoch + 1,
			EndpointVersion: 1,
			DataAddr:        ask.DataAddr,
			CtrlAddr:        ask.CtrlAddr,
		}

	default:
		p.mu.Unlock()
		return ErrUnknownIntent
	}

	// P14 S5 — durable write-through. If a store is wired, the
	// freshly minted record is persisted under pub.mu BEFORE the
	// in-memory state is committed. State mutation is deliberately
	// sequenced after Put: on Put failure we simply return without
	// touching p.state / p.writeSeqByVolume, so no rollback is
	// needed. Subscribers never see a fact that isn't durable yet.
	if p.store != nil {
		// Compute the per-volume WriteSeq for this mint. Starts
		// at the highest previously-persisted value for the
		// volume (set during reload) + 1.
		nextSeq := p.writeSeqByVolume[ask.VolumeID] + 1
		record := DurableRecord{
			VolumeID:        next.VolumeID,
			ReplicaID:       next.ReplicaID,
			Epoch:           next.Epoch,
			EndpointVersion: next.EndpointVersion,
			DataAddr:        next.DataAddr,
			CtrlAddr:        next.CtrlAddr,
			WriteSeq:        nextSeq,
		}
		if err := p.store.Put(record); err != nil {
			// State mutation hasn't happened yet; nothing to
			// unwind. Just report the failure.
			p.mu.Unlock()
			return fmt.Errorf("authority: durable Put failed, rolling back mint: %w", err)
		}
		p.writeSeqByVolume[ask.VolumeID] = nextSeq
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
		s.deliver(next)
	}
	return nil
}

// maxPublishedEpochForVolume scans the publisher's authoring state
// and returns the maximum Epoch currently published on any
// (VolumeID, *) key for the given volume. Zero return means no
// prior publish exists for this volume at all — the caller should
// treat that as "no per-volume authority line to advance".
//
// Must be called with p.mu held; reads p.state.
func maxPublishedEpochForVolume(state map[subKey]assignmentState, volumeID string) uint64 {
	var max uint64
	for k, st := range state {
		if !st.present || k.volumeID != volumeID {
			continue
		}
		if st.info.Epoch > max {
			max = st.info.Epoch
		}
	}
	return max
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

// VolumeBridge subscribes to every (volumeID, replicaID) in the
// given accepted-topology set and forwards each delivery to the
// same consumer. It is the system-owned retarget path for the
// first P14 S3 accepted topology: when authority moves from r1 to
// r2 via a cross-slot Reassign, the inner Bridge for r2 delivers
// the new AssignmentInfo and the consumer's OnAssignment handles
// the identity advance through its existing monotonicity guards.
// No test-side teardown-and-retarget is needed.
//
// VolumeBridge does NOT discover replica IDs — the accepted
// topology set is passed in explicitly. Discovery belongs to
// policy (S3) and to future operator / governance surfaces
// (P15), not to this primitive.
//
// VolumeBridge blocks until ctx is cancelled. Cancelling ctx tears
// down every inner Bridge goroutine cleanly via their own
// per-subscription cancels. Returns when all inner bridges have
// exited.
func VolumeBridge(ctx context.Context, pub *Publisher, consumer AssignmentConsumer, volumeID string, replicaIDs ...string) {
	if len(replicaIDs) == 0 {
		<-ctx.Done()
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(replicaIDs))
	for _, rid := range replicaIDs {
		go func(rid string) {
			defer wg.Done()
			Bridge(ctx, pub, consumer, volumeID, rid)
		}(rid)
	}
	wg.Wait()
}
