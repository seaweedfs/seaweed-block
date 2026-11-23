package authority

import (
	"errors"
	"log"
	"sort"
	"sync"
	"time"
)

// ============================================================
// P14 S4 — Observation Store
//
// Push-ingested latest-wins observation store per replica slot. The
// store computes derived freshness on read, based on wall-clock
// time vs ObservedAt + FreshnessWindow. Expired observations stay
// visible; they are just semantically ineligible for supported
// snapshot synthesis. See sketch §6.
// ============================================================

// ObservationStore holds the latest observation per reporting
// ServerID, plus a bootstrap timestamp used by the snapshot
// builder for the "never-observed-yet within grace" case.
//
// Push ingest only (sketch §10). The store does not pull from
// runtime or adapter state. Mutation triggers a registered
// rebuild hook so the host can re-synthesize reactively
// (sketch §11).
type ObservationStore struct {
	config    FreshnessConfig
	now       func() time.Time
	startedAt time.Time

	mu           sync.Mutex
	observations map[string]Observation // key: ServerID
	revision     uint64
	onMutation   func()

	// supersededCount counts slot reports that were dropped because a
	// newer ObservedAt was stored for that server/volume/replica slot.
	// Diagnostic only — PCDD-DELAYED-HB-001 evidence
	// surface. Not an authority input; publisher / controller /
	// evidence minting paths MUST NOT read it. Exposed via
	// SupersededCount().
	supersededCount uint64
	supersededSlots map[[3]string]uint64
}

// SupersededCount returns the total number of slot reports dropped
// because a newer report of that same server/volume/replica was stored.
// Diagnostic surface for PCDD-DELAYED-HB-001.
// NOT authority input. Monotonic across the lifetime of the store.
func (s *ObservationStore) SupersededCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.supersededCount
}

// SupersededSlotCount is the diagnostic count for one reporting slot.
// Like SupersededCount, it is not an authority input.
func (s *ObservationStore) SupersededSlotCount(serverID, volumeID, replicaID string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.supersededSlots[[3]string{serverID, volumeID, replicaID}]
}

// SlotFact returns the most-recently-observed SlotFact for a
// (volumeID, replicaID) pair across all reporting servers, or
// (zero, false) if no server has reported that slot.
//
// Used by master-side peer-set construction (G5-5A): supporting
// replicas don't have a published authority line (Publisher.apply
// only mints for the bound replica), so master falls back to the
// last-heartbeat-observed DataAddr/CtrlAddr to populate
// AssignmentFact.peers for primary fan-out. Topology membership is
// the allow-list — the caller must confirm the (volumeID, replicaID)
// is a declared slot before consulting this method.
//
// The returned SlotFact is the value reported by the LATEST
// observation across all servers — multiple servers may report the
// same (volumeID, replicaID) (e.g., during reassignment or topology
// confusion). The caller's authority/topology gates upstream of
// this method enforce the membership invariant; this method just
// answers "what addr did the cluster last hear about for this
// (volume, replica)?".
//
// Returns (zero, false) when:
//   - no observation has reported any slot for this (volumeID, replicaID), OR
//   - observations exist but the matching slot has empty DataAddr
//     (caller-side fail-closed: skip the peer rather than synthesize
//     a descriptor with an unusable addr), OR
//   - all matching observations are EXPIRED (now > obs.ExpiresAt) per
//     architect round 54 finding-1: "expired heartbeat addr should
//     fail closed" — same discipline as snapshot synthesis.
//
// NOT authority input. Read-only; safe to call concurrently.
func (s *ObservationStore) SlotFact(volumeID, replicaID string) (SlotFact, bool) {
	if volumeID == "" || replicaID == "" {
		return SlotFact{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var (
		best      SlotFact
		bestObsAt time.Time
		found     bool
	)
	for _, obs := range s.observations {
		// Fail-closed on stale heartbeats (architect round 54
		// finding-1). An expired observation's DataAddr cannot be
		// trusted for primary→replica dial; skip and let caller
		// surface the slot as "no observation" until the next
		// fresh heartbeat lands.
		if !obs.ExpiresAt.IsZero() && now.After(obs.ExpiresAt) {
			continue
		}
		for _, slot := range obs.Slots {
			if slot.VolumeID != volumeID || slot.ReplicaID != replicaID {
				continue
			}
			if slotExpired(slot, obs, now) {
				continue
			}
			if slot.DataAddr == "" {
				// Fail-closed: an observation that names the slot but
				// without a usable DataAddr is unusable for primary
				// fan-out.
				continue
			}
			candidateObsAt := slotObservedAt(slot, obs)
			if !found || candidateObsAt.After(bestObsAt) {
				best = slot
				best.Frontends = append([]FrontendTargetFact(nil), slot.Frontends...)
				bestObsAt = candidateObsAt
				found = true
			}
		}
	}
	return best, found
}

// SlotFactOnServer never substitutes a same-replica claim from another host.
func (s *ObservationStore) SlotFactOnServer(serverID, volumeID, replicaID string) (SlotFact, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obs, ok := s.observations[serverID]
	now := s.now()
	if !ok || (!obs.ExpiresAt.IsZero() && now.After(obs.ExpiresAt)) {
		return SlotFact{}, false
	}
	for _, slot := range obs.Slots {
		if slot.VolumeID == volumeID && slot.ReplicaID == replicaID && !slotExpired(slot, obs, now) && slot.DataAddr != "" && slot.CtrlAddr != "" {
			return cloneSlot(slot), true
		}
	}
	return SlotFact{}, false
}

// ReplicaIDsForVolume returns fresh, usable replica IDs currently
// observed for a volume. It is a read-only evidence helper for
// dynamic-lifecycle status surfaces; callers that own a static
// topology must keep using topology membership as their allow-list.
func (s *ObservationStore) ReplicaIDsForVolume(volumeID string) []string {
	if volumeID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	seen := map[string]struct{}{}
	for _, obs := range s.observations {
		if !obs.ExpiresAt.IsZero() && now.After(obs.ExpiresAt) {
			continue
		}
		for _, slot := range obs.Slots {
			if slot.VolumeID != volumeID || slot.ReplicaID == "" {
				continue
			}
			if slotExpired(slot, obs, now) || slot.DataAddr == "" {
				continue
			}
			seen[slot.ReplicaID] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for replicaID := range seen {
		out = append(out, replicaID)
	}
	sort.Strings(out)
	return out
}

// SetNowForTest replaces the store's clock source. Tests use this
// to drive expiry / freshness deterministically. Symmetric with
// authority.TopologyController.SetNowForTest. Production code
// MUST NOT call this; a boundary-guard test enforces that the
// identifier appears only in _test.go files.
func (s *ObservationStore) SetNowForTest(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

// NewObservationStore constructs a store with the given freshness
// configuration. `now` is injected for deterministic testing; pass
// time.Now in production.
func NewObservationStore(config FreshnessConfig, now func() time.Time) *ObservationStore {
	if now == nil {
		now = time.Now
	}
	cfg := config.withDefaults()
	return &ObservationStore{
		config:          cfg,
		now:             now,
		startedAt:       now(),
		observations:    map[string]Observation{},
		supersededSlots: make(map[[3]string]uint64),
	}
}

// SetOnMutation registers a callback that fires whenever a
// successful ingest changes store contents. The callback must
// NOT block; typical use is a non-blocking signal into the host's
// rebuild goroutine.
//
// Registered after construction rather than at construction so
// the host can assemble store and controller before wiring.
func (s *ObservationStore) SetOnMutation(fn func()) {
	s.mu.Lock()
	s.onMutation = fn
	s.mu.Unlock()
}

// ErrInvalidObservation is returned from Ingest for clearly
// malformed observations. Valid observations may still be
// semantically ineligible (expired, conflicting) — that is the
// builder's job, not the store's.
var ErrInvalidObservation = errors.New("observation_store: invalid observation")

// Ingest validates and stores an observation, computes its
// ExpiresAt, and fires the registered mutation callback if
// anything changed.
//
// Latest-wins per (ServerID, volume, replica). A slot report strictly
// older than that slot's stored fact is ignored; other slots in the
// observation are accepted independently. No error is returned for
// superseded reports.
func (s *ObservationStore) Ingest(obs Observation) error {
	_, err := s.ingest(obs)
	return err
}

// SlotHealthTransition is a slot whose explicit unhealthy reason changed
// with an accepted observation: Old is the reason in the same server's
// previously stored fact for that slot (whether or not it had expired;
// "" when there was none), New the reason just stored.
type SlotHealthTransition struct {
	VolumeID  string
	ReplicaID string
	Old       string
	New       string
}

// IngestWithHealthTransitions ingests like Ingest and returns, derived
// from the accepted mutation itself under the store lock, every slot
// whose unhealthy reason changed. A refused (older) observation returns
// none, and expiry never produces one: only an accepted report does.
func (s *ObservationStore) IngestWithHealthTransitions(obs Observation) ([]SlotHealthTransition, error) {
	return s.ingest(obs)
}

func (s *ObservationStore) ingest(obs Observation) ([]SlotHealthTransition, error) {
	if obs.ServerID == "" {
		return nil, ErrInvalidObservation
	}
	if obs.ObservedAt.IsZero() {
		return nil, ErrInvalidObservation
	}
	// One fact per volume/replica slot: a report that names a slot twice
	// is refused before any mutation, so storage and health transitions
	// can never disagree about which entry is the slot (#134).
	seen := make(map[string]struct{}, len(obs.Slots))
	for _, slot := range obs.Slots {
		k := slotKey(slot)
		if _, dup := seen[k]; dup {
			return nil, ErrInvalidObservation
		}
		seen[k] = struct{}{}
	}

	// Always compute ExpiresAt from store config — do not trust
	// caller-supplied ExpiresAt to prevent out-of-band freshness
	// fabrication.
	obs.ExpiresAt = obs.ObservedAt.Add(s.config.FreshnessWindow)
	for i := range obs.Slots {
		obs.Slots[i].ObservedAt = obs.ObservedAt
		obs.Slots[i].ExpiresAt = obs.ExpiresAt
	}

	var fn func()
	var transitions []SlotHealthTransition
	type supersededReport struct {
		volume, replica string
		sentAt, newest  time.Time
		total           uint64
	}
	var dropped []supersededReport
	// Both the all-superseded and mixed-report paths release mu before
	// returning. Logging never runs under the store lock.
	defer func() {
		for _, d := range dropped {
			log.Printf("heartbeat superseded server=%s volume=%s replica=%s sent_at=%s newest=%s total=%d",
				obs.ServerID, d.volume, d.replica, d.sentAt.UTC().Format(time.RFC3339Nano), d.newest.UTC().Format(time.RFC3339Nano), d.total)
		}
	}()
	s.mu.Lock()
	prev, hadPrev := s.observations[obs.ServerID]
	// Health transitions, from this accepted report against the same
	// slot's stored fact. V1-SPEC r2.53: timestamps order one sender's
	// slot only; a later report from another slot cannot supersede it.
	accepted := make([]SlotFact, 0, len(obs.Slots))
	for _, slot := range obs.Slots {
		old := ""
		var newest time.Time
		if hadPrev {
			for _, p := range prev.Slots {
				if slotKey(p) == slotKey(slot) {
					old = p.UnhealthyReason
					newest = slotObservedAt(p, prev)
					break
				}
			}
		}
		if newest.After(slot.ObservedAt) {
			s.supersededCount++
			key := [3]string{obs.ServerID, slot.VolumeID, slot.ReplicaID}
			s.supersededSlots[key]++
			dropped = append(dropped, supersededReport{slot.VolumeID, slot.ReplicaID, slot.ObservedAt, newest, s.supersededSlots[key]})
			continue
		}
		accepted = append(accepted, slot)
		if old != slot.UnhealthyReason {
			transitions = append(transitions, SlotHealthTransition{
				VolumeID: slot.VolumeID, ReplicaID: slot.ReplicaID, Old: old, New: slot.UnhealthyReason,
			})
		}
	}
	if len(obs.Slots) > 0 && len(accepted) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	obs.Slots = accepted
	if hadPrev {
		obs.Slots = mergeServerSlots(prev.Slots, obs.Slots)
		if prev.ObservedAt.After(obs.ObservedAt) {
			// Preserve the newest server metadata without lending its
			// timestamp or freshness to another process's slot.
			obs.ObservedAt, obs.ExpiresAt, obs.Server = prev.ObservedAt, prev.ExpiresAt, prev.Server
		}
	}
	s.observations[obs.ServerID] = obs
	s.revision++
	fn = s.onMutation
	s.mu.Unlock()

	if fn != nil {
		fn()
	}
	return transitions, nil
}

func mergeServerSlots(prev []SlotFact, next []SlotFact) []SlotFact {
	if len(prev) == 0 {
		return cloneSlots(next)
	}
	out := make([]SlotFact, 0, len(prev)+len(next))
	replaced := make(map[string]struct{}, len(next))
	for _, slot := range next {
		replaced[slotKey(slot)] = struct{}{}
	}
	for _, slot := range prev {
		if _, ok := replaced[slotKey(slot)]; ok {
			continue
		}
		out = append(out, cloneSlot(slot))
	}
	for _, slot := range next {
		out = append(out, cloneSlot(slot))
	}
	return out
}

func slotKey(slot SlotFact) string {
	return slot.VolumeID + "\x00" + slot.ReplicaID
}

func cloneSlots(in []SlotFact) []SlotFact {
	out := make([]SlotFact, 0, len(in))
	for _, slot := range in {
		out = append(out, cloneSlot(slot))
	}
	return out
}

func cloneSlot(in SlotFact) SlotFact {
	out := in
	out.Frontends = append([]FrontendTargetFact(nil), in.Frontends...)
	return out
}

func slotExpired(slot SlotFact, obs Observation, now time.Time) bool {
	expiresAt := slot.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = obs.ExpiresAt
	}
	return !expiresAt.IsZero() && now.After(expiresAt)
}

func slotObservedAt(slot SlotFact, obs Observation) time.Time {
	if !slot.ObservedAt.IsZero() {
		return slot.ObservedAt
	}
	return obs.ObservedAt
}

// storeSnapshot is the immutable per-call value consumed by the
// snapshot builder. It deliberately DOES NOT reference the live
// store; the builder operates on this value alone. This is the
// API boundary that keeps supportability logic off the
// synthesized-output back-edge (sketch §12 one-way pipeline).
type storeSnapshot struct {
	observations    map[string]Observation
	serverFreshness map[string]ServerFreshness
	revision        uint64
	evaluatedAt     time.Time
	startedAt       time.Time
	pendingGrace    time.Duration
	freshnessWindow time.Duration
}

// ServerFreshness is the derived freshness state of one ServerID
// at evaluation time. Kept distinct from an implicit bool so
// pending-vs-unsupported remains explicit through the API.
// (Architect execution note.)
type ServerFreshness uint8

const (
	// ServerNeverObserved: the store has no observation for this
	// ServerID. Within PendingGrace of store start time, this is
	// pending; otherwise it surfaces as missing.
	ServerNeverObserved ServerFreshness = iota
	// ServerBootstrapping: never observed AND the store has been
	// running for less than PendingGrace. The consumer treats
	// this as pending, not unsupported.
	ServerBootstrapping
	// ServerMissing: never observed AND past the bootstrap grace.
	// The consumer treats this as unsupported with reason
	// ReasonMissingServerObservation.
	ServerMissing
	// ServerFresh: observed within FreshnessWindow.
	ServerFresh
	// ServerExpired: observed, but now > ExpiresAt. The fact
	// stays visible for diagnosis but is ineligible for
	// supported synthesis.
	ServerExpired
)

// Snapshot returns an immutable per-call snapshot of the store
// plus pre-computed per-server freshness state. The builder
// receives this and performs all supportability logic against it
// without ever touching the live store or synthesized output.
func (s *ObservationStore) Snapshot() storeSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	obsCopy := make(map[string]Observation, len(s.observations))
	freshness := make(map[string]ServerFreshness, len(s.observations))
	for id, obs := range s.observations {
		obsCopy[id] = obs
		if now.After(obs.ExpiresAt) {
			freshness[id] = ServerExpired
		} else {
			freshness[id] = ServerFresh
		}
	}
	return storeSnapshot{
		observations:    obsCopy,
		serverFreshness: freshness,
		revision:        s.revision,
		evaluatedAt:     now,
		startedAt:       s.startedAt,
		pendingGrace:    s.config.PendingGrace,
		freshnessWindow: s.config.FreshnessWindow,
	}
}

// freshnessFor returns the ServerFreshness of a given ServerID
// at evaluation time. If the ServerID is not present in the
// observations map, it returns either ServerBootstrapping or
// ServerMissing depending on whether the store is still within
// its bootstrap pending-grace window.
func (ss storeSnapshot) freshnessFor(serverID string) ServerFreshness {
	if f, ok := ss.serverFreshness[serverID]; ok {
		return f
	}
	if ss.evaluatedAt.Sub(ss.startedAt) < ss.pendingGrace {
		return ServerBootstrapping
	}
	return ServerMissing
}

// freshServerInventory returns the sorted set of ServerIDs with
// a fresh observation. This is the PRE-SNAPSHOT input used by
// supportability rules to decide missing-server cases — it is
// NOT ClusterSnapshot.Servers (which is an output; sketch §12).
func (ss storeSnapshot) freshServerInventory() []string {
	out := make([]string, 0, len(ss.serverFreshness))
	for id, f := range ss.serverFreshness {
		if f == ServerFresh {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// GC removes observations strictly older than retain. Typically
// called periodically by the host to bound store memory. GC only
// removes entries that cannot meaningfully influence any future
// decision; expired entries that are still within retain are
// kept visible.
//
// Returns the number of entries removed. A non-zero return fires
// the mutation callback, since supportability of some volume may
// have changed.
func (s *ObservationStore) GC(retain time.Duration) int {
	var fn func()
	var removed int
	s.mu.Lock()
	now := s.now()
	for id, obs := range s.observations {
		if now.Sub(obs.ObservedAt) > retain {
			delete(s.observations, id)
			removed++
		}
	}
	if removed > 0 {
		s.revision++
		fn = s.onMutation
	}
	s.mu.Unlock()

	if fn != nil {
		fn()
	}
	return removed
}
