package authority

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// ============================================================
// P14 S4 — Observation Store
//
// Push-ingested latest-wins observation store per ServerID. The
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
	config     FreshnessConfig
	now        func() time.Time
	startedAt  time.Time

	mu           sync.Mutex
	observations map[string]Observation // key: ServerID
	revision     uint64
	onMutation   func()
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
		config:       cfg,
		now:          now,
		startedAt:    now(),
		observations: map[string]Observation{},
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
// Latest-wins per ServerID. If an incoming observation has an
// ObservedAt strictly older than the currently-stored one from
// the same server, the store keeps the newer one (ignores the
// stale ingest). No error is returned for that case; it's a
// routine ordering event, not a bug.
func (s *ObservationStore) Ingest(obs Observation) error {
	if obs.ServerID == "" {
		return ErrInvalidObservation
	}
	if obs.ObservedAt.IsZero() {
		return ErrInvalidObservation
	}

	// Always compute ExpiresAt from store config — do not trust
	// caller-supplied ExpiresAt to prevent out-of-band freshness
	// fabrication.
	obs.ExpiresAt = obs.ObservedAt.Add(s.config.FreshnessWindow)

	var fn func()
	s.mu.Lock()
	if prev, ok := s.observations[obs.ServerID]; ok {
		if prev.ObservedAt.After(obs.ObservedAt) {
			// Out-of-order ingest: keep the newer one already
			// stored. No mutation fired.
			s.mu.Unlock()
			return nil
		}
	}
	s.observations[obs.ServerID] = obs
	s.revision++
	fn = s.onMutation
	s.mu.Unlock()

	if fn != nil {
		fn()
	}
	return nil
}

// storeSnapshot is the immutable per-call value consumed by the
// snapshot builder. It deliberately DOES NOT reference the live
// store; the builder operates on this value alone. This is the
// API boundary that keeps supportability logic off the
// synthesized-output back-edge (sketch §12 one-way pipeline).
type storeSnapshot struct {
	observations     map[string]Observation
	serverFreshness  map[string]ServerFreshness
	revision         uint64
	evaluatedAt      time.Time
	startedAt        time.Time
	pendingGrace     time.Duration
	freshnessWindow  time.Duration
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
