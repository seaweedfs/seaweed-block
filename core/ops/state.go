// Package ops provides the Phase 05 read-only HTTP inspection surface
// for the V3 runnable sparrow. It exposes /status /projection /trace
// for outside readers and testers without creating a second semantic
// route — every endpoint is a thin read over state already owned by
// the adapter and engine.
//
// This package MUST NOT:
//   - make decisions based on projection values
//   - call engine.Apply directly
//   - accept any write/mutation verbs with real side effects
//   - redefine semantic truth
//
// The boundary: ops is the operator-facing mirror of engine state,
// never an alternative authority path.
package ops

import (
	"sync"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// State holds the most recent adapter snapshot for HTTP inspection.
// The sparrow updates it after each demo; the HTTP handlers read it.
//
// Intentionally narrow: a current-demo label plus references to the
// adapter's Projection() and Trace() accessors. No independent
// decision data is stored here — that would be a second authority
// path. Everything is derived from adapter state at read time.
type State struct {
	mu          sync.RWMutex
	currentDemo string
	adapter     *adapter.VolumeReplicaAdapter
}

// NewState returns an empty State. The sparrow calls Update() as it
// runs each demo; handlers read the latest snapshot.
func NewState() *State {
	return &State{}
}

// Update records the current demo label and adapter reference.
// Safe to call from the sparrow run loop; the HTTP handlers will
// see the new adapter's projection/trace on the next request.
func (s *State) Update(demo string, a *adapter.VolumeReplicaAdapter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentDemo = demo
	s.adapter = a
}

// Snapshot returns the current demo label, projection, and trace.
// Returns zero values if no demo has run yet. Zero values are not
// errors — they are honest "nothing yet" outputs.
func (s *State) Snapshot() (demo string, proj engine.ReplicaProjection, trace []engine.TraceEntry) {
	s.mu.RLock()
	a := s.adapter
	demo = s.currentDemo
	s.mu.RUnlock()

	if a == nil {
		return demo, engine.ReplicaProjection{}, nil
	}
	return demo, a.Projection(), a.Trace()
}
