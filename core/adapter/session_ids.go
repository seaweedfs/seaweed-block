package adapter

import "sync"

// SessionIDs is one replica's session-id order: every session the primary
// opens toward that replica (live ship, catch-up, rebuild, fence, probe)
// takes its id here, so a later session always has a larger id. The
// replica admits a mutation lineage only if it is not older than the one it
// holds, so a recovery opened after a live session must outrank it.
type SessionIDs struct {
	mu   sync.Mutex
	last uint64
}

// Next returns a fresh id, larger than every id returned before.
func (s *SessionIDs) Next() uint64 { return s.NextAfter(0) }

// NextAfter returns a fresh id, larger than every id returned before and
// than after.
func (s *SessionIDs) NextAfter(after uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = max(s.last, after) + 1
	return s.last
}

// SessionIDSource is implemented by an executor that carries its replica's
// session order; the adapter takes its session ids there.
type SessionIDSource interface {
	SessionIDs() *SessionIDs
}
