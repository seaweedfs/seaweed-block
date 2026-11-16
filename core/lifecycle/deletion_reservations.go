package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Deletion reservations (review of #152 r2): the master-side exclusion of a
// chunk volume id that a delete has admitted. It is kept apart from the
// desired-volume records, so neither an unknown id (no VolumeRecord) nor the
// ordinary DeleteVolume/CreateVolume path can drop it.
//
//   - admitted: one or more deletes hold it (by token). An admitted delete
//     that gives back without irreversible effects aborts its token; the
//     reservation goes when no token is left.
//   - committed: a delete passed its irreversible boundary. Permanent: the
//     id is never created, placed, bound or assigned again.
//
// Writes are crash-safe and durable (synced temp file, atomic rename, synced
// directory). A write whose outcome is unknown fails closed: the store keeps
// the more protective state in memory and reports the error.

const (
	ReservationAdmitted  = "admitted"
	ReservationCommitted = "committed"

	reservationsFile = "reservations.json"
)

var (
	// ErrReservationCommitted refuses giving back a committed deletion.
	ErrReservationCommitted = errors.New("lifecycle: volume deletion is committed")
	// ErrReservationToken refuses an empty or unknown delete token.
	ErrReservationToken = errors.New("lifecycle: deletion token required")
)

// DeletionReservation is one volume's deletion exclusion.
type DeletionReservation struct {
	VolumeID string   `json:"volume_id"`
	State    string   `json:"state"`
	Tokens   []string `json:"tokens,omitempty"`
}

// DeletionReservationStore persists the reservations as one JSON document.
type DeletionReservationStore struct {
	mu      sync.Mutex
	dir     string
	records map[string]DeletionReservation

	// Test seams for persistence faults (nil in production).
	failWrite, failRename, failDirSync func() error
}

// OpenDeletionReservationStore opens or creates the store in dir.
func OpenDeletionReservationStore(dir string) (*DeletionReservationStore, error) {
	return openDeletionReservationStore(dir, syncBindingParent)
}

func openDeletionReservationStore(dir string, syncParent func(string) error) (*DeletionReservationStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty dir", ErrInvalidVolumeSpec)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lifecycle: mkdir %q: %w", dir, err)
	}
	// Persist the directory entry before any reservation can be
	// acknowledged (the binding store's initialization guarantee).
	if err := syncParent(filepath.Dir(dir)); err != nil {
		return nil, fmt.Errorf("lifecycle: sync the parent of %q: %w", dir, err)
	}
	s := &DeletionReservationStore{dir: dir, records: map[string]DeletionReservation{}}
	b, err := os.ReadFile(filepath.Join(dir, reservationsFile))
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, fmt.Errorf("lifecycle: read deletion reservations: %w", err)
	}
	if err := json.Unmarshal(b, &s.records); err != nil {
		return nil, fmt.Errorf("lifecycle: decode deletion reservations: %w", err)
	}
	for v, r := range s.records {
		if r.VolumeID != v || (r.State != ReservationAdmitted && r.State != ReservationCommitted) {
			return nil, fmt.Errorf("lifecycle: deletion reservation %q is invalid", v)
		}
	}
	return s, nil
}

// Get returns vol's reservation.
func (s *DeletionReservationStore) Get(vol string) (DeletionReservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[vol]
	return r, ok
}

// Reserved reports whether vol is excluded (admitted or committed).
func (s *DeletionReservationStore) Reserved(vol string) bool {
	_, ok := s.Get(vol)
	return ok
}

// Admit adds token's hold on vol (idempotent; a committed reservation stays
// committed).
func (s *DeletionReservationStore) Admit(vol, token string) (DeletionReservation, error) {
	if err := validateVolumeID(vol); err != nil {
		return DeletionReservation{}, err
	}
	if token == "" {
		return DeletionReservation{}, ErrReservationToken
	}
	return s.update(vol, true, func(r *DeletionReservation, ok bool) (bool, error) {
		if !ok {
			*r = DeletionReservation{VolumeID: vol, State: ReservationAdmitted}
		}
		if !containsToken(r.Tokens, token) {
			r.Tokens = append(r.Tokens, token)
			sort.Strings(r.Tokens)
		}
		return true, nil
	})
}

// Commit makes vol's reservation permanent; token must hold it.
func (s *DeletionReservationStore) Commit(vol, token string) (DeletionReservation, error) {
	return s.update(vol, true, func(r *DeletionReservation, ok bool) (bool, error) {
		if !ok || (r.State != ReservationCommitted && !containsToken(r.Tokens, token)) {
			return false, ErrReservationToken
		}
		r.State = ReservationCommitted
		return true, nil
	})
}

// Abort gives back token's hold; the reservation goes with its last token.
// A committed reservation is never given back.
func (s *DeletionReservationStore) Abort(vol, token string) (DeletionReservation, error) {
	return s.update(vol, false, func(r *DeletionReservation, ok bool) (bool, error) {
		if !ok {
			return false, nil // nothing held: already given back
		}
		if r.State == ReservationCommitted {
			return false, ErrReservationCommitted
		}
		kept := r.Tokens[:0:0]
		for _, t := range r.Tokens {
			if t != token {
				kept = append(kept, t)
			}
		}
		r.Tokens = kept
		return true, nil
	})
}

func containsToken(tokens []string, t string) bool {
	for _, x := range tokens {
		if x == t {
			return true
		}
	}
	return false
}

// update applies change to vol's reservation and persists it. protective
// says whether the change only adds exclusion: on a write whose outcome is
// unknown (rename or directory sync failed) a protective change is kept in
// memory (fail closed), a releasing one is not.
func (s *DeletionReservationStore) update(vol string, protective bool, change func(r *DeletionReservation, ok bool) (bool, error)) (DeletionReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[vol]
	r.Tokens = append([]string(nil), r.Tokens...)
	changed, err := change(&r, ok)
	if err != nil || !changed {
		return r, err
	}
	next := make(map[string]DeletionReservation, len(s.records)+1)
	for k, v := range s.records {
		next[k] = v
	}
	if r.State == ReservationAdmitted && len(r.Tokens) == 0 {
		delete(next, vol)
	} else {
		next[vol] = r
	}
	if err := s.persist(next, protective); err != nil {
		return r, err
	}
	s.records = next
	return r, nil
}

func (s *DeletionReservationStore) persist(next map[string]DeletionReservation, protective bool) error {
	b, err := json.Marshal(next)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, "reservations-*.tmp")
	if err != nil {
		return fmt.Errorf("lifecycle: deletion reservation write: %w", err)
	}
	defer os.Remove(f.Name())
	if s.failWrite != nil {
		err = s.failWrite()
	}
	if err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("lifecycle: deletion reservation write: %w", err) // nothing replaced
	}
	unknown := func(err error) error {
		if protective {
			s.records = next // may be on disk: keep the exclusion
		}
		return fmt.Errorf("lifecycle: deletion reservation outcome unknown: %w", err)
	}
	if s.failRename != nil {
		if err := s.failRename(); err != nil {
			return unknown(err)
		}
	}
	if err := os.Rename(f.Name(), filepath.Join(s.dir, reservationsFile)); err != nil {
		return unknown(err)
	}
	if s.failDirSync != nil {
		if err := s.failDirSync(); err != nil {
			return unknown(err)
		}
	}
	d, err := os.Open(s.dir)
	if err == nil {
		err = d.Sync()
		if cerr := d.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		return unknown(err)
	}
	return nil
}
