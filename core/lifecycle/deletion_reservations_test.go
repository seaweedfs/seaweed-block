package lifecycle

import (
	"errors"
	"testing"
)

func reopen(t *testing.T, dir string) *DeletionReservationStore {
	t.Helper()
	s, err := OpenDeletionReservationStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Admit holds by token, Abort gives a token back (the reservation goes with
// the last), Commit is permanent; every state survives a reopen.
func TestDeletionReservations_AdmitAbortCommitSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	if _, err := s.Admit("v1", ""); !errors.Is(err, ErrReservationToken) {
		t.Fatalf("admit without a token: %v", err)
	}
	for _, tok := range []string{"d1", "d2"} {
		if _, err := s.Admit("v1", tok); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Abort("v1", "d1"); err != nil || !reopen(t, dir).Reserved("v1") {
		t.Fatalf("abort of one of two tokens: %v; want still reserved after restart", err)
	}
	if _, err := s.Abort("v1", "d2"); err != nil || s.Reserved("v1") || reopen(t, dir).Reserved("v1") {
		t.Fatalf("abort of the last token: %v; want the reservation gone (also after restart)", err)
	}
	if _, err := s.Admit("v2", "d3"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit("v2", "other"); !errors.Is(err, ErrReservationToken) {
		t.Fatalf("commit by a token that holds nothing: %v", err)
	}
	if _, err := s.Commit("v2", "d3"); err != nil {
		t.Fatal(err)
	}
	r := reopen(t, dir)
	if got, ok := r.Get("v2"); !ok || got.State != ReservationCommitted {
		t.Fatalf("committed reservation after restart: %+v %t", got, ok)
	}
	if _, err := r.Abort("v2", "d3"); !errors.Is(err, ErrReservationCommitted) || !r.Reserved("v2") {
		t.Fatalf("abort of a committed deletion: %v; want refused, still reserved", err)
	}
	if _, err := r.Admit("v2", "d4"); err != nil {
		t.Fatalf("a rerun's admission of a committed deletion: %v", err)
	}
	if got, _ := r.Get("v2"); got.State != ReservationCommitted {
		t.Fatalf("admit downgraded a committed reservation: %+v", got)
	}
}

// Persistence faults fail closed: a failed write before the rename leaves
// nothing (and returns the error); an unknown outcome (rename or directory
// sync failed) keeps a protective change in memory and never a releasing
// one.
func TestDeletionReservations_PersistenceFaultsFailClosed(t *testing.T) {
	fail := errors.New("injected")
	dir := t.TempDir()
	s := reopen(t, dir)

	s.failWrite = func() error { return fail }
	if _, err := s.Admit("w", "d1"); !errors.Is(err, fail) || s.Reserved("w") || reopen(t, dir).Reserved("w") {
		t.Fatalf("admit with a failing write: %v; want an error and no reservation", err)
	}
	s.failWrite = nil

	s.failRename = func() error { return fail }
	if _, err := s.Admit("r", "d1"); !errors.Is(err, fail) || !s.Reserved("r") {
		t.Fatalf("admit with a failing rename: %v; want an error and the reservation kept in memory", err)
	}
	s.failRename = nil

	s.failDirSync = func() error { return fail }
	if _, err := s.Admit("y", "d1"); !errors.Is(err, fail) || !s.Reserved("y") {
		t.Fatalf("admit with a failing directory sync: %v; want an error and the reservation kept", err)
	}
	s.failDirSync = nil

	if _, err := s.Admit("a", "d1"); err != nil {
		t.Fatal(err)
	}
	s.failRename = func() error { return fail }
	if _, err := s.Abort("a", "d1"); !errors.Is(err, fail) || !s.Reserved("a") {
		t.Fatalf("abort with a failing rename: %v; want an error and the reservation kept", err)
	}
	s.failRename = nil
	if !reopen(t, dir).Reserved("a") {
		t.Fatal("a successfully written reservation is lost on restart")
	}
}
