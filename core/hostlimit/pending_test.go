package hostlimit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// While a caller holds HoldUrgentPending, Normal admissions on the host
// wait (ErrUrgentPending, an ErrSlotsHeld); urgent ones still run.
func TestNormalWaitsWhileUrgentPendingIsHeld(t *testing.T) {
	c := testConfig(t)
	l := newTest(t, c)
	release, held, err := HoldUrgentPending(c.Directory)
	if err != nil || !held {
		t.Fatalf("hold: held=%t err=%v", held, err)
	}
	if id, err := l.Acquire(context.Background(), Normal, nil); id != 0 || !errors.Is(err, ErrUrgentPending) || !errors.Is(err, ErrSlotsHeld) {
		t.Fatalf("normal admission with an urgent pending: id=%d err=%v", id, err)
	}
	id, err := l.Acquire(context.Background(), Urgent, nil)
	if err != nil {
		t.Fatalf("urgent admission refused: %v", err)
	}
	l.Release(id)
	release()
	waitAdmitted(t, l, Normal)
}

// An urgent cut holds the pending signal for its lease: another process's
// Normal admission waits until the urgent cut is released.
func TestUrgentLeaseHoldsThePendingSignal(t *testing.T) {
	c := testConfig(t)
	a, b := newTest(t, c), newTest(t, c)
	id, err := a.Acquire(context.Background(), Urgent, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Acquire(context.Background(), Normal, nil); !errors.Is(err, ErrUrgentPending) {
		t.Fatalf("normal admission during an urgent lease: %v", err)
	}
	a.Release(id)
	waitAdmitted(t, b, Normal)
}

func waitAdmitted(t *testing.T, l *Limiter, c Class) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		id, err := l.Acquire(context.Background(), c, nil)
		if err == nil {
			l.Release(id)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("still refused after the pending signal ended: %v", err)
		}
	}
}

func TestCheckLaunch(t *testing.T) {
	local := func(string) (string, bool) { return "", false }
	c := testConfig(t)
	if err := checkLaunch(c, nil, local); err != nil {
		t.Fatalf("a fresh local directory refused: %v", err)
	}
	refused := map[string]struct {
		c      Config
		roots  []string
		remote func(string) (string, bool)
		want   string
	}{
		"missing":   {DefaultConfig(filepath.Join(c.Directory, "missing")), nil, local, "missing"},
		"temp root": {c, []string{filepath.Dir(c.Directory)}, local, "temporary-file cleaners"},
		"remote":    {c, nil, func(string) (string, bool) { return "NFS", true }, "on NFS, a remote file system"},
		"9p":        {c, nil, func(string) (string, bool) { return "9p", true }, "on 9p, a remote file system"},
	}
	file := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	refused["file"] = struct {
		c      Config
		roots  []string
		remote func(string) (string, bool)
		want   string
	}{DefaultConfig(file), nil, local, "must be a directory"}
	for name, tc := range refused {
		if err := checkLaunch(tc.c, tc.roots, tc.remote); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: %v; want %q", name, err, tc.want)
		}
	}
	// A configuration published by other processes must be identical.
	l := newTest(t, c)
	id, err := l.Acquire(context.Background(), Normal, nil)
	if err != nil {
		t.Fatal(err)
	}
	l.Release(id)
	if err := checkLaunch(c, nil, local); err != nil {
		t.Fatalf("the same flags refused: %v", err)
	}
	other := c
	other.NormalSlots = 3
	if err := checkLaunch(other, nil, local); err == nil || !strings.Contains(err.Error(), "identical flags") {
		t.Fatalf("different flags accepted: %v", err)
	}
}
