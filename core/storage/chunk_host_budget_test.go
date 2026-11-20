package storage

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"testing"
)

func TestHostCOWCapAbortsCutAndRetainsPermitUntilJoin(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	c := hostlimit.DefaultConfig(t.TempDir())
	c.NormalSlots = 1
	c.UrgentSlots = 1
	c.COWBytes = ctChunkBytes - 1
	budget, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	contender, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { gate.unblock(); _ = l.Close(); budget.Close(); contender.Close() })
	l.cfg.Budget = budget
	before := f.stats().head.Gen
	done, errs := cowStartCut(t, l, gate)
	if _, err = s.Write(5, makeBlock(ctBlockSize, 0xeb)); err != nil {
		t.Fatal("host cap failed guest write:", err)
	}
	ticket, admitErr := contender.Admit(context.Background(), 1, nil)
	if admitErr == nil {
		contender.Release(ticket)
	}
	if !errors.Is(admitErr, hostlimit.ErrBusy) {
		t.Fatalf("revoked but unjoined cut lost permit: %v", admitErr)
	}
	gate.unblock()
	cowWaitSignal(t, done, "host-budget abort settlement")
	if err = <-errs; !errors.Is(err, hostlimit.ErrRevoked) {
		t.Fatalf("cut abort: %v", err)
	}
	if f.stats().head.Gen != before {
		t.Fatal("host-cap aborted cut published")
	}
	b, err := s.Read(5)
	if err != nil || b[0] != 0xeb {
		t.Fatal("guest bytes lost after cut abort", err)
	}
	if l.Stats().CutsAborted["budget_revoked"] != 1 {
		t.Fatal("host cap abort not reported")
	}
}
