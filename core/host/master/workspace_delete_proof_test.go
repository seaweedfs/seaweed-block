package master

import (
	"errors"
	"testing"
)

// creatingStage2 is an RF3 volume whose stores are all attested and whose
// creation has not ended: its primary is gone (it never answers) and the
// survivors answer on valid tables, as the case sets.
func creatingStage2(t *testing.T, set func(p *switchProbe)) (*Host, *switchProbe) {
	t.Helper()
	h := newBootGraceMasterCreating(t, Config{})
	attestAllStores(t, h)
	p := probeOf(t, h)
	p.mu.Lock()
	p.currentUnknown, p.tableValid, p.empty = true, true, true
	if set != nil {
		set(p)
	}
	p.mu.Unlock()
	return h, p
}

func deleteAuthority(h *Host, token string) *volumeDeleteAuthority {
	return h.VolumeDeleteAuthority("vol-rf3", token)
}

// Stage 2, no write anywhere, the primary dead: the delete without the
// holder's acknowledgement is admitted and, read after the horizon, both
// survivors answered empty (N-K+1 = 2): it finishes.
func TestAStage2VolumeNoSurvivorHoldsAWriteOfIsDeleted(t *testing.T) {
	h, _ := creatingStage2(t, nil)
	defer closeTestMaster(t, h)
	a := deleteAuthority(h, "del-empty")
	if err := a.Reserve(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !h.attest.deleteWithoutAck("vol-rf3") {
		t.Fatal("a creating volume's delete was not admitted without the holder's acknowledgement")
	}
	if err := a.NoLiveLease(t.Context()); err != nil {
		t.Fatalf("a volume proven empty: %v", err)
	}
}

// The same volume after a quorum-acknowledged write (r2 holds it): the
// survivor's answer ends creation, so the delete without the holder's
// acknowledgement is not admitted (the ordinary delete applies).
func TestADeleteWithoutAckOfAVolumeThatServedIsRefused(t *testing.T) {
	h, _ := creatingStage2(t, func(p *switchProbe) { p.empty = false })
	defer closeTestMaster(t, h)
	if err := deleteAuthority(h, "del-served").Reserve(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.attest.deleteWithoutAck("vol-rf3") {
		t.Fatal("a volume a survivor holds a write of was admitted for a delete without acknowledgement")
	}
	if h.attest.awaitingFirstReady("vol-rf3") {
		t.Fatal("the survivor's write did not end creation")
	}
}

// Row mut_no_horizon: the survivors looked empty at the reservation, but a
// write was acknowledged before the holder's horizon passed: the proof is
// read after it, sees the write and refuses.
func TestADeleteWithoutAckReadsItsProofAfterTheHorizon(t *testing.T) {
	h, p := creatingStage2(t, nil)
	defer closeTestMaster(t, h)
	a := deleteAuthority(h, "del-late-write")
	if err := a.Reserve(t.Context()); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.empty = false // r2 now holds a write acknowledged before the horizon
	p.mu.Unlock()
	if err := a.NoLiveLease(t.Context()); !errors.Is(err, ErrDeleteNotProvenEmpty) {
		t.Fatalf("err=%v; want ErrDeleteNotProvenEmpty", err)
	}
}

// Row mut_unstable_read: a silent survivor is not empty. With r3 silent
// only one survivor answered empty, need 2: refused. So is a round whose
// answers carry no valid table.
func TestADeleteWithoutAckNeedsNMinusKPlusOneEmptyAnswers(t *testing.T) {
	for name, set := range map[string]func(p *switchProbe){
		"a silent survivor":     func(p *switchProbe) { p.r3Silent = true },
		"no valid table at all": func(p *switchProbe) { p.tableValid = false },
	} {
		t.Run(name, func(t *testing.T) {
			h, _ := creatingStage2(t, set)
			defer closeTestMaster(t, h)
			a := deleteAuthority(h, "del-unstable")
			if err := a.Reserve(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := a.NoLiveLease(t.Context()); !errors.Is(err, ErrDeleteNotProvenEmpty) {
				t.Fatalf("err=%v; want ErrDeleteNotProvenEmpty", err)
			}
		})
	}
}

// Once a delete without acknowledgement is admitted, creation never ends
// (the delete reads the record as it was admitted), also after a restart.
func TestCreationDoesNotEndAfterADeleteWithoutAck(t *testing.T) {
	cfg := Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir()}
	h := newBootGraceMasterCreating(t, cfg)
	attestAllStores(t, h)
	p := probeOf(t, h)
	p.mu.Lock()
	p.empty, p.tableValid = true, true
	p.mu.Unlock()
	if err := deleteAuthority(h, "del-freeze").Reserve(t.Context()); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.currentReady = true // a ready answer after the admission
	p.mu.Unlock()
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if h.inCreationPhase("vol-rf3", line, nil) == "" {
		t.Fatal("creation ended after the delete without acknowledgement was admitted")
	}
	h2 := restartCreatingMaster(t, h, cfg)
	defer closeTestMaster(t, h2)
	if !h2.attest.deleteWithoutAck("vol-rf3") || !h2.attest.awaitingFirstReady("vol-rf3") {
		t.Fatal("after a restart the admitted delete or the creation wait is lost")
	}
}
