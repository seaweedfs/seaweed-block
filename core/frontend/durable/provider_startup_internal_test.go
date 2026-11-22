package durable

import "testing"

func TestChunkStartupOutcomeRejectsOldGenerationAndCoalescesWake(t *testing.T) {
	h := &volHandle{recoverGen: 1}
	p := &DurableProvider{volumes: map[string]*volHandle{"v": h}, startupUpdates: make(chan struct{}, 1)}
	p.endChunkStartup(h, 1)
	h.recoverGen = 2
	h.startupEnded = 0 // RecoverVolume invalidates the old generation under p.mu.
	p.endChunkStartup(h, 1)
	if _, ok := p.ChunkStartupOutcome("v"); ok {
		t.Fatal("stale wait ended the new recovery")
	}
	p.endChunkStartup(h, 2) // full wake channel must not discard the current latched result
	<-p.ChunkStartupUpdates()
	if gen, ok := p.ChunkStartupOutcome("v"); !ok || gen != 2 {
		t.Fatalf("current result lost: %d %v", gen, ok)
	}
	p.endChunkStartup(h, 1)
	if gen, ok := p.ChunkStartupOutcome("v"); !ok || gen != 2 {
		t.Fatal("late old completion overwrote the current result")
	}
}

func TestChunkStartupExitRechecksGenerationBeforeChoice(t *testing.T) {
	h := &volHandle{recoverGen: 2, startupEnded: 2}
	p := &DurableProvider{volumes: map[string]*volHandle{"v": h}}
	calls := 0
	if p.ChooseChunkStartupExit("v", 1, func() { calls++ }) || calls != 0 {
		t.Fatal("stale observation selected process exit")
	}
	if !p.ChooseChunkStartupExit("v", 2, func() { calls++ }) || calls != 1 {
		t.Fatal("current result was not selectable")
	}
	p.closed = true
	if p.ChooseChunkStartupExit("v", 2, func() { calls++ }) || calls != 1 {
		t.Fatal("closed provider selected exit")
	}
}
