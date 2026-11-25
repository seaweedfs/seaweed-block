package master

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// An answer to an OLD round that arrives late, while a NEW round of the same
// volume and replica is open, is refused: it is evidence of another moment,
// so the new round never counts it (the new round sees no answer from r2).
func TestProbeRound_AnOldRoundsLateAnswerIsRefusedByTheNewRound(t *testing.T) {
	hub := newProbeHub()
	p := &roundEvidenceProvider{hub: hub, serverOf: func(_, r string) string { return "s2" }, ackProfile: "sync_quorum", wait: 200 * time.Millisecond, attested: attestedAs}

	var mu sync.Mutex
	var held *control.ProbeAnswer // round A's answer, held back
	type delivery struct {
		accepted, newerOpen bool
		roundA, roundB      uint64
	}
	delivered := make(chan delivery, 1)
	roundNode(t, hub, "s2", func(r *control.ProbeRound) *control.ProbeAnswer {
		mu.Lock()
		defer mu.Unlock()
		if held == nil {
			held = readyAnswer("s2")(r) // round A: answer later
			held.DurableEnd = 99
			return nil
		}
		// round B: only round A's answer arrives now; B's own never does.
		// Round B must be open at the refusal and distinct from A, so the
		// refusal is the new round's, not a closed hub's. Checked AFTER the
		// delivery: a round is never reopened, so B still registered then
		// proves it was open when A's answer was refused.
		d := delivery{roundA: held.RoundId, roundB: r.RoundId}
		d.accepted = hub.deliver(held)
		hub.mu.Lock()
		_, open := hub.rounds[r.RoundId]
		hub.mu.Unlock()
		d.newerOpen = open && r.RoundId != held.RoundId
		delivered <- d
		return nil
	})

	ask := []authority.ReplicaCandidate{{ReplicaID: "r2", ServerID: "s2"}}
	a, err := p.ProbePromotionCandidates("v1", authority.AuthorityBasis{}, ask)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Candidates) != 1 || a.Candidates[0].Ready {
		t.Fatalf("round A (no answer in time): %+v", a.Candidates)
	}
	b, err := p.ProbePromotionCandidates("v1", authority.AuthorityBasis{}, ask)
	if err != nil {
		t.Fatal(err)
	}
	var d delivery
	select {
	case d = <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("round A's answer was never delivered during round B")
	}
	mu.Lock()
	captured := held != nil
	mu.Unlock()
	if !captured || !d.newerOpen {
		t.Fatalf("no newer round open at the late delivery: captured=%v delivery=%+v", captured, d)
	}
	if d.accepted {
		t.Fatalf("the hub accepted round A's answer (round %d) while round B (round %d) was open", d.roundA, d.roundB)
	}
	if len(b.Candidates) != 1 || b.Candidates[0].Ready || b.Candidates[0].DurableLSN != 0 {
		t.Fatalf("round B counted round A's late answer: %+v", b.Candidates)
	}
}
