package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// roundNode is a node on the hub that answers its rounds with answer(round).
func roundNode(t *testing.T, hub *probeHub, serverID string, answer func(*control.ProbeRound) *control.ProbeAnswer) {
	t.Helper()
	rounds, cancel := hub.subscribe(serverID)
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); close(done) })
	go func() {
		for {
			select {
			case <-done:
				return
			case r := <-rounds:
				if a := answer(r); a != nil {
					hub.deliver(a)
				}
			}
		}
	}()
}

// storeOf is the store id a replica's node answers with in these tests.
func storeOf(replicaID string) storage.StoreID {
	return storage.StoreID{'s', byte(len(replicaID)), replicaID[len(replicaID)-1]}
}

// attestedAs is the primary's attestation: each replica on storeOf(replica).
func attestedAs(_, replicaID string) (storage.StoreID, bool) { return storeOf(replicaID), true }

func readyAnswer(server string) func(*control.ProbeRound) *control.ProbeAnswer {
	return func(r *control.ProbeRound) *control.ProbeAnswer {
		id := storeOf(r.ReplicaId)
		return &control.ProbeAnswer{ServerId: server, VolumeId: r.VolumeId, ReplicaId: r.ReplicaId, RoundId: r.RoundId, StoreId: id[:],
			DurableEnd: 9, LastDataEpoch: 2, LastDataFirstLsn: 5, TablePresent: true, TableValid: true, ReplicaReady: true}
	}
}

func TestProbeRound_CountsOnlyThisRoundsAnswersFromTheAskedNode(t *testing.T) {
	hub := newProbeHub()
	servers := map[string]string{"r2": "s2", "r3": "s3"}
	p := &roundEvidenceProvider{hub: hub, serverOf: func(_, r string) string { return servers[r] }, ackProfile: "sync-quorum", wait: 300 * time.Millisecond, attested: attestedAs}

	roundNode(t, hub, "s2", readyAnswer("s2"))
	// s3 answers with an OLD round id (a report from before this decision),
	// and also for r2 from the wrong node: neither may count.
	roundNode(t, hub, "s3", func(r *control.ProbeRound) *control.ProbeAnswer {
		old := readyAnswer("s3")(r)
		old.RoundId = r.RoundId + 1
		r2store := storeOf("r2")
		hub.deliver(&control.ProbeAnswer{ServerId: "s3", VolumeId: r.VolumeId, ReplicaId: "r2", RoundId: r.RoundId, StoreId: r2store[:], DurableEnd: 99, TablePresent: true, TableValid: true, ReplicaReady: true})
		return old
	})

	res, err := p.ProbePromotionCandidates("v1", authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: "r2", ServerID: "s2"}, {ReplicaID: "r3", ServerID: "s3"}})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]PromotionCandidateEvidence{}
	for _, c := range res.Candidates {
		got[c.ReplicaID] = c
	}
	if c := got["r2"]; !c.Ready || c.DurableLSN != 9 || c.LastDataEpoch != 2 || c.LastDataFirstLSN != 5 || c.StoreID != storeOf("r2") {
		t.Fatalf("r2 answered this round from its node: %+v", c)
	}
	if c := got["r3"]; c.Ready || c.DurableLSN != 0 {
		t.Fatalf("r3 answered only with another round's id: counted %+v", c)
	}
}

func TestProbeRound_InvalidTableOrNoAnswerIsNotReady(t *testing.T) {
	hub := newProbeHub()
	p := &roundEvidenceProvider{hub: hub, serverOf: func(_, r string) string { return map[string]string{"r2": "s2", "r3": "s3"}[r] }, wait: 200 * time.Millisecond, attested: attestedAs}
	roundNode(t, hub, "s2", func(r *control.ProbeRound) *control.ProbeAnswer {
		a := readyAnswer("s2")(r)
		a.TableValid = false // a recovery left its table invalid
		return a
	})
	// s3 has no round stream at all.
	res, _ := p.ProbePromotionCandidates("v1", authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: "r2", ServerID: "s2"}, {ReplicaID: "r3", ServerID: "s3"}})
	for _, c := range res.Candidates {
		if c.Ready {
			t.Fatalf("%s is ready: %+v", c.ReplicaID, c)
		}
		if c.ReplicaID == "r2" && !c.TableInvalid {
			t.Fatalf("r2's invalid table is not reported: %+v", c)
		}
	}
}

// A candidate is ready only on the store its primary attested (the store
// counted toward its write ACKs): another store (a replaced disk), no store id,
// or a replica never attested is not.
func TestProbeRound_OnlyTheAttestedStoreIsReady(t *testing.T) {
	servers := map[string]string{"r2": "s2", "r3": "s3", "r4": "s4"}
	for name, c := range map[string]struct {
		attested func(string, string) (storage.StoreID, bool)
		answer   func(*control.ProbeAnswer)
		ready    bool
	}{
		"the attested store":    {attestedAs, func(*control.ProbeAnswer) {}, true},
		"another store":         {attestedAs, func(a *control.ProbeAnswer) { a.StoreId = []byte("another-store-16") }, false},
		"no store id":           {attestedAs, func(a *control.ProbeAnswer) { a.StoreId = nil }, false},
		"never attested":        {func(string, string) (storage.StoreID, bool) { return storage.StoreID{}, false }, func(*control.ProbeAnswer) {}, false},
		"no attestation source": {nil, func(*control.ProbeAnswer) {}, false},
	} {
		t.Run(name, func(t *testing.T) {
			hub := newProbeHub()
			p := &roundEvidenceProvider{hub: hub, serverOf: func(_, r string) string { return servers[r] }, wait: 300 * time.Millisecond, attested: c.attested}
			roundNode(t, hub, "s2", func(r *control.ProbeRound) *control.ProbeAnswer {
				a := readyAnswer("s2")(r)
				c.answer(a)
				return a
			})
			res, err := p.ProbePromotionCandidates("v1", authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: "r2", ServerID: "s2"}})
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Candidates) != 1 || res.Candidates[0].Ready != c.ready {
				t.Fatalf("candidates=%+v; want ready=%t", res.Candidates, c.ready)
			}
		})
	}
}

func TestProbeRound_ALateAnswerAfterTheRoundClosedIsDropped(t *testing.T) {
	hub := newProbeHub()
	r := hub.open(7, "v1")
	hub.ask(7, r, "s2", "r2")
	hub.close(7)
	if hub.deliver(&control.ProbeAnswer{ServerId: "s2", VolumeId: "v1", ReplicaId: "r2", RoundId: 7}) {
		t.Fatal("an answer after its round closed was delivered")
	}
}
