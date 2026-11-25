package master

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// The promotion probe round. When the master decides a promotion it mints a
// fresh round id, sends a ProbeRound to the node of each replica it asks
// (the current primary and every candidate) over that node's authenticated
// round stream, and counts only the answers that carry this round id, from
// the node it asked, for the replica it asked. Nothing else a replica says
// is promotion evidence: a report, an old answer, an answer for another
// replica or from another node are never counted.

// probeHub routes rounds to subscribed nodes and answers to open rounds.
type probeHub struct {
	mu     sync.Mutex
	subs   map[string]map[chan *control.ProbeRound]struct{} // server id -> its streams
	rounds map[uint64]*openRound
}

type openRound struct {
	volumeID string
	asked    map[string]string // replica id -> the server id it was asked on
	answers  chan *control.ProbeAnswer
}

func newProbeHub() *probeHub {
	return &probeHub{subs: map[string]map[chan *control.ProbeRound]struct{}{}, rounds: map[uint64]*openRound{}}
}

// subscribe opens a node's round stream; the returned cancel closes it.
func (h *probeHub) subscribe(serverID string) (<-chan *control.ProbeRound, func()) {
	ch := make(chan *control.ProbeRound, 16)
	h.mu.Lock()
	if h.subs[serverID] == nil {
		h.subs[serverID] = map[chan *control.ProbeRound]struct{}{}
	}
	h.subs[serverID][ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs[serverID], ch)
		if len(h.subs[serverID]) == 0 {
			delete(h.subs, serverID)
		}
		h.mu.Unlock()
	}
}

// open registers a round; close removes it (late answers are then dropped).
func (h *probeHub) open(roundID uint64, volumeID string) *openRound {
	r := &openRound{volumeID: volumeID, asked: map[string]string{}, answers: make(chan *control.ProbeAnswer, 16)}
	h.mu.Lock()
	h.rounds[roundID] = r
	h.mu.Unlock()
	return r
}

func (h *probeHub) close(roundID uint64) {
	h.mu.Lock()
	delete(h.rounds, roundID)
	h.mu.Unlock()
}

// ask sends the round for replicaID to its node; false when the node has no
// open round stream (it cannot answer: the replica is not ready).
func (h *probeHub) ask(roundID uint64, r *openRound, serverID, replicaID string) bool {
	msg := &control.ProbeRound{VolumeId: r.volumeID, ReplicaId: replicaID, RoundId: roundID}
	h.mu.Lock()
	defer h.mu.Unlock()
	r.asked[replicaID] = serverID
	sent := false
	for ch := range h.subs[serverID] {
		select {
		case ch <- msg:
			sent = true
		default: // a full stream is a slow node: its answer will be late
		}
	}
	return sent
}

// deliver hands an answer to its open round if it is the answer that round
// asked for: this round id, this volume, a replica the round asked, from the
// node it was asked on. Anything else is dropped (false).
func (h *probeHub) deliver(a *control.ProbeAnswer) bool {
	h.mu.Lock()
	r := h.rounds[a.GetRoundId()]
	ok := r != nil && a.GetVolumeId() == r.volumeID && r.asked[a.GetReplicaId()] != "" && r.asked[a.GetReplicaId()] == a.GetServerId()
	h.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case r.answers <- a:
		return true
	default:
		return false
	}
}

// roundEvidenceProvider is the PromotionEvidenceProvider of the probe round.
type roundEvidenceProvider struct {
	hub        *probeHub
	serverOf   func(volumeID, replicaID string) string
	ackProfile string
	required   map[string]uint64
	wait       time.Duration
}

func newRoundID() uint64 {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			continue
		}
		if id := binary.BigEndian.Uint64(b[:]); id != 0 {
			return id
		}
	}
}

func (p *roundEvidenceProvider) ProbePromotionCandidates(volumeID string, current authority.AuthorityBasis, candidates []authority.ReplicaCandidate) (PromotionProbeResult, error) {
	required, requiredKnown := p.required[volumeID]
	result := PromotionProbeResult{
		AckProfile:   p.ackProfile,
		SyncAckLSN:   required,
		SyncAckKnown: requiredKnown,
		Candidates:   make([]PromotionCandidateEvidence, 0, len(candidates)),
	}
	roundID := newRoundID()
	r := p.hub.open(roundID, volumeID)
	defer p.hub.close(roundID)

	want := map[string]bool{}
	askReplica := func(replicaID, serverID string) {
		if serverID == "" {
			serverID = p.serverOf(volumeID, replicaID)
		}
		if serverID != "" && p.hub.ask(roundID, r, serverID, replicaID) {
			want[replicaID] = true
		}
	}
	if current.ReplicaID != "" {
		askReplica(current.ReplicaID, "")
	}
	for _, c := range candidates {
		askReplica(c.ReplicaID, c.ServerID)
	}

	got := map[string]*control.ProbeAnswer{}
	timer := time.NewTimer(p.wait)
	defer timer.Stop()
	for len(got) < len(want) {
		select {
		case a := <-r.answers:
			got[a.GetReplicaId()] = a
		case <-timer.C:
			goto done
		}
	}
done:
	if current.ReplicaID != "" {
		if a := got[current.ReplicaID]; a != nil {
			result.CurrentKnown = true
			result.Current = PromotionCandidateEvidence{ReplicaID: current.ReplicaID, Ready: a.GetPrimaryReady(), DurableLSN: a.GetDurableEnd()}
		} else {
			result.Current = PromotionCandidateEvidence{ReplicaID: current.ReplicaID, ProbeAddr: "no answer"}
		}
	}
	for _, c := range candidates {
		a := got[c.ReplicaID]
		if a == nil {
			result.Candidates = append(result.Candidates, PromotionCandidateEvidence{ReplicaID: c.ReplicaID, ProbeAddr: "no answer"})
			continue
		}
		invalid := a.GetTablePresent() && !a.GetTableValid()
		result.Candidates = append(result.Candidates, PromotionCandidateEvidence{
			ReplicaID:        c.ReplicaID,
			DurableLSN:       a.GetDurableEnd(),
			LastDataEpoch:    a.GetLastDataEpoch(),
			LastDataFirstLSN: a.GetLastDataFirstLsn(),
			TableInvalid:     invalid,
			// A promotion target answers as a ready replica, from a store
			// whose epoch table is present and valid.
			Ready: a.GetReplicaReady() && a.GetTablePresent() && a.GetTableValid(),
		})
	}
	return result, nil
}
