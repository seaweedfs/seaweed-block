package master

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
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
	return h.send(r, serverID, replicaID, &control.ProbeRound{VolumeId: r.volumeID, ReplicaId: replicaID, RoundId: roundID})
}

// askFence asks a replica in a fencing round: it makes fenceEpoch its durable
// floor first (when its store is store), then answers.
func (h *probeHub) askFence(roundID uint64, r *openRound, serverID, replicaID string, fenceEpoch uint64, store storage.StoreID) bool {
	return h.send(r, serverID, replicaID, &control.ProbeRound{VolumeId: r.volumeID, ReplicaId: replicaID, RoundId: roundID, FenceEpoch: fenceEpoch, ExpectedStoreId: append([]byte(nil), store[:]...)})
}

func (h *probeHub) send(r *openRound, serverID, replicaID string, msg *control.ProbeRound) bool {
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
	// attested is the store the volume's primary last attested for a
	// replica (Host.AttestedStore); a candidate answering another store, or
	// one never attested, is not Ready.
	attested func(volumeID, replicaID string) (storage.StoreID, bool)
}

// FencePromotionCandidates is the fencing round of a decision to replace the
// primary at line epoch e: each survivor is asked to make e+1 its durable
// floor on its probed store FIRST and then answer (the answer is its promise
// to refuse the old primary). The answers are read as a probe round's; the
// gate counts only those whose floor is e+1.
func (p *roundEvidenceProvider) FencePromotionCandidates(volumeID string, current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	fence := current.Epoch + 1
	candidates := make([]authority.ReplicaCandidate, 0, len(survivors))
	stores := map[string]storage.StoreID{}
	for _, s := range survivors {
		candidates = append(candidates, authority.ReplicaCandidate{ReplicaID: s.ReplicaID})
		// The reference store: the one a primary last attested for it.
		if p.attested != nil {
			if ref, ok := p.attested(volumeID, s.ReplicaID); ok {
				stores[s.ReplicaID] = ref
			}
		}
	}
	result, err := p.round(volumeID, authority.AuthorityBasis{}, candidates, func(roundID uint64, r *openRound, serverID, replicaID string) bool {
		return p.hub.askFence(roundID, r, serverID, replicaID, fence, stores[replicaID])
	})
	// An answer from another store than the reference, or on an invalid
	// table, is no evidence: it carries no floor for the gate to count.
	for i := range result.Candidates {
		c := &result.Candidates[i]
		if ref, ok := stores[c.ReplicaID]; !ok || ref.IsZero() || c.StoreID != ref || !c.TableValid {
			c.Floor = 0
		}
	}
	result.FencedEpoch = fence
	return result, err
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
	return p.round(volumeID, current, candidates, p.hub.ask)
}

func (p *roundEvidenceProvider) round(volumeID string, current authority.AuthorityBasis, candidates []authority.ReplicaCandidate, ask func(roundID uint64, r *openRound, serverID, replicaID string) bool) (PromotionProbeResult, error) {
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
		if serverID != "" && ask(roundID, r, serverID, replicaID) {
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
			var store storage.StoreID
			if len(a.GetStoreId()) == len(store) {
				copy(store[:], a.GetStoreId())
			}
			// The current primary's own evidence (a heal continues it on it):
			// ready as a serving primary, on a valid table.
			result.Current = PromotionCandidateEvidence{ReplicaID: current.ReplicaID, Ready: a.GetPrimaryReady(), AwaitingReplicaStoresEpoch: a.GetAwaitingReplicaStoresEpoch(), DurableLSN: a.GetDurableEnd(),
				LastDataEpoch: a.GetLastDataEpoch(), LastDataFirstLSN: a.GetLastDataFirstLsn(), TableInvalid: a.GetTablePresent() && !a.GetTableValid(), TableValid: a.GetTablePresent() && a.GetTableValid(),
				StoreID: store, Floor: a.GetFloor(), AckProfile: a.GetAckProfile()}
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
		var store storage.StoreID
		if len(a.GetStoreId()) == len(store) {
			copy(store[:], a.GetStoreId())
		}
		attested := false
		if p.attested != nil && !store.IsZero() {
			want, ok := p.attested(volumeID, c.ReplicaID)
			attested = ok && want == store
		}
		result.Candidates = append(result.Candidates, PromotionCandidateEvidence{
			ReplicaID:        c.ReplicaID,
			DurableLSN:       a.GetDurableEnd(),
			LastDataEpoch:    a.GetLastDataEpoch(),
			LastDataFirstLSN: a.GetLastDataFirstLsn(),
			TableInvalid:     invalid,
			TableValid:       a.GetTablePresent() && a.GetTableValid(),
			StoreID:          store,
			Floor:            a.GetFloor(),
			AckProfile:       a.GetAckProfile(),
			// A promotion target answers as a ready replica, from a store
			// whose epoch table is present and valid, and whose id is the
			// store its primary attested (counted toward its write ACKs).
			Ready: a.GetReplicaReady() && a.GetTablePresent() && a.GetTableValid() && attested,
		})
	}
	result.ProfileMismatch = profileMismatch(p.ackProfile, got)
	return result, nil
}

// profileMismatch names the answers whose ack profile is missing or is not
// the master's (or says the master's own is unknown); "" when all agree.
func profileMismatch(master string, answers map[string]*control.ProbeAnswer) string {
	if !isKnownAckProfile(master) {
		return fmt.Sprintf("master ack profile %q unknown", master)
	}
	want := canonicalAckProfile(master)
	var bad []string
	for id, a := range answers {
		if got := canonicalAckProfile(a.GetAckProfile()); got != want {
			if got == "" {
				got = "none"
			}
			bad = append(bad, id+"="+got)
		}
	}
	if len(bad) == 0 {
		return ""
	}
	sort.Strings(bad)
	return "master " + want + ", answers " + strings.Join(bad, ",")
}
