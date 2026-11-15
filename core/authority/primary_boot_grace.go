package authority

import "time"

// Primary boot grace (#128).
//
// A freshly started volume process reports ReadyForPrimary=false until
// its engine has a promotion fact, so a plain restart of the current
// primary would otherwise fail over the moment a ready peer exists.
// The controller keeps the CURRENT primary through one boot grace:
//
//  1. the grace starts when the controller first records a new process
//     incarnation for the replica, and lasts at most PrimaryBootGrace
//     (the master caps it at its freshness window);
//  2. it covers only "not ready yet": the replica must stay placeable
//     (reachable, eligible, not withdrawn, on a reachable eligible
//     server). Any explicit unhealthy report fails over at once;
//  3. once it expires with the replica still unready, failover
//     proceeds as before;
//  4. it is granted at most once per incarnation (repeated not-ready
//     heartbeats never extend it), ends for good the moment the
//     replica reports ready, and is refused to a flapping replica
//     (PrimaryBootFlapRestarts incarnations within
//     PrimaryBootFlapWindow).
//
// Ordering: a fact changes the history only if its ObservedAt is
// strictly newer than the last fact recorded for that replica, so a
// delayed or superseded snapshot can never count a restart, reset a
// grace, or end one. A slot without an incarnation (an older volume
// binary) or without an observation time gets no grace. All state is
// guarded by TopologyController.mu.

type primaryBootGrace struct {
	incarnation    string
	lastObservedAt time.Time   // newest fact recorded for this replica
	graceStart     time.Time   // controller clock when the incarnation was first recorded
	ended          bool        // reported ready, expired, or never granted
	starts         []time.Time // incarnation starts within the flap window
}

func bootGraceKey(volumeID, replicaID string) string { return volumeID + "/" + replicaID }

// observeIncarnationsLocked records each slot's boot fact.
func (c *TopologyController) observeIncarnationsLocked(vol VolumeTopologySnapshot) {
	for _, slot := range vol.Slots {
		c.recordBootFactLocked(vol.VolumeID, slot)
	}
}

// recordBootFactLocked is the only writer of bootGrace.
func (c *TopologyController) recordBootFactLocked(volumeID string, slot ReplicaCandidate) {
	if slot.Incarnation == "" || slot.ObservedAt.IsZero() {
		return
	}
	key := bootGraceKey(volumeID, slot.ReplicaID)
	g := c.bootGrace[key]
	switch {
	case g == nil:
		g = c.startIncarnation(nil, slot.Incarnation)
		c.bootGrace[key] = g
	case !slot.ObservedAt.After(g.lastObservedAt):
		return // not newer than what is recorded: never changes history
	case g.incarnation != slot.Incarnation:
		g = c.startIncarnation(g, slot.Incarnation)
		c.bootGrace[key] = g
	}
	g.lastObservedAt = slot.ObservedAt
	if slot.ReadyForPrimary {
		g.ended = true
	}
}

// startIncarnation returns the grace record for a newly recorded
// incarnation: its start is counted, and the grace is refused when that
// makes the replica a flapper.
func (c *TopologyController) startIncarnation(prev *primaryBootGrace, incarnation string) *primaryBootGrace {
	now := c.now()
	var starts []time.Time
	if prev != nil {
		for _, t := range prev.starts {
			if now.Sub(t) < c.config.PrimaryBootFlapWindow {
				starts = append(starts, t)
			}
		}
	}
	starts = append(starts, now)
	return &primaryBootGrace{
		incarnation: incarnation,
		graceStart:  now,
		ended:       len(starts) >= c.config.PrimaryBootFlapRestarts,
		starts:      starts,
	}
}

// inPrimaryBootGraceLocked reports whether the current primary's slot
// is inside its boot grace now. Expiry ends the grace for good.
func (c *TopologyController) inPrimaryBootGraceLocked(volumeID string, current ReplicaCandidate) bool {
	if current.Incarnation == "" || current.ReadyForPrimary {
		return false
	}
	g := c.bootGrace[bootGraceKey(volumeID, current.ReplicaID)]
	if g == nil || g.incarnation != current.Incarnation || g.ended {
		return false
	}
	if c.now().Sub(g.graceStart) >= c.config.PrimaryBootGrace {
		g.ended = true
		return false
	}
	return true
}

// PrimaryBootGraceHolds records the current primary's fresh slot fact
// (ordered, as above) and reports whether a reassignment away from it
// must wait: the slot is placeable by its own report and inside its
// boot grace. The caller passes the replica's own fresh heartbeat slot
// with its server's reachability and eligibility folded in, never a
// slot synthesized from placement. The master's promotion gates call it
// while holding no lock the controller takes.
func (c *TopologyController) PrimaryBootGraceHolds(volumeID string, current ReplicaCandidate) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !current.Reachable || !current.Eligible || current.Withdrawn {
		return false
	}
	c.recordBootFactLocked(volumeID, current)
	return c.inPrimaryBootGraceLocked(volumeID, current)
}

// PrimaryBootGrace returns the effective boot-grace bound.
func (c *TopologyController) PrimaryBootGrace() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.PrimaryBootGrace
}

// FreshSlotCandidate returns the replica's own fresh heartbeat slot for
// the volume, with the reporting server's reachability and eligibility
// folded in. It reports false when no fresh slot exists, or when more
// than one fresh server claims the replica.
func (ss storeSnapshot) FreshSlotCandidate(volumeID, replicaID string) (ReplicaCandidate, bool) {
	var out ReplicaCandidate
	found := false
	for id, obs := range ss.observations {
		if ss.serverFreshness[id] != ServerFresh {
			continue
		}
		for _, s := range obs.Slots {
			if s.VolumeID != volumeID || s.ReplicaID != replicaID || slotExpired(s, obs, ss.evaluatedAt) {
				continue
			}
			if found {
				return ReplicaCandidate{}, false
			}
			found = true
			out = ReplicaCandidate{
				ReplicaID:       s.ReplicaID,
				ServerID:        obs.ServerID,
				DataAddr:        s.DataAddr,
				CtrlAddr:        s.CtrlAddr,
				Reachable:       s.Reachable && obs.Server.Reachable,
				ReadyForPrimary: s.ReadyForPrimary,
				Eligible:        s.Eligible && obs.Server.Eligible,
				Withdrawn:       s.Withdrawn,
				EvidenceScore:   s.EvidenceScore,
				Incarnation:     s.Incarnation,
				ObservedAt:      slotObservedAt(s, obs),
			}
		}
	}
	return out, found
}
