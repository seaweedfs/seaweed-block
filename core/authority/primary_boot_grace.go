package authority

import "time"

// Primary boot grace (#128).
//
// A freshly started volume process reports ReadyForPrimary=false until
// its engine has a promotion fact, so a plain restart of the current
// primary would otherwise fail over the moment a ready peer exists.
// The controller keeps the CURRENT primary through one boot grace:
//
//  1. the grace starts when the controller first sees a new process
//     incarnation for the replica, and lasts at most PrimaryBootGrace
//     (the master wires it to its freshness window);
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
// A replica that reports no incarnation (an older volume binary) gets
// no grace. All state is guarded by TopologyController.mu.

type primaryBootGrace struct {
	incarnation string
	graceStart  time.Time
	ended       bool        // reported ready, expired, or never granted
	starts      []time.Time // incarnation starts within the flap window
}

func bootGraceKey(volumeID, replicaID string) string { return volumeID + "/" + replicaID }

// observeIncarnationsLocked records each slot's incarnation. It is the
// only writer of bootGrace and runs once per supported snapshot.
func (c *TopologyController) observeIncarnationsLocked(vol VolumeTopologySnapshot) {
	now := c.now()
	for _, slot := range vol.Slots {
		if slot.Incarnation == "" {
			continue
		}
		key := bootGraceKey(vol.VolumeID, slot.ReplicaID)
		g := c.bootGrace[key]
		if g == nil || g.incarnation != slot.Incarnation {
			g = c.startIncarnation(g, slot.Incarnation, now)
			c.bootGrace[key] = g
		}
		if slot.ReadyForPrimary {
			g.ended = true
		}
	}
}

// startIncarnation returns the grace record for a newly seen
// incarnation: its start is counted, and the grace is refused when
// that makes the replica a flapper.
func (c *TopologyController) startIncarnation(prev *primaryBootGrace, incarnation string, now time.Time) *primaryBootGrace {
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

// PrimaryInBootGrace reports whether a direct reassignment away from
// the current primary must wait: the slot is, or on this observation
// would become, inside its boot grace. It does not record anything.
// The master's promotion gate calls it before a direct reassign; the
// gate holds no controller-visible lock while calling.
func (c *TopologyController) PrimaryInBootGrace(volumeID string, current ReplicaCandidate) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current.Incarnation == "" || current.ReadyForPrimary {
		return false
	}
	g := c.bootGrace[bootGraceKey(volumeID, current.ReplicaID)]
	if g == nil || g.incarnation != current.Incarnation {
		// Not seen yet: the grace this observation would start.
		next := c.startIncarnation(g, current.Incarnation, c.now())
		return !next.ended
	}
	return c.inPrimaryBootGraceLocked(volumeID, current)
}

// PrimaryBootGrace returns the effective boot-grace bound.
func (c *TopologyController) PrimaryBootGrace() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.PrimaryBootGrace
}
