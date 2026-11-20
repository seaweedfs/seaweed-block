package authority

import (
	"fmt"
	"log"
	"time"
)

// Lease headroom observability. Headroom is the durable horizon minus now:
// how long the published lease still covers a holder if no extension lands.
// Extensions start below 1.25 TTL of headroom and grants are clamped below
// 1 TTL, so a lagging authority store shows first as an extension in flight
// with headroom under 1.125 TTL (the alarm), and only later as clamps.

// LeaseStats is a snapshot of the workspace lease renewal counters.
type LeaseStats struct {
	// HeadroomAlarms counts alarm streaks: renewals that found an extension
	// in flight with headroom below 1.125 TTL, once per volume per streak.
	HeadroomAlarms uint64
	// ClampsTotal counts grants clamped to the durable horizon.
	ClampsTotal uint64
	// ClampsLastMinute counts clamped grants in the last minute.
	ClampsLastMinute uint64
	// MinHeadroom is the smallest headroom observed at a grant; valid only
	// when MinHeadroomObserved.
	MinHeadroom         time.Duration
	MinHeadroomObserved bool
}

const clampBuckets = 60 // one per second of the last minute

// leaseCounters is guarded by Publisher.leaseMu.
type leaseCounters struct {
	alarms      uint64
	clamps      uint64
	buckets     [clampBuckets]struct{ sec, n int64 } // clamps by unix second
	minHeadroom int64
	minObserved bool
	// Per-minute summary, emitted lazily by the first renewal after the
	// window: no goroutine of its own.
	windowStart  time.Time
	windowClamps uint64
	windowMin    int64
	windowMinSet bool
}

func (c *leaseCounters) clampsSince(now time.Time) uint64 {
	cut := now.Unix() - clampBuckets
	var n uint64
	for _, b := range c.buckets {
		if b.n > 0 && b.sec > cut && b.sec <= now.Unix() {
			n += uint64(b.n)
		}
	}
	return n
}

func (c *leaseCounters) recordClamp(now time.Time) {
	c.clamps++
	c.windowClamps++
	sec := now.Unix()
	b := &c.buckets[uint64(sec)%clampBuckets]
	if b.sec != sec {
		b.sec, b.n = sec, 0
	}
	b.n++
}

// observeGrant records the headroom of a grant and returns a per-minute
// summary note when a window with clamps has just ended. Call it before
// recordClamp of the same grant (it may open a new window).
func (c *leaseCounters) observeGrant(now time.Time, headroom int64) string {
	var note string
	if c.windowStart.IsZero() {
		c.windowStart = now
	} else if now.Sub(c.windowStart) >= time.Minute {
		if c.windowClamps > 0 {
			note = fmt.Sprintf("workspace lease: %d clamped grants in the last %s (total %d, min headroom %s); the authority store is lagging",
				c.windowClamps, now.Sub(c.windowStart).Round(time.Second), c.clamps, time.Duration(c.windowMin).Round(time.Millisecond))
		}
		c.windowStart, c.windowClamps, c.windowMinSet = now, 0, false
	}
	if !c.windowMinSet || headroom < c.windowMin {
		c.windowMin, c.windowMinSet = headroom, true
	}
	if !c.minObserved || headroom < c.minHeadroom {
		c.minHeadroom, c.minObserved = headroom, true
	}
	return note
}

// WorkspaceLeaseStats returns a snapshot of the lease renewal counters.
func (p *Publisher) WorkspaceLeaseStats() LeaseStats {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	c := &p.leaseStats
	return LeaseStats{
		HeadroomAlarms:      c.alarms,
		ClampsTotal:         c.clamps,
		ClampsLastMinute:    c.clampsSince(p.workspaceNow()),
		MinHeadroom:         time.Duration(c.minHeadroom),
		MinHeadroomObserved: c.minObserved,
	}
}

// headroomAlarmLocked runs the alarm state machine of one renewal. Caller
// holds leaseMu. It returns a note to log after the unlock.
//
//	state      | condition at a renewal                        | next       | effect
//	-----------+-----------------------------------------------+------------+------------------
//	quiet      | extending && headroom < 1.125 TTL             | alarmed    | count + log once
//	quiet      | otherwise                                     | quiet      | -
//	alarmed    | headroom >= 1.25 TTL                          | quiet      | streak ends
//	alarmed    | !extending && headroom >= 1.125 TTL           | quiet      | streak ends
//	alarmed    | otherwise                                     | alarmed    | -
func (p *Publisher) headroomAlarmLocked(volume string, lv *leaseView, headroom int64) string {
	ttl := int64(p.workspaceTTL)
	low, high := ttl+ttl/8, ttl+ttl/4
	if lv.alarmed {
		if headroom >= high || (!lv.extending && headroom >= low) {
			lv.alarmed = false
		}
		return ""
	}
	if !lv.extending || headroom >= low {
		return ""
	}
	lv.alarmed = true
	p.leaseStats.alarms++
	return fmt.Sprintf("workspace lease: volume=%s headroom %s below 1.125 TTL while the extension write is pending; the authority store is lagging",
		volume, time.Duration(headroom).Round(time.Millisecond))
}

func (p *Publisher) logLease(note string) {
	if note == "" {
		return
	}
	if p.leaseLog != nil {
		p.leaseLog(note)
		return
	}
	log.Print(note)
}
