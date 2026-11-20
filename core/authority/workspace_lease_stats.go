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
// with headroom under 1.125 TTL, and only later as clamps.
//
// The extension store write latency (LeaseStats.ExtensionWrite*) is
// completed-write telemetry: a write is sampled when its Put returns, so a
// write that is still blocked shows up only after it returns, possibly after
// headroom is already lost; it is not an early warning by itself. The alarm
// is sampled at renewals, never by a timer: it fires at the latest
// together with the first clamped grant of a streak (logged before the clamp
// line), and earlier only if a renewal lands in the 1-1.125 TTL band while an
// extension is pending. A renewer slower than that band may see both on one
// renewal.

// LeaseStats is a snapshot of the workspace lease renewal counters.
type LeaseStats struct {
	// HeadroomAlarms counts alarm streaks, once per volume per streak: a
	// renewal that found headroom below 1.125 TTL with an extension in
	// flight, or the first clamped grant (headroom below 1 TTL) of a streak.
	HeadroomAlarms uint64
	// ClampsTotal counts grants clamped to the durable horizon.
	ClampsTotal uint64
	// ClampsLastMinute counts clamped grants in the last minute.
	ClampsLastMinute uint64
	// MinHeadroom is the smallest headroom observed at a grant; valid only
	// when MinHeadroomObserved.
	MinHeadroom         time.Duration
	MinHeadroomObserved bool
	// ExtensionWriteLast is the duration of the latest lease extension's
	// durable store write (the write alone, failed or not); valid only when
	// ExtensionWriteObserved. Completed-write telemetry: recorded when the
	// Put returns, so a write still blocked is not visible here yet.
	ExtensionWriteLast     time.Duration
	ExtensionWriteObserved bool
	// ExtensionWriteMaxLastMinute is the slowest extension write in the last
	// minute (rolling, one bucket per second like ClampsLastMinute); valid
	// only when ExtensionWriteInLastMinute.
	ExtensionWriteMaxLastMinute time.Duration
	ExtensionWriteInLastMinute  bool
}

const clampBuckets = 60 // one per second of the last minute

// leaseCounters is guarded by Publisher.leaseMu.
type leaseCounters struct {
	alarms      uint64
	clamps      uint64
	buckets     [clampBuckets]struct{ sec, n int64 } // clamps by unix second
	minHeadroom int64
	minObserved bool
	// Extension store writes: the latest, and the slowest per second of the
	// last minute.
	writeLast     int64
	writeObserved bool
	writeBuckets  [clampBuckets]struct {
		sec, max int64
		set      bool
	}
	// Per-minute summary, emitted by the first renewal attempt after the
	// window, granted or refused: no goroutine or timer of its own. With no
	// renewal attempts at all nothing is emitted; the counters above (the
	// status endpoint) still show the clamps.
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

// recordExtensionWrite records the duration of one extension store write
// that returned at now.
func (c *leaseCounters) recordExtensionWrite(now time.Time, d time.Duration) {
	c.writeLast, c.writeObserved = int64(d), true
	sec := now.Unix()
	b := &c.writeBuckets[uint64(sec)%clampBuckets]
	if !b.set || b.sec != sec {
		b.sec, b.max, b.set = sec, int64(d), true
	} else if int64(d) > b.max {
		b.max = int64(d)
	}
}

// extensionWriteMaxSince returns the slowest extension write of the last
// minute, and whether there was one.
func (c *leaseCounters) extensionWriteMaxSince(now time.Time) (int64, bool) {
	cut := now.Unix() - clampBuckets
	max, ok := int64(0), false
	for _, b := range c.writeBuckets {
		if b.set && b.sec > cut && b.sec <= now.Unix() && (!ok || b.max > max) {
			max, ok = b.max, true
		}
	}
	return max, ok
}

// summaryDue closes a window that has lasted a minute and returns its summary
// note when it had clamps. Every renewal attempt calls it, granted or refused,
// so a stall through expiry (only refusals after the clamps) is still
// summarized. It opens no window: the first grant does.
func (c *leaseCounters) summaryDue(now time.Time) string {
	if c.windowStart.IsZero() || now.Sub(c.windowStart) < time.Minute {
		return ""
	}
	var note string
	if c.windowClamps > 0 {
		note = fmt.Sprintf("workspace lease: %d clamped grants in the last %s (total %d, min headroom %s); the authority store is lagging",
			c.windowClamps, now.Sub(c.windowStart).Round(time.Second), c.clamps, time.Duration(c.windowMin).Round(time.Millisecond))
	}
	c.windowStart, c.windowClamps, c.windowMinSet = now, 0, false
	return note
}

// observeGrant records the headroom of a grant and returns a per-minute
// summary note when a window with clamps has just ended. Call it before
// recordClamp of the same grant (it may open a new window).
func (c *leaseCounters) observeGrant(now time.Time, headroom int64) string {
	if c.windowStart.IsZero() {
		c.windowStart = now
	}
	note := c.summaryDue(now)
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
	now := p.workspaceNow()
	writeMax, writeRecent := c.extensionWriteMaxSince(now)
	return LeaseStats{
		HeadroomAlarms:              c.alarms,
		ClampsTotal:                 c.clamps,
		ClampsLastMinute:            c.clampsSince(now),
		MinHeadroom:                 time.Duration(c.minHeadroom),
		MinHeadroomObserved:         c.minObserved,
		ExtensionWriteLast:          time.Duration(c.writeLast),
		ExtensionWriteObserved:      c.writeObserved,
		ExtensionWriteMaxLastMinute: time.Duration(writeMax),
		ExtensionWriteInLastMinute:  writeRecent,
	}
}

// headroomAlarmLocked runs the alarm state machine of one renewal, before the
// renewal clamps (a grant clamps exactly when headroom < 1 TTL). Caller holds
// leaseMu. It returns a note to log after the unlock, before the clamp note.
//
//	state      | condition at a renewal                        | next       | effect
//	-----------+-----------------------------------------------+------------+------------------
//	quiet      | extending && headroom < 1.125 TTL             | alarmed    | count + log once
//	quiet      | headroom < 1 TTL (this grant clamps)          | alarmed    | count + log once
//	quiet      | otherwise                                     | quiet      | -
//	alarmed    | headroom >= 1.25 TTL                          | quiet      | streak ends
//	alarmed    | !extending && headroom >= 1.125 TTL           | quiet      | streak ends
//	alarmed    | otherwise                                     | alarmed    | -
//
// A clamp streak (headroom < 1 TTL) therefore never starts without an alarm
// on the same or an earlier renewal, and never re-alarms while it lasts.
func (p *Publisher) headroomAlarmLocked(volume string, lv *leaseView, headroom int64) string {
	ttl := int64(p.workspaceTTL)
	low, high := ttl+ttl/8, ttl+ttl/4
	if lv.alarmed {
		if headroom >= high || (!lv.extending && headroom >= low) {
			lv.alarmed = false
		}
		return ""
	}
	var why string
	switch {
	case lv.extending && headroom < low:
		why = "below 1.125 TTL while the extension write is pending"
	case headroom < ttl:
		why = "below 1 TTL: this grant is the first clamped to the durable horizon"
	default:
		return ""
	}
	lv.alarmed = true
	p.leaseStats.alarms++
	return fmt.Sprintf("workspace lease: volume=%s headroom %s %s; the authority store is lagging",
		volume, time.Duration(headroom).Round(time.Millisecond), why)
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
