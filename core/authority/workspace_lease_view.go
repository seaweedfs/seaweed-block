package authority

import (
	"context"
	"fmt"
	"log"
	"time"
)

// leaseDerived is the part of a volume's lease view that is a pure function
// of published publisher state (derive). Writers recompute it under
// p.mu -> leaseMu at every point where an input changes, so that whenever
// p.mu is released the view equals derive(published) (INV-LEASE-VIEW-001).
type leaseDerived struct {
	bound         bool // an authority line exists for the volume
	replica       string
	epoch         uint64
	ownerFromMove bool
	issuedHost    string // NewHost of the issued move that minted the line; "" if none
	leasePresent  bool
	revoking      bool
	movePending   bool // the last workspace move is reserved, not issued
	uncertain     bool // a workspace write of the volume failed (INV-AUTH-UNCERTAIN-001)
	storeless     bool
	horizon       int64 // published Lease.ExpiresAtNS: durable upper bound of every grant
	maxSkewNS     int64
	lease         WorkspaceLease // the published (durable) lease
}

// leaseView is one volume's renewal state. Guarded by Publisher.leaseMu.
// deadline (the latest grant, or the restart rule's now+TTL) and extending
// are owned by the view; d is derived.
type leaseView struct {
	d         leaseDerived
	deadline  time.Time
	extending bool
	clamped   bool // a renewal was clamped below now+TTL; logged once per streak
	alarmed   bool // a headroom alarm streak is open (headroomAlarmLocked)
}

// deriveLeaseLocked computes the derived lease view from published state.
// Caller holds p.mu.
func (p *Publisher) deriveLeaseLocked(volume string) leaseDerived {
	d := leaseDerived{uncertain: p.workspaceUncertain[volume], storeless: p.store == nil, maxSkewNS: int64(p.workspaceSkew(nil))}
	if _, line, ok := p.workspaceLineLocked(volume); ok {
		d.bound, d.replica, d.epoch = true, line.info.ReplicaID, line.info.Epoch
		d.issuedHost, d.ownerFromMove = line.info.ServerID, line.info.OwnerFromMove
	}
	if s := p.workspace[volume]; s != nil {
		d.movePending = len(s.Moves) > 0 && !s.Moves[len(s.Moves)-1].Issued
		if s.Lease != nil {
			d.leasePresent, d.revoking, d.horizon, d.lease = true, s.Lease.Revoking, s.Lease.ExpiresAtNS, *s.Lease
			d.maxSkewNS = int64(p.workspaceSkew(s.Lease))
		}
	}
	return d
}

// refreshLeaseViewLocked republishes the derived view of volume. Caller holds
// p.mu; this takes leaseMu (the only p.mu -> leaseMu edge besides deadline
// reads). Called at every mutation of a derive input: the authority line, the
// published workspace state, uncertainty, and closing.
func (p *Publisher) refreshLeaseViewLocked(volume string) {
	d := p.deriveLeaseLocked(volume)
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	lv := p.leaseViews[volume]
	if lv == nil {
		lv = &leaseView{}
		p.leaseViews[volume] = lv
	}
	lv.d = d
}

// publishIssuedLeaseLocked publishes an issued move's derived view (the new
// epoch and the new holder's lease) and that lease's initial deadline in one
// leaseMu section: a renewal of the new epoch sees both or neither, so the
// issue never lowers a deadline a renewal has raised. Caller holds p.mu.
func (p *Publisher) publishIssuedLeaseLocked(volume string, deadline time.Time) {
	d := p.deriveLeaseLocked(volume)
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	lv := p.leaseViews[volume]
	if lv == nil {
		lv = &leaseView{}
		p.leaseViews[volume] = lv
	}
	lv.d = d
	if deadline.After(lv.deadline) {
		lv.deadline = deadline
	}
}

// leaseDeadlineLocked reads the granted deadline. Caller holds p.mu.
func (p *Publisher) leaseDeadlineLocked(volume string) time.Time {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	if lv := p.leaseViews[volume]; lv != nil {
		return lv.deadline
	}
	return time.Time{}
}

// setLeaseDeadlineLocked replaces the deadline (restart rule) or,
// with raise, raises it to at least t (a grant). Caller holds p.mu.
func (p *Publisher) setLeaseDeadlineLocked(volume string, t time.Time, raise bool) {
	d := p.deriveLeaseLocked(volume)
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	lv := p.leaseViews[volume]
	if lv == nil {
		lv = &leaseView{d: d}
		p.leaseViews[volume] = lv
	}
	if !raise || t.After(lv.deadline) {
		lv.deadline = t
	}
}

// replyExpiryNS is the expiry reported to a host: never the raw durable
// horizon, only what was granted (min(horizon, deadline)).
func (lv *leaseView) replyExpiryNS() int64 {
	if lv.deadline.IsZero() {
		return lv.d.horizon // unreachable: a published lease always has a deadline
	}
	if dl := lv.deadline.UnixNano(); dl < lv.d.horizon {
		return dl
	}
	return lv.d.horizon
}

// WorkspaceLeaseReply returns the published lease with its expiry clamped to
// what was granted (min(durable horizon, granted deadline)), for replies that
// report a lease to a host or operator. ok is false without a lease.
func (p *Publisher) WorkspaceLeaseReply(volume string) (WorkspaceLease, bool) {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	lv := p.leaseViews[volume]
	if lv == nil || !lv.d.leasePresent {
		return WorkspaceLease{}, false
	}
	l := lv.d.lease
	l.ExpiresAtNS = lv.replyExpiryNS()
	return l, true
}

// PublishedLine is a volume's committed authority line as the lease view
// publishes it.
type PublishedLine struct {
	ReplicaID string
	Epoch     uint64
	// IssuedHost is the target host of the issued move that minted this line;
	// "" when no issued move did (a placement or static topology line).
	IssuedHost    string
	OwnerFromMove bool
}

// PublishedAuthorityLine reads one committed line under leaf leaseMu. Identity
// checks on renewal must not wait for p.mu, which spans authority store I/O
// (INV-LEASE-VIEW-001, INV-WS-RENEW-NONBLOCKING-001).
func (p *Publisher) PublishedAuthorityLine(volume string) (PublishedLine, bool) {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	lv := p.leaseViews[volume]
	if lv == nil || !lv.d.bound {
		return PublishedLine{}, false
	}
	return PublishedLine{ReplicaID: lv.d.replica, Epoch: lv.d.epoch, IssuedHost: lv.d.issuedHost, OwnerFromMove: lv.d.ownerFromMove}, true
}

// PublishedAuthorityLines returns the committed authority line of every
// volume that has one. It reads the lease views under the leaf leaseMu only:
// it never waits on p.mu, which writers hold across store writes, and it
// sees only state those writers already published (a reserved, unissued
// move never shows here).
func (p *Publisher) PublishedAuthorityLines() map[string]PublishedLine {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	out := make(map[string]PublishedLine, len(p.leaseViews))
	for volume, lv := range p.leaseViews {
		if lv.d.bound {
			out[volume] = PublishedLine{ReplicaID: lv.d.replica, Epoch: lv.d.epoch, IssuedHost: lv.d.issuedHost, OwnerFromMove: lv.d.ownerFromMove}
		}
	}
	return out
}

// RenewWorkspaceLease answers a live holder from memory: it takes only the
// leaf leaseMu, does no I/O and never waits on p.mu or the store. The grant
// is min(now+TTL, durable horizon); a background extender (an ordinary p.mu
// writer) pushes the horizon ahead. A first grant and an expired re-grant
// after the shared move barrier write synchronously.
func (p *Publisher) RenewWorkspaceLease(ctx context.Context, volume, holder string, epoch uint64) (WorkspaceLease, error) {
	if err := ctx.Err(); err != nil {
		return WorkspaceLease{}, err
	}
	p.leaseMu.Lock()
	now := p.workspaceNow() // read under leaseMu: the forced-barrier argument needs it
	// unlock releases leaseMu on a path that grants nothing and logs a due
	// per-minute summary after the unlock: every renewal attempt, granted or
	// refused, may close the window (a stall through expiry refuses only).
	unlock := func() {
		summary := p.leaseStats.summaryDue(now)
		p.leaseMu.Unlock()
		p.logLease(summary)
	}
	lv := p.leaseViews[volume]
	if lv == nil || !lv.d.bound || lv.d.replica != holder || lv.d.epoch != epoch {
		unlock()
		return WorkspaceLease{}, fmt.Errorf("workspace lease: stale authority")
	}
	if lv.d.movePending {
		unlock()
		return WorkspaceLease{}, fmt.Errorf("workspace lease: move pending")
	}
	if !lv.d.leasePresent {
		unlock() // never re-enter p.mu while holding the leaf
		return p.firstWorkspaceGrant(ctx, volume, holder, epoch)
	}
	if lv.d.revoking { // tell the host to drain; never extend
		l := lv.d.lease
		l.ExpiresAtNS = lv.replyExpiryNS()
		unlock()
		return l, nil
	}
	if now.UnixNano() >= lv.d.horizon || !now.Before(lv.deadline) {
		unlock()
		return p.regrantWorkspaceLease(ctx, volume, holder, epoch)
	}
	// Where the synchronous write used to refuse (persistWorkspaceLocked):
	// a renewal never bypasses uncertainty (INV-AUTH-UNCERTAIN-001).
	if lv.d.storeless || lv.d.uncertain {
		unlock()
		return WorkspaceLease{}, fmt.Errorf("workspace durable store unavailable or uncertain")
	}
	// The headroom alarm is sampled here, before the clamp: at the latest it
	// fires on the first clamped grant of a streak (headroomAlarmLocked).
	headroom := lv.d.horizon - now.UnixNano()
	alarm := p.headroomAlarmLocked(volume, lv, headroom)
	grant := now.Add(p.workspaceTTL).UnixNano()
	clamped := grant > lv.d.horizon
	if clamped {
		grant = lv.d.horizon
	}
	// A grant without positive host validity (expiry minus half the skew
	// bound, leaseFact) would make the host fence on an invalid reply and lose
	// its remaining validity; refuse with an error instead.
	if grant-lv.d.maxSkewNS/2 <= now.UnixNano() {
		horizon := lv.d.horizon // the view is leaseMu's: read it before the unlock (#242)
		summary := p.leaseStats.summaryDue(now)
		p.leaseMu.Unlock()
		p.logLease(alarm)
		p.logLease(summary)
		return WorkspaceLease{}, fmt.Errorf("workspace lease: durable horizon %s leaves no host validity", time.Unix(0, horizon).UTC().Format(time.RFC3339Nano))
	}
	if g := time.Unix(0, grant); g.After(lv.deadline) {
		lv.deadline = g
	}
	summary := p.leaseStats.observeGrant(now, headroom)
	if clamped {
		p.leaseStats.recordClamp(now)
	}
	var note string
	switch {
	case clamped && !lv.clamped:
		lv.clamped = true
		note = fmt.Sprintf("workspace lease: grants of volume=%s are clamped to the durable horizon %s (%s remaining); extensions are behind",
			volume, time.Unix(0, lv.d.horizon).UTC().Format(time.RFC3339Nano), time.Duration(lv.d.horizon-now.UnixNano()).Round(time.Millisecond))
	case !clamped && lv.clamped:
		lv.clamped = false
		note = fmt.Sprintf("workspace lease: grants of volume=%s are no longer clamped (horizon %s)", volume, time.Unix(0, lv.d.horizon).UTC().Format(time.RFC3339Nano))
	}
	if lv.d.horizon < now.Add(p.workspaceTTL+p.workspaceTTL/4).UnixNano() && !lv.extending && !p.leaseClosing {
		lv.extending = true
		p.extWG.Add(1) // under leaseMu with !closing: no Add can follow CloseWorkspaceExtenders
		go p.extendWorkspaceLease(volume, holder, epoch, now)
	}
	l := WorkspaceLease{ReplicaID: holder, Epoch: epoch, ExpiresAtNS: grant, MaxClockSkewNS: lv.d.maxSkewNS}
	p.leaseMu.Unlock()
	p.logLease(alarm)
	p.logLease(note)
	p.logLease(summary)
	return l, nil
}

// firstWorkspaceGrant creates a lease synchronously under p.mu (one write per
// assignment epoch) for an epoch that has none: a move issue already started
// its holder's lease. A host whose first renewal fails does not fence: it has
// no deadline yet and retries. The durable horizon starts at now+1.5*TTL.
func (p *Publisher) firstWorkspaceGrant(ctx context.Context, volume, holder string, epoch uint64) (WorkspaceLease, error) {
	p.mu.Lock()
	_, line, ok := p.workspaceLineLocked(volume)
	if !ok || line.info.ReplicaID != holder || line.info.Epoch != epoch {
		p.mu.Unlock()
		return WorkspaceLease{}, fmt.Errorf("workspace lease: stale authority")
	}
	s := cloneWorkspace(p.workspace[volume])
	if s == nil {
		s = &WorkspaceState{}
	}
	if len(s.Moves) > 0 && !s.Moves[len(s.Moves)-1].Issued {
		p.mu.Unlock()
		return WorkspaceLease{}, fmt.Errorf("workspace lease: move pending")
	}
	if s.Lease != nil {
		// Another writer created the lease after the view was read. Do not
		// re-enter the memory path from here (no mutual recursion): the host has
		// no deadline yet, so a failed first renewal does not fence it, and its
		// retry takes the memory path.
		p.mu.Unlock()
		return WorkspaceLease{}, fmt.Errorf("workspace lease: created concurrently; retry")
	}
	lease, grant := p.newWorkspaceLease(holder, epoch, p.workspaceNow())
	s.Lease = lease
	if err := p.persistWorkspaceLocked(volume, s); err != nil {
		p.mu.Unlock()
		return WorkspaceLease{}, err
	}
	p.setLeaseDeadlineLocked(volume, grant, true)
	p.mu.Unlock()
	return WorkspaceLease{ReplicaID: holder, Epoch: epoch, ExpiresAtNS: grant.UnixNano(), MaxClockSkewNS: lease.MaxClockSkewNS}, nil
}

// INV-WS-LEASE-REGRANT-001: serialize an expired re-grant with move issue,
// reusing the forced-move barrier so the old process's validity is past.
// The live renewal path releases leaseMu before entering this writer.
func (p *Publisher) regrantWorkspaceLease(ctx context.Context, volume, holder string, epoch uint64) (WorkspaceLease, error) {
	// INV-WS-RENEW-NONBLOCKING-001: a held writer refuses this attempt;
	// a returning process can retry after that writer has settled.
	if !p.mu.TryLock() {
		return WorkspaceLease{}, fmt.Errorf("workspace lease: authority writer busy; retry")
	}
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return WorkspaceLease{}, err
	}
	_, line, ok := p.workspaceLineLocked(volume)
	s := cloneWorkspace(p.workspace[volume])
	if !ok || line.info.ReplicaID != holder || line.info.Epoch != epoch || s == nil || s.Lease == nil ||
		s.Lease.ReplicaID != holder || s.Lease.Epoch != epoch {
		return WorkspaceLease{}, fmt.Errorf("workspace lease: stale authority")
	}
	if s.Lease.Revoking || s.Ack != nil || (len(s.Moves) > 0 && !s.Moves[len(s.Moves)-1].Issued) {
		return WorkspaceLease{}, fmt.Errorf("workspace lease: revocation or move pending")
	}
	if p.store == nil || p.workspaceUncertain[volume] {
		return WorkspaceLease{}, fmt.Errorf("workspace durable store unavailable or uncertain")
	}
	_, now, err := p.forcedMoveBarrierLocked(volume, s.Lease)
	if err != nil {
		return WorkspaceLease{}, err
	}
	lease, grant := p.newWorkspaceLease(holder, epoch, now)
	lease.ExpiresAtNS = max(lease.ExpiresAtNS, s.Lease.ExpiresAtNS)
	lease.MaxClockSkewNS = int64(p.workspaceSkew(s.Lease))
	s.Lease = lease
	if err := p.persistWorkspaceLocked(volume, s); err != nil {
		return WorkspaceLease{}, err
	}
	p.setLeaseDeadlineLocked(volume, grant, true)
	return WorkspaceLease{ReplicaID: holder, Epoch: epoch, ExpiresAtNS: grant.UnixNano(), MaxClockSkewNS: lease.MaxClockSkewNS}, nil
}

// newWorkspaceLease starts a lease at now: the durable horizon is
// now+1.5*TTL and the grant (the deadline) now+TTL. A first grant, expired
// re-grant and move issue start a lease this way.
func (p *Publisher) newWorkspaceLease(holder string, epoch uint64, now time.Time) (*WorkspaceLease, time.Time) {
	return &WorkspaceLease{ReplicaID: holder, Epoch: epoch, ExpiresAtNS: now.Add(p.workspaceTTL + p.workspaceTTL/2).UnixNano(), MaxClockSkewNS: int64(p.workspaceSkew(nil))}, now.Add(p.workspaceTTL)
}

// extendWorkspaceLease is an ordinary writer: under p.mu it rechecks the
// lease and writes horizon H = trigger + 1.5*TTL through
// persistWorkspaceLocked, so its write and its publication are one p.mu
// section. Any write error is an unknown write and latches uncertainty
// (INV-AUTH-UNCERTAIN-001); a failed recheck is a known no-write.
func (p *Publisher) extendWorkspaceLease(volume, holder string, epoch uint64, trigger time.Time) {
	defer p.extWG.Done()
	var note string
	defer func() {
		if note != "" {
			log.Print(note)
		}
	}()
	p.mu.Lock()
	defer p.mu.Unlock()
	defer func() {
		p.leaseMu.Lock()
		if lv := p.leaseViews[volume]; lv != nil {
			lv.extending = false
		}
		p.leaseMu.Unlock()
	}()
	p.leaseMu.Lock()
	closing := p.leaseClosing
	p.leaseMu.Unlock()
	if closing {
		return
	}
	_, line, ok := p.workspaceLineLocked(volume)
	s := cloneWorkspace(p.workspace[volume])
	if !ok || line.info.ReplicaID != holder || line.info.Epoch != epoch || s == nil || s.Lease == nil || s.Lease.Revoking ||
		s.Lease.ReplicaID != holder || s.Lease.Epoch != epoch || (len(s.Moves) > 0 && !s.Moves[len(s.Moves)-1].Issued) ||
		p.workspaceUncertain[volume] || p.store == nil {
		return
	}
	h := trigger.Add(p.workspaceTTL + p.workspaceTTL/2).UnixNano()
	old := s.Lease.ExpiresAtNS
	if h <= old {
		return
	}
	s.Lease.ExpiresAtNS = h
	write, wrote, err := p.persistWorkspaceTimedLocked(volume, s)
	if wrote {
		// The store write alone, failed or not, recorded when it returns
		// (completed-write telemetry; a blocked write is not seen until then).
		p.leaseMu.Lock()
		p.leaseStats.recordExtensionWrite(p.workspaceNow(), write)
		p.leaseMu.Unlock()
	}
	took := write.Round(time.Millisecond)
	if err != nil {
		if p.extendFailures[volume] == 0 {
			note = fmt.Sprintf("workspace lease: extension of volume=%s failed after %s (horizon %s, %s remaining); the volume is now uncertain and its renewals are refused: %v",
				volume, took, time.Unix(0, old).UTC().Format(time.RFC3339Nano), time.Until(time.Unix(0, old)).Round(time.Millisecond), err)
		}
		p.extendFailures[volume]++
		return
	}
	if n := p.extendFailures[volume]; n > 0 {
		note = fmt.Sprintf("workspace lease: extension of volume=%s recovered after %d failures, took %s, horizon now %s", volume, n, took, time.Unix(0, h).UTC().Format(time.RFC3339Nano))
		delete(p.extendFailures, volume)
	}
}

// CloseWorkspaceExtenders stops admitting lease extensions and joins every
// admitted extender. Owners call it before closing the authority store:
// closing is set under p.mu -> leaseMu, so an extender either finished its
// write before or sees closing and does not write.
func (p *Publisher) CloseWorkspaceExtenders() {
	p.mu.Lock()
	p.leaseMu.Lock()
	p.leaseClosing = true
	p.leaseMu.Unlock()
	p.mu.Unlock()
	p.extWG.Wait()
}
