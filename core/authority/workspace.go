package authority

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WorkspaceHead is read from the configured HEAD source, never supplied as
// operator evidence. Detached means the old holder's final cut is committed.
// FilerMtimeNS is the HEAD entry's modification time as the filer reports it
// with that same read (1 s resolution; the entry's mtime is set by the cut
// that wrote it); 0 means the source gave none (unknown, never invented).
type WorkspaceHead struct {
	Gen, Epoch, RemoteLSN uint64
	Holder                string
	Detached              bool
	// Deleting: HEAD is deleting, purging or deleted (returned with
	// ErrWorkspaceHeadDeleting): no claim can move it again.
	Deleting     bool
	FilerMtimeNS int64
}

// ErrWorkspaceHeadUnclaimed is a HEAD reader's answer for a volume's initial
// HEAD that no holder ever claimed (epoch 0, no holder, active): the head
// returned with it carries its generation and epoch 0. It names no identity,
// so a caller that needs one treats it as any read error; the delete
// admission reads it as never started (TERM-LIVE-HOLDER-001).
var ErrWorkspaceHeadUnclaimed = errors.New("HEAD never claimed: epoch 0, no holder")

// ErrWorkspaceHeadDeleting is a HEAD reader's answer for a HEAD that is
// deleting, purging or deleted: the head returned with it has Deleting set.
// A caller that needs an active or detached identity treats it as any read
// error; the delete admission reads it as past the fence (a re-run of a
// delete that stopped midway).
var ErrWorkspaceHeadDeleting = errors.New("HEAD is deleting, purging or deleted")

// ErrWorkspaceOutcomeUncertain marks a move whose outcome is unknown: its
// issuing write failed after it may have reached the store, or the volume's
// workspace persistence is already uncertain. Memory shows it unissued, but
// a reload may find it issued; a definite refusal must not be reported.
var ErrWorkspaceOutcomeUncertain = errors.New("workspace move outcome unknown: an authority write may have landed")

type WorkspaceLease struct {
	ReplicaID      string
	Epoch          uint64
	ExpiresAtNS    int64
	MaxClockSkewNS int64 `json:",omitempty"`
	Revoking       bool
}

type WorkspaceRevokeAck struct {
	ReplicaID      string
	Epoch, HeadGen uint64
}

type WorkspaceMoveRecord struct {
	RequestID, VolumeID, OldHost, NewHost, Kind string
	OldReplicaID                                string `json:"OldHolder"` // preserve persisted record encoding
	NewReplicaID                                string `json:"NewHolder"`
	TargetDataAddr, TargetCtrlAddr              string
	OldEpoch, NewEpoch, HeadGen, RemoteLSN      uint64
	Issued                                      bool
}

type WorkspaceState struct {
	Lease *WorkspaceLease       `json:",omitempty"`
	Ack   *WorkspaceRevokeAck   `json:",omitempty"`
	Moves []WorkspaceMoveRecord `json:",omitempty"`
}

func cloneWorkspace(s *WorkspaceState) *WorkspaceState {
	if s == nil {
		return nil
	}
	out := *s
	if s.Lease != nil {
		v := *s.Lease
		out.Lease = &v
	}
	if s.Ack != nil {
		v := *s.Ack
		out.Ack = &v
	}
	out.Moves = append([]WorkspaceMoveRecord(nil), s.Moves...)
	return &out
}

// Clock and TTL are server configuration, never accepted from an RPC request.
func WithWorkspaceLeaseConfig(ttl time.Duration, now func() time.Time) PublisherOption {
	return func(p *Publisher) {
		if ttl > 0 {
			p.workspaceTTL = ttl
		}
		if now != nil {
			p.workspaceNow = now
		}
	}
}

// WithWorkspaceMaxClockSkew configures the bound between clocks. Zero keeps
// the default: two nominal TTL/3 renewal intervals. Existing grants retain
// their persisted bound when configuration is reduced after a restart.
func WithWorkspaceMaxClockSkew(skew time.Duration) PublisherOption {
	return func(p *Publisher) {
		if skew > 0 {
			p.workspaceMaxClockSkew = skew
		}
	}
}
func (p *Publisher) workspaceSkew(lease *WorkspaceLease) time.Duration {
	skew := p.workspaceMaxClockSkew
	if skew <= 0 {
		skew = p.workspaceTTL / 3 * 2
	}
	if lease != nil && time.Duration(lease.MaxClockSkewNS) > skew {
		skew = time.Duration(lease.MaxClockSkewNS)
	}
	return skew
}

// Replica IDs are logical assignment identities; HEAD.Holder is qualified
// as <server-id>/<replica-id> and is checked separately against ExpectedHost.
type WorkspaceMoveRequest struct {
	RequestID, VolumeID, ExpectedReplicaID, TargetReplicaID string
	ExpectedHost, TargetHost                                string
	ExpectedEpoch                                           uint64
	TargetDataAddr, TargetCtrlAddr                          string
	Kind                                                    string
	Force                                                   bool
}

// MoveWorkspace is the authority-owned guarded RF1 move seam. The host layer
// validates volume eligibility and reads HEAD before calling this boundary.
// RecordWorkspacePlacement serializes initial desired placement persistence
// against authority publication. persist must not call back into Publisher.
func (p *Publisher) RecordWorkspacePlacement(ctx context.Context, volume string, persist func() error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, _, ok := p.workspaceLineLocked(volume); ok {
		return fmt.Errorf("workspace placement: volume already assigned; use move")
	}
	return persist()
}

// RecordWorkspaceDeletion protects reservation release against live or pending
// authority. A current clean ACK is proof of drain; a lease alone is not.
// RecordWorkspaceDeletion admits a delete of volume (INV-WS-DELETE-NO-LIVE-HOLDER-001)
// when it has no live holder (TERM-LIVE-HOLDER-001): no line (and no
// placement that may have queued one), a clean revocation ACK for the line,
// or a HEAD below the line's epoch (never started). head is HEAD as the
// caller read it before taking any lock (nil: not read, which refuses a line
// without an ACK, as before); owner is the line's owner "<server>/<replica>"
// ("" when unknown), used only to name the refusal. A Sealed HEAD without an
// ACK keeps the ACK rule. The delete's HEAD compare-and-swap to deleting is
// the fence against a claim that lands after the read.
func (p *Publisher) RecordWorkspaceDeletion(ctx context.Context, volume string, pendingPlacement bool, head *WorkspaceHead, owner string, persist func() error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s := p.workspace[volume]
	if s != nil && len(s.Moves) > 0 && !s.Moves[len(s.Moves)-1].Issued {
		return fmt.Errorf("workspace delete: move pending")
	}
	_, line, ok := p.workspaceLineLocked(volume)
	var ack *WorkspaceRevokeAck
	if s != nil {
		ack = s.Ack
	}
	switch v := WorkspaceHolderState(ok, line.info.Epoch, line.info.ReplicaID, owner, ack, head); v {
	case HolderNoLine:
		if pendingPlacement {
			return fmt.Errorf("workspace delete: placement may have queued authority")
		}
	case HolderDrained, HolderNeverStarted, HolderDeleting:
	default:
		return fmt.Errorf("workspace delete: live authority requires clean revocation ACK: revoke the line with its sealed generation (sw-block ops workspace revoke --epoch E --sealed-gen G) and wait for the recorded ACK, then delete (holder: %s)", v)
	}
	return persist()
}

func validateWorkspaceMoveRequest(req WorkspaceMoveRequest) error {
	// A same-host clean move still needs its seal/ACK; a forced recovery
	// still needs its expired lease barrier and persisted lost range.
	// Every move preserves its logical replica slot.
	// Refuse before locking or persisting a reservation/authority epoch.
	if req.ExpectedReplicaID != req.TargetReplicaID {
		return fmt.Errorf("workspace move: v1 requires the same logical replica id")
	}
	if req.RequestID == "" || req.VolumeID == "" || req.ExpectedHost == "" || req.TargetHost == "" || req.ExpectedReplicaID == "" || req.ExpectedEpoch == 0 || req.TargetReplicaID == "" ||
		req.TargetDataAddr == "" || req.TargetCtrlAddr == "" || (req.Kind != "clean" && req.Kind != "forced") || req.Force != (req.Kind == "forced") {
		return fmt.Errorf("workspace move: invalid request")
	}
	return nil
}

// issuedWorkspaceReplayLocked finds req's journal record: replay is true when
// req repeats an issued move (same request id, same validated body); an id
// reused with a different body is an error. Caller holds p.mu.
func (p *Publisher) issuedWorkspaceReplayLocked(req WorkspaceMoveRequest) (WorkspaceMoveRecord, bool, error) {
	if s := p.workspace[req.VolumeID]; s != nil {
		for _, r := range s.Moves {
			if r.RequestID == req.RequestID {
				if !sameWorkspaceRequest(r, req) {
					return WorkspaceMoveRecord{}, false, fmt.Errorf("workspace move request id reused")
				}
				if r.Issued {
					return r, true, nil
				}
			}
		}
	}
	return WorkspaceMoveRecord{}, false, nil
}

// IssuedWorkspaceMove reports whether req is an exact replay of an issued
// move, with the same validation MoveWorkspace applies before its replay. It
// only reads: MoveWorkspace of the same request returns that record and
// mints nothing. Admission rules for new work (strict journal isolation) let
// such a replay through: they refuse new decisions, never an issued one.
func (p *Publisher) IssuedWorkspaceMove(req WorkspaceMoveRequest) bool {
	if validateWorkspaceMoveRequest(req) != nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, replay, err := p.issuedWorkspaceReplayLocked(req)
	return replay && err == nil
}

// WorkspaceMoveOutcomeError classifies a failed move at the reply boundary.
// M2 Uncertain / INV-AUTH-UNCERTAIN-001: admission cannot promise no effect
// while an earlier workspace write may have landed. Actual request cancellation
// stays typed; dependency errors do not bypass uncertainty.
func (p *Publisher) WorkspaceMoveOutcomeError(ctx context.Context, volume string, err error) error {
	if err == nil {
		return nil
	}
	if callerErr := ctx.Err(); callerErr != nil {
		return callerErr
	}
	p.mu.Lock()
	uncertain := p.workspaceUncertain[volume]
	p.mu.Unlock()
	if uncertain {
		return errors.Join(ErrWorkspaceOutcomeUncertain, err)
	}
	return err
}

func (p *Publisher) MoveWorkspace(ctx context.Context, req WorkspaceMoveRequest, head WorkspaceHead) (_ WorkspaceMoveRecord, resultErr error) {
	defer func() { resultErr = p.WorkspaceMoveOutcomeError(ctx, req.VolumeID, resultErr) }()
	if err := ctx.Err(); err != nil {
		return WorkspaceMoveRecord{}, err
	}
	if err := validateWorkspaceMoveRequest(req); err != nil {
		return WorkspaceMoveRecord{}, err
	}
	p.mu.Lock()
	if p.deleteReserved[req.VolumeID] {
		p.mu.Unlock()
		return WorkspaceMoveRecord{}, ErrWorkspaceDeleteReserved
	}
	r, replay, err := p.issuedWorkspaceReplayLocked(req)
	p.mu.Unlock()
	if err != nil {
		return WorkspaceMoveRecord{}, err
	}
	if replay {
		return r, nil
	}
	err = p.applyWithWorkspaceMove(AssignmentAsk{VolumeID: req.VolumeID, ReplicaID: req.TargetReplicaID, DataAddr: req.TargetDataAddr, CtrlAddr: req.TargetCtrlAddr, Intent: IntentReassign}, &workspaceGrant{req, head})
	if err != nil {
		return WorkspaceMoveRecord{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.workspace[req.VolumeID].Moves {
		if r.RequestID == req.RequestID {
			return r, nil
		}
	}
	return WorkspaceMoveRecord{}, fmt.Errorf("workspace move journal missing")
}

func (p *Publisher) BeginWorkspaceRevocation(ctx context.Context, volume, holder string, epoch uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, line, ok := p.workspaceLineLocked(volume)
	s := cloneWorkspace(p.workspace[volume])
	if !ok || line.info.ReplicaID != holder || line.info.Epoch != epoch || s == nil || s.Lease == nil {
		return fmt.Errorf("workspace revoke: no matching lease")
	}
	s.Lease.Revoking = true
	return p.persistWorkspaceLocked(volume, s)
}

func (p *Publisher) AckWorkspaceRevocation(ctx context.Context, volume string, ack WorkspaceRevokeAck) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, line, ok := p.workspaceLineLocked(volume)
	s := cloneWorkspace(p.workspace[volume])
	if !ok || line.info.ReplicaID != ack.ReplicaID || line.info.Epoch != ack.Epoch || ack.HeadGen == 0 || s == nil || s.Lease == nil || !s.Lease.Revoking {
		return fmt.Errorf("workspace revoke ACK: stale or unsolicited")
	}
	lease := *s.Lease
	lease.ExpiresAtNS = min(lease.ExpiresAtNS, p.leaseDeadlineLocked(volume).UnixNano())
	if p.WorkspaceLeaseValidity(lease) == 0 {
		return fmt.Errorf("workspace revoke ACK: lease expired; forced move required")
	}
	if s.Ack != nil && *s.Ack != ack {
		return fmt.Errorf("workspace revoke ACK changed")
	}
	s.Ack = &ack
	return p.persistWorkspaceLocked(volume, s)
}

func (p *Publisher) WorkspaceState(volume string) *WorkspaceState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneWorkspace(p.workspace[volume])
}

// WorkspaceSourceMoved proves that a subscribing server was the source of an
// issued move and the current line now belongs to another known server. A new
// target with no prior line is not a moved source. This startup-only read takes
// the publisher lock; lease renewals keep their separate nonblocking view.
func (p *Publisher) WorkspaceSourceMoved(volume, replica, server string) (PublishedLine, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.workspaceUncertain[volume] || server == "" {
		return PublishedLine{}, false
	}
	line, found := p.PublishedAuthorityLine(volume)
	owner, moved := line.Owner()
	if !found || !moved || owner == "" || owner == server {
		return PublishedLine{}, false
	}
	if s := p.workspace[volume]; s != nil {
		for _, r := range s.Moves {
			if r.Issued && r.OldHost == server && r.OldReplicaID == replica && r.OldEpoch < line.Epoch {
				return line, true
			}
		}
	}
	return PublishedLine{}, false
}

func (p *Publisher) workspaceLineLocked(volume string) (subKey, assignmentState, bool) {
	var key subKey
	var line assignmentState
	for k, s := range p.state {
		if k.volumeID == volume && s.present && s.info.Epoch > line.info.Epoch {
			key, line = k, s
		}
	}
	return key, line, line.present
}

func (p *Publisher) persistWorkspaceLocked(volume string, s *WorkspaceState) error {
	_, _, err := p.persistWorkspaceTimedLocked(volume, s)
	return err
}

// persistWorkspaceTimedLocked is persistWorkspaceLocked that also reports how
// long the durable store write itself took (write) and whether it was
// attempted (wrote is false when it refused before writing). Caller holds
// p.mu.
func (p *Publisher) persistWorkspaceTimedLocked(volume string, s *WorkspaceState) (write time.Duration, wrote bool, err error) {
	if p.store == nil || p.workspaceUncertain[volume] {
		return 0, false, fmt.Errorf("workspace durable store unavailable or uncertain")
	}
	_, line, ok := p.workspaceLineLocked(volume)
	if !ok {
		return 0, false, fmt.Errorf("workspace authority missing")
	}
	i := line.info
	record := DurableRecord{VolumeID: volume, ReplicaID: i.ReplicaID, Epoch: i.Epoch, EndpointVersion: i.EndpointVersion, DataAddr: i.DataAddr, CtrlAddr: i.CtrlAddr,
		WriteSeq: p.writeSeqByVolume[volume] + 1, Workspace: cloneWorkspace(s)}
	start := time.Now()
	err = p.store.Put(record)
	write = time.Since(start)
	if err != nil {
		p.workspaceUncertain[volume] = true
		p.refreshLeaseViewLocked(volume)
		return write, true, err
	}
	p.writeSeqByVolume[volume] = record.WriteSeq
	p.workspace[volume] = cloneWorkspace(s)
	p.refreshLeaseViewLocked(volume)
	return write, true, nil
}

type workspaceGrant struct {
	request WorkspaceMoveRequest
	head    WorkspaceHead
}

func sameWorkspaceRequest(r WorkspaceMoveRecord, q WorkspaceMoveRequest) bool {
	return r.VolumeID == q.VolumeID && r.OldReplicaID == q.ExpectedReplicaID && r.OldEpoch == q.ExpectedEpoch && r.NewReplicaID == q.TargetReplicaID && r.Kind == q.Kind && r.OldHost == q.ExpectedHost && r.NewHost == q.TargetHost &&
		r.TargetDataAddr == q.TargetDataAddr && r.TargetCtrlAddr == q.TargetCtrlAddr
}

// WorkspaceForcedMoveAdmitted reports whether a forced move of volume away
// from holder at epoch is admitted now: the lease expiry and the granted or
// restart deadline, each plus the clock-skew margin, have passed. A caller
// reads HEAD only after this returns nil: by then the holder's own lease has
// expired, so it starts no new claim or cut. HEAD is not frozen: a claim the
// holder or an earlier one sent may still land (ownership only, RemoteLSN
// unchanged), and the next holder's higher-epoch claim supersedes it. The
// move itself checks admission again. A request id the journal already holds is
// left to MoveWorkspace: its replay never uses a new HEAD.
func (p *Publisher) WorkspaceForcedMoveAdmitted(ctx context.Context, volume, requestID, holder string, epoch uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, line, ok := p.workspaceLineLocked(volume)
	s := p.workspace[volume]
	if s != nil {
		for _, r := range s.Moves {
			if r.RequestID == requestID {
				return nil
			}
		}
	}
	if !ok || line.info.Epoch != epoch || line.info.ReplicaID != holder || s == nil || s.Lease == nil {
		return fmt.Errorf("workspace move: missing or changed authority/lease")
	}
	_, err := p.forcedMoveAdmissionLocked(volume, s.Lease)
	return err
}

// forcedMoveAdmissionLocked returns the audit bound of the lost range when a
// forced move is admitted. Caller holds p.mu.
func (p *Publisher) forcedMoveAdmissionLocked(volume string, lease *WorkspaceLease) (int64, error) {
	lostUntil, _, err := p.forcedMoveBarrierLocked(volume, lease)
	if err != nil {
		return 0, err
	}
	return lostUntil, nil
}

// forcedMoveBarrierLocked evaluates forced admission at master time now. It
// returns the barrier (the audit bound of the lost range, wall time: the
// later of lease expiry and the granted or restart deadline, each plus
// skew) whether or not it has passed, and an error while it has not.
// Caller holds p.mu.
func (p *Publisher) forcedMoveBarrierLocked(volume string, lease *WorkspaceLease) (lostUntil int64, now time.Time, err error) {
	now = p.workspaceNow()
	skew := p.workspaceSkew(lease)
	deadline := p.leaseDeadlineLocked(volume)
	// The audit bound only, in wall time: the admission below keeps both
	// predicates (the deadline's monotonic reading included).
	lostUntil = time.Unix(0, lease.ExpiresAtNS).Add(skew).UnixNano()
	if d := deadline.Add(skew).UnixNano(); d > lostUntil {
		lostUntil = d
	}
	if now.Before(time.Unix(0, lease.ExpiresAtNS).Add(skew)) || now.Before(deadline.Add(skew)) {
		return lostUntil, now, fmt.Errorf("workspace forced move lease expiry plus clock-skew margin has not elapsed")
	}
	return lostUntil, now, nil
}

// issueWorkspaceMove marks the move issued and starts the new holder's lease
// in the same write as the new epoch, so a holder that dies before its first
// renewal still leaves a lease that expires and admits a forced move.
func (p *Publisher) issueWorkspaceMove(s *WorkspaceState, idx int, now time.Time) {
	r := &s.Moves[idx]
	r.Issued = true
	s.Lease, _ = p.newWorkspaceLease(r.NewReplicaID, r.NewEpoch, now)
	s.Ack = nil
}

// Called only under Publisher.mu. First persist a reservation with the OLD
// assignment. The subsequent authority write includes that same journal record.
// A crash at either boundary cannot expose an epoch without its grant record.
// The returned time is the issue time the new holder's lease starts from.
func (p *Publisher) prepareWorkspaceMoveLocked(ask AssignmentAsk, g workspaceGrant) (*WorkspaceState, uint64, time.Time, error) {
	q, h := g.request, g.head
	_, line, ok := p.workspaceLineLocked(q.VolumeID)
	s := cloneWorkspace(p.workspace[q.VolumeID])
	if !ok || line.info.Epoch != q.ExpectedEpoch || line.info.ReplicaID != q.ExpectedReplicaID || s == nil || s.Lease == nil {
		return nil, 0, time.Time{}, fmt.Errorf("workspace move: missing or changed authority/lease")
	}
	var lostUntil int64
	if q.Kind == "forced" {
		var err error
		if lostUntil, err = p.forcedMoveAdmissionLocked(q.VolumeID, s.Lease); err != nil {
			return nil, 0, time.Time{}, err
		}
	}
	// A restart may resume the identical durable reservation; it never grants
	// a different request the reserved epoch.
	for idx, r := range s.Moves {
		if r.RequestID == q.RequestID {
			if r.Issued || !sameWorkspaceRequest(r, q) {
				return nil, 0, time.Time{}, fmt.Errorf("workspace move reservation differs")
			}
			// A forced reservation issues only with the exact range recorded
			// before it; a missing one (a reservation older than lost ranges,
			// or a skipped side file) is refused, never recomputed: a new
			// request records a fresh range.
			if r.Kind == "forced" {
				if _, ok := p.lostRangeLocked(q.VolumeID, r.RequestID, r.NewEpoch); !ok {
					return nil, 0, time.Time{}, fmt.Errorf("workspace forced move reservation %s has no recorded lost range; retry with a new request id", r.RequestID)
				}
			}
			now := p.workspaceNow()
			p.issueWorkspaceMove(s, idx, now)
			return s, r.NewEpoch, now, nil
		}
	}
	if !workspaceHeadAdmits(q, h) {
		return nil, 0, time.Time{}, fmt.Errorf("workspace move: HEAD not owned by expected holder")
	}
	if q.Kind == "clean" {
		if !h.Detached || !s.Lease.Revoking || s.Ack == nil || s.Ack.ReplicaID != q.ExpectedReplicaID || s.Ack.Epoch != q.ExpectedEpoch || s.Ack.HeadGen != h.Gen {
			return nil, 0, time.Time{}, fmt.Errorf("workspace clean move needs detached HEAD and matching revocation ACK")
		}
	}
	max := maxPublishedEpochForVolume(p.state, q.VolumeID)
	for _, r := range s.Moves {
		if r.NewEpoch > max {
			max = r.NewEpoch
		}
	}
	if max == ^uint64(0) {
		return nil, 0, time.Time{}, fmt.Errorf("workspace epoch exhausted")
	}
	r := WorkspaceMoveRecord{RequestID: q.RequestID, VolumeID: q.VolumeID, OldHost: q.ExpectedHost, NewHost: q.TargetHost, OldReplicaID: q.ExpectedReplicaID, NewReplicaID: q.TargetReplicaID, Kind: q.Kind,
		OldEpoch: q.ExpectedEpoch, NewEpoch: max + 1, HeadGen: h.Gen, RemoteLSN: h.RemoteLSN, TargetDataAddr: q.TargetDataAddr, TargetCtrlAddr: q.TargetCtrlAddr}
	if q.Kind == "forced" {
		lost := WorkspaceLostRange{RequestID: r.RequestID, NewEpoch: r.NewEpoch, OldReplicaID: r.OldReplicaID, OldEpoch: r.OldEpoch, LostAfterLSN: r.RemoteLSN, LostUntilNS: lostUntil, HeadTimeNS: h.FilerMtimeNS}
		if err := p.recordLostRangeLocked(q.VolumeID, lost); err != nil {
			return nil, 0, time.Time{}, err
		}
	}
	s.Moves = append(s.Moves, r)
	if err := p.persistWorkspaceLocked(q.VolumeID, s); err != nil {
		return nil, 0, time.Time{}, err
	}
	now := p.workspaceNow()
	p.issueWorkspaceMove(s, len(s.Moves)-1, now)
	return s, r.NewEpoch, now, nil
}

// workspaceHeadAdmits is the HEAD rule of a move: HEAD is the expected
// holder's at the expected epoch, or, for a forced move only, any published
// HEAD at an older epoch. Admitted (the expected holder's lease has expired)
// and HEAD read after that: a HEAD at an older epoch means the expected
// holder never claimed it, or an earlier holder's late claim landed. The
// lost range starts at this HEAD's RemoteLSN. A HEAD at the expected or a
// higher epoch that is not the expected holder's stays refused.
func workspaceHeadAdmits(q WorkspaceMoveRequest, h WorkspaceHead) bool {
	owned := h.Gen != 0 && h.Epoch == q.ExpectedEpoch && h.Holder == q.ExpectedHost+"/"+q.ExpectedReplicaID
	if !owned && q.Kind == "forced" {
		owned = h.Gen != 0 && h.Epoch < q.ExpectedEpoch
	}
	return owned
}

// WorkspaceForcedMovePreview is a forced move's admission as the publisher
// would judge it now. It is advisory: MoveWorkspace checks again.
type WorkspaceForcedMovePreview struct {
	Admitted bool
	Reason   string // why not admitted, or a note on an admitted replay
	// LostUntilNS is the forced barrier (master wall time): the move is
	// admissible from then on, and it bounds the lost range. 0 when there
	// is no lease of the expected holder to judge.
	LostUntilNS int64
	NowNS       int64 // the master time of this judgement
	Replay      bool  // the request id is an issued move: a move replays it
}

// PreviewWorkspaceForcedMove runs the forced admission of req (request
// validation, journal replay, the expected line and lease, the barrier)
// without reserving, writing or minting anything. The HEAD rule is
// WorkspaceMoveHeadAdmits, applied by the caller to a HEAD read after an
// admitted preview, as for the move itself.
func (p *Publisher) PreviewWorkspaceForcedMove(req WorkspaceMoveRequest) WorkspaceForcedMovePreview {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := WorkspaceForcedMovePreview{NowNS: p.workspaceNow().UnixNano()}
	if req.Kind != "forced" {
		out.Reason = "workspace move preview: forced moves only"
		return out
	}
	if err := validateWorkspaceMoveRequest(req); err != nil {
		out.Reason = err.Error()
		return out
	}
	if _, replay, err := p.issuedWorkspaceReplayLocked(req); err != nil {
		out.Reason = err.Error()
		return out
	} else if replay {
		out.Admitted, out.Replay = true, true
		out.Reason = "request id already issued: a move with it returns the stored grant"
		if lr, ok := p.lostRangeForRequestLocked(req.VolumeID, req.RequestID); ok {
			out.LostUntilNS = lr.LostUntilNS
		}
		return out
	}
	_, line, ok := p.workspaceLineLocked(req.VolumeID)
	s := p.workspace[req.VolumeID]
	if !ok || line.info.Epoch != req.ExpectedEpoch || line.info.ReplicaID != req.ExpectedReplicaID || s == nil || s.Lease == nil {
		out.Reason = "workspace move: missing or changed authority/lease"
		return out
	}
	lostUntil, now, err := p.forcedMoveBarrierLocked(req.VolumeID, s.Lease)
	out.LostUntilNS, out.NowNS = lostUntil, now.UnixNano()
	if err != nil {
		out.Reason = err.Error()
		return out
	}
	out.Admitted = true
	return out
}

// WorkspaceMoveHeadAdmits reports whether head passes the HEAD rule of req
// (the same predicate a move applies).
func WorkspaceMoveHeadAdmits(req WorkspaceMoveRequest, head WorkspaceHead) bool {
	return workspaceHeadAdmits(req, head)
}
