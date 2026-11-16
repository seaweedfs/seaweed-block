package authority

import (
	"context"
	"fmt"
	"time"
)

// WorkspaceHead is read from the configured HEAD source, never supplied as
// operator evidence. Detached means the old holder's final cut is committed.
type WorkspaceHead struct {
	Gen, Epoch, RemoteLSN uint64
	Holder                string
	Detached              bool
}

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

func (p *Publisher) RenewWorkspaceLease(ctx context.Context, volume, holder string, epoch uint64) (WorkspaceLease, error) {
	if err := ctx.Err(); err != nil {
		return WorkspaceLease{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, line, ok := p.workspaceLineLocked(volume)
	if !ok || line.info.ReplicaID != holder || line.info.Epoch != epoch {
		return WorkspaceLease{}, fmt.Errorf("workspace lease: stale authority")
	}
	s := cloneWorkspace(p.workspace[volume])
	if s == nil {
		s = &WorkspaceState{}
	}
	if len(s.Moves) > 0 && !s.Moves[len(s.Moves)-1].Issued {
		return WorkspaceLease{}, fmt.Errorf("workspace lease: move pending")
	}
	now := p.workspaceNow()
	if s.Lease != nil {
		if s.Lease.Revoking {
			return *s.Lease, nil
		} // tell the host to drain; never extend
		if now.UnixNano() >= s.Lease.ExpiresAtNS || !now.Before(p.workspaceLeaseDeadline[volume]) {
			return WorkspaceLease{}, fmt.Errorf("workspace lease expired or revoking")
		}
	}
	lease := WorkspaceLease{ReplicaID: holder, Epoch: epoch, ExpiresAtNS: now.Add(p.workspaceTTL).UnixNano(), MaxClockSkewNS: int64(p.workspaceSkew(s.Lease))}
	s.Lease = &lease
	if err := p.persistWorkspaceLocked(volume, s); err != nil {
		return WorkspaceLease{}, err
	}
	p.workspaceLeaseDeadline[volume] = now.Add(p.workspaceTTL)
	return lease, nil
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
func (p *Publisher) RecordWorkspaceDeletion(ctx context.Context, volume string, pendingPlacement bool, persist func() error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s := p.workspace[volume]
	if s != nil && len(s.Moves) > 0 && !s.Moves[len(s.Moves)-1].Issued {
		return fmt.Errorf("workspace delete: move pending")
	}
	if _, line, ok := p.workspaceLineLocked(volume); ok {
		if s == nil || s.Ack == nil || s.Ack.Epoch != line.info.Epoch || s.Ack.ReplicaID != line.info.ReplicaID {
			return fmt.Errorf("workspace delete: live authority requires clean revocation ACK")
		}
	} else if pendingPlacement {
		return fmt.Errorf("workspace delete: placement may have queued authority")
	}
	return persist()
}

func (p *Publisher) MoveWorkspace(ctx context.Context, req WorkspaceMoveRequest, head WorkspaceHead) (WorkspaceMoveRecord, error) {
	if err := ctx.Err(); err != nil {
		return WorkspaceMoveRecord{}, err
	}
	// v1 moves a replica between hosts without changing its logical slot.
	// Refuse before locking or persisting a reservation/authority epoch.
	if req.ExpectedReplicaID != req.TargetReplicaID {
		return WorkspaceMoveRecord{}, fmt.Errorf("workspace move: v1 requires the same logical replica id")
	}
	if req.RequestID == "" || req.VolumeID == "" || req.ExpectedHost == "" || req.TargetHost == "" || req.ExpectedHost == req.TargetHost || req.ExpectedReplicaID == "" || req.ExpectedEpoch == 0 || req.TargetReplicaID == "" ||
		req.TargetDataAddr == "" || req.TargetCtrlAddr == "" || (req.Kind != "clean" && req.Kind != "forced") || req.Force != (req.Kind == "forced") {
		return WorkspaceMoveRecord{}, fmt.Errorf("workspace move: invalid request")
	}
	p.mu.Lock()
	if s := p.workspace[req.VolumeID]; s != nil {
		for _, r := range s.Moves {
			if r.RequestID == req.RequestID {
				if !sameWorkspaceRequest(r, req) {
					p.mu.Unlock()
					return WorkspaceMoveRecord{}, fmt.Errorf("workspace move request id reused")
				}
				if r.Issued {
					p.mu.Unlock()
					return r, nil
				}
			}
		}
	}
	p.mu.Unlock()
	err := p.applyWithWorkspaceMove(AssignmentAsk{VolumeID: req.VolumeID, ReplicaID: req.TargetReplicaID, DataAddr: req.TargetDataAddr, CtrlAddr: req.TargetCtrlAddr, Intent: IntentReassign}, &workspaceGrant{req, head})
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
	now := p.workspaceNow()
	if now.UnixNano() >= s.Lease.ExpiresAtNS || !now.Before(p.workspaceLeaseDeadline[volume]) {
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
	if p.store == nil || p.workspaceUncertain[volume] {
		return fmt.Errorf("workspace durable store unavailable or uncertain")
	}
	_, line, ok := p.workspaceLineLocked(volume)
	if !ok {
		return fmt.Errorf("workspace authority missing")
	}
	i := line.info
	record := DurableRecord{VolumeID: volume, ReplicaID: i.ReplicaID, Epoch: i.Epoch, EndpointVersion: i.EndpointVersion, DataAddr: i.DataAddr, CtrlAddr: i.CtrlAddr,
		WriteSeq: p.writeSeqByVolume[volume] + 1, Workspace: cloneWorkspace(s)}
	if err := p.store.Put(record); err != nil {
		p.workspaceUncertain[volume] = true
		return err
	}
	p.writeSeqByVolume[volume] = record.WriteSeq
	p.workspace[volume] = cloneWorkspace(s)
	return nil
}

type workspaceGrant struct {
	request WorkspaceMoveRequest
	head    WorkspaceHead
}

func sameWorkspaceRequest(r WorkspaceMoveRecord, q WorkspaceMoveRequest) bool {
	return r.VolumeID == q.VolumeID && r.OldReplicaID == q.ExpectedReplicaID && r.OldEpoch == q.ExpectedEpoch && r.NewReplicaID == q.TargetReplicaID && r.Kind == q.Kind && r.OldHost == q.ExpectedHost && r.NewHost == q.TargetHost &&
		r.TargetDataAddr == q.TargetDataAddr && r.TargetCtrlAddr == q.TargetCtrlAddr
}

// Called only under Publisher.mu. First persist a reservation with the OLD
// assignment. The subsequent authority write includes that same journal record.
// A crash at either boundary cannot expose an epoch without its grant record.
func (p *Publisher) prepareWorkspaceMoveLocked(ask AssignmentAsk, g workspaceGrant) (*WorkspaceState, uint64, error) {
	q, h := g.request, g.head
	_, line, ok := p.workspaceLineLocked(q.VolumeID)
	s := cloneWorkspace(p.workspace[q.VolumeID])
	if !ok || line.info.Epoch != q.ExpectedEpoch || line.info.ReplicaID != q.ExpectedReplicaID || s == nil || s.Lease == nil {
		return nil, 0, fmt.Errorf("workspace move: missing or changed authority/lease")
	}
	if q.Kind == "forced" {
		now := p.workspaceNow()
		skew := p.workspaceSkew(s.Lease)
		if now.Before(time.Unix(0, s.Lease.ExpiresAtNS).Add(skew)) || now.Before(p.workspaceLeaseDeadline[q.VolumeID].Add(skew)) {
			return nil, 0, fmt.Errorf("workspace forced move lease expiry plus clock-skew margin has not elapsed")
		}
	}
	// A restart may resume the identical durable reservation; it never grants
	// a different request the reserved epoch.
	for idx, r := range s.Moves {
		if r.RequestID == q.RequestID {
			if r.Issued || !sameWorkspaceRequest(r, q) {
				return nil, 0, fmt.Errorf("workspace move reservation differs")
			}
			s.Moves[idx].Issued = true
			s.Lease = nil
			s.Ack = nil
			return s, r.NewEpoch, nil
		}
	}
	if h.Gen == 0 || h.Epoch != q.ExpectedEpoch || h.Holder != q.ExpectedHost+"/"+q.ExpectedReplicaID {
		return nil, 0, fmt.Errorf("workspace move: HEAD not owned by expected holder")
	}
	if q.Kind == "clean" {
		if !h.Detached || !s.Lease.Revoking || s.Ack == nil || s.Ack.ReplicaID != q.ExpectedReplicaID || s.Ack.Epoch != q.ExpectedEpoch || s.Ack.HeadGen != h.Gen {
			return nil, 0, fmt.Errorf("workspace clean move needs detached HEAD and matching revocation ACK")
		}
	}
	max := maxPublishedEpochForVolume(p.state, q.VolumeID)
	for _, r := range s.Moves {
		if r.NewEpoch > max {
			max = r.NewEpoch
		}
	}
	if max == ^uint64(0) {
		return nil, 0, fmt.Errorf("workspace epoch exhausted")
	}
	r := WorkspaceMoveRecord{RequestID: q.RequestID, VolumeID: q.VolumeID, OldHost: q.ExpectedHost, NewHost: q.TargetHost, OldReplicaID: q.ExpectedReplicaID, NewReplicaID: q.TargetReplicaID, Kind: q.Kind,
		OldEpoch: q.ExpectedEpoch, NewEpoch: max + 1, HeadGen: h.Gen, RemoteLSN: h.RemoteLSN, TargetDataAddr: q.TargetDataAddr, TargetCtrlAddr: q.TargetCtrlAddr}
	s.Moves = append(s.Moves, r)
	if err := p.persistWorkspaceLocked(q.VolumeID, s); err != nil {
		return nil, 0, err
	}
	s.Moves[len(s.Moves)-1].Issued = true
	s.Lease = nil
	s.Ack = nil
	return s, r.NewEpoch, nil
}
