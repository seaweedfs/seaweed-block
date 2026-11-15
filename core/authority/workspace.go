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
	ReplicaID   string
	Epoch       uint64
	ExpiresAtNS int64
	Revoking    bool
}

type WorkspaceRevokeAck struct {
	ReplicaID      string
	Epoch, HeadGen uint64
}

type WorkspaceMoveRecord struct {
	RequestID, VolumeID, OldHost, NewHost, Kind string
	OldHolder, NewHolder                        string
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
	lease := WorkspaceLease{ReplicaID: holder, Epoch: epoch, ExpiresAtNS: now.Add(p.workspaceTTL).UnixNano()}
	s.Lease = &lease
	if err := p.persistWorkspaceLocked(volume, s); err != nil {
		return WorkspaceLease{}, err
	}
	p.workspaceLeaseDeadline[volume] = now.Add(p.workspaceTTL)
	return lease, nil
}

type WorkspaceMoveRequest struct {
	RequestID, VolumeID, ExpectedHolder, TargetHolder string
	ExpectedHost, TargetHost                          string
	ExpectedEpoch                                     uint64
	TargetDataAddr, TargetCtrlAddr                    string
	Kind                                              string
	Force                                             bool
}

// MoveWorkspace is the authority-owned guarded RF1 move seam. The host layer
// validates volume eligibility and reads HEAD before calling this boundary.
func (p *Publisher) MoveWorkspace(ctx context.Context, req WorkspaceMoveRequest, head WorkspaceHead) (WorkspaceMoveRecord, error) {
	if err := ctx.Err(); err != nil {
		return WorkspaceMoveRecord{}, err
	}
	if req.RequestID == "" || req.VolumeID == "" || req.ExpectedHost == "" || req.TargetHost == "" || req.ExpectedHost == req.TargetHost || req.ExpectedHolder == "" || req.ExpectedEpoch == 0 || req.TargetHolder == "" || req.ExpectedHolder == req.TargetHolder ||
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
	err := p.applyWithWorkspaceMove(AssignmentAsk{VolumeID: req.VolumeID, ReplicaID: req.TargetHolder, DataAddr: req.TargetDataAddr, CtrlAddr: req.TargetCtrlAddr, Intent: IntentReassign}, &workspaceGrant{req, head})
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
	return r.VolumeID == q.VolumeID && r.OldHolder == q.ExpectedHolder && r.OldEpoch == q.ExpectedEpoch && r.NewHolder == q.TargetHolder && r.Kind == q.Kind && r.OldHost == q.ExpectedHost && r.NewHost == q.TargetHost &&
		r.TargetDataAddr == q.TargetDataAddr && r.TargetCtrlAddr == q.TargetCtrlAddr
}

// Called only under Publisher.mu. First persist a reservation with the OLD
// assignment. The subsequent authority write includes that same journal record.
// A crash at either boundary cannot expose an epoch without its grant record.
func (p *Publisher) prepareWorkspaceMoveLocked(ask AssignmentAsk, g workspaceGrant) (*WorkspaceState, uint64, error) {
	q, h := g.request, g.head
	_, line, ok := p.workspaceLineLocked(q.VolumeID)
	s := cloneWorkspace(p.workspace[q.VolumeID])
	if !ok || line.info.Epoch != q.ExpectedEpoch || line.info.ReplicaID != q.ExpectedHolder || s == nil || s.Lease == nil {
		return nil, 0, fmt.Errorf("workspace move: missing or changed authority/lease")
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
	if h.Gen == 0 || h.Epoch != q.ExpectedEpoch || h.Holder != q.ExpectedHolder {
		return nil, 0, fmt.Errorf("workspace move: HEAD not owned by expected holder")
	}
	if q.Kind == "clean" {
		if !h.Detached || !s.Lease.Revoking || s.Ack == nil || s.Ack.ReplicaID != q.ExpectedHolder || s.Ack.Epoch != q.ExpectedEpoch || s.Ack.HeadGen != h.Gen {
			return nil, 0, fmt.Errorf("workspace clean move needs detached HEAD and matching revocation ACK")
		}
	} else {
		now := p.workspaceNow()
		if now.UnixNano() < s.Lease.ExpiresAtNS || now.Before(p.workspaceLeaseDeadline[q.VolumeID]) {
			return nil, 0, fmt.Errorf("workspace forced move lease has not expired")
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
	r := WorkspaceMoveRecord{RequestID: q.RequestID, VolumeID: q.VolumeID, OldHost: q.ExpectedHost, NewHost: q.TargetHost, OldHolder: q.ExpectedHolder, NewHolder: q.TargetHolder, Kind: q.Kind,
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
