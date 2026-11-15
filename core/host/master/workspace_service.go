package master

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *services) workspaceVolume(volume string) error {
	if !s.host.cfg.WorkspaceRPC {
		return status.Error(codes.FailedPrecondition, "workspace RPC disabled")
	}
	if s.host.lifecycle == nil {
		return status.Error(codes.FailedPrecondition, "lifecycle store unavailable")
	}
	r, ok := s.host.lifecycle.Volumes.GetVolume(volume)
	if !ok || r.Spec.ReplicationFactor != 1 {
		return status.Error(codes.FailedPrecondition, "workspace move requires an existing RF1 volume")
	}
	return nil
}

func workspaceError(err error) error {
	if err == nil {
		return nil
	}
	if err == context.Canceled {
		return status.Error(codes.Canceled, err.Error())
	}
	if err == context.DeadlineExceeded {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.FailedPrecondition, err.Error())
}

func leaseFact(volume string, l authority.WorkspaceLease) *control.WorkspaceLeaseFact {
	// Client anchors this duration to its request START, not response arrival.
	// Never grant more than the server's configured 30s TTL after a restart.
	remaining := time.Until(time.Unix(0, l.ExpiresAtNS))
	if remaining > 30*time.Second {
		remaining = 30 * time.Second
	}
	if remaining < 0 {
		remaining = 0
	}
	return &control.WorkspaceLeaseFact{VolumeId: volume, Holder: l.ReplicaID, Epoch: l.Epoch, ExpiresAtNs: l.ExpiresAtNS, ValidForNs: int64(remaining), Revoking: l.Revoking}
}

func (s *services) RenewWorkspaceLease(ctx context.Context, q *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	l, err := s.host.Publisher().RenewWorkspaceLease(ctx, q.GetVolumeId(), q.GetHolder(), q.GetEpoch())
	if err != nil {
		return nil, workspaceError(err)
	}
	return leaseFact(q.GetVolumeId(), l), nil
}

func (s *services) RevokeWorkspace(ctx context.Context, q *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	if err := s.host.Publisher().BeginWorkspaceRevocation(ctx, q.GetVolumeId(), q.GetHolder(), q.GetEpoch()); err != nil {
		return nil, workspaceError(err)
	}
	state := s.host.Publisher().WorkspaceState(q.GetVolumeId())
	if state == nil || state.Lease == nil {
		return nil, status.Error(codes.FailedPrecondition, "workspace moved during revocation")
	}
	return leaseFact(q.GetVolumeId(), *state.Lease), nil
}

func (s *services) AckWorkspaceRevoked(ctx context.Context, q *control.WorkspaceRevocationAck) (*control.WorkspaceEmpty, error) {
	id := q.GetIdentity()
	if err := s.workspaceVolume(id.GetVolumeId()); err != nil {
		return nil, err
	}
	if err := s.host.Publisher().AckWorkspaceRevocation(ctx, id.GetVolumeId(), authority.WorkspaceRevokeAck{ReplicaID: id.GetHolder(), Epoch: id.GetEpoch(), HeadGen: q.GetHeadGen()}); err != nil {
		return nil, workspaceError(err)
	}
	return &control.WorkspaceEmpty{}, nil
}

func grantFact(r authority.WorkspaceMoveRecord) *control.WorkspaceGrant {
	return &control.WorkspaceGrant{RequestId: r.RequestID, VolumeId: r.VolumeID, OldHolder: r.OldHolder, NewHolder: r.NewHolder, Kind: r.Kind,
		OldEpoch: r.OldEpoch, NewEpoch: r.NewEpoch, HeadGen: r.HeadGen, RemoteLsn: r.RemoteLSN, Issued: r.Issued, OldHost: r.OldHost, NewHost: r.NewHost}
}

func (s *services) MoveWorkspace(ctx context.Context, q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	oldHost := ""
	if state := s.host.Publisher().WorkspaceState(q.GetVolumeId()); state != nil {
		for _, r := range state.Moves {
			if r.Issued && r.NewHolder == q.GetExpectedHolder() && r.NewEpoch == q.GetExpectedEpoch() {
				oldHost = r.NewHost
			}
		}
	}
	if oldHost == "" {
		if placement, ok := s.host.lifecycle.Placements.GetPlacement(q.GetVolumeId()); ok {
			for _, slot := range placement.Slots {
				if slot.ReplicaID == q.GetExpectedHolder() {
					oldHost = slot.ServerID
				}
			}
		}
	}
	target, ok := s.host.lifecycle.Nodes.GetNode(q.GetTargetServerId())
	if oldHost == "" || !ok || target.ServerID == oldHost || target.DataAddr != q.GetTargetDataAddr() || target.CtrlAddr != q.GetTargetCtrlAddr() {
		return nil, status.Error(codes.FailedPrecondition, "move needs known source slot and registered target endpoints")
	}
	// Always bound the configured reader, even when the caller omitted a deadline.
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	head, err := s.host.cfg.WorkspaceHeadReader(readCtx, q.GetVolumeId())
	if err != nil {
		return nil, workspaceError(err)
	}
	r, err := s.host.Publisher().MoveWorkspace(ctx, authority.WorkspaceMoveRequest{RequestID: q.GetRequestId(), VolumeID: q.GetVolumeId(), ExpectedHolder: q.GetExpectedHolder(), ExpectedHost: oldHost, TargetHost: target.ServerID,
		ExpectedEpoch: q.GetExpectedEpoch(), TargetHolder: q.GetTargetHolder(), TargetDataAddr: q.GetTargetDataAddr(), TargetCtrlAddr: q.GetTargetCtrlAddr(), Kind: q.GetKind(), Force: q.GetForce()}, head)
	if err != nil {
		return nil, workspaceError(err)
	}
	return grantFact(r), nil
}

func (s *services) QueryWorkspace(ctx context.Context, q *control.WorkspaceVolume) (*control.WorkspaceStatus, error) {
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	out := &control.WorkspaceStatus{}
	state := s.host.Publisher().WorkspaceState(q.GetVolumeId())
	if state == nil {
		return out, nil
	}
	if state.Lease != nil {
		out.Lease = leaseFact(q.GetVolumeId(), *state.Lease)
	}
	if state.Ack != nil {
		out.RevocationAck = &control.WorkspaceRevocationAck{Identity: &control.WorkspaceIdentity{VolumeId: q.GetVolumeId(), Holder: state.Ack.ReplicaID, Epoch: state.Ack.Epoch}, HeadGen: state.Ack.HeadGen}
	}
	for _, r := range state.Moves {
		out.Moves = append(out.Moves, grantFact(r))
	}
	return out, nil
}
