package master

import (
	"context"
	"errors"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// VolumeDeleteAuthority binds the layout delete's two authority operations
// to its lifecycle volume and existing deletion token.
func (h *Host) VolumeDeleteAuthority(volume, token string) *volumeDeleteAuthority {
	return &volumeDeleteAuthority{host: h, volume: volume, token: token}
}

type volumeDeleteAuthority struct {
	host          *Host
	volume, token string
}

func (a *volumeDeleteAuthority) Reserve(ctx context.Context) error {
	h := a.host
	h.workspaceMu.Lock()
	defer h.workspaceMu.Unlock()
	if h.lifecycle == nil {
		return errors.New("workspace delete: lifecycle store unavailable")
	}
	persist := func() error {
		if _, err := h.lifecycle.Deletions.Admit(a.volume, a.token); err != nil {
			return err
		}
		_, err := h.lifecycle.Deletions.Commit(a.volume, a.token)
		return err
	}
	// A volume whose creation never completed (the master's durable record
	// still awaits a replica store) is deleted without its holder: a live
	// lease does not refuse the reservation, which stops every grant; the
	// HEAD update still waits for no live lease. A volume that ever completed
	// creation keeps the live-lease refusal.
	if h.inCreationPhase(a.volume, authority.AuthorityBasis{}, nil) != "" {
		return h.Publisher().ReserveNeverCreatedWorkspaceDeletion(ctx, a.volume, persist)
	}
	return h.Publisher().ReserveWorkspaceDeletion(ctx, a.volume, persist)
}

func (a *volumeDeleteAuthority) NoLiveLease(ctx context.Context) error {
	if a.host.lifecycle == nil || !a.host.lifecycle.Deletions.CommittedSnapshot(a.volume) {
		return errors.New("workspace delete: committed reservation required")
	}
	return a.host.Publisher().NoLiveWorkspaceLease(ctx, a.volume)
}

// ReserveVolumeDeletion exposes the same committed reservation used by startup
// and every positive lease writer. It does not reuse abortable admission.
func (s *services) ReserveVolumeDeletion(ctx context.Context, req *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	if req.GetVolumeId() == "" || req.GetToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and token are required")
	}
	if err := s.host.VolumeDeleteAuthority(req.GetVolumeId(), req.GetToken()).Reserve(ctx); err != nil {
		if errors.Is(err, authority.ErrWorkspaceDeleteRefused) {
			return nil, status.Errorf(codes.FailedPrecondition, "reserve volume deletion: %v", err)
		}
		return nil, status.Errorf(codes.Unavailable, "reserve volume deletion: %v", err)
	}
	_, known := s.host.lifecycle.Volumes.GetVolume(req.GetVolumeId())
	return &control.MarkVolumeDeletingResponse{Known: known, State: lifecycle.ReservationCommitted}, nil
}

// CheckVolumeNoLiveLease reads the master's authoritative barrier at the
// delete's CAS boundary, including granted-but-unclaimed lines.
func (s *services) CheckVolumeNoLiveLease(ctx context.Context, req *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	if req.GetVolumeId() == "" || req.GetToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and token are required")
	}
	if err := s.host.VolumeDeleteAuthority(req.GetVolumeId(), req.GetToken()).NoLiveLease(ctx); err != nil {
		return nil, workspaceError(err)
	}
	_, known := s.host.lifecycle.Volumes.GetVolume(req.GetVolumeId())
	return &control.MarkVolumeDeletingResponse{Known: known, State: lifecycle.ReservationCommitted}, nil
}
