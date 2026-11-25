package master

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
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
	return h.Publisher().ReserveWorkspaceDeletion(ctx, a.volume, func() error {
		if _, err := h.lifecycle.Deletions.Admit(a.volume, a.token); err != nil {
			return err
		}
		_, err := h.lifecycle.Deletions.Commit(a.volume, a.token)
		return err
	})
}

func (a *volumeDeleteAuthority) NoLiveLease(ctx context.Context) error {
	if a.host.lifecycle == nil || !a.host.lifecycle.Deletions.CommittedSnapshot(a.volume) {
		return errors.New("workspace delete: committed reservation required")
	}
	return a.host.Publisher().NoLiveWorkspaceLease(ctx, a.volume)
}

// ReserveVolumeDeletion commits the layout volume delete's reservation
// (the token's; no later grant for the volume), refused while a line of the
// volume is live.
func (s *services) ReserveVolumeDeletion(ctx context.Context, req *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	if req.GetVolumeId() == "" || req.GetToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and token are required")
	}
	if err := s.host.VolumeDeleteAuthority(req.GetVolumeId(), req.GetToken()).Reserve(ctx); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "reserve volume deletion: %v", err)
	}
	return &control.MarkVolumeDeletingResponse{Known: true, State: lifecycle.ReservationCommitted}, nil
}

// CheckVolumeNoLiveLease answers OK only when the volume's delete is
// committed and no line of it holds a live lease.
func (s *services) CheckVolumeNoLiveLease(ctx context.Context, req *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if err := s.host.VolumeDeleteAuthority(req.GetVolumeId(), req.GetToken()).NoLiveLease(ctx); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "volume has a live lease or no committed reservation: %v", err)
	}
	return &control.MarkVolumeDeletingResponse{Known: true, State: lifecycle.ReservationCommitted}, nil
}
