package master

import (
	"context"
	"errors"
	"fmt"

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
	// The delete without the holder's acknowledgement (a live lease does
	// not refuse the reservation, which stops every grant) is admitted only
	// in stage 1, while the master's record still awaits a replica store:
	// the primary serves nothing until every store is recorded. The record
	// is marked durably first, in the lock that ends the wait, and the mark
	// freezes it in stage 1, so the primary never starts serving. Past stage
	// 1 (also while creation has not been recorded as ended: the volume may
	// be serving) the live-lease refusal stays; a broken primary's lease
	// ends by itself, and the ordinary delete then applies.
	if h.attest != nil {
		marked, err := h.attest.markDeleteWithoutAck(a.volume, h.replicaSlotsFor(a.volume))
		if err != nil {
			return fmt.Errorf("workspace delete: record the delete without acknowledgement: %w", err)
		}
		if marked {
			return h.Publisher().ReserveNeverCreatedWorkspaceDeletion(ctx, a.volume, persist)
		}
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
