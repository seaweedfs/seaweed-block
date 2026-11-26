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
	// while creation has not ended, recorded durably before the reservation
	// so creation never ends after it; it finishes only on proof that the
	// volume is empty (NoLiveLease). A volume whose creation ended keeps the
	// live-lease refusal. The line, so a ready answer or a survivor's write
	// read now ends creation before the delete is decided.
	line, _ := h.Publisher().VolumeAuthorityLine(a.volume)
	if h.attest != nil && h.inCreationPhase(a.volume, line, nil) != "" {
		marked, err := h.attest.markDeleteWithoutAck(a.volume)
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
	if err := a.host.Publisher().NoLiveWorkspaceLease(ctx, a.volume); err != nil {
		return err
	}
	if a.host.attest == nil || !a.host.attest.deleteWithoutAck(a.volume) {
		return nil
	}
	return a.host.provenEmptyForDelete(a.volume)
}

// ErrDeleteNotProvenEmpty refuses a delete without the holder's
// acknowledgement of a volume not proven empty.
var ErrDeleteNotProvenEmpty = errors.New("workspace delete: the volume is not proven empty")

// provenEmptyForDelete is the proof a delete without the holder's
// acknowledgement finishes on. It is read here, after the reservation
// stopped every grant and the holder's lease horizon passed, so no write
// can be acknowledged after it. Proven empty is stage 1 (the primary of a
// volume whose stores are not all attested serves nothing), or N-K+1
// survivors (the fencing round's count) that answered, each with a valid
// table at end 0; a survivor that did not answer is not empty, and one that
// holds a write refuses the delete for good.
func (h *Host) provenEmptyForDelete(volumeID string) error {
	if waiting, _ := h.attest.awaited(volumeID, h.replicaSlotsFor(volumeID)); waiting > 0 {
		return nil
	}
	line, ok := h.Publisher().VolumeAuthorityLine(volumeID)
	if !ok || !line.Assigned {
		return fmt.Errorf("%w: no line to read its survivors at", ErrDeleteNotProvenEmpty)
	}
	h.promotionMu.RLock()
	prober := h.promotionProber
	h.promotionMu.RUnlock()
	if prober == nil {
		return fmt.Errorf("%w: no promotion gate to read the survivors", ErrDeleteNotProvenEmpty)
	}
	survivors := h.survivorCandidates(volumeID, line.ReplicaID)
	res, err := prober.ProbePromotionCandidates(volumeID, line, survivors)
	if err != nil {
		return fmt.Errorf("%w: survivor round: %v", ErrDeleteNotProvenEmpty, err)
	}
	if !isKnownAckProfile(res.AckProfile) {
		return fmt.Errorf("%w: unknown ack profile %q", ErrDeleteNotProvenEmpty, res.AckProfile)
	}
	empty := map[string]bool{}
	for _, c := range res.Candidates {
		if c.ReplicaID == "" || c.ReplicaID == line.ReplicaID || !c.TableValid {
			continue
		}
		if c.DurableLSN > 0 {
			return fmt.Errorf("%w: survivor %s holds a write (end %d): the volume served", ErrDeleteNotProvenEmpty, c.ReplicaID, c.DurableLSN)
		}
		empty[c.ReplicaID] = true
	}
	if need := unknownFrontierSurvivorsNeeded(res.AckProfile, len(survivors)); len(survivors) == 0 || len(empty) < need {
		return fmt.Errorf("%w: %d of %d survivors answered empty, need %d", ErrDeleteNotProvenEmpty, len(empty), len(survivors), need)
	}
	return nil
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
