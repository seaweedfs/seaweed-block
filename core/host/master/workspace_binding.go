package master

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func bindingFact(b lifecycle.WorkspaceBinding) *control.WorkspaceBinding {
	return &control.WorkspaceBinding{VolumeId: b.VolumeID, ServerId: b.ServerID, DataAddr: b.DataAddr, CtrlAddr: b.CtrlAddr, Retired: b.Retired}
}

func (s *services) RegisterWorkspaceBinding(ctx context.Context, q *control.WorkspaceBinding) (*control.WorkspaceBinding, error) {
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, workspaceError(err)
	}
	if err := s.host.reconcileWorkspaceBindings(); err != nil {
		return nil, workspaceError(err)
	}
	if _, ok := s.host.lifecycle.Nodes.GetNode(q.GetServerId()); !ok {
		return nil, workspaceError(fmt.Errorf("workspace binding: unknown server"))
	}
	b := lifecycle.WorkspaceBinding{VolumeID: q.GetVolumeId(), ServerID: q.GetServerId(), DataAddr: q.GetDataAddr(), CtrlAddr: q.GetCtrlAddr(), Retired: q.GetRetired()}
	if err := s.host.workspaceEndpointsAvailable(b); err != nil {
		return nil, workspaceError(err)
	}
	if err := s.host.lifecycle.WorkspaceBindings.Register(b); err != nil {
		return nil, workspaceError(err)
	}
	return bindingFact(b), nil
}

// Caller holds workspaceMu through the subsequent local commit. Check both
// prepared reservations and live legacy lines, including this volume's source.
func (h *Host) workspaceEndpointsAvailable(b lifecycle.WorkspaceBinding) error {
	if err := h.lifecycle.WorkspaceBindings.CheckAvailable(b); err != nil {
		return err
	}
	// Legacy assignments also reserve their live endpoints. Merely preparing
	// the first explicit target must not make the old source pair reusable.
	for _, v := range h.lifecycle.Volumes.ListVolumes() {
		line, ok := h.Publisher().VolumeAuthorityLine(v.Spec.VolumeID)
		if !ok {
			continue
		}
		state := h.Publisher().WorkspaceState(v.Spec.VolumeID)
		if state != nil && state.Ack != nil && state.Ack.Epoch == line.Epoch {
			continue
		}
		if b.EndpointsOverlap(lifecycle.WorkspaceBinding{DataAddr: line.DataAddr, CtrlAddr: line.CtrlAddr}) {
			if b.VolumeID == v.Spec.VolumeID && h.workspaceSourceHost(b.VolumeID, line.ReplicaID, line.Epoch) == b.ServerID && b.DataAddr == line.DataAddr && b.CtrlAddr == line.CtrlAddr {
				continue
			}
			return fmt.Errorf("workspace binding: endpoint belongs to live assignment")
		}
	}
	return nil
}

// workspaceSourceHost resolves explicit holder membership for
// INV-AUTH-HOLDER-IDENTITY-001; endpoints never select a server.
func (h *Host) workspaceSourceHost(volume, replica string, epoch uint64) string {
	current, _ := h.Publisher().PublishedAuthorityLine(volume)
	return h.workspaceLineOwner(current, volume, replica, epoch)
}

// workspaceLineOwner is TERM-HOLDER-001's single owner decision function
// (INV-AUTH-HOLDER-IDENTITY-001). A captured current line never takes the
// publisher's store-write lock (INV-WS-RENEW-NONBLOCKING-001); only historical
// lines need the move journal. Endpoints never identify a server.
func (h *Host) workspaceLineOwner(current authority.PublishedLine, volume, replica string, epoch uint64) string {
	if current.ReplicaID == replica && current.Epoch == epoch {
		if server, fromMove := current.Owner(); fromMove { // the single owner decision (PublishedLine.Owner)
			return server
		}
		return h.placementSourceHost(volume, replica)
	}
	if state := h.Publisher().WorkspaceState(volume); state != nil {
		for _, r := range state.Moves {
			if r.Issued && r.NewReplicaID == replica && r.NewEpoch == epoch {
				return r.NewHost
			}
			if r.Issued && r.OldReplicaID == replica && r.OldEpoch == epoch {
				return r.OldHost
			}
		}
	}
	return h.placementSourceHost(volume, replica)
}

// placementSourceHost uses committed membership, never endpoint inference
// (INV-AUTH-HOLDER-IDENTITY-001, INV-WS-RENEW-NONBLOCKING-001).
func (h *Host) placementSourceHost(volume, replica string) string {
	if h.lifecycle != nil {
		if p, ok := h.lifecycle.Placements.GetPlacementSnapshot(volume); ok {
			for _, slot := range p.Slots {
				if slot.ReplicaID == replica {
					return slot.ServerID
				}
			}
		}
	}
	for _, v := range h.topo.Volumes {
		if v.VolumeID == volume {
			for _, slot := range v.Slots {
				if slot.ReplicaID == replica {
					return slot.ServerID
				}
			}
		}
	}
	return ""
}

// Recover retirement after a crash between the authority ACK/issued record
// and binding persistence. Epoch-tagged retirements cannot retire a later
// re-registration when an old Move/ACK is retried or replayed on restart.
// Caller owns workspaceMu, or Host.New has not exposed the host yet.
func (h *Host) reconcileWorkspaceBindings() error {
	for _, v := range h.lifecycle.Volumes.ListVolumes() {
		volume := v.Spec.VolumeID
		state := h.Publisher().WorkspaceState(volume)
		if state == nil {
			continue
		}
		for _, m := range state.Moves {
			if m.Issued {
				if err := h.lifecycle.WorkspaceBindings.Retire(volume, m.OldHost, m.OldEpoch); err != nil {
					return err
				}
			}
		}
		if a := state.Ack; a != nil {
			host := h.workspaceSourceHost(volume, a.ReplicaID, a.Epoch)
			if host != "" {
				if err := h.lifecycle.WorkspaceBindings.Retire(volume, host, a.Epoch); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *services) PlaceWorkspace(ctx context.Context, q *control.WorkspacePlacement) (*control.WorkspaceEmpty, error) {
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	list, _, err := s.host.lifecycle.WorkspaceBindings.Get(q.GetVolumeId())
	if err != nil {
		return nil, workspaceError(err)
	}
	var binding *lifecycle.WorkspaceBinding
	for _, b := range list {
		if b.ServerID == q.GetServerId() && !b.Retired {
			copy := b
			binding = &copy
		}
	}
	if binding == nil || q.GetReplicaId() == "" {
		return nil, workspaceError(fmt.Errorf("workspace placement: active prepared binding and replica required"))
	}
	if _, ok := s.host.lifecycle.Nodes.GetNode(q.GetServerId()); !ok {
		return nil, workspaceError(fmt.Errorf("workspace placement: unknown server"))
	}
	identical := func(p lifecycle.PlacementIntent) bool {
		return len(p.Slots) == 1 && p.Slots[0].ServerID == binding.ServerID && p.Slots[0].ReplicaID == q.GetReplicaId() && p.Slots[0].DataAddr == binding.DataAddr && p.Slots[0].CtrlAddr == binding.CtrlAddr
	}
	// Strict journal isolation refuses new work only: repeating the
	// recorded placement keeps its idempotent result.
	if p, ok := s.host.lifecycle.Placements.GetPlacement(q.GetVolumeId()); !(ok && identical(p)) && s.host.colocatedTargetRefused(binding.ServerID) {
		return nil, workspaceError(fmt.Errorf("workspace placement: node %q shares the authority store's file system (strict journal isolation)", binding.ServerID))
	}
	err = s.host.Publisher().RecordWorkspacePlacement(ctx, q.GetVolumeId(), func() error {
		if p, ok := s.host.lifecycle.Placements.GetPlacement(q.GetVolumeId()); ok {
			if identical(p) {
				return nil
			}
			return fmt.Errorf("workspace placement: existing intent differs")
		}
		_, e := s.host.lifecycle.Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: q.GetVolumeId(), DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{VolumeID: q.GetVolumeId(), ServerID: binding.ServerID, ReplicaID: q.GetReplicaId(), Source: lifecycle.PlacementSourceExistingReplica, DataAddr: binding.DataAddr, CtrlAddr: binding.CtrlAddr}}})
		return e
	})
	if err != nil {
		return nil, workspaceError(err)
	}
	return &control.WorkspaceEmpty{}, nil
}

// admitWorkspacePublication owns the last local boundary before an ordinary
// Publisher mint. Guarded Move already owns workspaceMu in its RPC path.
func (h *Host) admitWorkspacePublication(ask authority.AssignmentAsk, publish func() error) error {
	if h.lifecycle == nil {
		return publish()
	}
	h.workspaceMu.Lock()
	defer h.workspaceMu.Unlock()
	if h.lifecycle.Deletions.Reserved(ask.VolumeID) {
		return fmt.Errorf("workspace publication: volume is being deleted")
	}
	if _, alive := h.lifecycle.Volumes.GetVolume(ask.VolumeID); !alive {
		return fmt.Errorf("workspace publication: no lifecycle VolumeRecord for %s; register it (CreateVolume) before publishing", ask.VolumeID)
	}
	bindings, bound, err := h.lifecycle.WorkspaceBindings.Get(ask.VolumeID)
	if err != nil {
		return err
	}
	var epoch uint64
	if line, ok := h.Publisher().VolumeAuthorityLine(ask.VolumeID); ok {
		epoch = line.Epoch
	}
	server := h.workspaceSourceHost(ask.VolumeID, ask.ReplicaID, epoch)
	b := lifecycle.WorkspaceBinding{VolumeID: ask.VolumeID, ServerID: server, DataAddr: ask.DataAddr, CtrlAddr: ask.CtrlAddr}
	if bound {
		matched := false
		for _, r := range bindings {
			if !r.Retired && r.ServerID == server && r.DataAddr == ask.DataAddr && r.CtrlAddr == ask.CtrlAddr {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("workspace publication: no matching active binding")
		}
	}
	if err := h.lifecycle.WorkspaceBindings.CheckAvailable(b); err != nil {
		return err
	}
	// A supported snapshot and its direct promotion fallback may both be
	// dequeued before either commits. Apply the product loop's existing
	// already-current check again here, rather than advancing the epoch twice.
	if ask.Intent == authority.IntentReassign && h.assignmentAskAlreadyCurrent(ask.VolumeID, ask.ReplicaID, ask.DataAddr, ask.CtrlAddr) {
		return nil
	}
	// The same mutex covers Register/Delete. It stays held through the actual
	// Publisher persistence and in-memory commit, not just queue admission.
	return publish()
}
