package master

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
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
	if s.host.lifecycle.Deletions.Reserved(volume) {
		return status.Error(codes.FailedPrecondition, "workspace: volume is being deleted")
	}
	return nil
}

// workspaceVolumeForRenewal is workspaceVolume for lease renewal: it reads
// the published snapshots of the lifecycle stores instead of taking their
// mutexes, which writers hold across file I/O. A renewal during an in-flight
// lifecycle write sees the last published state; deletion admission itself
// stays in the publisher (RecordWorkspaceDeletion needs a clean revocation
// ACK for a live authority line).
func (s *services) workspaceVolumeForRenewal(volume string) error {
	if !s.host.cfg.WorkspaceRPC {
		return status.Error(codes.FailedPrecondition, "workspace RPC disabled")
	}
	if s.host.lifecycle == nil {
		return status.Error(codes.FailedPrecondition, "lifecycle store unavailable")
	}
	r, ok := s.host.lifecycle.Volumes.GetVolumeSnapshot(volume)
	if !ok || r.Spec.ReplicationFactor != 1 {
		return status.Error(codes.FailedPrecondition, "workspace move requires an existing RF1 volume")
	}
	if s.host.lifecycle.Deletions.ReservedSnapshot(volume) {
		return status.Error(codes.FailedPrecondition, "workspace: volume is being deleted")
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
	// The host fences half a skew bound before the stored master expiry.
	// Never grant more than the server's configured 30s TTL after a restart.
	skew := time.Duration(l.MaxClockSkewNS)
	// Older persisted leases predate the bound; use the production default.
	if skew <= 0 {
		skew = 20 * time.Second
	}
	remaining := time.Until(time.Unix(0, l.ExpiresAtNS).Add(-skew / 2))
	if remaining > 30*time.Second {
		remaining = 30 * time.Second
	}
	if remaining < 0 {
		remaining = 0
	}
	return &control.WorkspaceLeaseFact{VolumeId: volume, ReplicaId: l.ReplicaID, Epoch: l.Epoch, ExpiresAtNs: l.ExpiresAtNS, ValidForNs: int64(remaining), Revoking: l.Revoking}
}

func (s *services) RenewWorkspaceLease(ctx context.Context, q *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	if err := s.workspaceVolumeForRenewal(q.GetVolumeId()); err != nil {
		return nil, err
	}
	l, err := s.host.Publisher().RenewWorkspaceLease(ctx, q.GetVolumeId(), q.GetReplicaId(), q.GetEpoch())
	if err != nil {
		return nil, workspaceError(err)
	}
	return leaseFact(q.GetVolumeId(), l), nil
}

func (s *services) RevokeWorkspace(ctx context.Context, q *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	if err := s.host.Publisher().BeginWorkspaceRevocation(ctx, q.GetVolumeId(), q.GetReplicaId(), q.GetEpoch()); err != nil {
		return nil, workspaceError(err)
	}
	// Report what was granted, never the durable horizon.
	lease, ok := s.host.Publisher().WorkspaceLeaseReply(q.GetVolumeId())
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "workspace moved during revocation")
	}
	return leaseFact(q.GetVolumeId(), lease), nil
}

func (s *services) AckWorkspaceRevoked(ctx context.Context, q *control.WorkspaceRevocationAck) (*control.WorkspaceEmpty, error) {
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	id := q.GetIdentity()
	if err := s.workspaceVolume(id.GetVolumeId()); err != nil {
		return nil, err
	}
	if err := s.host.Publisher().AckWorkspaceRevocation(ctx, id.GetVolumeId(), authority.WorkspaceRevokeAck{ReplicaID: id.GetReplicaId(), Epoch: id.GetEpoch(), HeadGen: q.GetHeadGen()}); err != nil {
		return nil, workspaceError(err)
	}
	if err := s.host.reconcileWorkspaceBindings(); err != nil {
		return nil, workspaceError(err)
	}
	return &control.WorkspaceEmpty{}, nil
}

func grantFact(r authority.WorkspaceMoveRecord) *control.WorkspaceGrant {
	return &control.WorkspaceGrant{RequestId: r.RequestID, VolumeId: r.VolumeID, OldReplicaId: r.OldReplicaID, NewReplicaId: r.NewReplicaID, Kind: r.Kind,
		OldEpoch: r.OldEpoch, NewEpoch: r.NewEpoch, HeadGen: r.HeadGen, RemoteLsn: r.RemoteLSN, Issued: r.Issued, OldHost: r.OldHost, NewHost: r.NewHost}
}

func (s *services) MoveWorkspace(ctx context.Context, q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	// A forced move reads HEAD only after its lease admission holds: once
	// the expected holder's grants have expired, it can no longer change
	// HEAD, so the HEAD read below is the one the move and its lost range
	// rest on. The publisher re-checks admission when it issues the move.
	if q.GetKind() == "forced" {
		if err := s.host.Publisher().WorkspaceForcedMoveAdmitted(ctx, q.GetVolumeId(), q.GetRequestId(), q.GetExpectedReplicaId(), q.GetExpectedEpoch()); err != nil {
			return nil, workspaceError(err)
		}
	}
	// Always bound the configured reader, even when the caller omitted a deadline.
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	head, err := s.host.cfg.WorkspaceHeadReader(readCtx, q.GetVolumeId())
	if err != nil {
		return nil, workspaceError(err)
	}
	// Remote reads never hold the workspace mutation lock. Re-read every
	// local precondition below; Publisher checks the expected epoch atomically.
	// A refusal is logged after the lock is released (deferred first, so it
	// runs last): a slow log writer never holds workspaceMu.
	var refusal string
	defer func() {
		if refusal != "" {
			s.host.log.Printf("blockmaster: MoveWorkspace refused volume=%q request=%q: %s", q.GetVolumeId(), q.GetRequestId(), refusal)
		}
	}()
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, workspaceError(err)
	}
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	oldHost := s.host.workspaceSourceHost(q.GetVolumeId(), q.GetExpectedReplicaId(), q.GetExpectedEpoch())
	target, ok := s.host.lifecycle.Nodes.GetNode(q.GetTargetServerId())
	bindings, explicit, err := s.host.lifecycle.WorkspaceBindings.Get(q.GetVolumeId())
	if err != nil {
		return nil, workspaceError(err)
	}
	// A volume without per-volume bindings may move to any port on the
	// target node's registered hosts (a host runs several slots): the host
	// match ties the endpoints to that node's registration (it trusts the
	// registration; it is not an identity proof). The collision check below
	// still refuses endpoints another assignment or binding owns.
	matched := !explicit && sameHost(q.GetTargetDataAddr(), target.DataAddr) && sameHost(q.GetTargetCtrlAddr(), target.CtrlAddr)
	endpointErr := lifecycle.ValidateWorkspaceEndpoints(lifecycle.WorkspaceBinding{VolumeID: q.GetVolumeId(), ServerID: q.GetTargetServerId(), DataAddr: q.GetTargetDataAddr(), CtrlAddr: q.GetTargetCtrlAddr()})
	for _, b := range bindings {
		if !b.Retired && b.ServerID == q.GetTargetServerId() && b.DataAddr == q.GetTargetDataAddr() && b.CtrlAddr == q.GetTargetCtrlAddr() {
			matched = true
		}
	}
	switch {
	case endpointErr != nil:
		refusal = fmt.Sprintf("move target endpoints invalid: %v", endpointErr)
	case oldHost == "":
		refusal = fmt.Sprintf("move source unknown: no issued move or placement names replica %q at epoch %d", q.GetExpectedReplicaId(), q.GetExpectedEpoch())
	case !ok:
		refusal = fmt.Sprintf("move target node %q is not registered", q.GetTargetServerId())
	case target.ServerID == oldHost:
		refusal = fmt.Sprintf("move target %q is the current source host", target.ServerID)
	case !matched:
		refusal = fmt.Sprintf("move target endpoints %s/%s on %q are not registered (%s)", q.GetTargetDataAddr(), q.GetTargetCtrlAddr(), target.ServerID, registeredTargetEndpoints(target, bindings, explicit))
	}
	if refusal != "" {
		return nil, status.Error(codes.FailedPrecondition, refusal)
	}
	if err = s.host.workspaceEndpointsAvailable(lifecycle.WorkspaceBinding{VolumeID: q.GetVolumeId(), ServerID: target.ServerID, DataAddr: q.GetTargetDataAddr(), CtrlAddr: q.GetTargetCtrlAddr()}); err != nil {
		return nil, workspaceError(err)
	}
	r, err := s.host.Publisher().MoveWorkspace(ctx, authority.WorkspaceMoveRequest{RequestID: q.GetRequestId(), VolumeID: q.GetVolumeId(), ExpectedReplicaID: q.GetExpectedReplicaId(), ExpectedHost: oldHost, TargetHost: target.ServerID,
		ExpectedEpoch: q.GetExpectedEpoch(), TargetReplicaID: q.GetTargetReplicaId(), TargetDataAddr: q.GetTargetDataAddr(), TargetCtrlAddr: q.GetTargetCtrlAddr(), Kind: q.GetKind(), Force: q.GetForce()}, head)
	if err != nil {
		return nil, workspaceError(err)
	}
	if err = s.host.reconcileWorkspaceBindings(); err != nil {
		return nil, workspaceError(err)
	}
	return grantFact(r), nil
}

// sameHost reports whether two host:port addresses name the same host:
// canonical host parts (equivalent IP spellings equal), no name resolution.
func sameHost(a, b string) bool {
	ha := lifecycle.WorkspaceEndpointHost(a)
	return ha != "" && ha == lifecycle.WorkspaceEndpointHost(b)
}

// registeredTargetEndpoints describes what a move to target may name: the
// node's own pair for a volume without explicit bindings, else its live
// bindings on that node.
func registeredTargetEndpoints(target lifecycle.NodeRegistration, bindings []lifecycle.WorkspaceBinding, explicit bool) string {
	if !explicit {
		return fmt.Sprintf("not on the node's hosts: it registers %s/%s; no per-volume binding is prepared", target.DataAddr, target.CtrlAddr)
	}
	var live []string
	for _, b := range bindings {
		if !b.Retired && b.ServerID == target.ServerID {
			live = append(live, b.DataAddr+"/"+b.CtrlAddr)
		}
	}
	if len(live) == 0 {
		return "the volume has explicit bindings but none live on that node"
	}
	return "live bindings on that node: " + strings.Join(live, ", ")
}

func (s *services) QueryWorkspace(ctx context.Context, q *control.WorkspaceVolume) (*control.WorkspaceStatus, error) {
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	out := &control.WorkspaceStatus{}
	bindings, _, err := s.host.lifecycle.WorkspaceBindings.Get(q.GetVolumeId())
	if err != nil {
		return nil, workspaceError(err)
	}
	for _, b := range bindings {
		out.Bindings = append(out.Bindings, bindingFact(b))
	}
	state := s.host.Publisher().WorkspaceState(q.GetVolumeId())
	if state == nil {
		return out, nil
	}
	if lease, ok := s.host.Publisher().WorkspaceLeaseReply(q.GetVolumeId()); ok {
		out.Lease = leaseFact(q.GetVolumeId(), lease) // granted expiry, not the durable horizon
	}
	if state.Ack != nil {
		out.RevocationAck = &control.WorkspaceRevocationAck{Identity: &control.WorkspaceIdentity{VolumeId: q.GetVolumeId(), ReplicaId: state.Ack.ReplicaID, Epoch: state.Ack.Epoch}, HeadGen: state.Ack.HeadGen}
	}
	for _, r := range state.Moves {
		out.Moves = append(out.Moves, grantFact(r))
	}
	return out, nil
}
