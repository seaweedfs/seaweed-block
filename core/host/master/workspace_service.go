package master

import (
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/auth"
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
	if !s.host.layoutStoreReady(volume) {
		return status.Error(codes.FailedPrecondition, "workspace: created, store not ready; retry CreateVolume")
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
	if !s.host.layoutStoreReady(volume) {
		return status.Error(codes.FailedPrecondition, "workspace: created, store not ready; retry CreateVolume")
	}
	if s.host.lifecycle.Deletions.ReservedSnapshot(volume) {
		if s.host.lifecycle.Deletions.CommittedSnapshot(volume) {
			return leaseRefusal("workspace: volume is being deleted", control.WorkspaceLeaseRefusal_DELETED, 0)
		}
		return status.Error(codes.FailedPrecondition, "workspace: volume is being deleted")
	}
	return nil
}

// leaseRefusal is a renewal refusal with its typed kind as a status detail
// (code FailedPrecondition and text as before). Only BARRIER, MOVED and
// DELETED are sent; a refusal without a detail is OTHER.
func leaseRefusal(text string, kind control.WorkspaceLeaseRefusal_Kind, retryAfter time.Duration) error {
	st := status.New(codes.FailedPrecondition, text)
	if d, err := st.WithDetails(&control.WorkspaceLeaseRefusal{Kind: kind, RetryAfterNs: int64(retryAfter)}); err == nil {
		st = d
	}
	return st.Err()
}

// renewalMoved reports whether a holder mismatch is a permanent, known move:
// the published line is at a newer epoch than the caller's and names another
// replica, or is owned by another known server. An unknown owner, the same
// owner at a newer epoch, a line at or below the caller's epoch, or no line is
// not (OTHER: a bounded wait, then exit 75): a false MOVED is exit 78, which
// the shipped unit does not restart.
func (s *services) renewalMoved(volume, replica, server string, epoch uint64) bool {
	line, ok := s.host.Publisher().PublishedAuthorityLine(volume)
	if !ok {
		return false
	}
	return lineMovedFrom(line.Epoch, line.ReplicaID, func() string {
		return s.host.workspaceLineOwner(line, volume, line.ReplicaID, line.Epoch)
	}, replica, server, epoch)
}

// lineMovedFrom is renewalMoved's decision on the published line (epoch,
// replica, owner) against the caller (replica, server, epoch).
func lineMovedFrom(lineEpoch uint64, lineReplica string, owner func() string, replica, server string, epoch uint64) bool {
	if server == "" || lineEpoch <= epoch {
		return false
	}
	if lineReplica != replica {
		return true
	}
	o := owner()
	return o != "" && o != server
}

// isWorkspaceVolume reports whether volume is a workspace volume (workspace RPC on, an existing RF1 volume), from the
// lock-free lifecycle snapshot. Unlike workspaceVolumeForRenewal it ignores a deletion reservation: a volume being
// deleted is still a workspace volume, and its assignment stream keeps the holder host check.
func (s *services) isWorkspaceVolume(volume string) bool {
	if !s.host.cfg.WorkspaceRPC || s.host.lifecycle == nil {
		return false
	}
	r, ok := s.host.lifecycle.Volumes.GetVolumeSnapshot(volume)
	return ok && r.Spec.ReplicationFactor == 1
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

func leaseFact(p *authority.Publisher, volume string, l authority.WorkspaceLease) *control.WorkspaceLeaseFact {
	// The client anchors this duration to its request START, not response arrival.
	return &control.WorkspaceLeaseFact{VolumeId: volume, ReplicaId: l.ReplicaID, Epoch: l.Epoch, ExpiresAtNs: l.ExpiresAtNS, ValidForNs: int64(p.WorkspaceLeaseValidity(l)), Revoking: l.Revoking}
}

// holderMatches implements INV-AUTH-HOLDER-IDENTITY-001 for both holder RPCs.
// The committed line and placement snapshot avoid store I/O on renewal.
// A non-nil error is the host refusal: the server id is unregistered, or the
// call's network peer is not on its registered host (refuseWorkspaceHolderHost).
func (s *services) holderMatches(ctx context.Context, volume, replica, server string, epoch uint64) (bool, error) {
	if err := s.host.refuseWorkspaceHolderHost(ctx, "workspace lease", server); err != nil {
		return false, err
	}
	line, ok := s.host.Publisher().PublishedAuthorityLine(volume)
	if !ok || server == "" || line.ReplicaID != replica || line.Epoch != epoch {
		return false, nil
	}
	return server == s.host.workspaceLineOwner(line, volume, replica, epoch), nil
}

func (s *services) RenewWorkspaceLease(ctx context.Context, q *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	// Phase 0: the holder's server id is the caller's certificate's id.
	if err := auth.RequireNode(ctx, q.GetServerId()); err != nil {
		return nil, err
	}
	if err := s.workspaceVolumeForRenewal(q.GetVolumeId()); err != nil {
		return nil, err
	}
	if ok, err := s.holderMatches(ctx, q.GetVolumeId(), q.GetReplicaId(), q.GetServerId(), q.GetEpoch()); err != nil {
		return nil, identityRefusal(err)
	} else if !ok {
		const text = "workspace lease: server identity does not match the current holder"
		if s.renewalMoved(q.GetVolumeId(), q.GetReplicaId(), q.GetServerId(), q.GetEpoch()) {
			return nil, leaseRefusal(text, control.WorkspaceLeaseRefusal_MOVED, 0)
		}
		return nil, status.Error(codes.FailedPrecondition, text)
	}
	l, err := s.host.Publisher().RenewWorkspaceLease(ctx, q.GetVolumeId(), q.GetReplicaId(), q.GetEpoch())
	var barrier *authority.LeaseBarrierError
	if errors.As(err, &barrier) {
		return nil, leaseRefusal(err.Error(), control.WorkspaceLeaseRefusal_BARRIER, barrier.RetryAfter)
	}
	if err != nil {
		return nil, workspaceError(err)
	}
	return leaseFact(s.host.Publisher(), q.GetVolumeId(), l), nil
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
	return leaseFact(s.host.Publisher(), q.GetVolumeId(), lease), nil
}

func (s *services) AckWorkspaceRevoked(ctx context.Context, q *control.WorkspaceRevocationAck) (_ *control.WorkspaceEmpty, err error) {
	if err := auth.RequireNode(ctx, q.GetIdentity().GetServerId()); err != nil {
		return nil, err
	}
	// Each step's time is logged with the answer: a holder bounds its call
	// (volume.WorkspaceAckTimeout), and a miss names the step that ate it.
	arrived := time.Now()
	var locked, headRead, recorded time.Time
	defer func() {
		at := func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}
			return t.Sub(arrived).String()
		}
		s.host.log.Printf("blockmaster: AckWorkspaceRevoked volume=%q replica=%q epoch=%d lock=%s head=%s recorded=%s answered=%s err=%v",
			q.GetIdentity().GetVolumeId(), q.GetIdentity().GetReplicaId(), q.GetIdentity().GetEpoch(), at(locked), at(headRead), at(recorded), time.Since(arrived), err)
	}()
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	locked = time.Now()
	id := q.GetIdentity()
	if err := s.workspaceVolume(id.GetVolumeId()); err != nil {
		return nil, err
	}
	if ok, err := s.holderMatches(ctx, id.GetVolumeId(), id.GetReplicaId(), id.GetServerId(), id.GetEpoch()); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	} else if !ok {
		return nil, status.Error(codes.FailedPrecondition, "workspace ACK: server identity does not match the current holder")
	}
	// The holder ACK proves its local drain. Read the store-owned stop here:
	// detached at this epoch is stable, but operator metadata may advance gen.
	if s.host.cfg.WorkspaceHeadReader == nil {
		return nil, status.Error(codes.Unavailable, "workspace ACK: detached HEAD reader unavailable")
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	head, herr := s.host.cfg.WorkspaceHeadReader(readCtx, id.GetVolumeId())
	cancel()
	headRead = time.Now()
	if herr != nil {
		return nil, status.Errorf(codes.Unavailable, "workspace ACK: read detached HEAD: %v", herr)
	}
	holder := id.GetServerId() + "/" + id.GetReplicaId()
	if err := s.host.Publisher().AckWorkspaceRevocation(ctx, id.GetVolumeId(), authority.WorkspaceRevokeAck{ReplicaID: id.GetReplicaId(), Epoch: id.GetEpoch(), HeadGen: q.GetHeadGen(), Holder: holder}, head); err != nil {
		return nil, workspaceError(err)
	}
	recorded = time.Now()
	if err := s.host.reconcileWorkspaceBindings(); err != nil {
		return nil, workspaceError(err)
	}
	return &control.WorkspaceEmpty{}, nil
}

func grantFact(r authority.WorkspaceMoveRecord) *control.WorkspaceGrant {
	return &control.WorkspaceGrant{RequestId: r.RequestID, VolumeId: r.VolumeID, OldReplicaId: r.OldReplicaID, NewReplicaId: r.NewReplicaID, Kind: r.Kind,
		OldEpoch: r.OldEpoch, NewEpoch: r.NewEpoch, HeadGen: r.HeadGen, RemoteLsn: r.RemoteLSN, Issued: r.Issued, OldHost: r.OldHost, NewHost: r.NewHost}
}

// lostRangeFact is the wire form of a stored lost range.
func lostRangeFact(r authority.WorkspaceLostRangeReadout) *control.WorkspaceLostRange {
	return &control.WorkspaceLostRange{RequestId: r.RequestID, VolumeId: r.VolumeID, NewEpoch: r.NewEpoch, OldReplicaId: r.OldReplicaID, OldEpoch: r.OldEpoch,
		LostAfterLsn: r.LostAfterLSN, LostUntilNs: r.LostUntilNS, HeadGen: r.HeadGen, HeadTimeNs: r.HeadTimeNS, HeadTimeKnown: r.HeadTimeNS != 0}
}

// grantWithLostRange is grantFact plus, for an issued forced grant, its
// stored lost range; a range that cannot be read leaves lost_range unset and
// says why in lost_range_error (the readout is then unproven, not empty).
func (s *services) grantWithLostRange(r authority.WorkspaceMoveRecord) *control.WorkspaceGrant {
	g := grantFact(r)
	if !r.Issued || r.Kind != "forced" {
		return g
	}
	ranges, err := s.host.Publisher().WorkspaceLostRangeReadouts(r.VolumeID, r.RequestID)
	switch {
	case err != nil:
		g.LostRangeError = err.Error()
	case len(ranges) != 1:
		g.LostRangeError = fmt.Sprintf("%v: %d ranges for request %q", authority.ErrWorkspaceLostRangeMissing, len(ranges), r.RequestID)
	default:
		g.LostRange = lostRangeFact(ranges[0])
	}
	return g
}

// logForcedGrant records an issued forced move (also a replay of one).
func (s *services) logForcedGrant(g *control.WorkspaceGrant, replay bool, postIssue error) {
	if g == nil || !g.GetIssued() || g.GetKind() != "forced" {
		return
	}
	lost := "lost_range=UNPROVEN (" + g.GetLostRangeError() + ")"
	if l := g.GetLostRange(); l != nil {
		head := "unknown"
		if l.GetHeadTimeKnown() {
			head = time.Unix(0, l.GetHeadTimeNs()).UTC().Format(time.RFC3339Nano)
		}
		lost = fmt.Sprintf("lost_after_lsn=%d lost_until_ns=%d lost_until=%s head_gen=%d head_filer_time=%s",
			l.GetLostAfterLsn(), l.GetLostUntilNs(), time.Unix(0, l.GetLostUntilNs()).UTC().Format(time.RFC3339Nano), l.GetHeadGen(), head)
	}
	if postIssue != nil {
		lost += fmt.Sprintf(" answer_failed_after_issue=%q", postIssue.Error())
	}
	s.host.log.Printf("blockmaster: MoveWorkspace forced move issued volume=%q request=%q old=%s/%s@%d new=%s/%s@%d replay=%t %s",
		g.GetVolumeId(), g.GetRequestId(), g.GetOldHost(), g.GetOldReplicaId(), g.GetOldEpoch(), g.GetNewHost(), g.GetNewReplicaId(), g.GetNewEpoch(), replay, lost)
}

func (s *services) MoveWorkspace(ctx context.Context, q *control.WorkspaceMove) (_ *control.WorkspaceGrant, resultErr error) {
	// Phase 0: a forced move (it may lose acknowledged writes) needs an
	// admin; a clean move an operator (the method's rule).
	if q.GetKind() == "forced" || q.GetForce() {
		if err := auth.RequireRole(ctx, auth.RoleAdmin); err != nil {
			return nil, err
		}
	}
	// Remote reads never hold the workspace mutation lock. Re-read every
	// local precondition below; Publisher checks the expected epoch atomically.
	// A refusal is logged after the lock is released (deferred first, so it
	// runs last): a slow log writer never holds workspaceMu.
	// An issued forced grant is logged the same way, also after the unlock.
	var refusal string
	var issued *control.WorkspaceGrant
	var replay bool
	var postIssue, unknown error
	defer func() {
		// M2 Uncertain reply precedence applies even to preflight/HEAD errors.
		outcome := s.host.Publisher().WorkspaceMoveOutcomeError(ctx, q.GetVolumeId(), resultErr)
		if callerErr := ctx.Err(); resultErr != nil && callerErr != nil {
			resultErr = workspaceError(callerErr)
		} else if errors.Is(outcome, authority.ErrWorkspaceOutcomeUncertain) {
			unknown, refusal = outcome, ""
			resultErr = status.Errorf(codes.Internal, "workspace move %q of volume %q has an UNKNOWN outcome: %v; the volume's workspace authority stays uncertain until the master restarts; after the restart, rerun with the same request id", q.GetRequestId(), q.GetVolumeId(), outcome)
		}
		if refusal != "" {
			s.host.log.Printf("blockmaster: MoveWorkspace refused volume=%q request=%q: %s", q.GetVolumeId(), q.GetRequestId(), refusal)
		}
		s.logForcedGrant(issued, replay, postIssue)
		if unknown != nil {
			s.host.log.Printf("blockmaster: MoveWorkspace outcome unknown volume=%q request=%q kind=%q: %v", q.GetVolumeId(), q.GetRequestId(), q.GetKind(), unknown)
		}
	}()

	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	// A forced move reads HEAD only after its lease admission holds: the
	// expected holder's lease has then expired, so it starts no new claim or
	// cut before this read. A claim it already sent may still land after it
	// (WorkspaceForcedMoveAdmitted). The publisher re-checks admission when
	// it issues the move.
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
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, workspaceError(err)
	}
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	req, target, replay, why, err := s.workspaceMoveCheckLocked(q)
	if err != nil {
		return nil, workspaceError(err)
	}
	if refusal = why; refusal != "" {
		return nil, status.Error(codes.FailedPrecondition, refusal)
	}
	if err = s.host.workspaceEndpointsAvailable(lifecycle.WorkspaceBinding{VolumeID: q.GetVolumeId(), ServerID: target.ServerID, DataAddr: q.GetTargetDataAddr(), CtrlAddr: q.GetTargetCtrlAddr()}); err != nil {
		return nil, workspaceError(err)
	}
	r, err := s.host.Publisher().MoveWorkspace(ctx, req, head)
	if err != nil {
		// A move that is issued in spite of the error (it failed after the
		// issue) is never a refusal: the caller recovers its grant with the
		// same request id.
		if s.host.Publisher().IssuedWorkspaceMove(req) {
			issued, postIssue = s.issuedRecord(req), err
			return nil, postIssueError(q, err)
		}
		return nil, workspaceError(err)
	}
	// The move is issued from here on: record it for the audit log before
	// anything else can fail.
	issued = s.grantWithLostRange(r)
	if err = s.host.reconcileWorkspaceBindings(); err != nil {
		postIssue = err
		return nil, postIssueError(q, err)
	}
	return issued, nil
}

// postIssueError reports a failure after q's move was issued: never
// FailedPrecondition (a definite refusal); Internal, which clients treat as
// an unknown outcome, with the same-request-id recovery.
func postIssueError(q *control.WorkspaceMove, err error) error {
	return status.Errorf(codes.Internal, "workspace move %q of volume %q is ISSUED, but a step after the issue failed: %v; rerun with the same request id to get its grant",
		q.GetRequestId(), q.GetVolumeId(), err)
}

// issuedRecord is the answer for req's issued journal record, if any.
func (s *services) issuedRecord(req authority.WorkspaceMoveRequest) *control.WorkspaceGrant {
	if st := s.host.Publisher().WorkspaceState(req.VolumeID); st != nil {
		for _, m := range st.Moves {
			if m.RequestID == req.RequestID && m.Issued {
				return s.grantWithLostRange(m)
			}
		}
	}
	return nil
}

// workspaceMoveCheckLocked builds the authority request of q and the
// blockmaster's own refusal of it ("" when none): endpoints, source host,
// target node and binding. It reads and writes nothing else; MoveWorkspace
// and PreviewWorkspaceMove share it. Caller holds workspaceMu.
func (s *services) workspaceMoveCheckLocked(q *control.WorkspaceMove) (req authority.WorkspaceMoveRequest, target lifecycle.NodeRegistration, replay bool, refusal string, err error) {
	oldHost := s.host.workspaceSourceHost(q.GetVolumeId(), q.GetExpectedReplicaId(), q.GetExpectedEpoch())
	target, ok := s.host.lifecycle.Nodes.GetNode(q.GetTargetServerId())
	bindings, explicit, err := s.host.lifecycle.WorkspaceBindings.Get(q.GetVolumeId())
	if err != nil {
		return authority.WorkspaceMoveRequest{}, lifecycle.NodeRegistration{}, false, "", err
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
	req = authority.WorkspaceMoveRequest{RequestID: q.GetRequestId(), VolumeID: q.GetVolumeId(), ExpectedReplicaID: q.GetExpectedReplicaId(), ExpectedHost: oldHost, TargetHost: target.ServerID,
		ExpectedEpoch: q.GetExpectedEpoch(), TargetReplicaID: q.GetTargetReplicaId(), TargetDataAddr: q.GetTargetDataAddr(), TargetCtrlAddr: q.GetTargetCtrlAddr(), Kind: q.GetKind(), Force: q.GetForce()}
	// Strict journal isolation refuses new work only: an exact replay of an
	// issued move (its reply lost, the target found co-located since) gets
	// its durable grant back from MoveWorkspace, never a definite refusal.
	replay = ok && s.host.Publisher().IssuedWorkspaceMove(req)
	switch {
	case endpointErr != nil:
		refusal = fmt.Sprintf("move target endpoints invalid: %v", endpointErr)
	case oldHost == "":
		refusal = fmt.Sprintf("move source unknown: no issued move or placement names replica %q at epoch %d", q.GetExpectedReplicaId(), q.GetExpectedEpoch())
	case !ok:
		refusal = fmt.Sprintf("move target node %q is not registered", q.GetTargetServerId())
	// The authority still checks the clean seal/ACK or forced lease barrier
	// after these endpoint and binding checks, also for a same-host target.
	case !replay && s.host.colocatedTargetRefused(target.ServerID):
		refusal = fmt.Sprintf("move target node %q shares the authority store's file system (strict journal isolation)", target.ServerID)
	case !matched:
		refusal = fmt.Sprintf("move target endpoints %s/%s on %q are not registered (%s)", q.GetTargetDataAddr(), q.GetTargetCtrlAddr(), target.ServerID, registeredTargetEndpoints(target, bindings, explicit))
	}
	return req, target, replay, refusal, nil
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
		out.Lease = leaseFact(s.host.Publisher(), q.GetVolumeId(), lease) // granted expiry, not the durable horizon
	}
	if state.Ack != nil {
		out.RevocationAck = &control.WorkspaceRevocationAck{Identity: &control.WorkspaceIdentity{VolumeId: q.GetVolumeId(), ReplicaId: state.Ack.ReplicaID, Epoch: state.Ack.Epoch}, HeadGen: state.Ack.HeadGen}
	}
	for _, r := range state.Moves {
		out.Moves = append(out.Moves, s.grantWithLostRange(r))
	}
	return out, nil
}

// QueryWorkspaceLostRange returns the stored lost ranges of the volume's
// issued forced moves in issue order, or the one of request_id. A forced
// move whose range is missing or failed its checks fails the call with
// DataLoss: the readout is unproven, never "no loss".
func (s *services) QueryWorkspaceLostRange(ctx context.Context, q *control.WorkspaceLostRangeQuery) (*control.WorkspaceLostRanges, error) {
	if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
		return nil, err
	}
	ranges, err := s.host.Publisher().WorkspaceLostRangeReadouts(q.GetVolumeId(), q.GetRequestId())
	switch {
	case errors.Is(err, authority.ErrWorkspaceLostRangeMissing):
		return nil, status.Error(codes.DataLoss, err.Error())
	case errors.Is(err, authority.ErrWorkspaceNoForcedMove):
		return nil, status.Error(codes.NotFound, err.Error())
	case err != nil:
		return nil, workspaceError(err)
	}
	out := &control.WorkspaceLostRanges{}
	for _, r := range ranges {
		out.Ranges = append(out.Ranges, lostRangeFact(r))
	}
	return out, nil
}

// PreviewWorkspaceMove answers what MoveWorkspace would do with a forced
// move now, writing nothing: the blockmaster's own checks, the publisher's
// forced admission (with the barrier, i.e. when it becomes admissible) and,
// only once admitted, the HEAD read and HEAD rule, as the move does. A
// refusal is an answer (admitted false, reason), not an RPC error.
func (s *services) PreviewWorkspaceMove(ctx context.Context, q *control.WorkspaceMove) (*control.WorkspaceMovePreview, error) {
	out := &control.WorkspaceMovePreview{}
	if !s.host.cfg.WorkspaceRPC {
		return nil, status.Error(codes.FailedPrecondition, "workspace RPC disabled")
	}
	if q.GetKind() != "forced" || !q.GetForce() {
		out.Reason = "workspace move preview: forced moves only (kind forced, force true)"
		out.MasterTimeNs = time.Now().UnixNano()
		return out, nil
	}
	var req authority.WorkspaceMoveRequest
	var replay bool
	if err := func() error {
		s.host.workspaceMu.Lock()
		defer s.host.workspaceMu.Unlock()
		if err := s.workspaceVolume(q.GetVolumeId()); err != nil {
			out.Reason = status.Convert(err).Message()
			return nil
		}
		r, target, _, why, err := s.workspaceMoveCheckLocked(q)
		if err != nil {
			return err
		}
		req = r
		pv := s.host.Publisher().PreviewWorkspaceForcedMove(req)
		out.Admitted, out.LostUntilNs, out.MasterTimeNs = pv.Admitted, pv.LostUntilNS, pv.NowNS
		switch {
		case why != "":
			out.Admitted, out.Reason = false, why
		case !pv.Admitted:
			out.Reason = pv.Reason
		case pv.Replay:
			replay, out.Reason = true, pv.Reason
		default:
			if err := s.host.workspaceEndpointsAvailable(lifecycle.WorkspaceBinding{VolumeID: q.GetVolumeId(), ServerID: target.ServerID, DataAddr: q.GetTargetDataAddr(), CtrlAddr: q.GetTargetCtrlAddr()}); err != nil {
				out.Admitted, out.Reason = false, err.Error()
			}
		}
		return nil
	}(); err != nil {
		return nil, workspaceError(err)
	}
	if out.MasterTimeNs == 0 {
		out.MasterTimeNs = time.Now().UnixNano()
	}
	if !out.Admitted || replay { // a replay never reads a new HEAD
		return out, nil
	}
	// As for the move: HEAD is read only after admission, outside the lock,
	// bounded even when the caller set no deadline.
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	head, err := s.host.cfg.WorkspaceHeadReader(readCtx, q.GetVolumeId())
	if err != nil {
		out.Admitted, out.HeadError = false, err.Error()
		out.Reason = "workspace move: HEAD read failed: " + err.Error()
		return out, nil
	}
	out.HeadRead = true
	out.HeadGen, out.HeadEpoch, out.HeadHolder, out.HeadRemoteLsn = head.Gen, head.Epoch, head.Holder, head.RemoteLSN
	out.HeadTimeNs, out.HeadTimeKnown = head.FilerMtimeNS, head.FilerMtimeNS != 0
	if !authority.WorkspaceMoveHeadAdmits(req, head) {
		out.Admitted, out.Reason = false, "workspace move: HEAD not owned by expected holder"
	}
	return out, nil
}
