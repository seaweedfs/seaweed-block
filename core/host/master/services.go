package master

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/ops"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storeident"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// adapter import is still needed: the AssignmentService fan-in
// uses adapter.AssignmentInfo as the delivery channel element
// type from publisher.Subscribe. No composite literal of
// adapter.AssignmentInfo is constructed on the master side —
// values flow through unchanged from publisher state.

// services is the master-side implementation of the control-plane
// gRPC services defined in core/rpc/proto/v3_control.proto.
//
// Boundary: every handler here converts wire messages to
// authority-layer types via HeartbeatToObservation or LastPublished
// reads; no handler constructs an AssignmentAsk or mints epochs.
type services struct {
	control.UnimplementedObservationServiceServer
	control.UnimplementedAssignmentServiceServer
	control.UnimplementedEvidenceServiceServer
	control.UnimplementedClusterEvidenceServiceServer
	control.UnimplementedLifecycleServiceServer
	control.UnimplementedFailbackServiceServer
	control.UnimplementedWorkspaceServiceServer

	host *Host
	// seq is incremented per ReportHeartbeat handler invocation.
	// gRPC runs each unary RPC on its own goroutine, so seq MUST
	// be atomic. Diagnostic only — the client uses the sequence
	// to correlate log lines, not to drive authority.
	seq atomic.Uint64
}

func newServices(h *Host) *services {
	return &services{host: h}
}

func (s *services) CreateVolume(ctx context.Context, req *control.CreateVolumeRequest) (*control.CreateVolumeResponse, error) {
	stores := s.host.Lifecycle()
	if stores == nil {
		return nil, status.Error(codes.FailedPrecondition, "lifecycle store is not configured")
	}
	spec := lifecycleSpecFromWire(req)
	// A reserved id (a chunk volume delete admitted or committed) is never
	// created again; the reservation is checked under the lock its writer
	// holds.
	// held through the create (workspace_protocol.md: reservation checks
	// and commits stay under workspaceMu).
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	if stores.Deletions.Reserved(spec.VolumeID) {
		return nil, status.Errorf(codes.FailedPrecondition, "create volume: %s is being deleted or was deleted; its id is never reused", spec.VolumeID)
	}
	// One replicated factor per cluster: the controller validates every
	// replicated volume it decides against the master's one slot count and
	// refuses the whole decision when one differs, so a volume of another
	// replication factor would stop every replicated volume's assignment and
	// failover. Single-replica volumes never reach that check.
	// A replicated lifecycle volume is bound, minted and promoted only by the
	// lifecycle product loop: with it off the volume never gets a primary.
	if spec.ReplicationFactor > 1 && !s.host.cfg.LifecycleProductLoop {
		return nil, status.Errorf(codes.FailedPrecondition, "create volume: %s has replication factor %d but --lifecycle-product-loop-interval is 0 (off): a replicated volume is bound and promoted only by that loop; set --lifecycle-product-loop-interval (for example 100ms)", spec.VolumeID, spec.ReplicationFactor)
	}
	if c := s.host.ctrl; c != nil && spec.ReplicationFactor > 1 && spec.ReplicationFactor != c.ExpectedSlotsPerVolume() {
		return nil, status.Errorf(codes.FailedPrecondition, "create volume: %s has replication factor %d, but this master decides replicated volumes of %d replicas (--expected-slots-per-volume); one replicated factor per cluster", spec.VolumeID, spec.ReplicationFactor, c.ExpectedSlotsPerVolume())
	}
	// A retry may carry a different restore tuple. The target's durable
	// binding decides that conflict before looking up the caller's new source
	// snapshot, which might itself be absent.
	if existing, ok := stores.Volumes.GetVolume(spec.VolumeID); ok &&
		(existing.Spec.SourceVolume != spec.SourceVolume || existing.Spec.SourceSnapshot != spec.SourceSnapshot || existing.Spec.CreateRequestID != spec.CreateRequestID) {
		return nil, status.Errorf(codes.AlreadyExists, "create volume: %s is bound to another restore request", spec.VolumeID)
	}
	if spec.SourceVolume != "" || spec.SourceSnapshot != "" || spec.CreateRequestID != "" {
		if spec.SourceVolume == "" || spec.SourceSnapshot == "" || spec.CreateRequestID == "" || spec.SourceVolume == spec.VolumeID {
			return nil, status.Error(codes.InvalidArgument, "restore needs a distinct source volume, snapshot and request id")
		}
		source, ok := stores.Volumes.GetVolume(spec.SourceVolume)
		if !ok || !source.StoreReady {
			return nil, status.Errorf(codes.FailedPrecondition, "restore source %q is not store-ready", spec.SourceVolume)
		}
		if source.Spec.ReplicationFactor != 1 || spec.ReplicationFactor > 1 {
			return nil, status.Error(codes.FailedPrecondition, "restore of an RF>1 volume is not supported")
		}
		if s.host.cfg.LayoutReadVolume == nil || s.host.cfg.LayoutReadHead == nil || s.host.cfg.LayoutCreateTarget == nil {
			return nil, status.Error(codes.FailedPrecondition, "master has no restore store")
		}
		v, err := s.host.cfg.LayoutReadVolume(ctx, source.Spec)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read restore source VOLUME: %v", err)
		}
		id, err := layout.ParseVolumeID(source.Spec.LayoutID)
		if err != nil || v.ID != id || v.Name != source.Spec.VolumeID || v.CheckIdentity() != nil || v.Geometry.Check() != nil {
			return nil, status.Error(codes.FailedPrecondition, "restore source VOLUME identity or geometry is invalid")
		}
		head, err := s.host.cfg.LayoutReadHead(ctx, source.Spec)
		if err != nil {
			if errors.Is(err, layout.ErrHeadAbsent) || errors.Is(err, layout.ErrVolumeDeleting) {
				return nil, status.Errorf(codes.FailedPrecondition, "restore source HEAD is unavailable: %v", err)
			}
			return nil, status.Errorf(codes.Unavailable, "read restore source HEAD: %v", err)
		}
		if head.VolumeID != id || head.State != layout.StateLive && head.State != layout.StateDetached {
			return nil, status.Error(codes.FailedPrecondition, "restore source HEAD is absent, foreign or not live/detached")
		}
		if _, ok := head.Snapshots[spec.SourceSnapshot]; !ok {
			return nil, status.Errorf(codes.NotFound, "restore source snapshot %q is absent", spec.SourceSnapshot)
		}
		spec.SourceLayoutID = source.Spec.LayoutID
		if (spec.SizeBytes != 0 && spec.SizeBytes != v.Geometry.SizeBytes) || (spec.BlockSize != 0 && spec.BlockSize != v.Geometry.BlockSize) || (spec.ChunkBytes != 0 && spec.ChunkBytes != v.Geometry.ChunkBytes) {
			return nil, status.Error(codes.InvalidArgument, "restore target geometry must match source VOLUME")
		}
		spec.SizeBytes, spec.BlockSize, spec.ChunkBytes = v.Geometry.SizeBytes, v.Geometry.BlockSize, v.Geometry.ChunkBytes
		if spec.ReplicationFactor == 0 {
			spec.ReplicationFactor = source.Spec.ReplicationFactor
		}
		if spec.Protocol == "" {
			spec.Protocol = source.Spec.Protocol
		}
		if spec.FrontendTransport == "" {
			spec.FrontendTransport = source.Spec.FrontendTransport
		}
	}
	if createVolumeHook != nil {
		createVolumeHook(spec.VolumeID)
	}
	s.host.log.Printf("blockmaster: CreateVolume volume=%q protocol=%q replication_factor=%d pvc=%q namespace=%q", spec.VolumeID, spec.Protocol, spec.ReplicationFactor, spec.PVCName, spec.PVCNamespace)
	rec, err := s.host.createLifecycleVolume(ctx, spec)
	if err != nil {
		if errors.Is(err, layout.ErrNoSnapshot) {
			return nil, status.Errorf(codes.NotFound, "restore snapshot: %v", err)
		}
		if errors.Is(err, layout.ErrForeignSource) || errors.Is(err, layout.ErrVolumeDeleting) || errors.Is(err, layout.ErrExists) {
			return nil, status.Errorf(codes.FailedPrecondition, "restore target: %v", err)
		}
		if errors.Is(err, errLayoutStoreNotReady) {
			return nil, status.Errorf(codes.Unavailable, "create volume: created, store not ready; retry: %v", err)
		}
		return nil, lifecycleError("create volume", err)
	}
	return lifecycleVolumeResponse(rec)
}

var errLayoutStoreNotReady = errors.New("layout store not ready")

// createLifecycleVolume is called under workspaceMu. A failed filer create
// leaves the durable record not-ready; replay uses its same identity and
// completes VOLUME/HEAD before publishing the one-way placement hint.
func (h *Host) createLifecycleVolume(ctx context.Context, spec lifecycle.VolumeSpec) (lifecycle.VolumeRecord, error) {
	rec, err := h.lifecycle.Volumes.CreateVolume(spec)
	if err != nil {
		return lifecycle.VolumeRecord{}, err
	}
	if h.cfg.LayoutCreateVolume == nil && rec.Spec.SourceVolume == "" {
		if h.cfg.WorkspaceRPC {
			return lifecycle.VolumeRecord{}, fmt.Errorf("%w: master has no filer writer", errLayoutStoreNotReady)
		}
		return rec, nil
	}
	if rec.StoreReady {
		// A missing HEAD after the volume was made ready is damage, not a
		// half-finished create. Start checks the filer and refuses it; a
		// CreateVolume replay must never seed an empty HEAD over its lineage.
		return rec, nil
	}
	var createErr error
	if rec.Spec.SourceVolume != "" {
		source, ok := h.lifecycle.Volumes.GetVolume(rec.Spec.SourceVolume)
		if !ok || source.Spec.LayoutID != rec.Spec.SourceLayoutID || h.cfg.LayoutCreateTarget == nil {
			return lifecycle.VolumeRecord{}, fmt.Errorf("%w: restore source or target creator is unavailable", errLayoutStoreNotReady)
		}
		createErr = h.cfg.LayoutCreateTarget(ctx, source.Spec, rec.Spec, rec.Spec.SourceSnapshot)
	} else {
		createErr = h.cfg.LayoutCreateVolume(ctx, rec.Spec)
	}
	if createErr != nil {
		return lifecycle.VolumeRecord{}, fmt.Errorf("%w: %w", errLayoutStoreNotReady, createErr)
	}
	return h.lifecycle.Volumes.MarkStoreReady(rec.Spec.VolumeID, rec.Spec.LayoutID)
}

func lifecycleVolumeResponse(rec lifecycle.VolumeRecord) (*control.CreateVolumeResponse, error) {
	if rec.Spec.LayoutID == "" {
		return nil, status.Error(codes.FailedPrecondition, "volume has no layout identity; recreate the legacy volume")
	}
	return &control.CreateVolumeResponse{
		VolumeId:          rec.Spec.VolumeID,
		LayoutId:          rec.Spec.LayoutID,
		SizeBytes:         rec.Spec.SizeBytes,
		BlockSize:         rec.Spec.BlockSize,
		ChunkBytes:        rec.Spec.ChunkBytes,
		StoreReady:        rec.StoreReady,
		ReplicationFactor: int32(rec.Spec.ReplicationFactor),
		Protocol:          rec.Spec.Protocol,
		FrontendTransport: rec.Spec.FrontendTransport,
		PvcName:           rec.Spec.PVCName,
		PvcNamespace:      rec.Spec.PVCNamespace,
		PvcUid:            rec.Spec.PVCUID,
		PvName:            rec.Spec.PVName,
	}, nil
}

// GetVolume resolves a name using durable lifecycle intent only. It never
// creates a record or infers an identity from the filer or an assignment.
func (s *services) GetVolume(ctx context.Context, req *control.GetLifecycleVolumeRequest) (*control.CreateVolumeResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	stores := s.host.Lifecycle()
	if stores == nil {
		return nil, status.Error(codes.FailedPrecondition, "lifecycle store is not configured")
	}
	rec, ok := stores.Volumes.GetVolume(req.GetVolumeId())
	if !ok {
		return nil, status.Error(codes.NotFound, "volume not found")
	}
	return lifecycleVolumeResponse(rec)
}

func (s *services) DeleteVolume(ctx context.Context, req *control.DeleteVolumeRequest) (*control.DeleteVolumeResponse, error) {
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, workspaceError(err)
	}
	stores := s.host.Lifecycle()
	if stores == nil {
		return nil, status.Error(codes.FailedPrecondition, "lifecycle store is not configured")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	_, bound, err := stores.WorkspaceBindings.Get(req.GetVolumeId())
	if err != nil {
		return nil, lifecycleError("delete workspace bindings", err)
	}
	_, placed := stores.Placements.GetPlacement(req.GetVolumeId())
	// The lifecycle delete does not read HEAD: a line without an ACK is
	// refused here even when it never started (INV-WS-DELETE-NO-LIVE-HOLDER-001
	// admits that only through the chunk volume delete and its HEAD fence).
	err = s.host.Publisher().RecordWorkspaceDeletion(ctx, req.GetVolumeId(), bound && placed, nil, "", func() error {
		if e := stores.Placements.DeletePlacement(req.GetVolumeId()); e != nil {
			return e
		}
		if e := stores.Volumes.DeleteVolume(req.GetVolumeId()); e != nil {
			return e
		}
		return stores.WorkspaceBindings.DeleteVolume(req.GetVolumeId())
	})
	if err != nil {
		return nil, workspaceError(err)
	}
	return &control.DeleteVolumeResponse{}, nil
}

// MarkVolumeDeleting admits a chunk volume delete (sw-block ops chunk
// volume-delete). It holds workspaceMu, the lock every assignment,
// placement and binding write takes, and runs the mark inside
// RecordWorkspaceDeletion's check under the publisher lock: a volume with a
// live authority line (and no clean revocation ACK), a pending move or a
// queued placement is refused; otherwise the record is marked deleting, and
// from then on publication admission, the placement reconciler, the
// workload planner, binding/placement/move RPCs and CreateVolume all refuse
// it.
func (s *services) MarkVolumeDeleting(ctx context.Context, req *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	// HEAD and the line's owner, read before any lock (as MoveWorkspace
	// does), bounded: a HEAD below the line's epoch proves the line never
	// started (TERM-LIVE-HOLDER-001). A read that fails leaves head nil,
	// which refuses a line without an ACK (fail closed).
	head, owner := s.deleteHolderEvidence(ctx, req.GetVolumeId())
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, workspaceError(err)
	}
	stores := s.host.Lifecycle()
	if stores == nil {
		return nil, status.Error(codes.FailedPrecondition, "lifecycle store is not configured")
	}
	vol := req.GetVolumeId()
	if vol == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	_, bound, err := stores.WorkspaceBindings.Get(vol)
	if err != nil {
		return nil, lifecycleError("mark volume deleting", err)
	}
	_, placed := stores.Placements.GetPlacement(vol)
	var res lifecycle.DeletionReservation
	var persistErr error
	// A rerun of a delete whose token already holds the reservation (or a
	// committed one) was decided by its first admission: it is not decided
	// again. It goes on to the catalog step, whose HEAD compare-and-swap is
	// the fence: a HEAD a holder claimed in between is refused there and the
	// reservation given back, so neither the volume nor its id is stranded.
	if r, held := stores.Deletions.Get(vol); held && (r.State == lifecycle.ReservationCommitted || slices.Contains(r.Tokens, req.GetToken())) {
		res, persistErr = stores.Deletions.Admit(vol, req.GetToken())
		if persistErr != nil {
			return nil, status.Errorf(codes.Unavailable, "mark volume deleting: %v", persistErr)
		}
		_, known := stores.Volumes.GetVolume(vol)
		s.host.log.Printf("blockmaster: MarkVolumeDeleting volume=%q known=%t state=%s (rerun of a held delete)", vol, known, res.State)
		return &control.MarkVolumeDeletingResponse{Known: known, State: res.State}, nil
	}
	err = s.host.Publisher().RecordWorkspaceDeletion(ctx, vol, bound && placed, head, owner, func() error {
		res, persistErr = stores.Deletions.Admit(vol, req.GetToken())
		return persistErr
	})
	switch {
	case persistErr != nil:
		// The admission check passed; the reservation write failed or its
		// outcome is unknown (the store keeps it in memory, fail closed).
		return nil, status.Errorf(codes.Unavailable, "mark volume deleting: %v", persistErr)
	case err != nil:
		return nil, status.Errorf(codes.FailedPrecondition, "mark volume deleting: %v", err)
	}
	_, known := stores.Volumes.GetVolume(vol)
	s.host.log.Printf("blockmaster: MarkVolumeDeleting volume=%q known=%t state=%s", vol, known, res.State)
	return &control.MarkVolumeDeletingResponse{Known: known, State: res.State}, nil
}

// ImportLifecycleVolume registers a desired volume from the cluster spec
// (blockmaster startup), under the same boundary as the CreateVolume RPC:
// a reserved id (a chunk volume delete admitted or committed) is skipped,
// never created again. imported is false for a skipped id.
func (h *Host) ImportLifecycleVolume(spec lifecycle.VolumeSpec) (imported bool, err error) {
	stores := h.Lifecycle()
	if stores == nil {
		return false, fmt.Errorf("cluster volume import requires --lifecycle-store")
	}
	h.workspaceMu.Lock()
	defer h.workspaceMu.Unlock()
	if stores.Deletions.Reserved(spec.VolumeID) {
		return false, nil
	}
	if _, err := h.createLifecycleVolume(context.Background(), spec); err != nil {
		return false, err
	}
	return true, nil
}

// createVolumeHook is a test seam: it runs in CreateVolume after the
// reservation check, before the create.
var createVolumeHook func(vol string)

// AbortVolumeDeleting gives a delete's admission back (the delete found it
// could not proceed and undid its own effects). A committed deletion is
// never given back (FAILED_PRECONDITION).
func (s *services) AbortVolumeDeleting(ctx context.Context, req *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	return s.settleVolumeDeleting(ctx, req, "abort", (*lifecycle.DeletionReservationStore).Abort)
}

// CommitVolumeDeleting makes a delete's reservation permanent: the delete
// is about to take irreversible steps.
func (s *services) CommitVolumeDeleting(ctx context.Context, req *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	return s.settleVolumeDeleting(ctx, req, "commit", (*lifecycle.DeletionReservationStore).Commit)
}

func (s *services) settleVolumeDeleting(ctx context.Context, req *control.MarkVolumeDeletingRequest, op string,
	apply func(*lifecycle.DeletionReservationStore, string, string) (lifecycle.DeletionReservation, error)) (*control.MarkVolumeDeletingResponse, error) {
	s.host.workspaceMu.Lock()
	defer s.host.workspaceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, workspaceError(err)
	}
	stores := s.host.Lifecycle()
	if stores == nil {
		return nil, status.Error(codes.FailedPrecondition, "lifecycle store is not configured")
	}
	if req.GetVolumeId() == "" || req.GetToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and token are required")
	}
	res, err := apply(stores.Deletions, req.GetVolumeId(), req.GetToken())
	switch {
	case errors.Is(err, lifecycle.ErrReservationCommitted), errors.Is(err, lifecycle.ErrReservationToken):
		return nil, status.Errorf(codes.FailedPrecondition, "%s volume deleting: %v", op, err)
	case err != nil:
		return nil, status.Errorf(codes.Unavailable, "%s volume deleting: %v", op, err)
	}
	s.host.log.Printf("blockmaster: %s VolumeDeleting volume=%q state=%s", op, req.GetVolumeId(), res.State)
	state := res.State
	if state == lifecycle.ReservationAdmitted && len(res.Tokens) == 0 {
		state = ""
	}
	return &control.MarkVolumeDeletingResponse{State: state}, nil
}

// liveVolumes is the desired volumes the placement reconciler may plan:
// every record whose id no delete has reserved.
func (st *LifecycleStores) liveVolumes() []lifecycle.VolumeRecord {
	all := st.Volumes.ListVolumes()
	out := all[:0:0]
	for _, v := range all {
		if !st.Deletions.Reserved(v.Spec.VolumeID) {
			out = append(out, v)
		}
	}
	return out
}

func (s *services) ExecuteFailback(ctx context.Context, req *control.FailbackRequest) (*control.FailbackResponse, error) {
	if !s.host.cfg.FailbackRuntimeRPC {
		return nil, status.Error(codes.FailedPrecondition, "failback runtime RPC is disabled")
	}
	result, err := s.host.FailbackAuthorityRuntime().ExecuteFailback(ctx, authority.FailbackRuntimeRequest{
		VolumeID:                     req.GetVolumeId(),
		ReplicaID:                    req.GetReplicaId(),
		TargetDataAddr:               req.GetTargetDataAddr(),
		TargetCtrlAddr:               req.GetTargetCtrlAddr(),
		ExpectedCurrentReplicaID:     req.GetExpectedCurrentReplicaId(),
		ExpectedCurrentEpoch:         req.GetExpectedCurrentEpoch(),
		AckEligible:                  req.GetAckEligible(),
		FrontendFencedBeforeFailback: req.GetFrontendFencedBeforeFailback(),
		DurableFrontierCovered:       req.GetDurableFrontierCovered(),
		NoCrossVolumeIdentityChange:  req.GetNoCrossVolumeIdentityChange(),
		EvidenceRefs:                 append([]string(nil), req.GetEvidenceRefs()...),
	})
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "execute failback: %v", err)
	}
	return &control.FailbackResponse{
		FailbackStarted:                   result.FailbackStarted,
		AuthorityEpochAdvanced:            result.AuthorityEpochAdvanced,
		SinglePrimaryAfterFailback:        result.SinglePrimaryAfterFailback,
		PublishTargetSwappedAfterFailback: result.PublishTargetSwappedAfterFailback,
		NoStorageMutation:                 result.NoStorageMutation,
		NoCrossVolumeIdentityChange:       result.NoCrossVolumeIdentityChange,
		EvidenceRefs:                      append([]string(nil), result.EvidenceRefs...),
	}, nil
}

// ReportHeartbeat receives a volume's observation. It validates
// field presence (rejects malformed reports), translates to
// authority.HeartbeatMessage, and calls the observation host's
// Ingest path.
func (s *services) ReportHeartbeat(ctx context.Context, r *control.HeartbeatReport) (*control.HeartbeatAck, error) {
	// Phase 0: a node reports only for its own server id (TERM-CALLER-001),
	// decided before the report is validated: a caller acting for another
	// server is refused whatever it sends.
	if err := auth.RequireNode(ctx, r.GetServerId()); err != nil {
		return nil, err
	}
	if err := validateHeartbeat(r); err != nil {
		return nil, err
	}
	// Journal isolation: alarm on a durable root sharing the authority
	// store's file system; under strict isolation refuse it unless the node
	// already serves volumes (refusing a serving node stops its renewals).
	if err := s.host.refuseColocatedHeartbeat(r.ServerId, storeident.Identity{MachineID: r.GetMachineId(), Dev: r.GetDurableRootDev(), FSID: r.GetDurableRootFsid()}); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	// INV-AUTH-HOLDER-IDENTITY-001: one server id names one host.
	if err := s.host.refuseServerIDFromOtherHost(r); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	msg := heartbeatFromWire(r)
	if err := s.host.ingestHeartbeatRecordingHealth(msg); err != nil {
		return nil, fmt.Errorf("ingest: %w", err)
	}
	// A primary's attestation of its write-ACK stores. Only the holder of
	// the volume's current line is taken; a failed write fails the report,
	// so the primary sends it again with its next heartbeat.
	if err := s.host.ingestStoreAttestation(r.GetServerId(), r.GetStoreAttestation()); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &control.HeartbeatAck{
		AcceptedAt: timestamppb.Now(),
		Sequence:   s.seq.Add(1),
	}, nil
}

func lifecycleSpecFromWire(req *control.CreateVolumeRequest) lifecycle.VolumeSpec {
	if req == nil {
		return lifecycle.VolumeSpec{}
	}
	return lifecycle.VolumeSpec{
		VolumeID:          req.GetVolumeId(),
		SizeBytes:         req.GetSizeBytes(),
		BlockSize:         req.GetBlockSize(),
		ChunkBytes:        req.GetChunkBytes(),
		ReplicationFactor: int(req.GetReplicationFactor()),
		Protocol:          req.GetProtocol(),
		FrontendTransport: req.GetFrontendTransport(),
		PVCName:           req.GetPvcName(),
		PVCNamespace:      req.GetPvcNamespace(),
		PVCUID:            req.GetPvcUid(),
		PVName:            req.GetPvName(),
		SourceVolume:      req.GetSourceVolume(),
		SourceSnapshot:    req.GetSourceSnapshot(),
		CreateRequestID:   req.GetCreateRequestId(),
	}
}

func lifecycleError(op string, err error) error {
	if errors.Is(err, lifecycle.ErrInvalidVolumeSpec) {
		return status.Errorf(codes.InvalidArgument, "%s: %v", op, err)
	}
	if errors.Is(err, lifecycle.ErrVolumeConflict) {
		return status.Errorf(codes.AlreadyExists, "%s: %v", op, err)
	}

	return status.Errorf(codes.Internal, "%s: %v", op, err)
}

// SubscribeAssignments is VOLUME-SCOPED, not replica-slot-scoped.
// On subscribe, master opens a publisher subscription on EVERY
// accepted-topology replica slot for the volume and fans the
// deliveries into this stream. The subscriber thus sees the CURRENT
// volume-authority line, regardless of which replica it targets.
//
// Why not per-(vol, rid) subscription:
//
//	After failover r1@1 -> r2@2, the per-(vol, r1) subscriber would
//	receive r1@1 catch-up and nothing after. The volume process
//	holding replica r1 would never learn that the volume's
//	authority has moved to r2. It would keep treating r1@1 as
//	current. When a real data-path frontend later binds, that
//	stale projection becomes a split/out-of-control risk.
//
// Request still carries replica_id — it IDENTIFIES the caller for
// the volume side's "am I still the current replica?" check. The
// master does not gate subscription on replica_id; it gates on
// volume_id and fans in ALL replica slots.
//
// The volume side (core/host/volume/host.go:streamOnce) filters
// received facts by self.ReplicaID before calling
// decodeAssignmentFact: facts for OTHER replicas are recorded as
// "I've been superseded" events and NOT fed into the local
// adapter.
//
// Dedupe: within one stream, we suppress deliveries whose
// lexicographic (Epoch, EndpointVersion) is NOT strictly greater
// than the last pair we sent (epoch-only dedupe would drop valid
// RefreshEndpoint).
//
// RefreshEndpoint is same-epoch by design: it keeps Epoch and
// bumps EndpointVersion (publisher contract). Epoch-only dedupe
// would silently drop a legitimate r1@1,EV=2 after r1@1,EV=1 had
// already been sent — breaking the endpoint refresh route on the
// product RPC before consumers see it. Lex ordering keeps same-line
// catch-up and forward-progress safe:
//
//   - (Epoch, EV) strictly greater than last-sent: send.
//   - equal pair (identical duplicate): drop.
//   - lex smaller pair (stale late fan-in re-delivery): drop —
//     this is the no-reverse guarantee.
func (s *services) SubscribeAssignments(req *control.SubscribeRequest, stream control.AssignmentService_SubscribeAssignmentsServer) error {
	if req.VolumeId == "" {
		return fmt.Errorf("subscribe: volume_id required")
	}
	// Phase 0: a node subscribes only as its own server id, for every volume.
	if err := auth.RequireNode(stream.Context(), req.GetServerId()); err != nil {
		return err
	}
	pub := s.host.boot.Publisher
	ctx := stream.Context()

	// Look up accepted topology for this volume to find every
	// replica slot. Subscribe to each. If the volume is not in
	// accepted topology, there is nothing to subscribe to —
	// return an error rather than streaming an empty line set.
	slots := s.host.replicaSlotsFor(req.VolumeId)
	if len(slots) == 0 {
		return fmt.Errorf("subscribe: volume %q not in accepted topology", req.VolumeId)
	}
	// INV-AUTH-HOLDER-IDENTITY-001: a workspace volume's line is delivered only to a registered server id streaming
	// from its registered host, so a second host with a copied --server-id never receives (never adopts) it.
	if s.isWorkspaceVolume(req.VolumeId) {
		if err := s.host.refuseWorkspaceHolderHost(ctx, "assignment stream", req.GetServerId()); err != nil {
			return identityRefusal(err)
		}
		if line, moved := pub.WorkspaceSourceMoved(req.VolumeId, req.ReplicaId, req.ServerId); moved {
			owner, _ := line.Owner()
			return leaseRefusal(fmt.Sprintf("workspace moved to holder %s at epoch %d", owner, line.Epoch), control.WorkspaceLeaseRefusal_MOVED, 0)
		}
	}

	type recv struct {
		ch     <-chan adapter.AssignmentInfo
		cancel func()
	}
	recvs := make([]recv, 0, len(slots))
	for _, rid := range slots {
		ch, cancel := pub.Subscribe(req.VolumeId, rid)
		recvs = append(recvs, recv{ch: ch, cancel: cancel})
	}
	defer func() {
		for _, r := range recvs {
			r.cancel()
		}
	}()

	// Fan-in via one forwarding goroutine per channel, merged
	// into a single ordered send channel.
	merged := make(chan adapter.AssignmentInfo, len(recvs))
	for _, r := range recvs {
		go func(ch <-chan adapter.AssignmentInfo) {
			for v := range ch {
				select {
				case <-ctx.Done():
					return
				case merged <- v:
				}
			}
		}(r.ch)
	}

	var (
		lastEpoch uint64
		lastEV    uint64
		sentAny   bool
		// current rebuilds the fact of the last line sent (no local copy of
		// the assignment: only the publisher mints one).
		current func() *control.AssignmentFact
	)
	// A change of the volume's store record re-sends the current fact: its
	// descriptors carry the recorded stores (the same generation; the
	// primary updates its peers' expected stores in place).
	var storesChanged <-chan struct{}
	if s.host.attest != nil {
		storesChanged = s.host.attest.watch(req.VolumeId)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-storesChanged:
			storesChanged = s.host.attest.watch(req.VolumeId)
			if current != nil {
				if err := stream.Send(current()); err != nil {
					return err
				}
			}
		case info, ok := <-merged:
			if !ok {
				return nil
			}
			// Lex dedupe on (Epoch, EndpointVersion). First
			// delivery on a fresh subscription always fires
			// (current-line catch-up). After that, strictly
			// greater pair is required.
			if sentAny {
				cur := info.Epoch
				if cur < lastEpoch {
					continue
				}
				if cur == lastEpoch && info.EndpointVersion <= lastEV {
					continue
				}
			}
			// Generation derivation requires each of
			// Epoch and EndpointVersion to fit in uint32; aliasing at
			// the 2^32 boundary would silently collapse distinct
			// authority lines onto the same generation and break the
			// monotonic stale-replay guard on the volume side. We
			// assert + skip rather than panic: the fact has no valid
			// generation, so we can't send it, but the master should
			// keep running so operators can page + fix upstream.
			if reason, ok := generationFitsUint32(info.Epoch, info.EndpointVersion); !ok {
				fmt.Printf("services: volume %s replica %s: %s; skipping fact\n",
					info.VolumeID, info.ReplicaID, reason)
				continue
			}

			lastEpoch = info.Epoch
			lastEV = info.EndpointVersion
			current = func() *control.AssignmentFact { return s.assignmentFact(info) }
			sentAny = true
			fact := s.assignmentFact(info)
			if err := stream.Send(fact); err != nil {
				return err
			}
		}
	}
}

// QueryVolumeStatus is a read-only projection surface. Reports the
// publisher's current authority line plus controller evidence. Mode,
// probe, and session facts are not returned by this surface.
func (s *services) QueryVolumeStatus(ctx context.Context, req *control.StatusRequest) (*control.StatusResponse, error) {
	if req.VolumeId == "" {
		return nil, fmt.Errorf("status: volume_id required")
	}
	if err := s.requireLineHolderNode(ctx, req.VolumeId); err != nil {
		return nil, err
	}
	pub := s.host.boot.Publisher
	ctrl := s.host.ctrl

	resp := &control.StatusResponse{VolumeId: req.VolumeId}

	if line, ok := pub.VolumeAuthorityLine(req.VolumeId); ok {
		resp.Assigned = line.Assigned
		resp.ReplicaId = line.ReplicaID
		resp.Epoch = line.Epoch
		resp.EndpointVersion = line.EndpointVersion
		replicaIDs := s.host.replicaSlotsFor(req.VolumeId)
		if len(replicaIDs) == 0 && line.ReplicaID != "" {
			replicaIDs = []string{line.ReplicaID}
		}
		replicaIDs = primaryFirstReplicaIDs(replicaIDs, line.ReplicaID)
		// the primary's portal comes from the server on the line, never a fresher same-replica claim elsewhere
		// (INV-AUTH-HOLDER-IDENTITY-001)
		resp.Frontends = statusFrontendsFor(s.host.obs, req.VolumeId, replicaIDs, line.Assigned, line.ReplicaID, func(replicaID string) (authority.SlotFact, bool) {
			if replicaID == line.ReplicaID {
				_, fact, ok := s.host.holderSlotFact(req.VolumeId, replicaID, line.Epoch)
				return fact, ok
			}
			return s.host.obs.Store().SlotFact(req.VolumeId, replicaID)
		})
		resp.UnhealthyReplicas = statusUnhealthyReplicas(s.host.obs, req.VolumeId, replicaIDs)
	}
	if ev, ok := ctrl.LastUnsupported(req.VolumeId); ok {
		resp.LastUnsupportedReason = ev.Reason
	}
	if ev, ok := ctrl.LastConvergenceStuck(req.VolumeId); ok {
		resp.LastConvergenceStuckAt = ev.StuckAt.UTC().Format(time.RFC3339Nano)
	}
	return resp, nil
}

func statusFrontendsForAssignedLine(obs *authority.ObservationHost, volumeID, replicaID string, assigned bool) []*control.FrontendTarget {
	return statusFrontendsForAssignedVolume(obs, volumeID, []string{replicaID}, assigned)
}

func primaryFirstReplicaIDs(replicaIDs []string, primaryReplicaID string) []string {
	if primaryReplicaID == "" || len(replicaIDs) == 0 {
		return replicaIDs
	}
	out := make([]string, 0, len(replicaIDs))
	seenPrimary := false
	for _, replicaID := range replicaIDs {
		if replicaID == primaryReplicaID {
			if !seenPrimary {
				out = append(out, replicaID)
				seenPrimary = true
			}
			continue
		}
	}
	if !seenPrimary {
		out = append(out, primaryReplicaID)
	}
	for _, replicaID := range replicaIDs {
		if replicaID == "" || replicaID == primaryReplicaID {
			continue
		}
		out = append(out, replicaID)
	}
	return out
}

func statusFrontendsForAssignedVolume(obs *authority.ObservationHost, volumeID string, replicaIDs []string, assigned bool) []*control.FrontendTarget {
	return statusFrontendsFor(obs, volumeID, replicaIDs, assigned, "", nil)
}

// statusFrontendsFor lists the frontends of replicaIDs in order; slotOf selects each replica's slot fact (nil: the
// freshest claim for that replica). When primary is set and its slot fact is missing, nothing is listed: a CSI client
// takes the first frontend as the primary, so a supporting replica's frontend must never stand in for it
// (INV-AUTH-HOLDER-IDENTITY-001, fail closed).
func statusFrontendsFor(obs *authority.ObservationHost, volumeID string, replicaIDs []string, assigned bool, primary string, slotOf func(replicaID string) (authority.SlotFact, bool)) []*control.FrontendTarget {
	if !assigned || obs == nil {
		return nil
	}
	if slotOf == nil {
		slotOf = func(replicaID string) (authority.SlotFact, bool) { return obs.Store().SlotFact(volumeID, replicaID) }
	}
	if primary != "" {
		if f, ok := slotOf(primary); !ok || len(f.Frontends) == 0 {
			return nil // no primary evidence, or a primary slot with no frontend: nothing to publish
		}
	}
	out := make([]*control.FrontendTarget, 0, len(replicaIDs))
	seen := map[string]struct{}{}
	for _, replicaID := range replicaIDs {
		if replicaID == "" {
			continue
		}
		slot, ok := slotOf(replicaID)
		if !ok {
			continue
		}
		for _, ft := range frontendTargetsToWire(slot.Frontends) {
			ft.ReplicaId = replicaID // CSI selects only a primary-owned target (INV-AUTH-HOLDER-IDENTITY-001)
			key := frontendTargetKey(ft)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, ft)
		}
	}
	return out
}

func frontendTargetKey(ft *control.FrontendTarget) string {
	if ft == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s|%d|%d", ft.GetProtocol(), normalizedFrontendTransport(ft.GetProtocol(), ft.GetTransport()), ft.GetAddr(), ft.GetIqn(), ft.GetNqn(), ft.GetLun(), ft.GetNsid())
}

func normalizedFrontendTransport(protocol, transport string) string {
	if protocol == "nvme" && transport == "" {
		return "tcp"
	}
	return transport
}

// validateHeartbeat rejects reports that are missing required
// fields. Malformed reports MUST NOT reach IngestHeartbeat — fail
// closed at the RPC boundary.
//
// Checks:
//   - non-nil report
//   - non-empty ServerID
//   - valid SentAt (not nil, not zero-protobuf)
//   - non-nil slot
//   - non-empty VolumeID / ReplicaID per slot
//   - non-empty DataAddr / CtrlAddr per slot. Every slot carried
//     over the wire represents a locally-hosted replica and MUST
//     name its data and control endpoints. Empty endpoints are
//     observation-layer poison (placement / failover would target
//     a replica with no way to reach it), so we reject at the
//     RPC boundary instead of silently letting the observation
//     layer filter it.
func validateHeartbeat(r *control.HeartbeatReport) error {
	if r == nil {
		return errors.New("heartbeat: nil report")
	}
	if r.ServerId == "" {
		return errors.New("heartbeat: empty ServerID")
	}
	if r.SentAt == nil || !r.SentAt.IsValid() {
		return errors.New("heartbeat: invalid SentAt")
	}
	seen := make(map[string]int, len(r.Slots))
	for i, s := range r.Slots {
		if s == nil {
			return fmt.Errorf("heartbeat: slot[%d] is nil", i)
		}
		if s.VolumeId == "" || s.ReplicaId == "" {
			return fmt.Errorf("heartbeat: slot[%d] missing VolumeID or ReplicaID", i)
		}
		key := s.VolumeId + "/" + s.ReplicaId
		if j, dup := seen[key]; dup {
			return fmt.Errorf("heartbeat: slot[%d] repeats slot[%d] (%s)", i, j, key)
		}
		seen[key] = i
		if s.DataAddr == "" || s.CtrlAddr == "" {
			return fmt.Errorf("heartbeat: slot[%d] (%s/%s) missing DataAddr or CtrlAddr", i, s.VolumeId, s.ReplicaId)
		}
		for j, ft := range s.Frontends {
			if ft == nil {
				return fmt.Errorf("heartbeat: slot[%d].frontends[%d] is nil", i, j)
			}
			if ft.Protocol == "" || ft.Addr == "" {
				return fmt.Errorf("heartbeat: slot[%d].frontends[%d] missing protocol or addr", i, j)
			}
			switch ft.Protocol {
			case "iscsi":
				if ft.Transport != "" {
					return fmt.Errorf("heartbeat: slot[%d].frontends[%d] iscsi transport must be empty", i, j)
				}
				if ft.Iqn == "" {
					return fmt.Errorf("heartbeat: slot[%d].frontends[%d] iscsi missing iqn", i, j)
				}
			case "nvme":
				if ft.Transport != "" && ft.Transport != "tcp" && ft.Transport != "rdma" {
					return fmt.Errorf("heartbeat: slot[%d].frontends[%d] invalid nvme transport %q", i, j, ft.Transport)
				}
				if ft.Nqn == "" {
					return fmt.Errorf("heartbeat: slot[%d].frontends[%d] nvme missing nqn", i, j)
				}
			default:
				return fmt.Errorf("heartbeat: slot[%d].frontends[%d] unsupported protocol %q", i, j, ft.Protocol)
			}
		}
	}
	return nil
}

// generationFitsUint32 reports whether the (epoch, endpointVersion)
// pair can be safely packed into one uint64 as `(epoch<<32) | ev`.
// Returns (reason, false) when either exceeds 2^32-1 — in that case
// SubscribeAssignments skips the fact rather than silently alias
// distinct authority lines onto the same peer_set_generation.
//
// Paired with TestGenerationFitsUint32_* pins in services_test.go.
// Named as a pure function so tests can exercise boundary cases
// without driving a full gRPC stream.
func generationFitsUint32(epoch, endpointVersion uint64) (reason string, ok bool) {
	const uint32Max = uint64(1) << 32
	if epoch >= uint32Max {
		return fmt.Sprintf("epoch %d exceeds uint32 — peer_set_generation would alias", epoch), false
	}
	if endpointVersion >= uint32Max {
		return fmt.Sprintf("endpoint_version %d exceeds uint32 — peer_set_generation would alias", endpointVersion), false
	}
	return "", true
}

// collectPeers builds the AssignmentFact.peers list for a volume.
// Topology membership is the allow-list — only declared slot
// members can appear in peers (never accumulate from arbitrary
// observed nodes).
//
// Per-slot resolution:
//
//  1. AUTHORITY path: for slots whose authority is published (the
//     bound replica + any historically-bound replicas with retained
//     records), pull DataAddr/CtrlAddr from `LastPublished` (this
//     is the freshest authoritative addr — refreshed via
//     IntentRefreshEndpoint).
//
//  2. OBSERVATION path: for slots that ARE declared in topology
//     but have NO published authority line (the common case for
//     supporting replicas — Publisher.apply only mints for the
//     bound replica), pull DataAddr/CtrlAddr from the observation
//     store's freshest non-expired heartbeat. pub.state stays
//     authority-only; observation does not mint presence records.
//
// Both paths stamp peer.Epoch / peer.EndpointVersion with the
// SUBSCRIBING PRIMARY's lineage, NOT the peer's own. Rationale:
// the descriptor's Epoch/EV become the live-ship lineage downstream
// at ReplicaPeer construction; live ships travel under primary's
// authority. Using a retained historical LastPublished epoch/EV
// for the peer would mis-stamp frames against the current
// authority and break the replica's apply gate.
//
// Fail-closed cases (slot is skipped, NOT silently substituted):
//   - slot is the self replica (primary doesn't ship to itself)
//   - slot has no published line AND no fresh observation
//   - observation exists but reports empty DataAddr (unusable)
//   - observation exists but is expired (now > ExpiresAt) — stale
//     heartbeat addrs are unsafe for live dial
//
// Without this fallback, a cross-host replicated write would hang at
// fsync barrier because primary's peers list is empty and nothing
// fans out.
func (s *services) collectPeers(volumeID string, selfInfo adapter.AssignmentInfo) []*control.ReplicaDescriptor {
	slots := s.host.replicaSlotsFor(volumeID)
	if len(slots) == 0 {
		return nil
	}
	var obsStore *authority.ObservationStore
	if s.host.obs != nil {
		obsStore = s.host.obs.Store()
	}
	peers := resolvePeers(slots, selfInfo, volumeID, s.host.boot.Publisher, obsStore)
	for _, p := range peers {
		if id, ok := s.host.AttestedStore(volumeID, p.ReplicaId); ok {
			p.StoreId = append([]byte(nil), id[:]...)
		}
	}
	return peers
}

// peerLookup is the narrow read interface resolvePeers needs from
// the publisher. *authority.Publisher satisfies it; tests substitute
// a stub.
type peerLookup interface {
	LastPublished(volumeID, replicaID string) (adapter.AssignmentInfo, bool)
}

// peerObservation is the narrow read interface resolvePeers needs
// from the observation store. *authority.ObservationStore satisfies
// it; tests substitute a stub. nil is permitted (fail-closed: only
// path-1 published peers will resolve).
type peerObservation interface {
	SlotFact(volumeID, replicaID string) (authority.SlotFact, bool)
}

// resolvePeers implements the peer-set construction policy. Pure
// function; testable without constructing a full Host.
//
// LINEAGE RULE: peer descriptor
// Epoch/EndpointVersion ALWAYS comes from the subscribing primary's
// own line, regardless of whether the peer's address came from
// authority publication (path 1) or observation (path 2).
// Rationale: the peer descriptor is consumed by the primary's
// ReplicaPeer constructor to stamp live-ship frames; live-ship is
// always under PRIMARY's lineage. A retained historical
// LastPublished line for the peer (e.g., r2 was previously bound
// before reassignment to r1) carries r2's old Epoch/EV — using
// those as the live-ship lineage would mis-stamp frames against
// the current authority and the replica's apply gate would reject.
// Authority/observation supplies ADDRESSES; subscriber lineage
// stamps EPOCH/EV.
//
// PATH RULE: path 1 (authority) is preferred over path 2
// (observation) when both have data, because authority has the
// live-current DataAddr/CtrlAddr (refreshed via IntentRefreshEndpoint),
// whereas observation may show a stale address from before a
// re-bind.
func resolvePeers(slots []string, selfInfo adapter.AssignmentInfo, volumeID string, pub peerLookup, obsStore peerObservation) []*control.ReplicaDescriptor {
	peers := make([]*control.ReplicaDescriptor, 0, len(slots))
	for _, rid := range slots {
		if rid == selfInfo.ReplicaID {
			continue
		}
		// Path 1: published authority line — use authority addrs,
		// stamp subscriber lineage.
		if info, ok := pub.LastPublished(volumeID, rid); ok {
			peers = append(peers, &control.ReplicaDescriptor{
				ReplicaId:       info.ReplicaID,
				Epoch:           selfInfo.Epoch,
				EndpointVersion: selfInfo.EndpointVersion,
				DataAddr:        info.DataAddr,
				CtrlAddr:        info.CtrlAddr,
			})
			continue
		}
		// Path 2: observation-only — use heartbeat addrs, stamp
		// subscriber lineage. Same rationale as path 1.
		if obsStore != nil {
			if slot, ok := obsStore.SlotFact(volumeID, rid); ok {
				peers = append(peers, &control.ReplicaDescriptor{
					ReplicaId:       rid,
					Epoch:           selfInfo.Epoch,
					EndpointVersion: selfInfo.EndpointVersion,
					DataAddr:        slot.DataAddr,
					CtrlAddr:        slot.CtrlAddr,
				})
				continue
			}
		}
		// Else: fail-closed skip. Slot is declared in topology but
		// neither authority nor observation has a usable address yet
		// (cold-start before heartbeat lands, or replica down). The
		// next emission (after the supporting replica heartbeats in)
		// will include it.
	}
	return peers
}

// heartbeatFromWire is the wire -> authority-layer translator.
// Field-copy only. This is the master-side analog of the volume's
// decodeAssignmentFact. Unlike the volume decode path, the master
// side is allowed to import core/authority freely.
func heartbeatFromWire(r *control.HeartbeatReport) authority.HeartbeatMessage {
	slots := make([]authority.HeartbeatSlot, 0, len(r.Slots))
	for _, s := range r.Slots {
		slots = append(slots, authority.HeartbeatSlot{
			VolumeID:        s.VolumeId,
			ReplicaID:       s.ReplicaId,
			DataAddr:        s.DataAddr,
			CtrlAddr:        s.CtrlAddr,
			Frontends:       frontendTargetsFromWire(s.Frontends),
			Reachable:       s.Reachable,
			ReadyForPrimary: s.ReadyForPrimary,
			Eligible:        s.Eligible,
			Withdrawn:       s.Withdrawn,
			EvidenceScore:   s.EvidenceScore,
			Incarnation:     s.Incarnation,
			UnhealthyReason: s.UnhealthyReason,
			LocalRoleClaim:  authority.LocalRoleClaim(s.LocalRoleClaim),
		})
	}
	return authority.HeartbeatMessage{
		ServerID:  r.ServerId,
		SentAt:    r.SentAt.AsTime(),
		Reachable: r.Reachable,
		Eligible:  r.Eligible,
		Slots:     slots,
	}
}

func frontendTargetsFromWire(in []*control.FrontendTarget) []authority.FrontendTargetFact {
	if len(in) == 0 {
		return nil
	}
	out := make([]authority.FrontendTargetFact, 0, len(in))
	for _, ft := range in {
		if ft == nil {
			continue
		}
		out = append(out, authority.FrontendTargetFact{
			Protocol:  ft.Protocol,
			Transport: normalizedFrontendTransport(ft.Protocol, ft.Transport),
			Addr:      ft.Addr,
			IQN:       ft.Iqn,
			NQN:       ft.Nqn,
			LUN:       ft.Lun,
			NSID:      ft.Nsid,
		})
	}
	return out
}

func frontendTargetsToWire(in []authority.FrontendTargetFact) []*control.FrontendTarget {
	if len(in) == 0 {
		return nil
	}
	out := make([]*control.FrontendTarget, 0, len(in))
	for _, ft := range in {
		out = append(out, &control.FrontendTarget{
			Protocol:  ft.Protocol,
			Transport: normalizedFrontendTransport(ft.Protocol, ft.Transport),
			Addr:      ft.Addr,
			Iqn:       ft.IQN,
			Nqn:       ft.NQN,
			Lun:       ft.LUN,
			Nsid:      ft.NSID,
		})
	}
	return out
}

// statusUnhealthyReplicas lists the replicas whose fresh slot fact
// carries an explicit unhealthy reason (#128 rule 6).
func statusUnhealthyReplicas(obs *authority.ObservationHost, volumeID string, replicaIDs []string) []*control.ReplicaUnhealthy {
	if obs == nil {
		return nil
	}
	var out []*control.ReplicaUnhealthy
	for _, replicaID := range replicaIDs {
		if slot, ok := obs.Store().SlotFact(volumeID, replicaID); ok && slot.UnhealthyReason != "" {
			out = append(out, &control.ReplicaUnhealthy{ReplicaId: replicaID, Reason: slot.UnhealthyReason})
		}
	}
	return out
}

// ingestHeartbeatRecordingHealth ingests a heartbeat and records a
// replica_unhealthy / replica_unhealthy_cleared event for every slot
// whose unhealthy reason the accepted report changed (#128 rule 6). The
// transitions come from the accepted store mutation itself, so a refused
// report records nothing and expiry never fabricates a clear. healthMu
// keeps the events in accepted order across concurrent heartbeats.
func (h *Host) ingestHeartbeatRecordingHealth(msg authority.HeartbeatMessage) error {
	h.healthMu.Lock()
	defer h.healthMu.Unlock()
	transitions, err := h.obs.IngestHeartbeatWithHealthTransitions(msg)
	if err != nil {
		return err
	}
	for _, tr := range transitions {
		if tr.New == "" {
			h.events.append(ops.ClusterEvent{
				VolumeID: tr.VolumeID, ReplicaID: tr.ReplicaID,
				Type: "replica_unhealthy_cleared", Severity: "info",
				Message: "replica reported healthy again", OldValue: tr.Old,
			})
			continue
		}
		h.events.append(ops.ClusterEvent{
			VolumeID: tr.VolumeID, ReplicaID: tr.ReplicaID,
			Type: "replica_unhealthy", Severity: "warning",
			Message: "replica reported an explicit unhealthy fact: " + tr.New,
			Reason:  tr.New, OldValue: tr.Old, NewValue: tr.New,
		})
	}
	return nil
}

// lineAckProfile is the master's ack profile in canonical form, as every
// assignment names it ("" when the promotion gate has none configured).
func (h *Host) lineAckProfile() string {
	h.promotionMu.RLock()
	prober := h.promotionProber
	h.promotionMu.RUnlock()
	if ap, ok := prober.(ackProfiled); ok {
		return canonicalAckProfile(ap.AckProfile())
	}
	return ""
}

// assignmentFact encodes one captured publication at the stream boundary.
func (s *services) assignmentFact(info adapter.AssignmentInfo) *control.AssignmentFact {
	fact := &control.AssignmentFact{
		VolumeId:           info.VolumeID,
		ReplicaId:          info.ReplicaID,
		ServerId:           s.host.workspaceLineOwner(authority.PublishedLine{ReplicaID: info.ReplicaID, Epoch: info.Epoch, IssuedHost: info.ServerID, OwnerFromMove: info.OwnerFromMove}, info.VolumeID, info.ReplicaID, info.Epoch),
		Epoch:              info.Epoch,
		EndpointVersion:    info.EndpointVersion,
		DataAddr:           info.DataAddr,
		CtrlAddr:           info.CtrlAddr,
		PrimaryKind:        control.PrimaryKind(info.Primary.Kind),
		EvidenceDurableLsn: info.Primary.DurableLSN,
		EvidenceEpoch:      info.Primary.DataEpoch,
		EvidenceFirstLsn:   info.Primary.DataFirstLSN,
		LineIncarnation:    info.Incarnation,
		AckProfile:         s.host.lineAckProfile(),
	}
	if info.Primary.StoreID != ([16]byte{}) {
		id := info.Primary.StoreID
		fact.EvidenceStoreId = id[:]
	}
	// Populate the peer set from publisher LastPublished
	// queries for every non-self replica slot in this volume's
	// topology. peer_set_generation is derived
	// from the authoritative line's (Epoch, EndpointVersion)
	// packed into one uint64 as (epoch<<32 | ev). Assertion
	// above guarantees each fits in uint32 so no aliasing.
	// Monotonic across process lifetimes because Epoch/EV are
	// durable authority facts preserved across master restart.
	fact.Peers = s.collectPeers(info.VolumeID, info)
	fact.PeerSetGeneration = (info.Epoch << 32) | info.EndpointVersion
	if s.host.attest != nil {
		waiting, initial := s.host.attest.awaited(info.VolumeID, s.host.replicaSlotsFor(info.VolumeID))
		fact.ReplicaStoresAwaited, fact.ReplicaStoresInitial = uint32(waiting), uint32(initial)
	}
	return fact
}

// deleteHolderEvidence reads vol's HEAD with the configured reader, bounded
// at 5 s, and the published line's owner "<server>/<replica>". Either may be
// missing: no reader, a failed read or no line gives a nil head or "".
func (s *services) deleteHolderEvidence(ctx context.Context, vol string) (*authority.WorkspaceHead, string) {
	owner := ""
	if line, ok := s.host.Publisher().PublishedAuthorityLine(vol); ok {
		if server := s.host.workspaceLineOwner(line, vol, line.ReplicaID, line.Epoch); server != "" {
			owner = server + "/" + line.ReplicaID
		}
	}
	if s.host.cfg.WorkspaceHeadReader == nil || vol == "" {
		return nil, owner
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	head, err := s.host.cfg.WorkspaceHeadReader(readCtx, vol)
	if errors.Is(err, authority.ErrWorkspaceHeadUnclaimed) || errors.Is(err, authority.ErrWorkspaceHeadDeleting) {
		return &head, owner // the initial HEAD (never claimed), or one past the delete's fence
	}
	if err != nil {
		s.host.log.Printf("blockmaster: MarkVolumeDeleting volume=%q: HEAD unreadable (%v); a line without an ACK is refused", vol, err)
		return nil, owner
	}
	return &head, owner
}
