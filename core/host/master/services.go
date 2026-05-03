package master

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
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
// Boundary (T0 sketch §3): every handler here converts wire
// messages to authority-layer types via HeartbeatToObservation
// or LastPublished reads; no handler constructs an AssignmentAsk
// or mints epochs.
type services struct {
	control.UnimplementedObservationServiceServer
	control.UnimplementedAssignmentServiceServer
	control.UnimplementedEvidenceServiceServer
	control.UnimplementedLifecycleServiceServer

	host *Host
	// seq is incremented per ReportHeartbeat handler invocation.
	// gRPC runs each unary RPC on its own goroutine, so seq MUST
	// be atomic (round-4 architect medium-1 fix). Diagnostic
	// only — the client uses the sequence to correlate log
	// lines, not to drive authority.
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
	rec, err := stores.Volumes.CreateVolume(lifecycleSpecFromWire(req))
	if err != nil {
		return nil, lifecycleError("create volume", err)
	}
	return &control.CreateVolumeResponse{
		VolumeId:          rec.Spec.VolumeID,
		SizeBytes:         rec.Spec.SizeBytes,
		ReplicationFactor: int32(rec.Spec.ReplicationFactor),
	}, nil
}

func (s *services) DeleteVolume(ctx context.Context, req *control.DeleteVolumeRequest) (*control.DeleteVolumeResponse, error) {
	stores := s.host.Lifecycle()
	if stores == nil {
		return nil, status.Error(codes.FailedPrecondition, "lifecycle store is not configured")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if err := stores.Volumes.DeleteVolume(req.GetVolumeId()); err != nil {
		return nil, lifecycleError("delete volume", err)
	}
	return &control.DeleteVolumeResponse{}, nil
}

// ReportHeartbeat receives a volume's observation. It validates
// field presence (rejects malformed reports per T0 sketch §8.2.1
// rule 8), translates to authority.HeartbeatMessage, and calls
// the observation host's Ingest path.
func (s *services) ReportHeartbeat(ctx context.Context, r *control.HeartbeatReport) (*control.HeartbeatAck, error) {
	if err := validateHeartbeat(r); err != nil {
		return nil, err
	}
	msg := heartbeatFromWire(r)
	if err := s.host.obs.IngestHeartbeat(msg); err != nil {
		return nil, fmt.Errorf("ingest: %w", err)
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
		ReplicationFactor: int(req.GetReplicationFactor()),
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

// SubscribeAssignments is VOLUME-SCOPED, not replica-slot-scoped
// (round-4 architect high-1 fix). On subscribe, master opens a
// publisher subscription on EVERY accepted-topology replica slot
// for the volume and fans the deliveries into this stream. The
// subscriber thus sees the CURRENT volume-authority line,
// regardless of which replica it targets.
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
// adapter. See sketch §3.1 + round-4 architect notes.
//
// Dedupe: within one stream, we suppress deliveries whose
// lexicographic (Epoch, EndpointVersion) is NOT strictly
// greater than the last pair we sent (round-5 architect high
// fix — epoch-only dedupe dropped valid RefreshEndpoint).
//
// RefreshEndpoint is same-epoch by design: it keeps Epoch and
// bumps EndpointVersion (P14 S2 publisher contract). Epoch-only
// dedupe would silently drop a legitimate r1@1,EV=2 after
// r1@1,EV=1 had already been sent — breaking the endpoint
// refresh route on the product RPC before T1/G3 even consumes
// it. Lex ordering keeps same-line catch-up and forward-
// progress safe:
//
//   - (Epoch, EV) strictly greater than last-sent: send.
//   - equal pair (identical duplicate): drop.
//   - lex smaller pair (stale late fan-in re-delivery): drop —
//     this is the no-reverse guarantee.
func (s *services) SubscribeAssignments(req *control.SubscribeRequest, stream control.AssignmentService_SubscribeAssignmentsServer) error {
	if req.VolumeId == "" {
		return fmt.Errorf("subscribe: volume_id required")
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
	)
	for {
		select {
		case <-ctx.Done():
			return nil
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
			// T4a-5 P-refined generation derivation requires each of
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
			sentAny = true
			fact := &control.AssignmentFact{
				VolumeId:        info.VolumeID,
				ReplicaId:       info.ReplicaID,
				Epoch:           info.Epoch,
				EndpointVersion: info.EndpointVersion,
				DataAddr:        info.DataAddr,
				CtrlAddr:        info.CtrlAddr,
			}
			// T4a-5 P-refined: populate the peer set from publisher
			// LastPublished queries for every non-self replica slot in
			// this volume's topology. peer_set_generation is derived
			// from the authoritative line's (Epoch, EndpointVersion)
			// packed into one uint64 as (epoch<<32 | ev). Assertion
			// above guarantees each fits in uint32 so no aliasing.
			// Monotonic across process lifetimes because Epoch/EV are
			// durable authority facts preserved across master restart.
			fact.Peers = s.collectPeers(req.VolumeId, info)
			fact.PeerSetGeneration = (info.Epoch << 32) | info.EndpointVersion
			if err := stream.Send(fact); err != nil {
				return err
			}
		}
	}
}

// QueryVolumeStatus is a read-only projection surface. T0 reports
// the publisher's current authority line plus controller evidence.
// Mode / probe / session facts are G3+ territory — not returned.
func (s *services) QueryVolumeStatus(ctx context.Context, req *control.StatusRequest) (*control.StatusResponse, error) {
	if req.VolumeId == "" {
		return nil, fmt.Errorf("status: volume_id required")
	}
	pub := s.host.boot.Publisher
	ctrl := s.host.ctrl

	resp := &control.StatusResponse{VolumeId: req.VolumeId}

	if line, ok := pub.VolumeAuthorityLine(req.VolumeId); ok {
		resp.Assigned = line.Assigned
		resp.ReplicaId = line.ReplicaID
		resp.Epoch = line.Epoch
		resp.EndpointVersion = line.EndpointVersion
		resp.Frontends = statusFrontendsForAssignedLine(s.host.obs, req.VolumeId, line.ReplicaID, line.Assigned)
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
	if !assigned || obs == nil {
		return nil
	}
	if slot, ok := obs.Store().SlotFact(volumeID, replicaID); ok {
		return frontendTargetsToWire(slot.Frontends)
	}
	return nil
}

// validateHeartbeat rejects reports that are missing required
// fields. Malformed reports MUST NOT reach IngestHeartbeat —
// sketch §8.2.1 rule 8 (G1 malformed-reject) demands fail-closed.
//
// Checks (round-4 architect medium-2 strengthening):
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
	for i, s := range r.Slots {
		if s == nil {
			return fmt.Errorf("heartbeat: slot[%d] is nil", i)
		}
		if s.VolumeId == "" || s.ReplicaId == "" {
			return fmt.Errorf("heartbeat: slot[%d] missing VolumeID or ReplicaID", i)
		}
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
				if ft.Iqn == "" {
					return fmt.Errorf("heartbeat: slot[%d].frontends[%d] iscsi missing iqn", i, j)
				}
			case "nvme":
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
// Assertion landed at T4a-5.0 follow-up (QA finding #2). Paired with
// TestGenerationFitsUint32_* pins in services_test.go. Named as a
// pure function so tests can exercise boundary cases without
// driving a full gRPC stream.
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
// members can appear in peers (option R rejected in T4a-5.0
// discovery doc §9.5: never accumulate from arbitrary observed
// nodes).
//
// Per-slot resolution (G5-5A architect ratification 2026-04-27,
// refined at round-54 review):
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
//     authority-only — architect option (B) "mint presence
//     records" REJECTED.
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
//   - observation exists but is expired (now > ExpiresAt) —
//     architect round-54 finding 1: stale heartbeat addrs are
//     unsafe for live dial
//
// Without this fallback, a cross-host replicated write would hang at
// fsync barrier because primary's peers list is empty and nothing
// fans out. QA round 54 surfaced this as the headline G5-5 L3 gap.
func (s *services) collectPeers(volumeID string, selfInfo adapter.AssignmentInfo) []*control.ReplicaDescriptor {
	slots := s.host.replicaSlotsFor(volumeID)
	if len(slots) == 0 {
		return nil
	}
	var obsStore *authority.ObservationStore
	if s.host.obs != nil {
		obsStore = s.host.obs.Store()
	}
	return resolvePeers(slots, selfInfo, volumeID, s.host.boot.Publisher, obsStore)
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

// resolvePeers implements the G5-5A peer-set construction policy
// (architect ratification 2026-04-27, refined round-54).
// Pure function; testable without constructing a full Host.
//
// LINEAGE RULE (architect round 54 finding 2): peer descriptor
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
			Protocol: ft.Protocol,
			Addr:     ft.Addr,
			IQN:      ft.Iqn,
			NQN:      ft.Nqn,
			LUN:      ft.Lun,
			NSID:     ft.Nsid,
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
			Protocol: ft.Protocol,
			Addr:     ft.Addr,
			Iqn:      ft.IQN,
			Nqn:      ft.NQN,
			Lun:      ft.LUN,
			Nsid:     ft.NSID,
		})
	}
	return out
}
