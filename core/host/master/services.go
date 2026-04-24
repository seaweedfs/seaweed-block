package master

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// adapter import is still needed: the AssignmentService fan-in
// uses adapter.AssignmentInfo as the delivery channel element
// type from publisher.Subscribe. No composite literal of
// adapter.AssignmentInfo is constructed on the master side —
// values flow through unchanged from publisher state.

// services is the master-side implementation of the three
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
			fact.Peers = s.collectPeers(req.VolumeId, info.ReplicaID)
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
	}
	if ev, ok := ctrl.LastUnsupported(req.VolumeId); ok {
		resp.LastUnsupportedReason = ev.Reason
	}
	if ev, ok := ctrl.LastConvergenceStuck(req.VolumeId); ok {
		resp.LastConvergenceStuckAt = ev.StuckAt.UTC().Format(time.RFC3339Nano)
	}
	return resp, nil
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

// collectPeers builds the AssignmentFact.peers list for a volume
// from the publisher's most-recent authoritative lines. Only
// replicas OTHER than self_replica are included — the primary
// does not ship to itself. Slots with no published line yet are
// skipped (they'll appear in later emissions once their line
// lands; cold-start is therefore benign, not erroneous).
//
// This is the T4a-5 peer-set construction site. It is strictly
// a READ of publisher state — no caching, no derivation, no
// local accumulation (option R rejected in T4a-5.0 discovery
// doc §9.5). Peer membership comes from accepted topology
// (replicaSlotsFor) and per-peer line content comes from the
// publisher's durable LastPublished state — both are
// master-authoritative.
func (s *services) collectPeers(volumeID, selfReplicaID string) []*control.ReplicaDescriptor {
	slots := s.host.replicaSlotsFor(volumeID)
	if len(slots) == 0 {
		return nil
	}
	pub := s.host.boot.Publisher
	peers := make([]*control.ReplicaDescriptor, 0, len(slots))
	for _, rid := range slots {
		if rid == selfReplicaID {
			continue
		}
		info, ok := pub.LastPublished(volumeID, rid)
		if !ok {
			continue
		}
		peers = append(peers, &control.ReplicaDescriptor{
			ReplicaId:       info.ReplicaID,
			Epoch:           info.Epoch,
			EndpointVersion: info.EndpointVersion,
			DataAddr:        info.DataAddr,
			CtrlAddr:        info.CtrlAddr,
		})
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

