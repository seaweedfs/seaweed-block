package master

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// probeRoundWait bounds how long a round waits for its answers.
const probeRoundWait = 2 * time.Second

// slotServers is the latest replica -> server map per volume, from the
// snapshot the promotion gate walks; the round asks each replica's node.
type slotServers struct {
	mu sync.Mutex
	m  map[string]map[string]string
}

func (h *Host) noteSlotServers(volumeID string, slots []authority.ReplicaCandidate) {
	h.probeSlots.mu.Lock()
	defer h.probeSlots.mu.Unlock()
	if h.probeSlots.m == nil {
		h.probeSlots.m = map[string]map[string]string{}
	}
	m := map[string]string{}
	for _, s := range slots {
		if s.ReplicaID != "" && s.ServerID != "" {
			m[s.ReplicaID] = s.ServerID
		}
	}
	h.probeSlots.m[volumeID] = m
}

func (h *Host) slotServer(volumeID, replicaID string) string {
	h.probeSlots.mu.Lock()
	defer h.probeSlots.mu.Unlock()
	return h.probeSlots.m[volumeID][replicaID]
}

// NewRoundPromotionEvidenceProvider is the promotion evidence of the probe
// round: the ack profile and the required frontier from the workload plans'
// configuration, every candidate's evidence from this round's answers.
func (h *Host) NewRoundPromotionEvidenceProvider(ackProfile string, requiredLSNByVolume map[string]uint64) PromotionEvidenceProvider {
	return &roundEvidenceProvider{hub: h.probeHub, serverOf: h.slotServer, ackProfile: ackProfile, required: cloneRequiredLSNMap(requiredLSNByVolume), wait: probeRoundWait}
}

// SubscribeProbeRounds streams the probe rounds for the caller's node.
func (s *services) SubscribeProbeRounds(req *control.ProbeRoundSubscription, stream control.AssignmentService_SubscribeProbeRoundsServer) error {
	if err := auth.RequireNode(stream.Context(), req.GetServerId()); err != nil {
		return err
	}
	if req.GetServerId() == "" {
		return status.Error(codes.InvalidArgument, "server id is required")
	}
	rounds, cancel := s.host.probeHub.subscribe(req.GetServerId())
	defer cancel()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case r := <-rounds:
			if err := stream.Send(r); err != nil {
				return err
			}
		}
	}
}

// AnswerProbe takes one replica's answer to a probe round. It counts only for
// the open round that asked this node for this replica; anything else is
// dropped (and acknowledged: the answer itself is never an error).
func (s *services) AnswerProbe(ctx context.Context, a *control.ProbeAnswer) (*control.ProbeAck, error) {
	if err := auth.RequireNode(ctx, a.GetServerId()); err != nil {
		return nil, err
	}
	if a.GetServerId() == "" || a.GetVolumeId() == "" || a.GetReplicaId() == "" || a.GetRoundId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "server, volume, replica and round ids are required")
	}
	s.host.probeHub.deliver(a)
	return &control.ProbeAck{}, nil
}
