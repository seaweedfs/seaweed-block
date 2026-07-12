package master

import (
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

// ObservationSnapshot returns a read-only, operator-facing view of the
// master's current lifecycle, authority, and heartbeat facts. It does not
// mutate lifecycle stores, authority publisher state, observation state, or
// Kubernetes.
func (h *Host) ObservationSnapshot(now time.Time) ops.ClusterEvidence {
	cluster := ops.NewClusterEvidence(now)
	snapshot, ok := h.LifecycleSnapshot()
	if !ok {
		return cluster
	}

	cluster.Nodes = observationNodes(snapshot.Nodes, snapshot.Placements)
	cluster.Volumes = h.observationVolumes(snapshot)
	cluster.Events = h.events.list("")
	cluster.Status = clusterStatus(cluster.Volumes, cluster.Nodes)
	return cluster
}

func observationNodes(nodes []lifecycle.NodeRegistration, placements []lifecycle.PlacementIntent) []ops.NodeEvidence {
	replicaCounts := map[string]int{}
	for _, placement := range placements {
		for _, slot := range placement.Slots {
			if slot.ServerID != "" {
				replicaCounts[slot.ServerID]++
			}
		}
	}
	out := make([]ops.NodeEvidence, 0, len(nodes))
	for _, node := range nodes {
		managementIP := node.Labels[lifecycle.ManagementIPLabel]
		if managementIP == "" {
			managementIP = hostFromAddr(firstNonEmpty(node.DataAddr, node.Addr))
		}
		out = append(out, ops.NodeEvidence{
			NodeName:             node.ServerID,
			KubernetesNode:       node.Labels[lifecycle.KubernetesNodeNameLabel],
			InternalIP:           managementIP,
			FrontendIP:           node.Labels[lifecycle.FrontendIPLabel],
			FrontendNetworkClass: node.Labels[lifecycle.FrontendNetworkClassLabel],
			Schedulable:          true,
			Ready:                true,
			LastHeartbeatAt:      node.SeenAt,
			ReplicaCount:         replicaCounts[node.ServerID],
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].NodeName < out[j].NodeName
	})
	return out
}

func (h *Host) observationVolumes(snapshot LifecycleSnapshot) []ops.VolumeEvidence {
	records := map[string]lifecycle.VolumeRecord{}
	for _, rec := range snapshot.Volumes {
		records[rec.Spec.VolumeID] = rec
	}
	placements := map[string]lifecycle.PlacementIntent{}
	for _, placement := range snapshot.Placements {
		placements[placement.VolumeID] = placement
	}

	ids := make(map[string]struct{})
	for id := range records {
		ids[id] = struct{}{}
	}
	for id := range placements {
		ids[id] = struct{}{}
	}
	for _, v := range h.topo.Volumes {
		ids[v.VolumeID] = struct{}{}
	}

	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)

	out := make([]ops.VolumeEvidence, 0, len(ordered))
	for _, id := range ordered {
		out = append(out, h.observationVolume(id, records[id], placements[id]))
	}
	return out
}

func (h *Host) observationVolume(volumeID string, rec lifecycle.VolumeRecord, placement lifecycle.PlacementIntent) ops.VolumeEvidence {
	rf := rec.Spec.ReplicationFactor
	if rf == 0 {
		rf = placement.DesiredRF
	}
	volume := ops.VolumeEvidence{
		VolumeID:          volumeID,
		Namespace:         rec.Spec.PVCNamespace,
		PVCName:           rec.Spec.PVCName,
		PVName:            rec.Spec.PVName,
		ReplicationFactor: rf,
		DesiredReplicas:   desiredReplicaCount(rf, placement),
		Status:            ops.ObservationStatusOK,
	}

	line, hasLine := h.Publisher().VolumeAuthorityLine(volumeID)
	if hasLine && line.Assigned {
		volume.PrimaryReplica = line.ReplicaID
		volume.Epoch = line.Epoch
		volume.EndpointVersion = line.EndpointVersion
	}

	replicas := h.observationReplicas(volumeID, placement, line, hasLine)
	volume.Replicas = replicas
	volume.ObservedReplicas = countObservedReplicas(replicas)
	if volume.PrimaryReplica != "" {
		volume.PrimaryNode = serverForReplica(placement, volume.PrimaryReplica)
		volume.PublishTarget = publishTargetForReplica(replicas, volume.PrimaryReplica)
	}

	if volume.PrimaryReplica != "" {
		primary, ok := replicaEvidenceByID(replicas, volume.PrimaryReplica)
		if !ok || !primary.Observed {
			if volume.Status == ops.ObservationStatusOK {
				volume.Status = ops.ObservationStatusDegraded
				volume.Reason = ops.ReasonStatusEndpointUnreachable
			}
			volume.Conditions = append(volume.Conditions, ops.ObservationCondition{
				Type:     "PrimaryReady",
				Status:   "false",
				Reason:   ops.ReasonStatusEndpointUnreachable,
				Severity: "warning",
				Message:  "published primary has no fresh readiness observation",
			})
		} else if !primary.CandidateReady {
			reason := primary.CandidateReadyReason
			if reason == "" {
				reason = ops.ReasonNoPromotionReadyCandidate
			}
			if volume.Status == ops.ObservationStatusOK {
				volume.Status = ops.ObservationStatusDegraded
				volume.Reason = reason
			}
			volume.Conditions = append(volume.Conditions, ops.ObservationCondition{
				Type:     "PrimaryReady",
				Status:   "false",
				Reason:   reason,
				Severity: "warning",
				Message:  "published primary has not confirmed local readiness",
			})
		}
	}

	if volume.DesiredReplicas > 0 && volume.ObservedReplicas < volume.DesiredReplicas {
		volume.Status = ops.ObservationStatusDegraded
		volume.Reason = ops.ReasonObservedReplicasBelowDesired
		volume.Conditions = append(volume.Conditions, ops.ObservationCondition{
			Type:     "ReplicasObserved",
			Status:   "false",
			Reason:   ops.ReasonObservedReplicasBelowDesired,
			Severity: "warning",
			Message:  fmt.Sprintf("observed %d of %d desired replicas", volume.ObservedReplicas, volume.DesiredReplicas),
		})
	}
	if !hasLine || !line.Assigned {
		if volume.Status == ops.ObservationStatusOK {
			volume.Status = ops.ObservationStatusDegraded
			volume.Reason = ops.ReasonNoPromotionReadyCandidate
		}
		volume.Conditions = append(volume.Conditions, ops.ObservationCondition{
			Type:     "PrimaryAssigned",
			Status:   "false",
			Reason:   ops.ReasonNoPromotionReadyCandidate,
			Severity: "warning",
			Message:  "master has no published primary for this volume",
		})
	}
	if unsupported, ok := h.Controller().LastUnsupported(volumeID); ok {
		volume.Status = ops.ObservationStatusBlocked
		volume.Reason = unsupported.Reason
		volume.Conditions = append(volume.Conditions, ops.ObservationCondition{
			Type:     "AuthoritySupported",
			Status:   "false",
			Reason:   unsupported.Reason,
			Severity: "error",
			Message:  "authority controller rejected the latest observed topology",
		})
	}
	return volume
}

func (h *Host) observationReplicas(volumeID string, placement lifecycle.PlacementIntent, line authority.AuthorityBasis, hasLine bool) []ops.ReplicaEvidence {
	slots := placement.Slots
	if len(slots) == 0 {
		for _, v := range h.topo.Volumes {
			if v.VolumeID != volumeID {
				continue
			}
			for _, slot := range v.Slots {
				slots = append(slots, lifecycle.PlacementSlotIntent{
					ServerID:  slot.ServerID,
					ReplicaID: slot.ReplicaID,
					Source:    lifecycle.PlacementSourceExistingReplica,
				})
			}
		}
	}
	out := make([]ops.ReplicaEvidence, 0, len(slots))
	for _, slot := range slots {
		replicaID := slot.ReplicaID
		if replicaID == "" {
			replicaID = "unknown"
		}
		replica := ops.ReplicaEvidence{
			ReplicaID:       replicaID,
			ServerID:        slot.ServerID,
			KubernetesNode:  slot.ServerID,
			Role:            "unknown",
			ReplicationRole: "unavailable",
		}
		if hasLine && line.Assigned && replicaID == line.ReplicaID {
			replica.Role = "primary"
			replica.ReplicationRole = "primary"
		}
		if fact, ok := h.ObservationHost().Store().SlotFact(volumeID, replicaID); ok {
			replica.Observed = true
			replica.CandidateReady = fact.ReadyForPrimary
			replica.CandidateReadyReason = candidateReason(fact.ReadyForPrimary)
			if replica.Role != "primary" {
				if fact.ReadyForPrimary {
					replica.Role = "candidate"
					replica.ReplicationRole = "replica_ready"
				} else {
					replica.ReplicationRole = "not_ready"
				}
			}
			if len(fact.Frontends) > 0 {
				replica.FrontendProtocol = fact.Frontends[0].Protocol
				replica.FrontendAddr = fact.Frontends[0].Addr
				replica.FrontendNQN = fact.Frontends[0].NQN
				replica.FrontendNSID = fact.Frontends[0].NSID
			}
		} else {
			replica.CandidateReadyReason = ops.ReasonStatusEndpointUnreachable
			replica.Conditions = append(replica.Conditions, ops.ObservationCondition{
				Type:     "ReplicaObserved",
				Status:   "false",
				Reason:   ops.ReasonStatusEndpointUnreachable,
				Severity: "warning",
				Message:  "no fresh heartbeat observation for this placement slot",
			})
		}
		out = append(out, replica)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ReplicaID < out[j].ReplicaID
	})
	return out
}

func clusterStatus(volumes []ops.VolumeEvidence, nodes []ops.NodeEvidence) string {
	status := ops.ObservationStatusOK
	for _, node := range nodes {
		for _, condition := range node.Conditions {
			if condition.Severity == "error" {
				return ops.ObservationStatusBlocked
			}
			if condition.Severity == "warning" {
				status = ops.ObservationStatusDegraded
			}
		}
	}
	for _, volume := range volumes {
		switch volume.Status {
		case ops.ObservationStatusBlocked, ops.ObservationStatusInvalid:
			return ops.ObservationStatusBlocked
		case ops.ObservationStatusDegraded, ops.ObservationStatusRecovering, ops.ObservationStatusUnavailable:
			status = ops.ObservationStatusDegraded
		}
	}
	return status
}

func desiredReplicaCount(rf int, placement lifecycle.PlacementIntent) int {
	if placement.DesiredRF > 0 {
		return placement.DesiredRF
	}
	if len(placement.Slots) > 0 {
		return len(placement.Slots)
	}
	return rf
}

func countObservedReplicas(replicas []ops.ReplicaEvidence) int {
	n := 0
	for _, replica := range replicas {
		if replica.Observed {
			n++
		}
	}
	return n
}

func serverForReplica(placement lifecycle.PlacementIntent, replicaID string) string {
	for _, slot := range placement.Slots {
		if slot.ReplicaID == replicaID {
			return slot.ServerID
		}
	}
	return ""
}

func publishTargetForReplica(replicas []ops.ReplicaEvidence, replicaID string) string {
	for _, replica := range replicas {
		if replica.ReplicaID == replicaID {
			return replica.FrontendAddr
		}
	}
	return ""
}

func replicaEvidenceByID(replicas []ops.ReplicaEvidence, replicaID string) (ops.ReplicaEvidence, bool) {
	for _, replica := range replicas {
		if replica.ReplicaID == replicaID {
			return replica, true
		}
	}
	return ops.ReplicaEvidence{}, false
}

func candidateReason(ready bool) string {
	if ready {
		return ops.ReasonCandidateCoversRequiredFrontier
	}
	return ops.ReasonNoPromotionReadyCandidate
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func hostFromAddr(addr string) string {
	if addr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
}
