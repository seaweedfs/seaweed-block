package master

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *services) GetClusterStatus(ctx context.Context, req *control.GetClusterStatusRequest) (*control.ClusterStatusResponse, error) {
	cluster := s.host.ObservationSnapshot(time.Now().UTC())
	return clusterEvidenceToWire(cluster), nil
}

func (s *services) ListVolumes(ctx context.Context, req *control.ListVolumesRequest) (*control.ListVolumesResponse, error) {
	cluster := s.host.ObservationSnapshot(time.Now().UTC())
	out := &control.ListVolumesResponse{
		SchemaVersion: cluster.SchemaVersion,
		CapturedAt:    timestampOrNil(cluster.CapturedAt),
		Volumes:       make([]*control.VolumeEvidence, 0, len(cluster.Volumes)),
	}
	for _, volume := range cluster.Volumes {
		out.Volumes = append(out.Volumes, volumeEvidenceToWire(volume))
	}
	return out, nil
}

func (s *services) GetVolumeStatus(ctx context.Context, req *control.GetVolumeStatusRequest) (*control.VolumeEvidence, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	cluster := s.host.ObservationSnapshot(time.Now().UTC())
	for _, volume := range cluster.Volumes {
		if volume.VolumeID == req.GetVolumeId() {
			return volumeEvidenceToWire(volume), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
}

func (s *services) GetVolumeTimeline(ctx context.Context, req *control.GetVolumeTimelineRequest) (*control.VolumeTimelineResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	cluster := s.host.ObservationSnapshot(time.Now().UTC())
	out := &control.VolumeTimelineResponse{
		SchemaVersion: cluster.SchemaVersion,
		CapturedAt:    timestampOrNil(cluster.CapturedAt),
		VolumeId:      req.GetVolumeId(),
	}
	for _, event := range cluster.Events {
		if event.VolumeID == "" || event.VolumeID == req.GetVolumeId() {
			out.Events = append(out.Events, clusterEventToWire(event))
		}
	}
	return out, nil
}

func (s *services) WatchClusterEvents(req *control.WatchClusterEventsRequest, stream control.ClusterEvidenceService_WatchClusterEventsServer) error {
	for _, event := range s.host.events.listAfter("", req.GetSinceEventId()) {
		if err := stream.Send(clusterEventToWire(event)); err != nil {
			return err
		}
	}
	return nil
}

func (s *services) ReportClusterEvent(ctx context.Context, req *control.ClusterEvent) (*control.ClusterEventAck, error) {
	if err := validateExternalClusterEvent(req); err != nil {
		return nil, err
	}
	event := externalClusterEventFromWire(req)
	event = s.host.events.append(event)
	return &control.ClusterEventAck{
		AcceptedAt: timestamppb.Now(),
		EventId:    event.EventID,
	}, nil
}

func validateExternalClusterEvent(req *control.ClusterEvent) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "event is required")
	}
	if req.GetEventType() == "" {
		return status.Error(codes.InvalidArgument, "event_type is required")
	}
	if req.GetSeverity() == "" {
		return status.Error(codes.InvalidArgument, "severity is required")
	}
	switch req.GetEventType() {
	case ops.EventTypeCSIReattachObserved:
		if req.GetVolumeId() == "" {
			return status.Error(codes.InvalidArgument, "volume_id is required for csi_reattach_observed")
		}
		if req.GetNodeName() == "" {
			return status.Error(codes.InvalidArgument, "node_name is required for csi_reattach_observed")
		}
	default:
		return status.Errorf(codes.InvalidArgument, "event_type %q is not accepted from external observation clients", req.GetEventType())
	}
	return nil
}

func externalClusterEventFromWire(req *control.ClusterEvent) ops.ClusterEvent {
	switch req.GetEventType() {
	case ops.EventTypeCSIReattachObserved:
		return ops.ClusterEvent{
			VolumeID:        req.GetVolumeId(),
			ReplicaID:       req.GetReplicaId(),
			NodeName:        req.GetNodeName(),
			Type:            ops.EventTypeCSIReattachObserved,
			Severity:        "info",
			Message:         "CSI staged volume on node",
			Reason:          ops.EventTypeCSIReattachObserved,
			NewValue:        req.GetNewValue(),
			Epoch:           req.GetEpoch(),
			EndpointVersion: req.GetEndpointVersion(),
			EvidenceRef:     "csi-node",
		}
	default:
		return ops.ClusterEvent{}
	}
}

func clusterEvidenceToWire(cluster ops.ClusterEvidence) *control.ClusterStatusResponse {
	out := &control.ClusterStatusResponse{
		SchemaVersion:   cluster.SchemaVersion,
		CapturedAt:      timestampOrNil(cluster.CapturedAt),
		ProductRevision: cluster.ProductRevision,
		Status:          cluster.Status,
		Nodes:           make([]*control.NodeEvidence, 0, len(cluster.Nodes)),
		Volumes:         make([]*control.VolumeEvidence, 0, len(cluster.Volumes)),
		Conditions:      conditionsToWire(cluster.Conditions),
		Events:          make([]*control.ClusterEvent, 0, len(cluster.Events)),
		NonClaims:       append([]string(nil), cluster.NonClaims...),
	}
	for _, node := range cluster.Nodes {
		out.Nodes = append(out.Nodes, nodeEvidenceToWire(node))
	}
	for _, volume := range cluster.Volumes {
		out.Volumes = append(out.Volumes, volumeEvidenceToWire(volume))
	}
	for _, event := range cluster.Events {
		out.Events = append(out.Events, clusterEventToWire(event))
	}
	return out
}

func nodeEvidenceToWire(node ops.NodeEvidence) *control.NodeEvidence {
	return &control.NodeEvidence{
		NodeName:        node.NodeName,
		KubernetesNode:  node.KubernetesNode,
		PhysicalHost:    node.PhysicalHost,
		InternalIp:      node.InternalIP,
		Schedulable:     node.Schedulable,
		Ready:           node.Ready,
		LastHeartbeatAt: timestampOrNil(node.LastHeartbeatAt),
		ReplicaCount:    int32(node.ReplicaCount),
		RequiredImages:  append([]string(nil), node.RequiredImages...),
		MissingImages:   append([]string(nil), node.MissingImages...),
		Conditions:      conditionsToWire(node.Conditions),
	}
}

func volumeEvidenceToWire(volume ops.VolumeEvidence) *control.VolumeEvidence {
	out := &control.VolumeEvidence{
		VolumeId:          volume.VolumeID,
		Namespace:         volume.Namespace,
		PvcName:           volume.PVCName,
		PvName:            volume.PVName,
		ReplicationFactor: int32(volume.ReplicationFactor),
		AckProfile:        volume.AckProfile,
		ClaimProfile:      volume.ClaimProfile,
		DesiredReplicas:   int32(volume.DesiredReplicas),
		ObservedReplicas:  int32(volume.ObservedReplicas),
		Status:            volume.Status,
		Reason:            volume.Reason,
		PrimaryReplica:    volume.PrimaryReplica,
		PrimaryNode:       volume.PrimaryNode,
		PublishTarget:     volume.PublishTarget,
		Epoch:             volume.Epoch,
		EndpointVersion:   volume.EndpointVersion,
		Replicas:          make([]*control.ReplicaEvidence, 0, len(volume.Replicas)),
		Conditions:        conditionsToWire(volume.Conditions),
		NextActions:       append([]string(nil), volume.NextActions...),
		SupportBundleHint: volume.SupportBundleHint,
	}
	for _, replica := range volume.Replicas {
		out.Replicas = append(out.Replicas, replicaEvidenceToWire(replica))
	}
	return out
}

func replicaEvidenceToWire(replica ops.ReplicaEvidence) *control.ReplicaEvidence {
	return &control.ReplicaEvidence{
		ReplicaId:            replica.ReplicaID,
		ServerId:             replica.ServerID,
		KubernetesNode:       replica.KubernetesNode,
		PhysicalHost:         replica.PhysicalHost,
		Observed:             replica.Observed,
		Role:                 replica.Role,
		ReplicationRole:      replica.ReplicationRole,
		DurableLatched:       replica.DurableLatched,
		DurableFrontierKnown: replica.DurableFrontierKnown,
		DurableFrontierLsn:   replica.DurableFrontierLSN,
		CandidateReady:       replica.CandidateReady,
		CandidateReadyReason: replica.CandidateReadyReason,
		FrontendProtocol:     replica.FrontendProtocol,
		FrontendAddr:         replica.FrontendAddr,
		StatusAddr:           replica.StatusAddr,
		StalePrimaryFenced:   replica.StalePrimaryFenced,
		Conditions:           conditionsToWire(replica.Conditions),
		SupportBundlePath:    replica.SupportBundlePath,
	}
}

func conditionsToWire(in []ops.ObservationCondition) []*control.ObservationCondition {
	out := make([]*control.ObservationCondition, 0, len(in))
	for _, condition := range in {
		out = append(out, &control.ObservationCondition{
			Type:     condition.Type,
			Status:   condition.Status,
			Reason:   condition.Reason,
			Severity: condition.Severity,
			Message:  condition.Message,
		})
	}
	return out
}

func clusterEventToWire(event ops.ClusterEvent) *control.ClusterEvent {
	return &control.ClusterEvent{
		EventId:         event.EventID,
		EventTime:       timestampOrNil(event.EventTime),
		VolumeId:        event.VolumeID,
		ReplicaId:       event.ReplicaID,
		NodeName:        event.NodeName,
		EventType:       event.Type,
		Severity:        event.Severity,
		Message:         event.Message,
		ReasonCode:      event.Reason,
		OldValue:        event.OldValue,
		NewValue:        event.NewValue,
		Epoch:           event.Epoch,
		EndpointVersion: event.EndpointVersion,
		CorrelationId:   event.CorrelationID,
		EvidenceRef:     event.EvidenceRef,
	}
}

func clusterEventFromWire(event *control.ClusterEvent) ops.ClusterEvent {
	if event == nil {
		return ops.ClusterEvent{}
	}
	out := ops.ClusterEvent{
		EventID:         event.GetEventId(),
		VolumeID:        event.GetVolumeId(),
		ReplicaID:       event.GetReplicaId(),
		NodeName:        event.GetNodeName(),
		Type:            event.GetEventType(),
		Severity:        event.GetSeverity(),
		Message:         event.GetMessage(),
		Reason:          event.GetReasonCode(),
		OldValue:        event.GetOldValue(),
		NewValue:        event.GetNewValue(),
		Epoch:           event.GetEpoch(),
		EndpointVersion: event.GetEndpointVersion(),
		CorrelationID:   event.GetCorrelationId(),
		EvidenceRef:     event.GetEvidenceRef(),
	}
	if ts := event.GetEventTime(); ts != nil && ts.IsValid() {
		out.EventTime = ts.AsTime()
	}
	return out
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}
