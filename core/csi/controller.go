package csi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strconv"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	storageClassProtocolParameter      = "sw-block.seaweedfs.com/protocol"
	storageClassNVMeTransportParameter = "sw-block.seaweedfs.com/nvme-transport"
)

type ControllerServer struct {
	csipb.UnimplementedControllerServer
	lookup           PublishTargetLookup
	provisioner      VolumeProvisioner
	metadataResolver KubernetesMetadataResolver
	registrar        VolumeObjectRegistrar
	snapshotter      SnapshotProvisioner
}

const snapshotListTokenVersion = 1

type snapshotListToken struct {
	Version        int    `json:"version"`
	SourceVolumeID string `json:"source_volume_id,omitempty"`
	SnapshotID     string `json:"snapshot_id,omitempty"`
	LastSnapshotID string `json:"last_snapshot_id"`
}

func NewControllerServer(lookup PublishTargetLookup) *ControllerServer {
	return &ControllerServer{lookup: lookup}
}

func NewControllerServerWithProvisioner(lookup PublishTargetLookup, provisioner VolumeProvisioner) *ControllerServer {
	return &ControllerServer{lookup: lookup, provisioner: provisioner}
}

func NewControllerServerWithProvisionerAndMetadataResolver(lookup PublishTargetLookup, provisioner VolumeProvisioner, resolver KubernetesMetadataResolver) *ControllerServer {
	return &ControllerServer{lookup: lookup, provisioner: provisioner, metadataResolver: resolver}
}

func NewControllerServerWithProvisionerMetadataAndRegistrar(lookup PublishTargetLookup, provisioner VolumeProvisioner, resolver KubernetesMetadataResolver, registrar VolumeObjectRegistrar) *ControllerServer {
	return &ControllerServer{lookup: lookup, provisioner: provisioner, metadataResolver: resolver, registrar: registrar}
}

func NewControllerServerWithProvisionerMetadataRegistrarAndSnapshotter(lookup PublishTargetLookup, provisioner VolumeProvisioner, resolver KubernetesMetadataResolver, registrar VolumeObjectRegistrar, snapshotter SnapshotProvisioner) *ControllerServer {
	return &ControllerServer{lookup: lookup, provisioner: provisioner, metadataResolver: resolver, registrar: registrar, snapshotter: snapshotter}
}

func (s *ControllerServer) CreateVolume(ctx context.Context, req *csipb.CreateVolumeRequest) (*csipb.CreateVolumeResponse, error) {
	if s.provisioner == nil {
		return nil, status.Error(codes.Unimplemented, "dynamic provisioning is not configured")
	}
	spec, err := volumeSpecFromCreateRequest(req)
	if err != nil {
		return nil, err
	}
	if source := req.GetVolumeContentSource(); source != nil {
		snapshotSource := source.GetSnapshot()
		if snapshotSource == nil || snapshotSource.GetSnapshotId() == "" {
			return nil, status.Error(codes.InvalidArgument, "only snapshot volume content sources are supported")
		}
		if s.snapshotter == nil {
			return nil, status.Error(codes.InvalidArgument, "snapshot restore is not configured")
		}
		snapshotSpec, err := s.snapshotter.GetSnapshot(ctx, snapshotSource.GetSnapshotId())
		if err != nil {
			return nil, snapshotCSIError("get source snapshot", err)
		}
		if snapshotSpec.State != SnapshotStateReady {
			return nil, status.Errorf(codes.Aborted, "snapshot %q is not ready", snapshotSpec.SnapshotID)
		}
		if !snapshotSizeFitsCapacityRange(req.GetCapacityRange(), snapshotSpec.SizeBytes) {
			return nil, status.Errorf(codes.OutOfRange, "snapshot capacity %d is outside the requested capacity range", snapshotSpec.SizeBytes)
		}
		spec.SizeBytes = snapshotSpec.SizeBytes
		spec.SourceSnapshotID = snapshotSpec.SnapshotID
	}
	if err := s.resolveKubernetesMetadata(ctx, &spec); err != nil {
		return nil, err
	}
	log.Printf("blockcsi: CreateVolume volume=%q protocol=%q replication_factor=%d pvc=%q namespace=%q", spec.VolumeID, spec.Protocol, spec.ReplicationFactor, spec.PVCName, spec.PVCNamespace)
	created, err := s.provisioner.CreateVolume(ctx, spec)
	if err != nil {
		if errors.Is(err, ErrVolumeConflict) {
			return nil, status.Errorf(codes.AlreadyExists, "volume %q already exists with different spec", spec.VolumeID)
		}
		if code := status.Code(err); code != codes.Unknown {
			return nil, status.Errorf(code, "create volume intent: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "create volume intent: %v", err)
	}
	if !createdVolumeIdentityMatches(spec, created) {
		return nil, status.Errorf(codes.Internal, "create volume intent returned mismatched identity for %q", spec.VolumeID)
	}
	if s.registrar != nil {
		if err := s.registrar.EnsureVolumeObject(ctx, created); err != nil {
			return nil, status.Errorf(codes.Internal, "ensure SwBlockVolume object: %v", err)
		}
	}
	if created.SourceSnapshotID != "" {
		if err := s.snapshotter.RestoreSnapshot(ctx, created.SourceSnapshotID, created.VolumeID); err != nil {
			return nil, snapshotRestoreCSIError(err)
		}
	}
	protocol := normalizeProtocol(created.Protocol)
	volumeContext := map[string]string{
		"replicationFactor": strconv.Itoa(created.ReplicationFactor),
		"protocol":          string(protocol),
	}
	if protocol == ProtocolNVMe {
		volumeContext["nvmeTransport"] = string(normalizeFrontendTransport(protocol, created.FrontendTransport))
	}
	if iscsiMultipathFromContext(req.GetParameters()) {
		volumeContext["stage2_multipath"] = "true"
	}
	return &csipb.CreateVolumeResponse{
		Volume: &csipb.Volume{
			VolumeId:      created.VolumeID,
			CapacityBytes: int64(created.SizeBytes),
			VolumeContext: volumeContext,
			ContentSource: req.GetVolumeContentSource(),
		},
	}, nil
}

func (s *ControllerServer) CreateSnapshot(ctx context.Context, req *csipb.CreateSnapshotRequest) (*csipb.CreateSnapshotResponse, error) {
	if s.snapshotter == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot service is not configured")
	}
	if req.GetName() == "" || req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name and source volume ID are required")
	}
	snapshotSpec, err := s.snapshotter.CreateSnapshot(ctx, req.GetName(), req.GetSourceVolumeId())
	if err != nil {
		return nil, snapshotCSIError("create snapshot", err)
	}
	return &csipb.CreateSnapshotResponse{Snapshot: csiSnapshot(snapshotSpec)}, nil
}

func (s *ControllerServer) DeleteSnapshot(ctx context.Context, req *csipb.DeleteSnapshotRequest) (*csipb.DeleteSnapshotResponse, error) {
	if s.snapshotter == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot service is not configured")
	}
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}
	if err := s.snapshotter.DeleteSnapshot(ctx, req.GetSnapshotId()); err != nil {
		return nil, snapshotCSIError("delete snapshot", err)
	}
	return &csipb.DeleteSnapshotResponse{}, nil
}

func (s *ControllerServer) ListSnapshots(ctx context.Context, req *csipb.ListSnapshotsRequest) (*csipb.ListSnapshotsResponse, error) {
	if s.snapshotter == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot service is not configured")
	}
	if req.GetMaxEntries() < 0 {
		return nil, status.Error(codes.InvalidArgument, "max_entries must be non-negative")
	}
	snapshots, err := s.snapshotter.ListSnapshots(ctx, req.GetSourceVolumeId())
	if err != nil {
		return nil, snapshotCSIError("list snapshots", err)
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].SnapshotID < snapshots[j].SnapshotID })
	if req.GetSnapshotId() != "" {
		filtered := snapshots[:0]
		for _, item := range snapshots {
			if item.SnapshotID == req.GetSnapshotId() {
				filtered = append(filtered, item)
			}
		}
		snapshots = filtered
	}
	startAfter := ""
	if req.GetStartingToken() != "" {
		token, err := decodeSnapshotListToken(req.GetStartingToken())
		if err != nil || token.SourceVolumeID != req.GetSourceVolumeId() || token.SnapshotID != req.GetSnapshotId() {
			return nil, status.Error(codes.Aborted, "invalid snapshot starting token")
		}
		startAfter = token.LastSnapshotID
	}
	start := sort.Search(len(snapshots), func(i int) bool { return snapshots[i].SnapshotID > startAfter })
	snapshots = snapshots[start:]
	limit := len(snapshots)
	if req.GetMaxEntries() > 0 && int(req.GetMaxEntries()) < limit {
		limit = int(req.GetMaxEntries())
	}
	response := &csipb.ListSnapshotsResponse{Entries: make([]*csipb.ListSnapshotsResponse_Entry, 0, limit)}
	for _, item := range snapshots[:limit] {
		response.Entries = append(response.Entries, &csipb.ListSnapshotsResponse_Entry{Snapshot: csiSnapshot(item)})
	}
	if limit < len(snapshots) && limit > 0 {
		nextToken, err := encodeSnapshotListToken(snapshotListToken{
			Version: snapshotListTokenVersion, SourceVolumeID: req.GetSourceVolumeId(), SnapshotID: req.GetSnapshotId(), LastSnapshotID: snapshots[limit-1].SnapshotID,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode snapshot continuation: %v", err)
		}
		response.NextToken = nextToken
	}
	return response, nil
}

func (s *ControllerServer) resolveKubernetesMetadata(ctx context.Context, spec *VolumeSpec) error {
	if s.metadataResolver == nil || spec == nil || spec.PVCUID != "" || spec.PVCName == "" || spec.PVCNamespace == "" {
		return nil
	}
	uid, err := s.metadataResolver.ResolvePVCUID(ctx, spec.PVCName, spec.PVCNamespace)
	if err != nil {
		return status.Errorf(codes.Internal, "lookup pvc uid: %v", err)
	}
	if uid == "" {
		return status.Errorf(codes.Internal, "lookup pvc uid: empty uid for %s/%s", spec.PVCNamespace, spec.PVCName)
	}
	spec.PVCUID = uid
	return nil
}

func (s *ControllerServer) DeleteVolume(ctx context.Context, req *csipb.DeleteVolumeRequest) (*csipb.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if s.provisioner == nil {
		return nil, status.Error(codes.Unimplemented, "dynamic provisioning is not configured")
	}
	if err := s.provisioner.DeleteVolume(ctx, req.GetVolumeId()); err != nil {
		if code := status.Code(err); code != codes.Unknown {
			return nil, status.Errorf(code, "delete volume intent: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "delete volume intent: %v", err)
	}
	return &csipb.DeleteVolumeResponse{}, nil
}

func (s *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csipb.ControllerPublishVolumeRequest) (*csipb.ControllerPublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node ID is required")
	}
	if s.lookup == nil {
		return nil, status.Error(codes.FailedPrecondition, "publish target lookup is not configured")
	}
	target, err := s.lookup.LookupPublishTarget(ctx, req.GetVolumeId(), req.GetNodeId())
	if err != nil {
		if errors.Is(err, ErrPublishTargetNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q has no publish target", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "lookup publish target: %v", err)
	}
	pubCtx := publishContext(target)
	if iscsiMultipathFromContext(req.GetVolumeContext()) {
		target, pubCtx, err = s.waitForControllerMultipathPublishContext(ctx, req.GetVolumeId(), req.GetNodeId(), target, pubCtx, 2)
		if err != nil {
			return nil, err
		}
	}
	if len(pubCtx) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %q has no attachable frontend target", req.GetVolumeId())
	}
	if iscsiMultipathFromContext(req.GetVolumeContext()) {
		pubCtx["stage2_multipath"] = "true"
	}
	return &csipb.ControllerPublishVolumeResponse{PublishContext: pubCtx}, nil
}

func (s *ControllerServer) waitForControllerMultipathPublishContext(ctx context.Context, volumeID, nodeID string, target PublishTarget, pubCtx map[string]string, minPaths int) (PublishTarget, map[string]string, error) {
	if hasControllerMultipathPaths(pubCtx, minPaths) {
		return target, pubCtx, nil
	}
	if s.lookup == nil {
		return target, pubCtx, status.Errorf(codes.FailedPrecondition, "volume %q multipath publish target requires at least %d paths", volumeID, minPaths)
	}
	deadline := time.NewTimer(stage2MultipathPublishWait)
	defer deadline.Stop()
	ticker := time.NewTicker(stage2MultipathPublishPoll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return target, pubCtx, status.Errorf(codes.FailedPrecondition, "volume %q multipath publish target did not reach %d paths: %v", volumeID, minPaths, ctx.Err())
		case <-deadline.C:
			return target, pubCtx, status.Errorf(codes.FailedPrecondition, "volume %q multipath publish target did not reach %d paths", volumeID, minPaths)
		case <-ticker.C:
		}

		refreshedTarget, err := s.lookup.LookupPublishTarget(ctx, volumeID, nodeID)
		if err != nil {
			continue
		}
		refreshedCtx := publishContext(refreshedTarget)
		if len(refreshedCtx) == 0 {
			continue
		}
		target, pubCtx = refreshedTarget, refreshedCtx
		if hasControllerMultipathPaths(pubCtx, minPaths) {
			return target, pubCtx, nil
		}
	}
}

func hasControllerMultipathPaths(ctx map[string]string, minPaths int) bool {
	if !iscsiMultipathFromContext(ctx) {
		return false
	}
	if len(iscsiPortalsFromContext(ctx)) >= minPaths {
		return true
	}
	return len(nvmeAddrsFromContext(ctx)) >= minPaths
}

func (s *ControllerServer) ControllerUnpublishVolume(context.Context, *csipb.ControllerUnpublishVolumeRequest) (*csipb.ControllerUnpublishVolumeResponse, error) {
	return &csipb.ControllerUnpublishVolumeResponse{}, nil
}

func (s *ControllerServer) ControllerGetCapabilities(context.Context, *csipb.ControllerGetCapabilitiesRequest) (*csipb.ControllerGetCapabilitiesResponse, error) {
	caps := []csipb.ControllerServiceCapability_RPC_Type{
		csipb.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	}
	if s.provisioner != nil {
		caps = append(caps, csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME)
	}
	if s.snapshotter != nil && s.provisioner != nil {
		caps = append(caps,
			csipb.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			csipb.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		)
	}
	out := make([]*csipb.ControllerServiceCapability, 0, len(caps))
	for _, capType := range caps {
		out = append(out, &csipb.ControllerServiceCapability{
			Type: &csipb.ControllerServiceCapability_Rpc{
				Rpc: &csipb.ControllerServiceCapability_RPC{Type: capType},
			},
		})
	}
	return &csipb.ControllerGetCapabilitiesResponse{Capabilities: out}, nil
}

func csiSnapshot(spec SnapshotSpec) *csipb.Snapshot {
	return &csipb.Snapshot{
		SnapshotId:     spec.SnapshotID,
		SourceVolumeId: spec.SourceVolumeID,
		SizeBytes:      int64(spec.SizeBytes),
		CreationTime:   timestamppb.New(spec.CreatedAt),
		ReadyToUse:     spec.State == SnapshotStateReady,
	}
}

func snapshotCSIError(operation string, err error) error {
	if code := status.Code(err); code != codes.Unknown {
		return status.Errorf(code, "%s: %v", operation, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", operation, err)
}

func snapshotRestoreCSIError(err error) error {
	switch status.Code(err) {
	case codes.FailedPrecondition, codes.Aborted:
		return status.Errorf(codes.Aborted, "snapshot restore is not ready: %v", err)
	case codes.Unavailable, codes.DeadlineExceeded:
		return status.Errorf(status.Code(err), "restore snapshot: %v", err)
	case codes.InvalidArgument, codes.NotFound:
		return status.Errorf(status.Code(err), "restore snapshot: %v", err)
	default:
		return status.Errorf(codes.Internal, "restore snapshot: %v", err)
	}
}

func snapshotSizeFitsCapacityRange(capacity *csipb.CapacityRange, snapshotSize uint64) bool {
	if snapshotSize == 0 || snapshotSize > uint64(^uint64(0)>>1) {
		return false
	}
	size := int64(snapshotSize)
	return (capacity.GetRequiredBytes() <= 0 || size >= capacity.GetRequiredBytes()) &&
		(capacity.GetLimitBytes() <= 0 || size <= capacity.GetLimitBytes())
}

func createdVolumeIdentityMatches(requested, created VolumeSpec) bool {
	return created.VolumeID == requested.VolumeID &&
		created.SizeBytes == requested.SizeBytes &&
		created.ReplicationFactor == requested.ReplicationFactor &&
		normalizeProtocol(created.Protocol) == normalizeProtocol(requested.Protocol) &&
		normalizeFrontendTransport(normalizeProtocol(created.Protocol), created.FrontendTransport) == normalizeFrontendTransport(normalizeProtocol(requested.Protocol), requested.FrontendTransport) &&
		created.SourceSnapshotID == requested.SourceSnapshotID
}

func encodeSnapshotListToken(token snapshotListToken) (string, error) {
	raw, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeSnapshotListToken(encoded string) (snapshotListToken, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return snapshotListToken{}, err
	}
	var token snapshotListToken
	if err := json.Unmarshal(raw, &token); err != nil {
		return snapshotListToken{}, err
	}
	if token.Version != snapshotListTokenVersion || token.LastSnapshotID == "" {
		return snapshotListToken{}, errors.New("unsupported or incomplete snapshot list token")
	}
	return token, nil
}

func (s *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csipb.ValidateVolumeCapabilitiesRequest) (*csipb.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	if s.lookup == nil {
		return nil, status.Error(codes.FailedPrecondition, "publish target lookup is not configured")
	}
	if _, err := s.lookup.LookupPublishTarget(ctx, req.GetVolumeId(), ""); err != nil {
		if errors.Is(err, ErrPublishTargetNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q has no publish target", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "lookup publish target: %v", err)
	}
	return &csipb.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csipb.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

func volumeSpecFromCreateRequest(req *csipb.CreateVolumeRequest) (VolumeSpec, error) {
	if req.GetName() == "" {
		return VolumeSpec{}, status.Error(codes.InvalidArgument, "volume name is required")
	}
	size := req.GetCapacityRange().GetRequiredBytes()
	if size <= 0 {
		size = req.GetCapacityRange().GetLimitBytes()
	}
	if size <= 0 {
		return VolumeSpec{}, status.Error(codes.InvalidArgument, "capacity is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return VolumeSpec{}, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	if !supportsVolumeCapabilities(req.GetVolumeCapabilities()) {
		return VolumeSpec{}, status.Error(codes.InvalidArgument, "unsupported volume capability")
	}
	rf := 1
	if raw := req.GetParameters()["replicationFactor"]; raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			return VolumeSpec{}, status.Errorf(codes.InvalidArgument, "invalid replicationFactor %q", raw)
		}
		rf = v
	}
	protocol, err := protocolFromParameters(req.GetParameters())
	if err != nil {
		return VolumeSpec{}, err
	}
	frontendTransport, err := frontendTransportFromParameters(protocol, req.GetParameters())
	if err != nil {
		return VolumeSpec{}, err
	}
	return VolumeSpec{
		VolumeID:          req.GetName(),
		SizeBytes:         uint64(size),
		ReplicationFactor: rf,
		Protocol:          protocol,
		FrontendTransport: frontendTransport,
		PVCName:           req.GetParameters()["csi.storage.k8s.io/pvc/name"],
		PVCNamespace:      req.GetParameters()["csi.storage.k8s.io/pvc/namespace"],
		PVCUID:            req.GetParameters()["csi.storage.k8s.io/pvc/uid"],
		PVName:            req.GetParameters()["csi.storage.k8s.io/pv/name"],
		StorageClass:      req.GetParameters()["csi.storage.k8s.io/storageclass/name"],
	}, nil
}

func frontendTransportFromParameters(protocol Protocol, params map[string]string) (FrontendTransport, error) {
	raw := params[storageClassNVMeTransportParameter]
	if protocol != ProtocolNVMe {
		if raw != "" {
			return "", status.Errorf(codes.InvalidArgument, "%s is valid only with protocol=nvme", storageClassNVMeTransportParameter)
		}
		return "", nil
	}
	if raw == "" {
		return FrontendTransportTCP, nil
	}
	switch FrontendTransport(raw) {
	case FrontendTransportTCP, FrontendTransportRDMA:
		return FrontendTransport(raw), nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "invalid NVMe frontend transport %q", raw)
	}
}

func protocolFromParameters(params map[string]string) (Protocol, error) {
	protocol := Protocol("")
	source := ""
	for _, key := range []string{
		storageClassProtocolParameter,
		"protocol",
		"frontendProtocol",
	} {
		raw := params[key]
		if raw == "" {
			continue
		}
		switch Protocol(raw) {
		case ProtocolISCSI, ProtocolNVMe:
			if protocol != "" && protocol != Protocol(raw) {
				return "", status.Errorf(codes.InvalidArgument, "conflicting protocol parameters %q=%q and %q=%q", source, protocol, key, raw)
			}
			protocol = Protocol(raw)
			source = key
		default:
			return "", status.Errorf(codes.InvalidArgument, "invalid protocol %q", raw)
		}
	}
	if protocol != "" {
		return protocol, nil
	}
	return ProtocolISCSI, nil
}

func supportsVolumeCapabilities(caps []*csipb.VolumeCapability) bool {
	for _, cap := range caps {
		if cap == nil {
			return false
		}
		switch cap.GetAccessType().(type) {
		case *csipb.VolumeCapability_Mount:
		case *csipb.VolumeCapability_Block:
		default:
			return false
		}
		mode := cap.GetAccessMode()
		if mode == nil || mode.GetMode() != csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return false
		}
	}
	return true
}
