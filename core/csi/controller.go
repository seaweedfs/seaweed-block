package csi

import (
	"context"
	"errors"
	"log"
	"strconv"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const storageClassProtocolParameter = "sw-block.seaweedfs.com/protocol"

type ControllerServer struct {
	csipb.UnimplementedControllerServer
	lookup           PublishTargetLookup
	provisioner      VolumeProvisioner
	metadataResolver KubernetesMetadataResolver
	registrar        VolumeObjectRegistrar
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

func (s *ControllerServer) CreateVolume(ctx context.Context, req *csipb.CreateVolumeRequest) (*csipb.CreateVolumeResponse, error) {
	if s.provisioner == nil {
		return nil, status.Error(codes.Unimplemented, "dynamic provisioning is not configured")
	}
	spec, err := volumeSpecFromCreateRequest(req)
	if err != nil {
		return nil, err
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
		return nil, status.Errorf(codes.Internal, "create volume intent: %v", err)
	}
	if s.registrar != nil {
		if err := s.registrar.EnsureVolumeObject(ctx, created); err != nil {
			return nil, status.Errorf(codes.Internal, "ensure SwBlockVolume object: %v", err)
		}
	}
	protocol := normalizeProtocol(created.Protocol)
	volumeContext := map[string]string{
		"replicationFactor": strconv.Itoa(created.ReplicationFactor),
		"protocol":          string(protocol),
	}
	if iscsiMultipathFromContext(req.GetParameters()) {
		volumeContext["stage2_multipath"] = "true"
	}
	return &csipb.CreateVolumeResponse{
		Volume: &csipb.Volume{
			VolumeId:      created.VolumeID,
			CapacityBytes: int64(created.SizeBytes),
			VolumeContext: volumeContext,
		},
	}, nil
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
	return VolumeSpec{
		VolumeID:          req.GetName(),
		SizeBytes:         uint64(size),
		ReplicationFactor: rf,
		Protocol:          protocol,
		PVCName:           req.GetParameters()["csi.storage.k8s.io/pvc/name"],
		PVCNamespace:      req.GetParameters()["csi.storage.k8s.io/pvc/namespace"],
		PVCUID:            req.GetParameters()["csi.storage.k8s.io/pvc/uid"],
		PVName:            req.GetParameters()["csi.storage.k8s.io/pv/name"],
		StorageClass:      req.GetParameters()["csi.storage.k8s.io/storageclass/name"],
	}, nil
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
