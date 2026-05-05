package csi

import (
	"context"
	"errors"
	"strconv"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ControllerServer struct {
	csipb.UnimplementedControllerServer
	lookup      PublishTargetLookup
	provisioner VolumeProvisioner
}

func NewControllerServer(lookup PublishTargetLookup) *ControllerServer {
	return &ControllerServer{lookup: lookup}
}

func NewControllerServerWithProvisioner(lookup PublishTargetLookup, provisioner VolumeProvisioner) *ControllerServer {
	return &ControllerServer{lookup: lookup, provisioner: provisioner}
}

func (s *ControllerServer) CreateVolume(ctx context.Context, req *csipb.CreateVolumeRequest) (*csipb.CreateVolumeResponse, error) {
	if s.provisioner == nil {
		return nil, status.Error(codes.Unimplemented, "dynamic provisioning is not configured")
	}
	spec, err := volumeSpecFromCreateRequest(req)
	if err != nil {
		return nil, err
	}
	created, err := s.provisioner.CreateVolume(ctx, spec)
	if err != nil {
		if errors.Is(err, ErrVolumeConflict) {
			return nil, status.Errorf(codes.AlreadyExists, "volume %q already exists with different spec", spec.VolumeID)
		}
		return nil, status.Errorf(codes.Internal, "create volume intent: %v", err)
	}
	return &csipb.CreateVolumeResponse{
		Volume: &csipb.Volume{
			VolumeId:      created.VolumeID,
			CapacityBytes: int64(created.SizeBytes),
			VolumeContext: map[string]string{
				"replicationFactor": strconv.Itoa(created.ReplicationFactor),
			},
		},
	}, nil
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
	if len(pubCtx) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %q has no attachable frontend target", req.GetVolumeId())
	}
	return &csipb.ControllerPublishVolumeResponse{PublishContext: pubCtx}, nil
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
	return VolumeSpec{
		VolumeID:          req.GetName(),
		SizeBytes:         uint64(size),
		ReplicationFactor: rf,
		PVCName:           req.GetParameters()["csi.storage.k8s.io/pvc/name"],
		PVCNamespace:      req.GetParameters()["csi.storage.k8s.io/pvc/namespace"],
		PVCUID:            req.GetParameters()["csi.storage.k8s.io/pvc/uid"],
		PVName:            req.GetParameters()["csi.storage.k8s.io/pv/name"],
	}, nil
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
