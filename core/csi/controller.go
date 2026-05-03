package csi

import (
	"context"
	"errors"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ControllerServer struct {
	csipb.UnimplementedControllerServer
	lookup PublishTargetLookup
}

func NewControllerServer(lookup PublishTargetLookup) *ControllerServer {
	return &ControllerServer{lookup: lookup}
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
