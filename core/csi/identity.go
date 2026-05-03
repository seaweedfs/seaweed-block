package csi

import (
	"context"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	DriverName    = "block.csi.seaweedfs.com"
	DriverVersion = "0.1.0"
)

type IdentityServer struct {
	csipb.UnimplementedIdentityServer
}

func NewIdentityServer() *IdentityServer {
	return &IdentityServer{}
}

func (s *IdentityServer) GetPluginInfo(context.Context, *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	return &csipb.GetPluginInfoResponse{
		Name:          DriverName,
		VendorVersion: DriverVersion,
	}, nil
}

func (s *IdentityServer) GetPluginCapabilities(context.Context, *csipb.GetPluginCapabilitiesRequest) (*csipb.GetPluginCapabilitiesResponse, error) {
	return &csipb.GetPluginCapabilitiesResponse{
		Capabilities: []*csipb.PluginCapability{{
			Type: &csipb.PluginCapability_Service_{
				Service: &csipb.PluginCapability_Service{
					Type: csipb.PluginCapability_Service_CONTROLLER_SERVICE,
				},
			},
		}},
	}, nil
}

func (s *IdentityServer) Probe(context.Context, *csipb.ProbeRequest) (*csipb.ProbeResponse, error) {
	return &csipb.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
