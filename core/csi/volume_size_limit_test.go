package csi

import (
	"context"
	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
)

func TestCSICreateVolumeCapacityCapBeforeProvision(t *testing.T) {
	const capBytes = int64(17592186040320)
	p := &stubProvisioner{}
	s := NewControllerServerWithProvisioner(&stubLookup{}, p)
	req := &csipb.CreateVolumeRequest{Name: "v", CapacityRange: &csipb.CapacityRange{RequiredBytes: capBytes}, VolumeCapabilities: []*csipb.VolumeCapability{testVolumeCapability()}}
	if _, err := s.CreateVolume(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	p.calls = nil
	req.CapacityRange.RequiredBytes = capBytes + 4096
	_, err := s.CreateVolume(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "16 TiB - 4 KiB") {
		t.Fatalf("want clear InvalidArgument, got %v", err)
	}
	if len(p.calls) != 0 {
		t.Fatal("oversized request reached provisioner")
	}
}
