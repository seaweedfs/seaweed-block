package csi

import (
	"context"
	"errors"
	"fmt"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stubLookup struct {
	target PublishTarget
	err    error
	calls  []string
}

func (s *stubLookup) LookupPublishTarget(_ context.Context, volumeID, nodeID string) (PublishTarget, error) {
	s.calls = append(s.calls, volumeID+":"+nodeID)
	if s.err != nil {
		return PublishTarget{}, s.err
	}
	return s.target, nil
}

type stubProvisioner struct {
	created VolumeSpec
	err     error
	calls   []VolumeSpec
	deletes []string
}

func (s *stubProvisioner) CreateVolume(_ context.Context, spec VolumeSpec) (VolumeSpec, error) {
	s.calls = append(s.calls, spec)
	if s.err != nil {
		return VolumeSpec{}, s.err
	}
	if s.created.VolumeID != "" {
		return s.created, nil
	}
	return spec, nil
}

func (s *stubProvisioner) DeleteVolume(_ context.Context, volumeID string) error {
	s.deletes = append(s.deletes, volumeID)
	return s.err
}

type stubMetadataResolver struct {
	uid   string
	err   error
	calls []string
}

func (s *stubMetadataResolver) ResolvePVCUID(_ context.Context, name, namespace string) (string, error) {
	s.calls = append(s.calls, namespace+"/"+name)
	return s.uid, s.err
}

func TestControllerPublish_ReturnsISCSIPublishContextFromTargetFact(t *testing.T) {
	lookup := &stubLookup{target: PublishTarget{
		VolumeID:  "v1",
		ReplicaID: "r1",
		Protocol:  ProtocolISCSI,
		ISCSIAddr: "127.0.0.1:3260",
		IQN:       "iqn.2026-05.example.v3:v1",
	}}
	s := NewControllerServer(lookup)

	resp, err := s.ControllerPublishVolume(context.Background(), &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	if got := resp.GetPublishContext()["iscsiAddr"]; got != "127.0.0.1:3260" {
		t.Fatalf("iscsiAddr=%q", got)
	}
	if got := resp.GetPublishContext()["iqn"]; got != "iqn.2026-05.example.v3:v1" {
		t.Fatalf("iqn=%q", got)
	}
	if len(lookup.calls) != 1 || lookup.calls[0] != "v1:node-a" {
		t.Fatalf("lookup calls=%v", lookup.calls)
	}
}

func TestControllerPublish_FailsClosedWithoutVerifiedTarget(t *testing.T) {
	s := NewControllerServer(&stubLookup{err: ErrPublishTargetNotFound})
	_, err := s.ControllerPublishVolume(context.Background(), &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Fatalf("code=%v want NotFound", st.Code())
	}
}

func TestControllerPublish_FailsClosedWhenTargetHasNoFrontendFact(t *testing.T) {
	s := NewControllerServer(&stubLookup{target: PublishTarget{VolumeID: "v1", ReplicaID: "r1"}})
	_, err := s.ControllerPublishVolume(context.Background(), &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code=%v want FailedPrecondition", st.Code())
	}
}

func TestControllerPublish_PropagatesLookupErrorsAsInternal(t *testing.T) {
	s := NewControllerServer(&stubLookup{err: errors.New("backend down")})
	_, err := s.ControllerPublishVolume(context.Background(), &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("code=%v want Internal", st.Code())
	}
}

func TestControllerCapabilities_G15aDoesNotAdvertiseDynamicProvisioning(t *testing.T) {
	s := NewControllerServer(&stubLookup{})
	resp, err := s.ControllerGetCapabilities(context.Background(), &csipb.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities: %v", err)
	}
	got := map[csipb.ControllerServiceCapability_RPC_Type]bool{}
	for _, cap := range resp.GetCapabilities() {
		if rpc := cap.GetRpc(); rpc != nil {
			got[rpc.Type] = true
		}
	}
	if !got[csipb.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME] {
		t.Fatal("missing PUBLISH_UNPUBLISH_VOLUME")
	}
	for _, forbidden := range []csipb.ControllerServiceCapability_RPC_Type{
		csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csipb.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csipb.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	} {
		if got[forbidden] {
			t.Fatalf("G15a must not advertise %v", forbidden)
		}
	}
}

func TestG15c_ControllerCapabilities_AdvertiseDynamicProvisioningWhenConfigured(t *testing.T) {
	s := NewControllerServerWithProvisioner(&stubLookup{}, &stubProvisioner{})
	resp, err := s.ControllerGetCapabilities(context.Background(), &csipb.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities: %v", err)
	}
	got := map[csipb.ControllerServiceCapability_RPC_Type]bool{}
	for _, cap := range resp.GetCapabilities() {
		if rpc := cap.GetRpc(); rpc != nil {
			got[rpc.Type] = true
		}
	}
	if !got[csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME] {
		t.Fatal("missing CREATE_DELETE_VOLUME")
	}
}

func TestG15c_ControllerCreateVolume_RecordsDesiredIntentOnly(t *testing.T) {
	prov := &stubProvisioner{}
	s := NewControllerServerWithProvisioner(&stubLookup{}, prov)

	resp, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "pvc-a",
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: 1 << 30,
		},
		Parameters: map[string]string{
			"replicationFactor":                      "2",
			"csi.storage.k8s.io/pvc/name":            "demo-pvc",
			"csi.storage.k8s.io/pvc/namespace":       "demo-ns",
			"csi.storage.k8s.io/pv/name":             "pvc-a",
			"csi.storage.k8s.io/pvc/uid":             "uid-123",
			"csi.storage.k8s.io/serviceAccount.name": "ignored",
		},
		VolumeCapabilities: []*csipb.VolumeCapability{
			testVolumeCapability(),
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if len(prov.calls) != 1 {
		t.Fatalf("provisioner calls=%d want 1", len(prov.calls))
	}
	if got := prov.calls[0]; got.VolumeID != "pvc-a" || got.SizeBytes != 1<<30 || got.ReplicationFactor != 2 {
		t.Fatalf("spec=%+v", got)
	}
	if got := prov.calls[0]; got.PVCName != "demo-pvc" || got.PVCNamespace != "demo-ns" || got.PVCUID != "uid-123" || got.PVName != "pvc-a" {
		t.Fatalf("kubernetes metadata not preserved: %+v", got)
	}
	vol := resp.GetVolume()
	if vol.GetVolumeId() != "pvc-a" || vol.GetCapacityBytes() != 1<<30 {
		t.Fatalf("volume=%+v", vol)
	}
	if err := authorityContextGuard(vol.GetVolumeContext()); err != nil {
		t.Fatal(err)
	}
}

func TestG15c_ControllerCreateVolume_ResolvesPVCUIDWhenConfigured(t *testing.T) {
	prov := &stubProvisioner{}
	resolver := &stubMetadataResolver{uid: "uid-from-api"}
	s := NewControllerServerWithProvisionerAndMetadataResolver(&stubLookup{}, prov, resolver)

	_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "pvc-a",
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: 1 << 30,
		},
		Parameters: map[string]string{
			"replicationFactor":                "1",
			"csi.storage.k8s.io/pvc/name":      "demo-pvc",
			"csi.storage.k8s.io/pvc/namespace": "demo-ns",
			"csi.storage.k8s.io/pv/name":       "pvc-a",
		},
		VolumeCapabilities: []*csipb.VolumeCapability{
			testVolumeCapability(),
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if len(resolver.calls) != 1 || resolver.calls[0] != "demo-ns/demo-pvc" {
		t.Fatalf("resolver calls=%v", resolver.calls)
	}
	if got := prov.calls[0]; got.PVCUID != "uid-from-api" {
		t.Fatalf("pvc uid=%q", got.PVCUID)
	}
}

func TestG15c_ControllerCreateVolume_FailsWhenConfiguredPVCUIDLookupFails(t *testing.T) {
	prov := &stubProvisioner{}
	s := NewControllerServerWithProvisionerAndMetadataResolver(&stubLookup{}, prov, &stubMetadataResolver{err: errors.New("boom")})

	_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "pvc-a",
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: 1 << 30,
		},
		Parameters: map[string]string{
			"replicationFactor":                "1",
			"csi.storage.k8s.io/pvc/name":      "demo-pvc",
			"csi.storage.k8s.io/pvc/namespace": "demo-ns",
		},
		VolumeCapabilities: []*csipb.VolumeCapability{
			testVolumeCapability(),
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("code=%v want Internal", st.Code())
	}
	if len(prov.calls) != 0 {
		t.Fatalf("provisioner must not be called after uid lookup failure: %+v", prov.calls)
	}
}

func TestG15c_ControllerCreateVolume_RejectsMissingCapacity(t *testing.T) {
	s := NewControllerServerWithProvisioner(&stubLookup{}, &stubProvisioner{})
	_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "pvc-a",
		VolumeCapabilities: []*csipb.VolumeCapability{
			testVolumeCapability(),
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("code=%v want InvalidArgument", st.Code())
	}
}

func TestG15c_ControllerDeleteVolume_DelegatesToProvisioner(t *testing.T) {
	prov := &stubProvisioner{}
	s := NewControllerServerWithProvisioner(&stubLookup{}, prov)
	if _, err := s.DeleteVolume(context.Background(), &csipb.DeleteVolumeRequest{VolumeId: "pvc-a"}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if len(prov.deletes) != 1 || prov.deletes[0] != "pvc-a" {
		t.Fatalf("deletes=%v", prov.deletes)
	}
}

func TestValidateVolumeCapabilities_ConfirmsExistingTarget(t *testing.T) {
	s := NewControllerServer(&stubLookup{target: PublishTarget{VolumeID: "v1", ISCSIAddr: "127.0.0.1:3260", IQN: "iqn.x:v1"}})
	caps := []*csipb.VolumeCapability{testVolumeCapability()}
	resp, err := s.ValidateVolumeCapabilities(context.Background(), &csipb.ValidateVolumeCapabilitiesRequest{
		VolumeId:           "v1",
		VolumeCapabilities: caps,
	})
	if err != nil {
		t.Fatalf("ValidateVolumeCapabilities: %v", err)
	}
	if len(resp.GetConfirmed().GetVolumeCapabilities()) != 1 {
		t.Fatalf("confirmed caps=%d", len(resp.GetConfirmed().GetVolumeCapabilities()))
	}
}

func authorityContextGuard(ctx map[string]string) error {
	for _, k := range []string{"epoch", "endpointVersion", "assignment", "primary", "ready", "healthy"} {
		if _, ok := ctx[k]; ok {
			return fmt.Errorf("volume context must not carry authority-shaped field %q", k)
		}
	}
	return nil
}
