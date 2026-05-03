package csi

import (
	"context"
	"errors"
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
