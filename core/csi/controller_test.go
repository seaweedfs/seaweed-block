package csi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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

type stubVolumeRegistrar struct {
	err   error
	calls []VolumeSpec
}

type stubSnapshotter struct {
	created      SnapshotSpec
	snapshots    []SnapshotSpec
	createErr    error
	deleteErr    error
	getErr       error
	listErr      error
	restoreErr   error
	createCalls  []string
	deleteCalls  []string
	restoreCalls []string
	getCalls     []string
	listSources  []string
}

func (s *stubSnapshotter) CreateSnapshot(_ context.Context, name, sourceVolumeID string) (SnapshotSpec, error) {
	s.createCalls = append(s.createCalls, name+"/"+sourceVolumeID)
	if s.createErr != nil {
		return SnapshotSpec{}, s.createErr
	}
	return s.created, nil
}

func (s *stubSnapshotter) DeleteSnapshot(_ context.Context, snapshotID string) error {
	s.deleteCalls = append(s.deleteCalls, snapshotID)
	return s.deleteErr
}

func (s *stubSnapshotter) GetSnapshot(_ context.Context, snapshotID string) (SnapshotSpec, error) {
	s.getCalls = append(s.getCalls, snapshotID)
	if s.getErr != nil {
		return SnapshotSpec{}, s.getErr
	}
	for _, item := range s.snapshots {
		if item.SnapshotID == snapshotID {
			return item, nil
		}
	}
	if s.created.SnapshotID == snapshotID {
		return s.created, nil
	}
	return SnapshotSpec{}, status.Error(codes.NotFound, "snapshot not found")
}

func (s *stubSnapshotter) ListSnapshots(_ context.Context, sourceVolumeID string) ([]SnapshotSpec, error) {
	s.listSources = append(s.listSources, sourceVolumeID)
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]SnapshotSpec, 0, len(s.snapshots))
	for _, item := range s.snapshots {
		if sourceVolumeID == "" || item.SourceVolumeID == sourceVolumeID {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *stubSnapshotter) RestoreSnapshot(_ context.Context, snapshotID, targetVolumeID string) error {
	s.restoreCalls = append(s.restoreCalls, snapshotID+"/"+targetVolumeID)
	return s.restoreErr
}

func (s *stubVolumeRegistrar) EnsureVolumeObject(_ context.Context, spec VolumeSpec) error {
	s.calls = append(s.calls, spec)
	return s.err
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

func TestControllerPublish_DoesNotExposeCHAPSecretsInPublishContext(t *testing.T) {
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
		Secrets: map[string]string{
			"chapUsername": "user1",
			"chapSecret":   "secret1",
		},
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	if _, ok := resp.GetPublishContext()["chapUsername"]; ok {
		t.Fatalf("chapUsername leaked in publish_context: %+v", resp.GetPublishContext())
	}
	if _, ok := resp.GetPublishContext()["chapSecret"]; ok {
		t.Fatalf("chapSecret leaked in publish_context: %+v", resp.GetPublishContext())
	}
}

func TestControllerPublish_DoesNotExposeAuthorityGenerationInPublishContext(t *testing.T) {
	lookup := &stubLookup{target: PublishTarget{
		VolumeID:        "v1",
		ReplicaID:       "r2",
		Epoch:           2,
		EndpointVersion: 4,
		Protocol:        ProtocolISCSI,
		ISCSIAddr:       "127.0.0.2:3260",
		IQN:             "iqn.2026-05.example.v3:v1-r2",
	}}
	s := NewControllerServer(lookup)

	resp, err := s.ControllerPublishVolume(context.Background(), &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	for _, key := range []string{"epoch", "endpointVersion", "replicaID", "primary", "ready", "healthy"} {
		if _, ok := resp.GetPublishContext()[key]; ok {
			t.Fatalf("authority field %q leaked in publish_context: %+v", key, resp.GetPublishContext())
		}
	}
}

func TestControllerPublish_CarriesStage2MultipathRequestFromVolumeContext(t *testing.T) {
	lookup := &stubLookup{target: PublishTarget{
		VolumeID:  "v1",
		ReplicaID: "r1",
		Protocol:  ProtocolNVMe,
		NVMeAddr:  "127.0.0.1:4420",
		NVMeAddrs: []string{"127.0.0.1:4420", "127.0.0.1:4421"},
		NQN:       "nqn.2026-05.io.seaweedfs:v1",
		NSID:      1,
		Multipath: true,
	}}
	s := NewControllerServer(lookup)

	resp, err := s.ControllerPublishVolume(context.Background(), &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
		VolumeContext: map[string]string{
			"stage2_multipath": "true",
		},
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	if got := resp.GetPublishContext()["stage2_multipath"]; got != "true" {
		t.Fatalf("stage2_multipath=%q context=%v", got, resp.GetPublishContext())
	}
}

func TestControllerPublish_Stage2MultipathWaitsForNVMeAddrsBeforePublishing(t *testing.T) {
	lookup := &sequenceLookup{targets: []PublishTarget{
		{
			VolumeID:  "v1",
			ReplicaID: "r1",
			Protocol:  ProtocolNVMe,
			NVMeAddr:  "127.0.0.1:4420",
			NQN:       "nqn.2026-05.io.seaweedfs:v1",
			NSID:      1,
		},
		{
			VolumeID:  "v1",
			ReplicaID: "r1",
			Protocol:  ProtocolNVMe,
			NVMeAddr:  "127.0.0.1:4420",
			NVMeAddrs: []string{"127.0.0.1:4420", "127.0.0.1:4421"},
			NQN:       "nqn.2026-05.io.seaweedfs:v1",
			NSID:      1,
			Multipath: true,
		},
	}}
	s := NewControllerServer(lookup)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := s.ControllerPublishVolume(ctx, &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
		VolumeContext: map[string]string{
			"stage2_multipath": "true",
		},
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	publish := resp.GetPublishContext()
	if publish["stage2_multipath"] != "true" {
		t.Fatalf("stage2_multipath=%q context=%v", publish["stage2_multipath"], publish)
	}
	if publish["nvmeAddrs"] != "127.0.0.1:4420,127.0.0.1:4421" {
		t.Fatalf("nvmeAddrs=%q context=%v", publish["nvmeAddrs"], publish)
	}
	if len(lookup.calls) < 2 {
		t.Fatalf("lookup calls=%v, want initial lookup plus multipath wait refresh", lookup.calls)
	}
}

func TestControllerPublish_Stage2MultipathFailsClosedWithSingleNVMeAddr(t *testing.T) {
	lookup := &stubLookup{target: PublishTarget{
		VolumeID:  "v1",
		ReplicaID: "r1",
		Protocol:  ProtocolNVMe,
		NVMeAddr:  "127.0.0.1:4420",
		NQN:       "nqn.2026-05.io.seaweedfs:v1",
		NSID:      1,
	}}
	s := NewControllerServer(lookup)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := s.ControllerPublishVolume(ctx, &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
		VolumeContext: map[string]string{
			"stage2_multipath": "true",
		},
	})
	if err == nil {
		t.Fatal("expected stage2 multipath to fail closed with one NVMe path")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code=%v want FailedPrecondition err=%v", st.Code(), err)
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
		Secrets: map[string]string{
			"chapUsername": "user1",
			"chapSecret":   "secret1",
		},
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

func TestPhase175ControllerSnapshotLifecycleAndPagination(t *testing.T) {
	now := time.Now().UTC()
	snapshotter := &stubSnapshotter{
		created: SnapshotSpec{SnapshotID: "snap-2", Name: "daily", SourceVolumeID: "vol-a", CreatedAt: now, State: SnapshotStateReady, SizeBytes: 1 << 20},
		snapshots: []SnapshotSpec{
			{SnapshotID: "snap-3", SourceVolumeID: "vol-a", CreatedAt: now, State: SnapshotStateReady, SizeBytes: 1 << 20},
			{SnapshotID: "snap-1", SourceVolumeID: "vol-a", CreatedAt: now, State: SnapshotStateReady, SizeBytes: 1 << 20},
			{SnapshotID: "snap-other", SourceVolumeID: "vol-b", CreatedAt: now, State: SnapshotStateReady, SizeBytes: 1 << 20},
		},
	}
	s := NewControllerServerWithProvisionerMetadataRegistrarAndSnapshotter(&stubLookup{}, &stubProvisioner{}, nil, nil, snapshotter)
	caps, err := s.ControllerGetCapabilities(context.Background(), &csipb.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	advertised := false
	for _, capability := range caps.GetCapabilities() {
		advertised = advertised || capability.GetRpc().GetType() == csipb.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT
	}
	if !advertised {
		t.Fatal("configured controller did not advertise CREATE_DELETE_SNAPSHOT")
	}
	created, err := s.CreateSnapshot(context.Background(), &csipb.CreateSnapshotRequest{Name: "daily", SourceVolumeId: "vol-a"})
	if err != nil || created.GetSnapshot().GetSnapshotId() != "snap-2" || !created.GetSnapshot().GetReadyToUse() {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	first, err := s.ListSnapshots(context.Background(), &csipb.ListSnapshotsRequest{SourceVolumeId: "vol-a", MaxEntries: 1})
	if err != nil || len(first.GetEntries()) != 1 || first.GetEntries()[0].GetSnapshot().GetSnapshotId() != "snap-1" || first.GetNextToken() == "" {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	second, err := s.ListSnapshots(context.Background(), &csipb.ListSnapshotsRequest{SourceVolumeId: "vol-a", StartingToken: first.GetNextToken(), MaxEntries: 1})
	if err != nil || len(second.GetEntries()) != 1 || second.GetEntries()[0].GetSnapshot().GetSnapshotId() != "snap-3" || second.GetNextToken() != "" {
		t.Fatalf("second page=%+v err=%v", second, err)
	}
	if _, err := s.DeleteSnapshot(context.Background(), &csipb.DeleteSnapshotRequest{SnapshotId: "snap-2"}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(snapshotter.createCalls) != "[daily/vol-a]" || fmt.Sprint(snapshotter.deleteCalls) != "[snap-2]" {
		t.Fatalf("create=%v delete=%v", snapshotter.createCalls, snapshotter.deleteCalls)
	}
}

func TestPhase175ControllerCreateVolumeRestoresSnapshotBeforeSuccess(t *testing.T) {
	snapshotter := &stubSnapshotter{snapshots: []SnapshotSpec{{
		SnapshotID: "snap-source", SourceVolumeID: "source-vol", CreatedAt: time.Now().UTC(), State: SnapshotStateReady, SizeBytes: 1 << 20,
	}}}
	provisioner := &stubProvisioner{}
	s := NewControllerServerWithProvisionerMetadataRegistrarAndSnapshotter(&stubLookup{}, provisioner, nil, nil, snapshotter)
	request := &csipb.CreateVolumeRequest{
		Name: "restored-vol", CapacityRange: &csipb.CapacityRange{RequiredBytes: 1 << 20},
		VolumeCapabilities:  []*csipb.VolumeCapability{testVolumeCapability()},
		VolumeContentSource: &csipb.VolumeContentSource{Type: &csipb.VolumeContentSource_Snapshot{Snapshot: &csipb.VolumeContentSource_SnapshotSource{SnapshotId: "snap-source"}}},
	}
	response, err := s.CreateVolume(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(provisioner.calls) != 1 || provisioner.calls[0].SourceSnapshotID != "snap-source" {
		t.Fatalf("provisioned=%+v", provisioner.calls)
	}
	if fmt.Sprint(snapshotter.restoreCalls) != "[snap-source/restored-vol]" {
		t.Fatalf("restore calls=%v", snapshotter.restoreCalls)
	}
	if response.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId() != "snap-source" {
		t.Fatalf("response=%+v", response)
	}
}

func TestPhase175ControllerRestoreStaysFailClosedUntilRuntimeReady(t *testing.T) {
	snapshotter := &stubSnapshotter{
		snapshots:  []SnapshotSpec{{SnapshotID: "snap-source", SourceVolumeID: "source-vol", CreatedAt: time.Now().UTC(), State: SnapshotStateReady, SizeBytes: 1 << 20}},
		restoreErr: status.Error(codes.FailedPrecondition, "targets not ready"),
	}
	provisioner := &stubProvisioner{}
	s := NewControllerServerWithProvisionerMetadataRegistrarAndSnapshotter(&stubLookup{}, provisioner, nil, nil, snapshotter)
	_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "restored-vol", CapacityRange: &csipb.CapacityRange{RequiredBytes: 1 << 20}, VolumeCapabilities: []*csipb.VolumeCapability{testVolumeCapability()},
		VolumeContentSource: &csipb.VolumeContentSource{Type: &csipb.VolumeContentSource_Snapshot{Snapshot: &csipb.VolumeContentSource_SnapshotSource{SnapshotId: "snap-source"}}},
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("error=%v", err)
	}
	if len(provisioner.calls) != 1 || fmt.Sprint(snapshotter.restoreCalls) != "[snap-source/restored-vol]" {
		t.Fatalf("provisioned=%v restore=%v", provisioner.calls, snapshotter.restoreCalls)
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
	if got := prov.calls[0]; got.Protocol != ProtocolISCSI {
		t.Fatalf("protocol=%q want iscsi", got.Protocol)
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

func TestPhase44_ControllerCreateVolume_RegistersSwBlockVolumeObjectAfterProvisioning(t *testing.T) {
	prov := &stubProvisioner{}
	registrar := &stubVolumeRegistrar{}
	s := NewControllerServerWithProvisionerMetadataAndRegistrar(&stubLookup{}, prov, nil, registrar)

	_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "pvc-a",
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: 1 << 30,
		},
		Parameters: map[string]string{
			"replicationFactor":                         "1",
			"csi.storage.k8s.io/pvc/name":               "demo-pvc",
			"csi.storage.k8s.io/pvc/namespace":          "default",
			"csi.storage.k8s.io/pvc/uid":                "uid-123",
			"csi.storage.k8s.io/pv/name":                "pvc-a",
			"csi.storage.k8s.io/storageclass/name":      "sw-block-dynamic",
			"csi.storage.k8s.io/serviceAccount.name":    "ignored",
			"csi.storage.k8s.io/serviceAccount.secrets": "ignored",
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
	if len(registrar.calls) != 1 {
		t.Fatalf("registrar calls=%d want 1", len(registrar.calls))
	}
	got := registrar.calls[0]
	if got.VolumeID != "pvc-a" || got.PVCName != "demo-pvc" || got.PVCNamespace != "default" || got.PVCUID != "uid-123" || got.StorageClass != "sw-block-dynamic" {
		t.Fatalf("registered spec=%+v", got)
	}
}

func TestPhase44_ControllerCreateVolume_FailsWhenSwBlockVolumeRegistrationFails(t *testing.T) {
	prov := &stubProvisioner{}
	registrar := &stubVolumeRegistrar{err: errors.New("api rejected object")}
	s := NewControllerServerWithProvisionerMetadataAndRegistrar(&stubLookup{}, prov, nil, registrar)

	_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "pvc-a",
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: 1 << 30,
		},
		Parameters: map[string]string{
			"replicationFactor":                "1",
			"csi.storage.k8s.io/pvc/name":      "demo-pvc",
			"csi.storage.k8s.io/pvc/namespace": "default",
		},
		VolumeCapabilities: []*csipb.VolumeCapability{
			testVolumeCapability(),
		},
	})
	if err == nil {
		t.Fatal("expected registration failure")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal || !strings.Contains(st.Message(), "ensure SwBlockVolume object") {
		t.Fatalf("error=%v", err)
	}
	if len(prov.calls) != 1 {
		t.Fatalf("provisioner should be called before registration: %d", len(prov.calls))
	}
}

func TestG15c_ControllerCreateVolume_RecordsProtocolSelection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{name: "product-prefixed", params: map[string]string{storageClassProtocolParameter: "nvme"}},
		{name: "legacy-protocol", params: map[string]string{"protocol": "nvme"}},
		{name: "frontendProtocol", params: map[string]string{"frontendProtocol": "nvme"}},
		{name: "matching-duplicate-keys", params: map[string]string{storageClassProtocolParameter: "nvme", "protocol": "nvme"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prov := &stubProvisioner{}
			s := NewControllerServerWithProvisioner(&stubLookup{}, prov)

			resp, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
				Name: "pvc-a",
				CapacityRange: &csipb.CapacityRange{
					RequiredBytes: 1 << 30,
				},
				Parameters: tc.params,
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
			if got := prov.calls[0].Protocol; got != ProtocolNVMe {
				t.Fatalf("protocol=%q want nvme", got)
			}
			if got := resp.GetVolume().GetVolumeContext()["protocol"]; got != "nvme" {
				t.Fatalf("response protocol=%q want nvme", got)
			}
		})
	}
}

func TestPhase165_ControllerCreateVolumeCarriesNVMERDMATransport(t *testing.T) {
	prov := &stubProvisioner{}
	s := NewControllerServerWithProvisioner(&stubLookup{}, prov)
	resp, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "pvc-rdma",
		CapacityRange:      &csipb.CapacityRange{RequiredBytes: 1 << 30},
		VolumeCapabilities: []*csipb.VolumeCapability{testVolumeCapability()},
		Parameters: map[string]string{
			storageClassProtocolParameter:      "nvme",
			storageClassNVMeTransportParameter: "rdma",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if got := prov.calls[0].FrontendTransport; got != FrontendTransportRDMA {
		t.Fatalf("provisioned transport=%q want rdma", got)
	}
	if got := resp.GetVolume().GetVolumeContext()["nvmeTransport"]; got != "rdma" {
		t.Fatalf("volume context transport=%q want rdma", got)
	}
}

func TestPhase165_ControllerCreateVolumeRejectsInvalidNVMeTransportUse(t *testing.T) {
	for _, params := range []map[string]string{
		{storageClassProtocolParameter: "iscsi", storageClassNVMeTransportParameter: "rdma"},
		{storageClassProtocolParameter: "nvme", storageClassNVMeTransportParameter: "bogus"},
	} {
		s := NewControllerServerWithProvisioner(&stubLookup{}, &stubProvisioner{})
		_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
			Name: "pvc-invalid", CapacityRange: &csipb.CapacityRange{RequiredBytes: 1 << 30},
			VolumeCapabilities: []*csipb.VolumeCapability{testVolumeCapability()}, Parameters: params,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("CreateVolume params=%v error=%v want InvalidArgument", params, err)
		}
	}
}

func TestControllerCreateVolume_CarriesStage2MultipathVolumeContext(t *testing.T) {
	prov := &stubProvisioner{}
	s := NewControllerServerWithProvisioner(nil, prov)
	resp, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "pvc-a",
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: 1048576,
		},
		VolumeCapabilities: []*csipb.VolumeCapability{testVolumeCapability()},
		Parameters: map[string]string{
			"protocol":          "nvme",
			"replicationFactor": "2",
			"stage2_multipath":  "true",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if got := resp.GetVolume().GetVolumeContext()["stage2_multipath"]; got != "true" {
		t.Fatalf("stage2_multipath=%q context=%v", got, resp.GetVolume().GetVolumeContext())
	}
}

func TestG15c_ControllerCreateVolume_RejectsConflictingProtocolParameters(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{
			name: "product-prefixed-vs-legacy",
			params: map[string]string{
				storageClassProtocolParameter: "nvme",
				"protocol":                    "iscsi",
			},
		},
		{
			name: "product-prefixed-vs-frontendProtocol",
			params: map[string]string{
				storageClassProtocolParameter: "iscsi",
				"frontendProtocol":            "nvme",
			},
		},
		{
			name: "legacy-vs-frontendProtocol",
			params: map[string]string{
				"protocol":         "nvme",
				"frontendProtocol": "iscsi",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewControllerServerWithProvisioner(&stubLookup{}, &stubProvisioner{})
			_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
				Name: "pvc-a",
				CapacityRange: &csipb.CapacityRange{
					RequiredBytes: 1 << 30,
				},
				Parameters: tc.params,
				VolumeCapabilities: []*csipb.VolumeCapability{
					testVolumeCapability(),
				},
			})
			if err == nil {
				t.Fatal("expected conflicting protocol parameters to fail")
			}
			st, _ := status.FromError(err)
			if st.Code() != codes.InvalidArgument {
				t.Fatalf("code=%v want InvalidArgument", st.Code())
			}
		})
	}
}

func TestG15c_ControllerCreateVolume_RejectsInvalidProtocol(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{name: "legacy", params: map[string]string{"protocol": "nfs"}},
		{name: "product-prefixed", params: map[string]string{storageClassProtocolParameter: "nfs"}},
		{name: "frontendProtocol", params: map[string]string{"frontendProtocol": "nfs"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewControllerServerWithProvisioner(&stubLookup{}, &stubProvisioner{})
			_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
				Name: "pvc-a",
				CapacityRange: &csipb.CapacityRange{
					RequiredBytes: 1 << 30,
				},
				Parameters: tc.params,
				VolumeCapabilities: []*csipb.VolumeCapability{
					testVolumeCapability(),
				},
			})
			if err == nil {
				t.Fatal("expected invalid protocol error")
			}
			st, _ := status.FromError(err)
			if st.Code() != codes.InvalidArgument {
				t.Fatalf("code=%v want InvalidArgument", st.Code())
			}
		})
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
