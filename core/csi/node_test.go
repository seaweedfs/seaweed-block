package csi

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockISCSIUtil struct {
	discoveryErr    error
	configureErr    error
	loginErr        error
	logoutErr       error
	getDeviceResult string
	getDeviceErr    error
	loggedIn        map[string]bool
	calls           []string
}

func newMockISCSIUtil() *mockISCSIUtil {
	return &mockISCSIUtil{loggedIn: map[string]bool{}, getDeviceResult: "/dev/sda"}
}

func (m *mockISCSIUtil) Discovery(_ context.Context, portal string) error {
	m.calls = append(m.calls, "discovery:"+portal)
	return m.discoveryErr
}

func (m *mockISCSIUtil) ConfigureCHAP(_ context.Context, iqn, portal string, auth ISCSIAuth) error {
	m.calls = append(m.calls, "chap:"+iqn+":"+portal+":"+auth.Username+":"+auth.Secret)
	return m.configureErr
}

func (m *mockISCSIUtil) Login(_ context.Context, iqn, portal string) error {
	m.calls = append(m.calls, "login:"+iqn+":"+portal)
	if m.loginErr != nil {
		return m.loginErr
	}
	m.loggedIn[iqn] = true
	return nil
}

func (m *mockISCSIUtil) Logout(_ context.Context, iqn string) error {
	m.calls = append(m.calls, "logout:"+iqn)
	if m.logoutErr != nil {
		return m.logoutErr
	}
	delete(m.loggedIn, iqn)
	return nil
}

func (m *mockISCSIUtil) GetDeviceByIQN(_ context.Context, iqn string) (string, error) {
	m.calls = append(m.calls, "getdevice:"+iqn)
	return m.getDeviceResult, m.getDeviceErr
}

func (m *mockISCSIUtil) IsLoggedIn(_ context.Context, iqn string) (bool, error) {
	m.calls = append(m.calls, "isloggedin:"+iqn)
	return m.loggedIn[iqn], nil
}

func (m *mockISCSIUtil) RescanDevice(context.Context, string) error { return nil }

type mockMountUtil struct {
	formatAndMountErr error
	bindMountErr      error
	unmountErr        error
	mounted           map[string]bool
	calls             []string
}

func newMockMountUtil() *mockMountUtil {
	return &mockMountUtil{mounted: map[string]bool{}}
}

func (m *mockMountUtil) FormatAndMount(_ context.Context, device, target, fsType string) error {
	m.calls = append(m.calls, "formatandmount:"+device+":"+target+":"+fsType)
	if m.formatAndMountErr != nil {
		return m.formatAndMountErr
	}
	m.mounted[target] = true
	return nil
}

func (m *mockMountUtil) BindMount(_ context.Context, source, target string, readOnly bool) error {
	m.calls = append(m.calls, "bindmount:"+source+":"+target)
	if m.bindMountErr != nil {
		return m.bindMountErr
	}
	m.mounted[target] = true
	return nil
}

func (m *mockMountUtil) Unmount(_ context.Context, target string) error {
	m.calls = append(m.calls, "unmount:"+target)
	if m.unmountErr != nil {
		return m.unmountErr
	}
	delete(m.mounted, target)
	return nil
}

func (m *mockMountUtil) IsMounted(_ context.Context, target string) (bool, error) {
	return m.mounted[target], nil
}

func newTestNode(mi *mockISCSIUtil, mm *mockMountUtil) *NodeServer {
	return NewNodeServer(NodeConfig{
		NodeID:    "node-a",
		IQNPrefix: "iqn.2026-05.example.v3",
		ISCSIUtil: mi,
		MountUtil: mm,
	})
}

func testVolumeCapability() *csipb.VolumeCapability {
	return &csipb.VolumeCapability{
		AccessType: &csipb.VolumeCapability_Mount{
			Mount: &csipb.VolumeCapability_MountVolume{FsType: "ext4"},
		},
		AccessMode: &csipb.VolumeCapability_AccessMode{
			Mode: csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}
}

func TestNodeStage_UsesPublishContextBeforeVolumeContext(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"iscsiAddr": "127.0.0.9:3260",
			"iqn":       "iqn.fresh:v1",
		},
		VolumeContext: map[string]string{
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.stale:v1",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	if len(mi.calls) < 2 || mi.calls[1] != "discovery:127.0.0.9:3260" {
		t.Fatalf("expected discovery from publish_context, calls=%v", mi.calls)
	}
	info := ns.staged["v1"]
	if info == nil || info.iqn != "iqn.fresh:v1" {
		t.Fatalf("staged info=%+v", info)
	}
}

func TestNodeStage_ConfiguresCHAPBeforeLogin(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.v1",
		},
		Secrets: map[string]string{
			"chapUsername": "user1",
			"chapSecret":   "secret1",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	want := []string{
		"isloggedin:iqn.v1",
		"discovery:127.0.0.1:3260",
		"chap:iqn.v1:127.0.0.1:3260:user1:secret1",
		"login:iqn.v1:127.0.0.1:3260",
		"getdevice:iqn.v1",
	}
	for i, w := range want {
		if i >= len(mi.calls) || mi.calls[i] != w {
			t.Fatalf("calls=%v want prefix=%v", mi.calls, want)
		}
	}
}

func TestNodeStage_RejectsPartialCHAPContext(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.v1",
		},
		Secrets: map[string]string{
			"chapUsername": "user1",
		},
	})
	if err == nil {
		t.Fatal("expected partial CHAP context to fail")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code=%v want FailedPrecondition", st.Code())
	}
	if len(mi.calls) != 0 {
		t.Fatalf("expected fail before iscsi calls, got %v", mi.calls)
	}
}

func TestNodeStage_IdempotentWhenAlreadyMounted(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()
	mm.mounted[staging] = true

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.v1",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	if len(mi.calls) != 0 {
		t.Fatalf("expected no iscsi calls, got %v", mi.calls)
	}
}

func TestNodeStage_CleansUpLoginWhenMountFails(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	mm.formatAndMountErr = errors.New("mkfs failed")
	ns := newTestNode(mi, mm)

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.v1",
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	foundLogout := false
	for _, call := range mi.calls {
		if call == "logout:iqn.v1" {
			foundLogout = true
		}
	}
	if !foundLogout {
		t.Fatalf("expected cleanup logout, calls=%v", mi.calls)
	}
}

func TestNodeUnstage_PreservesStagedEntryOnFailure(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	mm.unmountErr = errors.New("device busy")
	ns := newTestNode(mi, mm)
	ns.staged["v1"] = &stagedVolumeInfo{iqn: "iqn.v1", iscsiAddr: "127.0.0.1:3260", transport: transportISCSI}

	_, err := ns.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if ns.staged["v1"] == nil {
		t.Fatal("staged entry should be preserved for retry")
	}
}

func TestNodeUnstage_RestartFallbackUsesTransportFileAndDerivedIQN(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()
	if err := writeTransportFile(staging, transportISCSI); err != nil {
		t.Fatalf("write transport: %v", err)
	}

	_, err := ns.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "Vol-A",
		StagingTargetPath: staging,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}
	found := false
	for _, call := range mi.calls {
		if call == "logout:iqn.2026-05.example.v3:vol-a" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected derived logout, calls=%v", mi.calls)
	}
	if got := readTransportFile(staging); got != "" {
		t.Fatalf("transport file should be removed, got %q", got)
	}
}

func TestTransportFileRejectsGarbage(t *testing.T) {
	staging := t.TempDir()
	if err := writeTransportFile(staging, "nvme\n"); err != nil {
		t.Fatal(err)
	}
	if got := readTransportFile(staging); got != "" {
		t.Fatalf("garbage transport accepted: %q", got)
	}
}

func TestNodePublish_BindMountsAndIsIdempotent(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()
	target := filepath.Join(t.TempDir(), "pod-target")

	_, err := ns.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		TargetPath:        target,
	})
	if err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	if len(mm.calls) == 0 || !strings.HasPrefix(mm.calls[len(mm.calls)-1], "bindmount:") {
		t.Fatalf("expected bind mount, calls=%v", mm.calls)
	}
	before := len(mm.calls)
	_, err = ns.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		TargetPath:        target,
	})
	if err != nil {
		t.Fatalf("idempotent NodePublishVolume: %v", err)
	}
	if len(mm.calls) != before {
		t.Fatalf("idempotent publish should not bind again: %v", mm.calls)
	}
}

func TestNodeStage_NoPublishTargetFailsClosed(t *testing.T) {
	ns := newTestNode(newMockISCSIUtil(), newMockMountUtil())
	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code=%v want FailedPrecondition", st.Code())
	}
}
