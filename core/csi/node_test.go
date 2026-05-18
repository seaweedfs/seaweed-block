package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockISCSIUtil struct {
	discoveryErr          error
	configureErr          error
	loginErr              error
	logoutErr             error
	getDeviceResult       string
	getDeviceErr          error
	multipathDeviceResult string
	multipathDeviceErr    error
	loggedIn              map[string]bool
	calls                 []string
}

func newMockISCSIUtil() *mockISCSIUtil {
	return &mockISCSIUtil{loggedIn: map[string]bool{}, getDeviceResult: "/dev/sda", multipathDeviceResult: "/dev/mapper/mpatha"}
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
	m.loggedIn[iqn+"@"+portal] = true
	return nil
}

func (m *mockISCSIUtil) Logout(_ context.Context, iqn string) error {
	m.calls = append(m.calls, "logout:"+iqn)
	if m.logoutErr != nil {
		return m.logoutErr
	}
	delete(m.loggedIn, iqn)
	for key := range m.loggedIn {
		if strings.HasPrefix(key, iqn+"@") {
			delete(m.loggedIn, key)
		}
	}
	return nil
}

func (m *mockISCSIUtil) GetDeviceByIQN(_ context.Context, iqn, portal string) (string, error) {
	m.calls = append(m.calls, "getdevice:"+iqn+":"+portal)
	return m.getDeviceResult, m.getDeviceErr
}

func (m *mockISCSIUtil) GetMultipathDeviceByIQN(_ context.Context, iqn string, minPaths int) (string, error) {
	m.calls = append(m.calls, "getmpath:"+iqn+":"+string(rune('0'+minPaths)))
	return m.multipathDeviceResult, m.multipathDeviceErr
}

func (m *mockISCSIUtil) IsLoggedIn(_ context.Context, iqn, portal string) (bool, error) {
	m.calls = append(m.calls, "isloggedin:"+iqn+":"+portal)
	return m.loggedIn[iqn+"@"+portal], nil
}

func (m *mockISCSIUtil) RescanDevice(context.Context, string) error { return nil }

type mockMountUtil struct {
	formatAndMountErr error
	bindMountErr      error
	unmountErr        error
	isMountedErr      error
	mounted           map[string]bool
	calls             []string
}

type mockNVMeUtil struct {
	connectErr      error
	disconnectErr   error
	getDeviceResult string
	getDeviceErr    error
	connected       map[string]bool
	calls           []string
}

func newMockNVMeUtil() *mockNVMeUtil {
	return &mockNVMeUtil{connected: map[string]bool{}, getDeviceResult: "/dev/nvme1n1"}
}

func (m *mockNVMeUtil) Connect(_ context.Context, addr, nqn string) error {
	m.calls = append(m.calls, "connect:"+addr+":"+nqn)
	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected[nqn] = true
	return nil
}

func (m *mockNVMeUtil) Disconnect(_ context.Context, nqn string) error {
	m.calls = append(m.calls, "disconnect:"+nqn)
	if m.disconnectErr != nil {
		return m.disconnectErr
	}
	delete(m.connected, nqn)
	return nil
}

func (m *mockNVMeUtil) GetDeviceByNQN(_ context.Context, nqn string) (string, error) {
	m.calls = append(m.calls, "getdevice:"+nqn)
	return m.getDeviceResult, m.getDeviceErr
}

func (m *mockNVMeUtil) IsConnected(_ context.Context, nqn string) (bool, error) {
	m.calls = append(m.calls, "isconnected:"+nqn)
	return m.connected[nqn], nil
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
	if m.isMountedErr != nil {
		return false, m.isMountedErr
	}
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

func newTestNodeWithNVMe(mi *mockISCSIUtil, mn *mockNVMeUtil, mm *mockMountUtil) *NodeServer {
	return NewNodeServer(NodeConfig{
		NodeID:    "node-a",
		IQNPrefix: "iqn.2026-05.example.v3",
		ISCSIUtil: mi,
		NVMeUtil:  mn,
		MountUtil: mm,
	})
}

func newTestNodeWithLookup(mi *mockISCSIUtil, mm *mockMountUtil, lookup PublishTargetLookup) *NodeServer {
	return NewNodeServer(NodeConfig{
		NodeID:    "node-a",
		IQNPrefix: "iqn.2026-05.example.v3",
		ISCSIUtil: mi,
		MountUtil: mm,
		Lookup:    lookup,
	})
}

type recordingEventReporter struct {
	events []ClusterEvent
	err    error
}

func (r *recordingEventReporter) ReportEvent(_ context.Context, event ClusterEvent) error {
	r.events = append(r.events, event)
	return r.err
}

type sequenceLookup struct {
	targets []PublishTarget
	calls   []string
}

func (s *sequenceLookup) LookupPublishTarget(_ context.Context, volumeID, nodeID string) (PublishTarget, error) {
	s.calls = append(s.calls, volumeID+":"+nodeID)
	if len(s.targets) == 0 {
		return PublishTarget{}, ErrPublishTargetNotFound
	}
	target := s.targets[0]
	if len(s.targets) > 1 {
		s.targets = s.targets[1:]
	}
	return target, nil
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

func TestNodeStage_RefreshesPublishTargetFromMasterLookup(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	lookup := &stubLookup{target: PublishTarget{
		VolumeID:  "v1",
		ReplicaID: "r2",
		Protocol:  ProtocolISCSI,
		ISCSIAddr: "127.0.0.1:3261",
		IQN:       "iqn.2026-05.example.v3:v1-r2",
	}}
	ns := newTestNodeWithLookup(mi, mm, lookup)
	staging := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"protocol":  "iscsi",
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.2026-05.example.v3:v1-r1",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	if got := readTargetFile(staging); got != "iqn.2026-05.example.v3:v1-r2" {
		t.Fatalf("target file=%q want refreshed promoted target", got)
	}
	if got := ns.staged["v1"].iscsiAddr; got != "127.0.0.1:3261" {
		t.Fatalf("staged portal=%q want refreshed promoted portal", got)
	}
	for _, want := range []string{
		"discovery:127.0.0.1:3261",
		"login:iqn.2026-05.example.v3:v1-r2:127.0.0.1:3261",
	} {
		found := false
		for _, call := range mi.calls {
			if call == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing call %q in %v", want, mi.calls)
		}
	}
	if len(lookup.calls) != 1 || lookup.calls[0] != "v1:node-a" {
		t.Fatalf("lookup calls=%v want v1:node-a", lookup.calls)
	}
}

func TestNodeStage_ReportsCSIReattachObservedAfterSuccessfulStage(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	lookup := &sequenceLookup{targets: []PublishTarget{{
		VolumeID:        "v1",
		ReplicaID:       "r2",
		Epoch:           2,
		EndpointVersion: 1,
		Protocol:        ProtocolISCSI,
		ISCSIAddr:       "127.0.0.1:3261",
		IQN:             "iqn.lookup:v1",
	}}}
	reporter := &recordingEventReporter{}
	ns := NewNodeServer(NodeConfig{
		NodeID:        "node-a",
		IQNPrefix:     "iqn.2026-05.example.v3",
		ISCSIUtil:     mi,
		MountUtil:     mm,
		Lookup:        lookup,
		EventReporter: reporter,
	})

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.stale:v1",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	if len(reporter.events) != 1 {
		t.Fatalf("events=%d want 1", len(reporter.events))
	}
	event := reporter.events[0]
	if event.Type != "csi_reattach_observed" || event.VolumeID != "v1" || event.ReplicaID != "r2" || event.NodeName != "node-a" {
		t.Fatalf("event=%+v", event)
	}
	if event.Epoch != 2 || event.EndpointVersion != 1 {
		t.Fatalf("event lineage epoch=%d ev=%d", event.Epoch, event.EndpointVersion)
	}
}

func TestNodeStage_UsesPortalSpecificISCSISessionAfterFailover(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	mi.loggedIn["iqn.2026-05.example.v3:v1@127.0.0.1:3260"] = true
	lookup := &stubLookup{target: PublishTarget{
		VolumeID:  "v1",
		ReplicaID: "r2",
		Protocol:  ProtocolISCSI,
		ISCSIAddr: "127.0.0.1:3261",
		IQN:       "iqn.2026-05.example.v3:v1",
	}}
	ns := newTestNodeWithLookup(mi, mm, lookup)

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"protocol":  "iscsi",
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.2026-05.example.v3:v1",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	wantPrefix := []string{
		"isloggedin:iqn.2026-05.example.v3:v1:127.0.0.1:3261",
		"discovery:127.0.0.1:3261",
		"login:iqn.2026-05.example.v3:v1:127.0.0.1:3261",
		"getdevice:iqn.2026-05.example.v3:v1:127.0.0.1:3261",
	}
	for i, want := range wantPrefix {
		if i >= len(mi.calls) || mi.calls[i] != want {
			t.Fatalf("calls=%v want prefix=%v", mi.calls, wantPrefix)
		}
	}
	if !mi.loggedIn["iqn.2026-05.example.v3:v1@127.0.0.1:3261"] {
		t.Fatalf("promoted portal was not logged in; loggedIn=%v", mi.loggedIn)
	}
}

func TestNodeStage_MultipathISCSIRequiresAtLeastTwoPortals(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"protocol":         "iscsi",
			"stage2_multipath": "true",
			"iscsiAddr":        "127.0.0.1:3260",
			"iqn":              "iqn.2026-05.example.v3:v1",
		},
	})
	if err == nil {
		t.Fatal("expected multipath stage to fail closed with one portal")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code=%v want FailedPrecondition err=%v", st.Code(), err)
	}
	if len(mi.calls) != 0 {
		t.Fatalf("multipath single-portal refusal should happen before iscsi calls, got %v", mi.calls)
	}
}

func TestNodeStage_MultipathISCSILoginsAllPathsAndMountsMultipathDevice(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"protocol":         "iscsi",
			"stage2_multipath": "true",
			"iscsiAddrs":       "127.0.0.1:3260,127.0.0.1:3261",
			"iqn":              "iqn.2026-05.example.v3:v1",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	for _, want := range []string{
		"discovery:127.0.0.1:3260",
		"login:iqn.2026-05.example.v3:v1:127.0.0.1:3260",
		"discovery:127.0.0.1:3261",
		"login:iqn.2026-05.example.v3:v1:127.0.0.1:3261",
		"getmpath:iqn.2026-05.example.v3:v1:2",
	} {
		if !containsString(mi.calls, want) {
			t.Fatalf("missing call %q in %v", want, mi.calls)
		}
	}
	if !containsString(mm.calls, "formatandmount:/dev/mapper/mpatha:"+staging+":ext4") {
		t.Fatalf("multipath stage must mount mapper device, calls=%v", mm.calls)
	}
	info := ns.staged["v1"]
	if info == nil || !info.multipath {
		t.Fatalf("staged info did not record multipath: %+v", info)
	}
	if got := strings.Join(info.iscsiAddrs, ","); got != "127.0.0.1:3260,127.0.0.1:3261" {
		t.Fatalf("staged portals=%q", got)
	}
	if got := readTargetFile(staging); got != "iqn.2026-05.example.v3:v1" {
		t.Fatalf("target file=%q", got)
	}
}

func TestNodeStage_MultipathISCSIWaitsForRefreshedMultiPortalTarget(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	lookup := &sequenceLookup{targets: []PublishTarget{
		{
			VolumeID:   "v1",
			ReplicaID:  "r1",
			Protocol:   ProtocolISCSI,
			ISCSIAddr:  "127.0.0.1:3260",
			IQN:        "iqn.2026-05.example.v3:v1",
			Multipath:  true,
			ISCSIAddrs: []string{"127.0.0.1:3260"},
		},
		{
			VolumeID:   "v1",
			ReplicaID:  "r1",
			Protocol:   ProtocolISCSI,
			ISCSIAddr:  "127.0.0.1:3260",
			IQN:        "iqn.2026-05.example.v3:v1",
			Multipath:  true,
			ISCSIAddrs: []string{"127.0.0.1:3260", "127.0.0.1:3261", "127.0.0.1:3262"},
		},
	}}
	ns := newTestNodeWithLookup(mi, mm, lookup)
	staging := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"protocol":         "iscsi",
			"stage2_multipath": "true",
			"iscsiAddr":        "127.0.0.1:3260",
			"iqn":              "iqn.2026-05.example.v3:v1",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	for _, want := range []string{
		"login:iqn.2026-05.example.v3:v1:127.0.0.1:3260",
		"login:iqn.2026-05.example.v3:v1:127.0.0.1:3261",
		"login:iqn.2026-05.example.v3:v1:127.0.0.1:3262",
		"getmpath:iqn.2026-05.example.v3:v1:3",
	} {
		if !containsString(mi.calls, want) {
			t.Fatalf("missing call %q in %v", want, mi.calls)
		}
	}
	if len(lookup.calls) < 2 {
		t.Fatalf("lookup calls=%v, want initial refresh plus multipath wait refresh", lookup.calls)
	}
	info := ns.staged["v1"]
	if info == nil || !info.multipath {
		t.Fatalf("staged info did not record multipath: %+v", info)
	}
	if got := strings.Join(info.iscsiAddrs, ","); got != "127.0.0.1:3260,127.0.0.1:3261,127.0.0.1:3262" {
		t.Fatalf("staged portals=%q", got)
	}
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func TestISCSISessionContainsTargetMatchesIQNAndPortal(t *testing.T) {
	out := strings.Join([]string{
		"tcp: [1] 127.0.0.1:3260,1 iqn.2026-05.example.v3:v1 (non-flash)",
		"tcp: [2] 127.0.0.1:3261,1 iqn.2026-05.example.v3:v1 (non-flash)",
	}, "\n")
	if !iscsiSessionContainsTarget(out, "iqn.2026-05.example.v3:v1", "127.0.0.1:3261") {
		t.Fatal("expected session match on promoted portal")
	}
	if iscsiSessionContainsTarget(out, "iqn.2026-05.example.v3:v1", "127.0.0.1:3262") {
		t.Fatal("must not match a different portal for the same IQN")
	}
}

func TestISCSIByPathMatchesPortal(t *testing.T) {
	for _, path := range []string{
		"/dev/disk/by-path/ip-127.0.0.1:3261-iscsi-iqn.2026-05.example.v3:v1-lun-1",
		"/dev/disk/by-path/ip-127.0.0.1-3261-iscsi-iqn.2026-05.example.v3:v1-lun-1",
	} {
		if !iscsiByPathMatchesPortal(path, "127.0.0.1:3261") {
			t.Fatalf("expected by-path match on promoted portal for %q", path)
		}
	}
	if iscsiByPathMatchesPortal("/dev/disk/by-path/ip-127.0.0.1:3261-iscsi-iqn.2026-05.example.v3:v1-lun-1", "127.0.0.1:3260") {
		t.Fatal("must not match by-path entry from a different portal")
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

func TestNodeStage_NVMeProtocolUsesNVMeTarget(t *testing.T) {
	mi, mn, mm := newMockISCSIUtil(), newMockNVMeUtil(), newMockMountUtil()
	ns := newTestNodeWithNVMe(mi, mn, mm)
	staging := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"protocol": "nvme",
			"nvmeAddr": "127.0.0.1:4420",
			"nqn":      "nqn.2026-05.io.seaweedfs:v1",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	want := []string{
		"isconnected:nqn.2026-05.io.seaweedfs:v1",
		"connect:127.0.0.1:4420:nqn.2026-05.io.seaweedfs:v1",
		"getdevice:nqn.2026-05.io.seaweedfs:v1",
	}
	for i, w := range want {
		if i >= len(mn.calls) || mn.calls[i] != w {
			t.Fatalf("nvme calls=%v want prefix=%v", mn.calls, want)
		}
	}
	if len(mi.calls) != 0 {
		t.Fatalf("nvme path must not call iscsi util, calls=%v", mi.calls)
	}
	if got := readTransportFile(staging); got != transportNVMe {
		t.Fatalf("transport=%q want nvme", got)
	}
	info := ns.staged["v1"]
	if info == nil || info.transport != transportNVMe || info.nqn != "nqn.2026-05.io.seaweedfs:v1" {
		t.Fatalf("staged info=%+v", info)
	}
}

func TestNodeStage_NVMeCleansUpConnectWhenMountFails(t *testing.T) {
	_, mn, mm := newMockISCSIUtil(), newMockNVMeUtil(), newMockMountUtil()
	mm.formatAndMountErr = errors.New("mkfs failed")
	ns := newTestNodeWithNVMe(newMockISCSIUtil(), mn, mm)

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"protocol": "nvme",
			"nvmeAddr": "127.0.0.1:4420",
			"nqn":      "nqn.2026-05.io.seaweedfs:v1",
		},
	})
	if err == nil {
		t.Fatal("expected mount failure")
	}
	foundDisconnect := false
	for _, call := range mn.calls {
		if call == "disconnect:nqn.2026-05.io.seaweedfs:v1" {
			foundDisconnect = true
		}
	}
	if !foundDisconnect {
		t.Fatalf("expected cleanup disconnect, calls=%v", mn.calls)
	}
	if ns.staged["v1"] != nil {
		t.Fatalf("staged entry must not be recorded after mount failure: %+v", ns.staged["v1"])
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
		"isloggedin:iqn.v1:127.0.0.1:3260",
		"discovery:127.0.0.1:3260",
		"chap:iqn.v1:127.0.0.1:3260:user1:secret1",
		"login:iqn.v1:127.0.0.1:3260",
		"getdevice:iqn.v1:127.0.0.1:3260",
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
	ns.staged["v1"] = &stagedVolumeInfo{iqn: "iqn.v1", iscsiAddr: "127.0.0.1:3260", transport: transportISCSI, stagingPath: staging}

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

func TestNodeStage_FailsClosedWhenStagingPathMountedForAnotherVolume(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()
	mm.mounted[staging] = true
	ns.staged["v1"] = &stagedVolumeInfo{iqn: "iqn.v1", iscsiAddr: "127.0.0.1:3260", transport: transportISCSI, stagingPath: staging}

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v2",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"iscsiAddr": "127.0.0.1:3261",
			"iqn":       "iqn.v2",
		},
	})
	if err == nil {
		t.Fatal("expected mounted staging path for another volume to fail closed")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code=%v want FailedPrecondition", st.Code())
	}
	if len(mi.calls) != 0 {
		t.Fatalf("expected fail before iscsi calls, got %v", mi.calls)
	}
}

func TestNodeStage_FailsClosedOnStaleLoggedInSessionWithoutStagedIdentity(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	mi.loggedIn["iqn.v1@127.0.0.1:3260"] = true
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
		t.Fatal("expected stale logged-in session without staged identity to fail closed")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code=%v want FailedPrecondition", st.Code())
	}
	for _, call := range mi.calls {
		if strings.HasPrefix(call, "getdevice:") {
			t.Fatalf("must fail before using stale session device, calls=%v", mi.calls)
		}
	}
}

func TestNodeStage_AllowsLoggedInSessionWithRestartIdentityFile(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	mi.loggedIn["iqn.v1@127.0.0.1:3260"] = true
	ns := newTestNode(mi, mm)
	staging := t.TempDir()
	if err := writeVolumeFile(staging, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := writeTransportFile(staging, transportISCSI); err != nil {
		t.Fatal(err)
	}

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
	wantPrefix := []string{
		"isloggedin:iqn.v1:127.0.0.1:3260",
		"getdevice:iqn.v1:127.0.0.1:3260",
	}
	for i, w := range wantPrefix {
		if i >= len(mi.calls) || mi.calls[i] != w {
			t.Fatalf("calls=%v want prefix=%v", mi.calls, wantPrefix)
		}
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

func TestNodeStage_DoesNotRecordStagedEntryWhenLoginFails(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	mi.loginErr = errors.New("auth rejected")
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
		t.Fatal("expected login error")
	}
	if ns.staged["v1"] != nil {
		t.Fatalf("staged entry must not be recorded after login failure: %+v", ns.staged["v1"])
	}
	if got := strings.Join(mi.calls, ","); strings.Contains(got, "logout:iqn.v1") {
		t.Fatalf("login failure must not logout a session it did not start successfully: %v", mi.calls)
	}
}

func TestNodeStage_CleansUpLoginWhenGetDeviceFails(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	mi.getDeviceErr = errors.New("device never appeared")
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
		t.Fatal("expected get device error")
	}
	foundLogout := false
	for _, call := range mi.calls {
		if call == "logout:iqn.v1" {
			foundLogout = true
		}
	}
	if !foundLogout {
		t.Fatalf("expected cleanup logout after get device failure, calls=%v", mi.calls)
	}
	if ns.staged["v1"] != nil {
		t.Fatalf("staged entry must not be recorded after get device failure: %+v", ns.staged["v1"])
	}
}

func TestNodeStage_CleansUpLoginWhenCreateStagingDirFails(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	parentFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(parentFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: filepath.Join(parentFile, "staging"),
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.v1",
		},
	})
	if err == nil {
		t.Fatal("expected create staging dir error")
	}
	foundLogout := false
	for _, call := range mi.calls {
		if call == "logout:iqn.v1" {
			foundLogout = true
		}
	}
	if !foundLogout {
		t.Fatalf("expected cleanup logout after mkdir failure, calls=%v", mi.calls)
	}
	if ns.staged["v1"] != nil {
		t.Fatalf("staged entry must not be recorded after mkdir failure: %+v", ns.staged["v1"])
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

func TestNodeUnstage_NotMountedStillLogsOutAndCleansState(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()
	if err := writeTransportFile(staging, transportISCSI); err != nil {
		t.Fatal(err)
	}
	ns.staged["v1"] = &stagedVolumeInfo{iqn: "iqn.v1", iscsiAddr: "127.0.0.1:3260", transport: transportISCSI, stagingPath: staging}

	_, err := ns.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}
	if ns.staged["v1"] != nil {
		t.Fatalf("staged entry should be removed after idempotent unstage: %+v", ns.staged["v1"])
	}
	if got := readTransportFile(staging); got != "" {
		t.Fatalf("transport file should be removed, got %q", got)
	}
	if got := readVolumeFile(staging); got != "" {
		t.Fatalf("volume file should be removed, got %q", got)
	}
	foundLogout := false
	for _, call := range mi.calls {
		if call == "logout:iqn.v1" {
			foundLogout = true
		}
	}
	if !foundLogout {
		t.Fatalf("expected logout even when staging path was not mounted, calls=%v", mi.calls)
	}
}

func TestNodeUnstage_NVMeDisconnectsAndCleansState(t *testing.T) {
	mn, mm := newMockNVMeUtil(), newMockMountUtil()
	ns := newTestNodeWithNVMe(newMockISCSIUtil(), mn, mm)
	staging := t.TempDir()
	if err := writeTransportFile(staging, transportNVMe); err != nil {
		t.Fatal(err)
	}
	ns.staged["v1"] = &stagedVolumeInfo{
		nqn:         "nqn.2026-05.io.seaweedfs:v1",
		nvmeAddr:    "127.0.0.1:4420",
		transport:   transportNVMe,
		stagingPath: staging,
	}

	_, err := ns.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}
	foundDisconnect := false
	for _, call := range mn.calls {
		if call == "disconnect:nqn.2026-05.io.seaweedfs:v1" {
			foundDisconnect = true
		}
	}
	if !foundDisconnect {
		t.Fatalf("expected nvme disconnect, calls=%v", mn.calls)
	}
	if ns.staged["v1"] != nil {
		t.Fatalf("staged entry should be removed after unstage: %+v", ns.staged["v1"])
	}
	if got := readTransportFile(staging); got != "" {
		t.Fatalf("transport file should be removed, got %q", got)
	}
}

func TestNodeUnstage_NVMeRestartFallbackUsesTargetFile(t *testing.T) {
	mn, mm := newMockNVMeUtil(), newMockMountUtil()
	ns := newTestNodeWithNVMe(newMockISCSIUtil(), mn, mm)
	staging := t.TempDir()
	if err := writeTransportFile(staging, transportNVMe); err != nil {
		t.Fatal(err)
	}
	if err := writeTargetFile(staging, "nqn.2026-05.io.seaweedfs:v1"); err != nil {
		t.Fatal(err)
	}

	_, err := ns.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}
	if len(mn.calls) != 1 || mn.calls[0] != "disconnect:nqn.2026-05.io.seaweedfs:v1" {
		t.Fatalf("nvme calls=%v", mn.calls)
	}
	if got := readTargetFile(staging); got != "" {
		t.Fatalf("target file should be removed, got %q", got)
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
	if got := readVolumeFile(staging); got != "" {
		t.Fatalf("volume file should be removed, got %q", got)
	}
}

func TestNodeStageUnstage_RepeatedCyclesLeaveNoLocalState(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()
	req := &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext: map[string]string{
			"iscsiAddr": "127.0.0.1:3260",
			"iqn":       "iqn.v1",
		},
	}

	for i := 0; i < 3; i++ {
		if _, err := ns.NodeStageVolume(context.Background(), req); err != nil {
			t.Fatalf("stage iter %d: %v", i, err)
		}
		if ns.staged["v1"] == nil {
			t.Fatalf("stage iter %d did not record staged state", i)
		}
		if got := readVolumeFile(staging); got != "v1" {
			t.Fatalf("stage iter %d volume file=%q", i, got)
		}
		if _, err := ns.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
			VolumeId:          "v1",
			StagingTargetPath: staging,
		}); err != nil {
			t.Fatalf("unstage iter %d: %v", i, err)
		}
		if ns.staged["v1"] != nil {
			t.Fatalf("unstage iter %d left staged state: %+v", i, ns.staged["v1"])
		}
		if got := readVolumeFile(staging); got != "" {
			t.Fatalf("unstage iter %d left volume file=%q", i, got)
		}
		if got := readTransportFile(staging); got != "" {
			t.Fatalf("unstage iter %d left transport file=%q", i, got)
		}
	}
}

func TestG15e_CSIReattachUsesFreshPublishTargetAfterUnstage(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	lookup := &stubLookup{target: PublishTarget{
		VolumeID:  "v1",
		ReplicaID: "r1",
		Protocol:  ProtocolISCSI,
		ISCSIAddr: "127.0.0.1:3260",
		IQN:       "iqn.2026-05.example.v3:v1-old",
	}}
	controller := NewControllerServer(lookup)
	staging := t.TempDir()
	target1 := filepath.Join(t.TempDir(), "pod-a")
	target2 := filepath.Join(t.TempDir(), "pod-b")

	firstPublish, err := controller.ControllerPublishVolume(context.Background(), &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
	})
	if err != nil {
		t.Fatalf("first ControllerPublishVolume: %v", err)
	}
	if _, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext:    firstPublish.GetPublishContext(),
	}); err != nil {
		t.Fatalf("first NodeStageVolume: %v", err)
	}
	if _, err := ns.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		TargetPath:        target1,
	}); err != nil {
		t.Fatalf("first NodePublishVolume: %v", err)
	}
	if _, err := ns.NodeUnpublishVolume(context.Background(), &csipb.NodeUnpublishVolumeRequest{
		VolumeId:   "v1",
		TargetPath: target1,
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if _, err := controller.ControllerUnpublishVolume(context.Background(), &csipb.ControllerUnpublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
	}); err != nil {
		t.Fatalf("ControllerUnpublishVolume: %v", err)
	}
	if _, err := ns.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
	}); err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}
	if mi.loggedIn["iqn.2026-05.example.v3:v1-old@127.0.0.1:3260"] {
		t.Fatalf("old IQN still logged in after unstage")
	}
	if ns.staged["v1"] != nil {
		t.Fatalf("unstage left staged state: %+v", ns.staged["v1"])
	}

	lookup.target = PublishTarget{
		VolumeID:  "v1",
		ReplicaID: "r1",
		Protocol:  ProtocolISCSI,
		ISCSIAddr: "127.0.0.2:3260",
		IQN:       "iqn.2026-05.example.v3:v1-new",
	}
	secondPublish, err := controller.ControllerPublishVolume(context.Background(), &csipb.ControllerPublishVolumeRequest{
		VolumeId: "v1",
		NodeId:   "node-a",
	})
	if err != nil {
		t.Fatalf("second ControllerPublishVolume: %v", err)
	}
	if got := secondPublish.GetPublishContext()["iqn"]; got != "iqn.2026-05.example.v3:v1-new" {
		t.Fatalf("second publish_context iqn=%q", got)
	}
	if _, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		VolumeCapability:  testVolumeCapability(),
		PublishContext:    secondPublish.GetPublishContext(),
	}); err != nil {
		t.Fatalf("second NodeStageVolume: %v", err)
	}
	if _, err := ns.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		TargetPath:        target2,
	}); err != nil {
		t.Fatalf("second NodePublishVolume: %v", err)
	}
	if !mi.loggedIn["iqn.2026-05.example.v3:v1-new@127.0.0.2:3260"] {
		t.Fatalf("fresh IQN was not logged in after reattach; loggedIn=%v", mi.loggedIn)
	}
	if got := readTargetFile(staging); got != "iqn.2026-05.example.v3:v1-new" {
		t.Fatalf("target file=%q want fresh IQN", got)
	}
	if len(lookup.calls) != 2 {
		t.Fatalf("ControllerPublish should be called once per attach cycle, calls=%v", lookup.calls)
	}
	for _, want := range []string{
		"logout:iqn.2026-05.example.v3:v1-old",
		"discovery:127.0.0.2:3260",
		"login:iqn.2026-05.example.v3:v1-new:127.0.0.2:3260",
	} {
		found := false
		for _, call := range mi.calls {
			if call == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing call %q in %v", want, mi.calls)
		}
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
	if err := writeVolumeFile(staging, "v1"); err != nil {
		t.Fatal(err)
	}

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

func TestNodePublish_FailsClosedWithoutStagingIdentity(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()
	target := filepath.Join(t.TempDir(), "pod-target")

	_, err := ns.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		TargetPath:        target,
	})
	if err == nil {
		t.Fatal("expected missing staging identity to fail")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code=%v want FailedPrecondition", st.Code())
	}
	for _, call := range mm.calls {
		if strings.HasPrefix(call, "bindmount:") {
			t.Fatalf("must fail before bind mount, calls=%v", mm.calls)
		}
	}
}

func TestNodePublish_FailsClosedWhenStagingPathBelongsToAnotherVolume(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	ns := newTestNode(mi, mm)
	staging := t.TempDir()
	target := filepath.Join(t.TempDir(), "pod-target")
	if err := writeVolumeFile(staging, "v2"); err != nil {
		t.Fatal(err)
	}

	_, err := ns.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: staging,
		TargetPath:        target,
	})
	if err == nil {
		t.Fatal("expected wrong staging identity to fail")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code=%v want FailedPrecondition", st.Code())
	}
	for _, call := range mm.calls {
		if strings.HasPrefix(call, "bindmount:") {
			t.Fatalf("must fail before bind mount, calls=%v", mm.calls)
		}
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
