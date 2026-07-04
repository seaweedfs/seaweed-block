package csi

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	transportISCSI = "iscsi"
	transportNVMe  = "nvme"
	transportFile  = ".transport"
	volumeFile     = ".volume"
	targetFile     = ".target"
)

var (
	stage2MultipathPublishWait = 90 * time.Second
	stage2MultipathPublishPoll = time.Second
)

type ISCSIUtil interface {
	Discovery(ctx context.Context, portal string) error
	ConfigureCHAP(ctx context.Context, iqn, portal string, auth ISCSIAuth) error
	Login(ctx context.Context, iqn, portal string) error
	Logout(ctx context.Context, iqn string) error
	GetDeviceByIQN(ctx context.Context, iqn, portal string) (string, error)
	GetMultipathDeviceByIQN(ctx context.Context, iqn string, minPaths int) (string, error)
	IsLoggedIn(ctx context.Context, iqn, portal string) (bool, error)
	RescanDevice(ctx context.Context, iqn string) error
}

type NVMeUtil interface {
	Connect(ctx context.Context, addr, nqn string) error
	Disconnect(ctx context.Context, nqn string) error
	GetDeviceByNQN(ctx context.Context, nqn string) (string, error)
	IsConnected(ctx context.Context, nqn string) (bool, error)
	IsPathConnected(ctx context.Context, nqn, addr string) (bool, error)
}

type ISCSIAuth struct {
	Username string
	Secret   string
}

type MountUtil interface {
	FormatAndMount(ctx context.Context, device, target, fsType string) error
	BindMount(ctx context.Context, source, target string, readOnly bool) error
	Unmount(ctx context.Context, target string) error
	IsMounted(ctx context.Context, target string) (bool, error)
}

type stagedVolumeInfo struct {
	iqn         string
	iscsiAddr   string
	iscsiAddrs  []string
	multipath   bool
	nqn         string
	nvmeAddr    string
	nvmeAddrs   []string
	transport   string
	fsType      string
	stagingPath string
}

type NodeServer struct {
	csipb.UnimplementedNodeServer
	nodeID    string
	iqnPrefix string
	iscsiUtil ISCSIUtil
	nvmeUtil  NVMeUtil
	mountUtil MountUtil
	lookup    PublishTargetLookup
	events    EventReporter
	logger    *log.Logger

	stagedMu sync.Mutex
	staged   map[string]*stagedVolumeInfo
}

type NodeConfig struct {
	NodeID        string
	IQNPrefix     string
	ISCSIUtil     ISCSIUtil
	NVMeUtil      NVMeUtil
	MountUtil     MountUtil
	Lookup        PublishTargetLookup
	EventReporter EventReporter
	Logger        *log.Logger
}

func NewNodeServer(cfg NodeConfig) *NodeServer {
	lg := cfg.Logger
	if lg == nil {
		lg = log.Default()
	}
	return &NodeServer{
		nodeID:    cfg.NodeID,
		iqnPrefix: cfg.IQNPrefix,
		iscsiUtil: cfg.ISCSIUtil,
		nvmeUtil:  cfg.NVMeUtil,
		mountUtil: cfg.MountUtil,
		lookup:    cfg.Lookup,
		events:    cfg.EventReporter,
		logger:    lg,
		staged:    make(map[string]*stagedVolumeInfo),
	}
}

func (s *NodeServer) NodeStageVolume(ctx context.Context, req *csipb.NodeStageVolumeRequest) (*csipb.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingPath := req.GetStagingTargetPath()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	if s.mountUtil == nil {
		return nil, status.Error(codes.FailedPrecondition, "node attach utilities are not configured")
	}
	mounted, err := s.mountUtil.IsMounted(ctx, stagingPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mount: %v", err)
	}
	if mounted {
		if err := s.validateMountedStagingVolume(volumeID, stagingPath); err != nil {
			return nil, err
		}
		transport := transportFromContext(req.GetPublishContext(), req.GetVolumeContext())
		if transport == "" {
			transport = readTransportFile(stagingPath)
		}
		if transport == transportNVMe || readTransportFile(stagingPath) == transportNVMe {
			if err := s.reconcileMountedNVMePaths(ctx, req, volumeID, stagingPath); err != nil {
				return nil, err
			}
		}
		s.logger.Printf("NodeStageVolume: %s already mounted at %s", volumeID, stagingPath)
		return &csipb.NodeStageVolumeResponse{}, nil
	}

	transport := transportFromContext(req.GetPublishContext(), req.GetVolumeContext())
	if transport == transportNVMe {
		return s.stageNVMe(ctx, req, volumeID, stagingPath)
	}
	return s.stageISCSI(ctx, req, volumeID, stagingPath)
}

func (s *NodeServer) stageISCSI(ctx context.Context, req *csipb.NodeStageVolumeRequest, volumeID, stagingPath string) (*csipb.NodeStageVolumeResponse, error) {
	if s.iscsiUtil == nil {
		return nil, status.Error(codes.FailedPrecondition, "iSCSI attach utility is not configured")
	}
	publish := s.refreshPublishContext(ctx, volumeID, req.GetPublishContext())
	publishContext := publish.Context
	multipathRequested := iscsiMultipathFromContext(req.GetPublishContext()) || iscsiMultipathFromContext(req.GetVolumeContext())
	if multipathRequested || iscsiMultipathFromContext(publishContext) {
		publish = s.waitForISCSIMultipathPublishContext(ctx, volumeID, publish, 2)
		publishContext = publish.Context
	}
	if multipathRequested && !iscsiMultipathFromContext(publishContext) {
		publishContext = cloneStringMap(publishContext)
		publishContext["stage2_multipath"] = "true"
		publish.Context = publishContext
	}
	portal, iqn := iscsiFromContext(publishContext)
	portals := iscsiPortalsFromContext(publishContext)
	auth := iscsiAuthFromContext(req.GetSecrets())
	if auth == (ISCSIAuth{}) {
		auth = iscsiAuthFromContext(publishContext)
	}
	if portal == "" || iqn == "" {
		portal, iqn = iscsiFromContext(req.GetVolumeContext())
		portals = iscsiPortalsFromContext(req.GetVolumeContext())
		if auth == (ISCSIAuth{}) {
			auth = iscsiAuthFromContext(req.GetVolumeContext())
		}
	}
	if portal == "" || iqn == "" {
		return nil, status.Error(codes.FailedPrecondition, "no iSCSI publish target")
	}
	if len(portals) == 0 {
		portals = []string{portal}
	}
	multipath := iscsiMultipathFromContext(publishContext)
	if !multipath {
		multipath = iscsiMultipathFromContext(req.GetVolumeContext())
	}
	if multipath && len(portals) < 2 {
		return nil, status.Errorf(codes.FailedPrecondition, "iSCSI multipath requires at least two portals, got %d", len(portals))
	}
	if err := validateISCSIAuth(auth); err != nil {
		return nil, err
	}

	loginStarted := false
	for _, p := range portals {
		loggedIn, err := s.iscsiUtil.IsLoggedIn(ctx, iqn, p)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "check iscsi login: %v", err)
		}
		if loggedIn && !s.hasStagedIdentity(volumeID, stagingPath) {
			return nil, status.Errorf(codes.FailedPrecondition, "iSCSI session for %q at %s is already logged in without staged volume identity", iqn, p)
		}
		if loggedIn {
			continue
		}
		if err := s.iscsiUtil.Discovery(ctx, p); err != nil {
			return nil, status.Errorf(codes.Internal, "iscsi discovery: %v", err)
		}
		if auth.Secret != "" {
			if err := s.iscsiUtil.ConfigureCHAP(ctx, iqn, p, auth); err != nil {
				return nil, status.Errorf(codes.Internal, "iscsi chap config: %v", err)
			}
		}
		if err := s.iscsiUtil.Login(ctx, iqn, p); err != nil {
			return nil, status.Errorf(codes.Internal, "iscsi login: %v", err)
		}
		loginStarted = true
	}

	success := false
	defer func() {
		if !success && loginStarted {
			_ = s.iscsiUtil.Logout(context.Background(), iqn)
		}
	}()

	device := ""
	var err error
	if multipath {
		device, err = s.iscsiUtil.GetMultipathDeviceByIQN(ctx, iqn, len(portals))
	} else {
		device, err = s.iscsiUtil.GetDeviceByIQN(ctx, iqn, portal)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get device: %v", err)
	}
	if device == "" {
		return nil, status.Error(codes.Internal, "get device: empty path")
	}
	if err := os.MkdirAll(stagingPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "create staging dir: %v", err)
	}
	fsType := "ext4"
	if mnt := req.GetVolumeCapability().GetMount(); mnt != nil && mnt.FsType != "" {
		fsType = mnt.FsType
	}
	if err := s.mountUtil.FormatAndMount(ctx, device, stagingPath, fsType); err != nil {
		return nil, status.Errorf(codes.Internal, "format and mount: %v", err)
	}
	if err := writeTransportFile(stagingPath, transportISCSI); err != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, err)
	}
	if err := writeVolumeFile(stagingPath, volumeID); err != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, err)
	}
	if err := writeTargetFile(stagingPath, iqn); err != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, err)
	}

	s.stagedMu.Lock()
	s.staged[volumeID] = &stagedVolumeInfo{
		iqn:         iqn,
		iscsiAddr:   portal,
		iscsiAddrs:  append([]string(nil), portals...),
		multipath:   multipath,
		transport:   transportISCSI,
		fsType:      fsType,
		stagingPath: stagingPath,
	}
	s.stagedMu.Unlock()

	s.logger.Printf("NodeStageVolume: %s staged transport=iscsi portal=%s portals=%s target=%s multipath=%v staging=%s", volumeID, portal, strings.Join(portals, ","), iqn, multipath, stagingPath)
	success = true
	s.reportCSIReattachObserved(ctx, volumeID, transportISCSI, portal, publish)
	return &csipb.NodeStageVolumeResponse{}, nil
}

func (s *NodeServer) stageNVMe(ctx context.Context, req *csipb.NodeStageVolumeRequest, volumeID, stagingPath string) (*csipb.NodeStageVolumeResponse, error) {
	if s.nvmeUtil == nil {
		return nil, status.Error(codes.FailedPrecondition, "NVMe attach utility is not configured")
	}
	publish := s.refreshPublishContext(ctx, volumeID, req.GetPublishContext())
	publishContext := publish.Context
	multipathRequested := iscsiMultipathFromContext(publishContext) || iscsiMultipathFromContext(req.GetPublishContext()) || iscsiMultipathFromContext(req.GetVolumeContext())
	if multipathRequested {
		publish = s.waitForNVMeMultipathPublishContext(ctx, volumeID, publish, 2)
		publishContext = publish.Context
	}
	addr, nqn := nvmeFromContext(publishContext)
	addrs := nvmeAddrsFromContext(publishContext)
	if addr == "" || nqn == "" {
		addr, nqn = nvmeFromContext(req.GetVolumeContext())
		addrs = nvmeAddrsFromContext(req.GetVolumeContext())
	}
	if addr == "" || nqn == "" {
		return nil, status.Error(codes.FailedPrecondition, "no NVMe publish target")
	}
	if len(addrs) == 0 {
		addrs = []string{addr}
	}
	if multipathRequested && len(addrs) < 2 {
		return nil, status.Errorf(codes.FailedPrecondition, "NVMe multipath requires at least two portals, got %d", len(addrs))
	}
	connectAllPaths := multipathRequested || len(addrs) > 1
	connected, err := s.nvmeUtil.IsConnected(ctx, nqn)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check nvme connect: %v", err)
	}
	if connected && !s.hasStagedIdentity(volumeID, stagingPath) {
		s.logger.Printf("NodeStageVolume: %s NVMe subsystem %q already connected without marker at %s; recreating staging identity", volumeID, nqn, stagingPath)
	}
	connectStarted := false
	if connectAllPaths {
		for _, pathAddr := range addrs {
			pathConnected, err := s.nvmeUtil.IsPathConnected(ctx, nqn, pathAddr)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "check nvme path connect: %v", err)
			}
			if !pathConnected {
				if err := s.nvmeUtil.Connect(ctx, pathAddr, nqn); err != nil {
					return nil, status.Errorf(codes.Internal, "nvme connect: %v", err)
				}
				connectStarted = true
			}
		}
	} else if !connected {
		if err := s.nvmeUtil.Connect(ctx, addr, nqn); err != nil {
			return nil, status.Errorf(codes.Internal, "nvme connect: %v", err)
		}
		connectStarted = true
	}
	if connectAllPaths {
		for _, pathAddr := range addrs {
			pathConnected, err := s.nvmeUtil.IsPathConnected(ctx, nqn, pathAddr)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "verify nvme path connect: %v", err)
			}
			if !pathConnected {
				return nil, status.Errorf(codes.FailedPrecondition, "NVMe multipath %q missing path %s", nqn, pathAddr)
			}
		}
	} else if !connected {
		connected = true
	}
	success := false
	defer func() {
		if !success && connectStarted {
			_ = s.nvmeUtil.Disconnect(context.Background(), nqn)
		}
	}()

	device, err := s.nvmeUtil.GetDeviceByNQN(ctx, nqn)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get nvme device: %v", err)
	}
	if device == "" {
		return nil, status.Error(codes.Internal, "get nvme device: empty path")
	}
	if err := os.MkdirAll(stagingPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "create staging dir: %v", err)
	}
	fsType := "ext4"
	if mnt := req.GetVolumeCapability().GetMount(); mnt != nil && mnt.FsType != "" {
		fsType = mnt.FsType
	}
	if err := s.mountUtil.FormatAndMount(ctx, device, stagingPath, fsType); err != nil {
		return nil, status.Errorf(codes.Internal, "format and mount: %v", err)
	}
	if err := writeTransportFile(stagingPath, transportNVMe); err != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, err)
	}
	if err := writeVolumeFile(stagingPath, volumeID); err != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, err)
	}
	if err := writeTargetFile(stagingPath, nqn); err != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, err)
	}

	s.stagedMu.Lock()
	s.staged[volumeID] = &stagedVolumeInfo{
		nqn:         nqn,
		nvmeAddr:    addr,
		nvmeAddrs:   append([]string(nil), addrs...),
		multipath:   len(addrs) > 1,
		transport:   transportNVMe,
		fsType:      fsType,
		stagingPath: stagingPath,
	}
	s.stagedMu.Unlock()

	s.logger.Printf("NodeStageVolume: %s staged transport=nvme portal=%s portals=%s target=%s multipath=%v staging=%s", volumeID, addr, strings.Join(addrs, ","), nqn, len(addrs) > 1, stagingPath)
	success = true
	s.reportCSIReattachObserved(ctx, volumeID, transportNVMe, addr, publish)
	return &csipb.NodeStageVolumeResponse{}, nil
}

func (s *NodeServer) reconcileMountedNVMePaths(ctx context.Context, req *csipb.NodeStageVolumeRequest, volumeID, stagingPath string) error {
	if s.nvmeUtil == nil {
		return status.Error(codes.FailedPrecondition, "NVMe attach utility is not configured")
	}
	publish := s.refreshPublishContext(ctx, volumeID, req.GetPublishContext())
	publishContext := publish.Context
	multipathRequested := iscsiMultipathFromContext(publishContext) || iscsiMultipathFromContext(req.GetPublishContext()) || iscsiMultipathFromContext(req.GetVolumeContext())
	if multipathRequested {
		publish = s.waitForNVMeMultipathPublishContext(ctx, volumeID, publish, 2)
		publishContext = publish.Context
	}
	addr, nqn := nvmeFromContext(publishContext)
	addrs := nvmeAddrsFromContext(publishContext)
	if addr == "" || nqn == "" {
		addr, nqn = nvmeFromContext(req.GetVolumeContext())
		addrs = nvmeAddrsFromContext(req.GetVolumeContext())
	}
	if addr == "" || nqn == "" {
		return nil
	}
	if len(addrs) == 0 {
		addrs = []string{addr}
	}
	existingNQN := readTargetFile(stagingPath)
	if existingNQN != "" && existingNQN != nqn {
		return status.Errorf(codes.FailedPrecondition, "mounted NVMe staging target mismatch: got %q want %q", existingNQN, nqn)
	}
	if multipathRequested && len(addrs) < 2 {
		return status.Errorf(codes.FailedPrecondition, "NVMe multipath requires at least two portals, got %d", len(addrs))
	}

	connectAllPaths := multipathRequested || len(addrs) > 1
	connectedNewPath := false
	if connectAllPaths {
		for _, pathAddr := range addrs {
			pathConnected, err := s.nvmeUtil.IsPathConnected(ctx, nqn, pathAddr)
			if err != nil {
				return status.Errorf(codes.Internal, "check nvme path connect: %v", err)
			}
			if pathConnected {
				continue
			}
			if err := s.nvmeUtil.Connect(ctx, pathAddr, nqn); err != nil {
				return status.Errorf(codes.Internal, "nvme reconnect path: %v", err)
			}
			connectedNewPath = true
		}
		for _, pathAddr := range addrs {
			pathConnected, err := s.nvmeUtil.IsPathConnected(ctx, nqn, pathAddr)
			if err != nil {
				return status.Errorf(codes.Internal, "verify nvme path connect: %v", err)
			}
			if !pathConnected {
				return status.Errorf(codes.FailedPrecondition, "NVMe multipath %q missing path %s", nqn, pathAddr)
			}
		}
	} else {
		connected, err := s.nvmeUtil.IsConnected(ctx, nqn)
		if err != nil {
			return status.Errorf(codes.Internal, "check nvme connect: %v", err)
		}
		if !connected {
			if err := s.nvmeUtil.Connect(ctx, addr, nqn); err != nil {
				return status.Errorf(codes.Internal, "nvme reconnect: %v", err)
			}
			connectedNewPath = true
		}
	}

	if err := writeTransportFile(stagingPath, transportNVMe); err != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, err)
	}
	if err := writeVolumeFile(stagingPath, volumeID); err != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, err)
	}
	if err := writeTargetFile(stagingPath, nqn); err != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, err)
	}

	s.stagedMu.Lock()
	s.staged[volumeID] = &stagedVolumeInfo{
		nqn:         nqn,
		nvmeAddr:    addr,
		nvmeAddrs:   append([]string(nil), addrs...),
		multipath:   len(addrs) > 1,
		transport:   transportNVMe,
		stagingPath: stagingPath,
	}
	s.stagedMu.Unlock()
	if connectedNewPath {
		s.logger.Printf("NodeStageVolume: %s reconciled mounted NVMe paths portals=%s target=%s", volumeID, strings.Join(addrs, ","), nqn)
		s.reportCSIReattachObserved(ctx, volumeID, transportNVMe, addr, publish)
	}
	return nil
}

func (s *NodeServer) reportCSIReattachObserved(ctx context.Context, volumeID, transport, targetAddr string, publish publishContextResult) {
	if s.events == nil {
		return
	}
	event := ClusterEvent{
		VolumeID:    volumeID,
		NodeName:    s.nodeID,
		Type:        EventTypeCSIReattachObserved,
		Severity:    EventSeverityInfo,
		Reason:      EventTypeCSIReattachObserved,
		NewValue:    targetAddr,
		Message:     fmt.Sprintf("CSI staged %s volume on node %s", transport, s.nodeID),
		EvidenceRef: "csi-node",
	}
	if publish.HasTarget {
		target := publish.Target
		event.ReplicaID = target.ReplicaID
		event.Epoch = target.Epoch
		event.EndpointVersion = target.EndpointVersion
		switch target.Protocol {
		case ProtocolISCSI:
			if target.ISCSIAddr != "" {
				event.NewValue = target.ISCSIAddr
			}
		case ProtocolNVMe:
			if target.NVMeAddr != "" {
				event.NewValue = target.NVMeAddr
			}
		}
	}
	reportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.events.ReportEvent(reportCtx, event); err != nil {
		s.logger.Printf("NodeStageVolume: %s report %s failed: %v (non-fatal)", volumeID, EventTypeCSIReattachObserved, err)
	}
}

func (s *NodeServer) NodeUnstageVolume(ctx context.Context, req *csipb.NodeUnstageVolumeRequest) (*csipb.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingPath := req.GetStagingTargetPath()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if s.mountUtil == nil {
		return nil, status.Error(codes.FailedPrecondition, "mount utility is not configured")
	}

	s.stagedMu.Lock()
	info := s.staged[volumeID]
	s.stagedMu.Unlock()

	iqn := ""
	if info != nil {
		iqn = info.iqn
	} else if s.iqnPrefix != "" {
		iqn = s.iqnPrefix + ":" + sanitizeIQN(volumeID)
	}
	transport := readTransportFile(stagingPath)
	if info != nil && info.transport != "" {
		transport = info.transport
	}
	if transport == "" {
		transport = transportISCSI
	}
	nqn := ""
	if info != nil {
		nqn = info.nqn
	} else if transport == transportNVMe {
		nqn = readTargetFile(stagingPath)
	}

	var firstErr error
	if err := s.mountUtil.Unmount(ctx, stagingPath); err != nil {
		firstErr = err
	}
	if transport == transportISCSI && iqn != "" && s.iscsiUtil != nil {
		if err := s.iscsiUtil.Logout(ctx, iqn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if transport == transportNVMe && nqn != "" && s.nvmeUtil != nil {
		if err := s.nvmeUtil.Disconnect(ctx, nqn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return nil, status.Errorf(codes.Internal, "unstage: %v", firstErr)
	}

	_ = os.Remove(filepath.Join(stagingPath, transportFile))
	_ = os.Remove(filepath.Join(stagingPath, volumeFile))
	_ = os.Remove(filepath.Join(stagingPath, targetFile))
	s.stagedMu.Lock()
	delete(s.staged, volumeID)
	s.stagedMu.Unlock()
	return &csipb.NodeUnstageVolumeResponse{}, nil
}

func (s *NodeServer) NodePublishVolume(ctx context.Context, req *csipb.NodePublishVolumeRequest) (*csipb.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	stagingPath := req.GetStagingTargetPath()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if s.mountUtil == nil {
		return nil, status.Error(codes.FailedPrecondition, "mount utility is not configured")
	}
	if err := s.validatePublishStagingVolume(volumeID, stagingPath); err != nil {
		return nil, err
	}
	mounted, err := s.mountUtil.IsMounted(ctx, targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mount: %v", err)
	}
	if mounted {
		return &csipb.NodePublishVolumeResponse{}, nil
	}
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "create target dir: %v", err)
	}
	if err := s.mountUtil.BindMount(ctx, stagingPath, targetPath, req.GetReadonly()); err != nil {
		return nil, status.Errorf(codes.Internal, "bind mount: %v", err)
	}
	return &csipb.NodePublishVolumeResponse{}, nil
}

func (s *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csipb.NodeUnpublishVolumeRequest) (*csipb.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if s.mountUtil == nil {
		return nil, status.Error(codes.FailedPrecondition, "mount utility is not configured")
	}
	mounted, err := s.mountUtil.IsMounted(ctx, req.GetTargetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mount: %v", err)
	}
	if mounted {
		if err := s.mountUtil.Unmount(ctx, req.GetTargetPath()); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount: %v", err)
		}
	}
	_ = os.RemoveAll(req.GetTargetPath())
	return &csipb.NodeUnpublishVolumeResponse{}, nil
}

func (s *NodeServer) NodeGetCapabilities(context.Context, *csipb.NodeGetCapabilitiesRequest) (*csipb.NodeGetCapabilitiesResponse, error) {
	caps := []csipb.NodeServiceCapability_RPC_Type{
		csipb.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
	}
	out := make([]*csipb.NodeServiceCapability, 0, len(caps))
	for _, capType := range caps {
		out = append(out, &csipb.NodeServiceCapability{
			Type: &csipb.NodeServiceCapability_Rpc{
				Rpc: &csipb.NodeServiceCapability_RPC{Type: capType},
			},
		})
	}
	return &csipb.NodeGetCapabilitiesResponse{Capabilities: out}, nil
}

func (s *NodeServer) NodeGetInfo(context.Context, *csipb.NodeGetInfoRequest) (*csipb.NodeGetInfoResponse, error) {
	return &csipb.NodeGetInfoResponse{
		NodeId:            s.nodeID,
		MaxVolumesPerNode: 256,
		AccessibleTopology: &csipb.Topology{Segments: map[string]string{
			fmt.Sprintf("topology.%s/node", DriverName): s.nodeID,
		}},
	}, nil
}

func iscsiFromContext(ctx map[string]string) (portal, iqn string) {
	if ctx == nil {
		return "", ""
	}
	portals := iscsiPortalsFromContext(ctx)
	if len(portals) > 0 {
		return portals[0], ctx["iqn"]
	}
	return ctx["iscsiAddr"], ctx["iqn"]
}

func iscsiPortalsFromContext(ctx map[string]string) []string {
	if ctx == nil {
		return nil
	}
	raw := ctx["iscsiAddrs"]
	if raw == "" {
		raw = ctx["iscsiAddr"]
	}
	var out []string
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		portal := strings.TrimSpace(part)
		if portal == "" || seen[portal] {
			continue
		}
		seen[portal] = true
		out = append(out, portal)
	}
	return out
}

func iscsiMultipathFromContext(ctx map[string]string) bool {
	if ctx == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(ctx["stage2_multipath"])) {
	case "1", "true", "yes", "required":
		return true
	default:
		return false
	}
}

func nvmeFromContext(ctx map[string]string) (addr, nqn string) {
	if ctx == nil {
		return "", ""
	}
	addrs := nvmeAddrsFromContext(ctx)
	if len(addrs) > 0 {
		return addrs[0], ctx["nqn"]
	}
	return ctx["nvmeAddr"], ctx["nqn"]
}

func nvmeAddrsFromContext(ctx map[string]string) []string {
	if ctx == nil {
		return nil
	}
	raw := ctx["nvmeAddrs"]
	if raw == "" {
		raw = ctx["nvmeAddr"]
	}
	var out []string
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		addr := strings.TrimSpace(part)
		if addr == "" || seen[addr] {
			continue
		}
		seen[addr] = true
		out = append(out, addr)
	}
	return out
}

func transportFromContext(contexts ...map[string]string) string {
	for _, ctx := range contexts {
		if ctx == nil {
			continue
		}
		switch ctx["protocol"] {
		case transportNVMe:
			return transportNVMe
		case transportISCSI:
			return transportISCSI
		}
	}
	for _, ctx := range contexts {
		if _, ok := ctx["nvmeAddr"]; ok && ctx["nqn"] != "" {
			return transportNVMe
		}
	}
	return transportISCSI
}

func iscsiAuthFromContext(ctx map[string]string) ISCSIAuth {
	if ctx == nil {
		return ISCSIAuth{}
	}
	return ISCSIAuth{
		Username: ctx["chapUsername"],
		Secret:   ctx["chapSecret"],
	}
}

type publishContextResult struct {
	Context   map[string]string
	Target    PublishTarget
	HasTarget bool
}

func (s *NodeServer) refreshPublishContext(ctx context.Context, volumeID string, fallback map[string]string) publishContextResult {
	result := publishContextResult{Context: fallback}
	if s.lookup == nil || volumeID == "" {
		return result
	}
	target, err := s.lookup.LookupPublishTarget(ctx, volumeID, s.nodeID)
	if err != nil {
		s.logger.Printf("NodeStageVolume: %s publish target refresh failed: %v", volumeID, err)
		return result
	}
	refreshed := publishContext(target)
	if len(refreshed) == 0 {
		s.logger.Printf("NodeStageVolume: %s publish target refresh returned no attachable frontend", volumeID)
		return result
	}
	return publishContextResult{Context: refreshed, Target: target, HasTarget: true}
}

func (s *NodeServer) waitForISCSIMultipathPublishContext(ctx context.Context, volumeID string, current publishContextResult, minPortals int) publishContextResult {
	if hasISCSIMultipathPortals(current.Context, minPortals) {
		return current
	}
	if s.lookup == nil || volumeID == "" {
		return current
	}
	deadline := time.NewTimer(stage2MultipathPublishWait)
	defer deadline.Stop()
	ticker := time.NewTicker(stage2MultipathPublishPoll)
	defer ticker.Stop()

	for {
		target, err := s.lookup.LookupPublishTarget(ctx, volumeID, s.nodeID)
		if err != nil {
			s.logger.Printf("NodeStageVolume: %s multipath publish target wait failed: %v", volumeID, err)
		} else if refreshed := publishContext(target); len(refreshed) > 0 {
			current = publishContextResult{Context: refreshed, Target: target, HasTarget: true}
			if hasISCSIMultipathPortals(current.Context, minPortals) {
				s.logger.Printf("NodeStageVolume: %s multipath publish target ready portals=%s", volumeID, strings.Join(iscsiPortalsFromContext(current.Context), ","))
				return current
			}
			if iscsiMultipathFromContext(current.Context) {
				s.logger.Printf("NodeStageVolume: %s waiting for multipath publish target portals=%d want>=%d", volumeID, len(iscsiPortalsFromContext(current.Context)), minPortals)
			}
		}

		select {
		case <-ctx.Done():
			return current
		case <-deadline.C:
			return current
		case <-ticker.C:
		}
	}
}

func hasISCSIMultipathPortals(ctx map[string]string, minPortals int) bool {
	return iscsiMultipathFromContext(ctx) && len(iscsiPortalsFromContext(ctx)) >= minPortals
}

func (s *NodeServer) waitForNVMeMultipathPublishContext(ctx context.Context, volumeID string, current publishContextResult, minPortals int) publishContextResult {
	if hasNVMeMultipathPortals(current.Context, minPortals) {
		return current
	}
	if s.lookup == nil {
		return current
	}
	deadline := time.NewTimer(stage2MultipathPublishWait)
	defer deadline.Stop()
	ticker := time.NewTicker(stage2MultipathPublishPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return current
		case <-deadline.C:
			return current
		case <-ticker.C:
			target, err := s.lookup.LookupPublishTarget(ctx, volumeID, s.nodeID)
			if err != nil {
				continue
			}
			if refreshed := publishContext(target); len(refreshed) > 0 {
				current = publishContextResult{Context: refreshed, Target: target, HasTarget: true}
				if hasNVMeMultipathPortals(current.Context, minPortals) {
					return current
				}
			}
		}
	}
}

func hasNVMeMultipathPortals(ctx map[string]string, minPortals int) bool {
	return iscsiMultipathFromContext(ctx) && len(nvmeAddrsFromContext(ctx)) >= minPortals
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (s *NodeServer) validateMountedStagingVolume(volumeID, stagingPath string) error {
	s.stagedMu.Lock()
	info := s.staged[volumeID]
	if info != nil && info.stagingPath == stagingPath {
		s.stagedMu.Unlock()
		return nil
	}
	for otherVolume, otherInfo := range s.staged {
		if otherVolume != volumeID && otherInfo != nil && otherInfo.stagingPath == stagingPath {
			s.stagedMu.Unlock()
			return status.Errorf(codes.FailedPrecondition, "staging path %q is already mounted for volume %q", stagingPath, otherVolume)
		}
	}
	s.stagedMu.Unlock()

	if got := readVolumeFile(stagingPath); got == volumeID {
		return nil
	} else if got != "" {
		return status.Errorf(codes.FailedPrecondition, "staging path %q is already mounted for volume %q", stagingPath, got)
	}
	return status.Errorf(codes.FailedPrecondition, "staging path %q is already mounted without sw-block volume identity", stagingPath)
}

func (s *NodeServer) validatePublishStagingVolume(volumeID, stagingPath string) error {
	s.stagedMu.Lock()
	info := s.staged[volumeID]
	if info != nil && info.stagingPath == stagingPath {
		s.stagedMu.Unlock()
		return nil
	}
	for otherVolume, otherInfo := range s.staged {
		if otherVolume != volumeID && otherInfo != nil && otherInfo.stagingPath == stagingPath {
			s.stagedMu.Unlock()
			return status.Errorf(codes.FailedPrecondition, "staging path %q belongs to volume %q", stagingPath, otherVolume)
		}
	}
	s.stagedMu.Unlock()

	if got := readVolumeFile(stagingPath); got == volumeID {
		return nil
	} else if got != "" {
		return status.Errorf(codes.FailedPrecondition, "staging path %q belongs to volume %q", stagingPath, got)
	}
	return status.Errorf(codes.FailedPrecondition, "staging path %q has no sw-block volume identity", stagingPath)
}

func (s *NodeServer) hasStagedIdentity(volumeID, stagingPath string) bool {
	s.stagedMu.Lock()
	info := s.staged[volumeID]
	if info != nil && info.stagingPath == stagingPath {
		s.stagedMu.Unlock()
		return true
	}
	s.stagedMu.Unlock()
	return readVolumeFile(stagingPath) == volumeID
}

func validateISCSIAuth(auth ISCSIAuth) error {
	if (auth.Username == "") != (auth.Secret == "") {
		return status.Error(codes.FailedPrecondition, "iSCSI CHAP username and secret must be set together")
	}
	return nil
}

func writeTransportFile(stagingPath, transport string) error {
	return os.WriteFile(filepath.Join(stagingPath, transportFile), []byte(transport), 0o600)
}

func writeVolumeFile(stagingPath, volumeID string) error {
	return os.WriteFile(filepath.Join(stagingPath, volumeFile), []byte(volumeID), 0o600)
}

func writeTargetFile(stagingPath, target string) error {
	return os.WriteFile(filepath.Join(stagingPath, targetFile), []byte(target), 0o600)
}

func readVolumeFile(stagingPath string) string {
	b, err := os.ReadFile(filepath.Join(stagingPath, volumeFile))
	if err != nil {
		return ""
	}
	return string(b)
}

func readTargetFile(stagingPath string) string {
	b, err := os.ReadFile(filepath.Join(stagingPath, targetFile))
	if err != nil {
		return ""
	}
	return string(b)
}

func readTransportFile(stagingPath string) string {
	b, err := os.ReadFile(filepath.Join(stagingPath, transportFile))
	if err != nil {
		return ""
	}
	s := string(b)
	if s == transportISCSI || s == transportNVMe {
		return s
	}
	return ""
}

func sanitizeIQN(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c+'a'-'A')
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}
