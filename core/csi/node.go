package csi

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	transportISCSI = "iscsi"
	transportNVMe  = "nvme"
	transportFile  = ".transport"
)

type ISCSIUtil interface {
	Discovery(ctx context.Context, portal string) error
	ConfigureCHAP(ctx context.Context, iqn, portal string, auth ISCSIAuth) error
	Login(ctx context.Context, iqn, portal string) error
	Logout(ctx context.Context, iqn string) error
	GetDeviceByIQN(ctx context.Context, iqn string) (string, error)
	IsLoggedIn(ctx context.Context, iqn string) (bool, error)
	RescanDevice(ctx context.Context, iqn string) error
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
	transport   string
	fsType      string
	stagingPath string
}

type NodeServer struct {
	csipb.UnimplementedNodeServer
	nodeID    string
	iqnPrefix string
	iscsiUtil ISCSIUtil
	mountUtil MountUtil
	logger    *log.Logger

	stagedMu sync.Mutex
	staged   map[string]*stagedVolumeInfo
}

type NodeConfig struct {
	NodeID    string
	IQNPrefix string
	ISCSIUtil ISCSIUtil
	MountUtil MountUtil
	Logger    *log.Logger
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
		mountUtil: cfg.MountUtil,
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
	if s.iscsiUtil == nil || s.mountUtil == nil {
		return nil, status.Error(codes.FailedPrecondition, "node attach utilities are not configured")
	}
	mounted, err := s.mountUtil.IsMounted(ctx, stagingPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mount: %v", err)
	}
	if mounted {
		s.logger.Printf("NodeStageVolume: %s already mounted at %s", volumeID, stagingPath)
		return &csipb.NodeStageVolumeResponse{}, nil
	}

	portal, iqn := iscsiFromContext(req.GetPublishContext())
	auth := iscsiAuthFromContext(req.GetSecrets())
	if auth == (ISCSIAuth{}) {
		auth = iscsiAuthFromContext(req.GetPublishContext())
	}
	if portal == "" || iqn == "" {
		portal, iqn = iscsiFromContext(req.GetVolumeContext())
		if auth == (ISCSIAuth{}) {
			auth = iscsiAuthFromContext(req.GetVolumeContext())
		}
	}
	if portal == "" || iqn == "" {
		return nil, status.Error(codes.FailedPrecondition, "no iSCSI publish target")
	}
	if err := validateISCSIAuth(auth); err != nil {
		return nil, err
	}

	loggedIn, err := s.iscsiUtil.IsLoggedIn(ctx, iqn)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check iscsi login: %v", err)
	}
	loginStarted := false
	if !loggedIn {
		if err := s.iscsiUtil.Discovery(ctx, portal); err != nil {
			return nil, status.Errorf(codes.Internal, "iscsi discovery: %v", err)
		}
		if auth.Secret != "" {
			if err := s.iscsiUtil.ConfigureCHAP(ctx, iqn, portal, auth); err != nil {
				return nil, status.Errorf(codes.Internal, "iscsi chap config: %v", err)
			}
		}
		if err := s.iscsiUtil.Login(ctx, iqn, portal); err != nil {
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

	device, err := s.iscsiUtil.GetDeviceByIQN(ctx, iqn)
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

	s.stagedMu.Lock()
	s.staged[volumeID] = &stagedVolumeInfo{
		iqn:         iqn,
		iscsiAddr:   portal,
		transport:   transportISCSI,
		fsType:      fsType,
		stagingPath: stagingPath,
	}
	s.stagedMu.Unlock()

	success = true
	return &csipb.NodeStageVolumeResponse{}, nil
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

	var firstErr error
	if err := s.mountUtil.Unmount(ctx, stagingPath); err != nil {
		firstErr = err
	}
	if transport == transportISCSI && iqn != "" && s.iscsiUtil != nil {
		if err := s.iscsiUtil.Logout(ctx, iqn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return nil, status.Errorf(codes.Internal, "unstage: %v", firstErr)
	}

	_ = os.Remove(filepath.Join(stagingPath, transportFile))
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
	return ctx["iscsiAddr"], ctx["iqn"]
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

func validateISCSIAuth(auth ISCSIAuth) error {
	if (auth.Username == "") != (auth.Secret == "") {
		return status.Error(codes.FailedPrecondition, "iSCSI CHAP username and secret must be set together")
	}
	return nil
}

func writeTransportFile(stagingPath, transport string) error {
	return os.WriteFile(filepath.Join(stagingPath, transportFile), []byte(transport), 0o600)
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
