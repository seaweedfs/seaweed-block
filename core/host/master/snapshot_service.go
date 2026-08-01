package master

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (h *Host) configureSnapshotCoordinator() error {
	cfg := h.cfg
	configured := cfg.SnapshotRoot != "" || cfg.SnapshotRuntimeCAFile != "" || cfg.SnapshotRuntimeTokenFile != "" ||
		cfg.SnapshotRuntimeClientCertFile != "" || cfg.SnapshotRuntimeClientKeyFile != "" || cfg.SnapshotAPIListen != "" ||
		cfg.SnapshotAPITLSCertFile != "" || cfg.SnapshotAPITLSKeyFile != "" || cfg.SnapshotAPIClientCAFile != "" || cfg.SnapshotAPITokenFile != ""
	if !configured {
		return nil
	}
	if cfg.SnapshotRoot == "" || cfg.SnapshotRuntimeCAFile == "" || cfg.SnapshotRuntimeTokenFile == "" ||
		cfg.SnapshotRuntimeClientCertFile == "" || cfg.SnapshotRuntimeClientKeyFile == "" || cfg.SnapshotAPIListen == "" ||
		cfg.SnapshotAPITLSCertFile == "" || cfg.SnapshotAPITLSKeyFile == "" || cfg.SnapshotAPIClientCAFile == "" || cfg.SnapshotAPITokenFile == "" {
		return fmt.Errorf("snapshot root, runtime mTLS/token, and API listen/mTLS/token must be configured together")
	}

	caPEM, err := os.ReadFile(cfg.SnapshotRuntimeCAFile)
	if err != nil {
		return fmt.Errorf("read runtime CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("runtime CA file contains no certificates")
	}
	tokenBytes, err := os.ReadFile(cfg.SnapshotRuntimeTokenFile)
	if err != nil {
		return fmt.Errorf("read runtime token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return fmt.Errorf("runtime token is empty")
	}
	clientCertificate, err := tls.LoadX509KeyPair(cfg.SnapshotRuntimeClientCertFile, cfg.SnapshotRuntimeClientKeyFile)
	if err != nil {
		return fmt.Errorf("load runtime client identity: %w", err)
	}
	apiTokenBytes, err := os.ReadFile(cfg.SnapshotAPITokenFile)
	if err != nil {
		return fmt.Errorf("read snapshot API token: %w", err)
	}
	apiToken := strings.TrimSpace(string(apiTokenBytes))
	if apiToken == "" {
		return fmt.Errorf("snapshot API token is empty")
	}
	apiCertificate, err := tls.LoadX509KeyPair(cfg.SnapshotAPITLSCertFile, cfg.SnapshotAPITLSKeyFile)
	if err != nil {
		return fmt.Errorf("load snapshot API TLS identity: %w", err)
	}
	apiClientCAPEM, err := os.ReadFile(cfg.SnapshotAPIClientCAFile)
	if err != nil {
		return fmt.Errorf("read snapshot API client CA: %w", err)
	}
	apiClientCAs := x509.NewCertPool()
	if !apiClientCAs.AppendCertsFromPEM(apiClientCAPEM) {
		return fmt.Errorf("snapshot API client CA file contains no certificates")
	}

	manager, err := snapshot.OpenManager(cfg.SnapshotRoot)
	if err != nil {
		return err
	}
	runtimeClient := newSnapshotRuntimeHTTPClient(&tls.Config{RootCAs: roots, Certificates: []tls.Certificate{clientCertificate}, MinVersion: tls.VersionTLS12})
	runtime, err := snapshot.NewHTTPSCaptureRuntime(runtimeClient, token)
	if err != nil {
		return err
	}
	restoreRuntime, err := snapshot.NewHTTPSRestoreRuntime(runtimeClient, token)
	if err != nil {
		return err
	}
	coordinator, err := snapshot.NewCoordinator(manager, h, runtime)
	if err != nil {
		return err
	}
	if err := coordinator.ConfigureRestore(h, restoreRuntime); err != nil {
		return err
	}
	h.snapshotCoordinator = coordinator
	h.snapshotAPIToken = apiToken
	h.snapshotAPILn, err = net.Listen("tcp", cfg.SnapshotAPIListen)
	if err != nil {
		return fmt.Errorf("snapshot API listen %q: %w", cfg.SnapshotAPIListen, err)
	}
	h.snapshotAPITLSConfig = &tls.Config{
		Certificates: []tls.Certificate{apiCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    apiClientCAs,
		MinVersion:   tls.VersionTLS12,
	}
	h.snapshotCaptureTimeout = cfg.SnapshotCaptureTimeout
	if h.snapshotCaptureTimeout <= 0 {
		h.snapshotCaptureTimeout = 30 * time.Minute
	}
	return nil
}

func newSnapshotRuntimeHTTPClient(tlsConfig *tls.Config) *http.Client {
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig:     tlsConfig,
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     30 * time.Second,
	}}
}

func (s *services) CreateSnapshot(ctx context.Context, req *control.CreateSnapshotRequest) (*control.SnapshotRecord, error) {
	coordinator, err := s.snapshotService()
	if err != nil {
		return nil, err
	}
	if err := s.authorizeSnapshot(ctx); err != nil {
		return nil, err
	}
	captureCtx, cancel := context.WithTimeout(ctx, s.host.snapshotCaptureTimeout)
	defer cancel()
	record, err := coordinator.Create(captureCtx, snapshot.CreateRequest{
		Name:           req.GetName(),
		SourceVolumeID: req.GetSourceVolumeId(),
	})
	if err != nil {
		return nil, snapshotRPCError("create snapshot", err)
	}
	return snapshotRecordToWire(record), nil
}

func (s *services) GetSnapshot(ctx context.Context, req *control.GetSnapshotRequest) (*control.SnapshotRecord, error) {
	coordinator, err := s.snapshotService()
	if err != nil {
		return nil, err
	}
	if err := s.authorizeSnapshot(ctx); err != nil {
		return nil, err
	}
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id is required")
	}
	record, ok := coordinator.Get(req.GetSnapshotId())
	if !ok {
		return nil, status.Error(codes.NotFound, snapshot.ErrNotFound.Error())
	}
	return snapshotRecordToWire(record), nil
}

func (s *services) ListSnapshots(ctx context.Context, req *control.ListSnapshotsRequest) (*control.ListSnapshotsResponse, error) {
	coordinator, err := s.snapshotService()
	if err != nil {
		return nil, err
	}
	if err := s.authorizeSnapshot(ctx); err != nil {
		return nil, err
	}
	records := coordinator.List(req.GetSourceVolumeId())
	out := &control.ListSnapshotsResponse{Snapshots: make([]*control.SnapshotRecord, 0, len(records))}
	for _, record := range records {
		out.Snapshots = append(out.Snapshots, snapshotRecordToWire(record))
	}
	return out, nil
}

func (s *services) DeleteSnapshot(ctx context.Context, req *control.DeleteSnapshotRequest) (*control.DeleteSnapshotResponse, error) {
	coordinator, err := s.snapshotService()
	if err != nil {
		return nil, err
	}
	if err := s.authorizeSnapshot(ctx); err != nil {
		return nil, err
	}
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id is required")
	}
	if err := coordinator.Delete(req.GetSnapshotId()); err != nil {
		return nil, snapshotRPCError("delete snapshot", err)
	}
	return &control.DeleteSnapshotResponse{}, nil
}

func (s *services) RestoreSnapshot(ctx context.Context, req *control.RestoreSnapshotRequest) (*control.RestoreSnapshotResponse, error) {
	coordinator, err := s.snapshotService()
	if err != nil {
		return nil, err
	}
	if err := s.authorizeSnapshot(ctx); err != nil {
		return nil, err
	}
	if req.GetSnapshotId() == "" || req.GetTargetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id and target_volume_id are required")
	}
	restoreCtx, cancel := context.WithTimeout(ctx, s.host.snapshotCaptureTimeout)
	defer cancel()
	result, err := coordinator.Restore(restoreCtx, req.GetSnapshotId(), req.GetTargetVolumeId())
	if err != nil {
		return nil, snapshotRPCError("restore snapshot", err)
	}
	return &control.RestoreSnapshotResponse{
		SnapshotId:      result.SnapshotID,
		TargetVolumeId:  result.TargetVolumeID,
		ReplicaCount:    uint32(result.ReplicaCount),
		AlreadyComplete: result.AlreadyComplete,
	}, nil
}

func (s *services) snapshotService() (*snapshot.Coordinator, error) {
	if s.host.snapshotCoordinator == nil {
		return nil, status.Error(codes.FailedPrecondition, "snapshot service is not configured")
	}
	return s.host.snapshotCoordinator, nil
}

func (s *services) authorizeSnapshot(ctx context.Context) error {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	want := "Bearer " + s.host.snapshotAPIToken
	if len(values) != 1 || len(values[0]) != len(want) || subtle.ConstantTimeCompare([]byte(values[0]), []byte(want)) != 1 {
		return status.Error(codes.Unauthenticated, "snapshot API authentication failed")
	}
	return nil
}

func snapshotRecordToWire(record snapshot.Record) *control.SnapshotRecord {
	return &control.SnapshotRecord{
		SnapshotId:     record.SnapshotID,
		Name:           record.Name,
		SourceVolumeId: record.SourceVolumeID,
		CreatedAt:      timestamppb.New(record.CreatedAt),
		State:          record.State,
		Frontier:       record.Frontier,
		SizeBytes:      record.SizeBytes,
		NumBlocks:      record.NumBlocks,
		BlockSize:      uint32(record.BlockSize),
		RecordCount:    record.RecordCount,
		DataBytes:      record.DataBytes,
		ArchiveBytes:   record.ArchiveBytes,
		ArchiveSha256:  record.ArchiveSHA256,
	}
}

func snapshotRPCError(operation string, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, snapshot.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, snapshot.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, snapshot.ErrNameConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, snapshot.ErrInUse), errors.Is(err, snapshot.ErrSourceNotReady):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, snapshot.ErrRestoreNotReady), errors.Is(err, snapshot.ErrRestoreNotApplied), errors.Is(err, snapshot.ErrRestoreUnsafe), errors.Is(err, snapshot.ErrRestoreConflict):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, snapshot.ErrAuthorityChanged):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Errorf(codes.Internal, "%s: %v", operation, err)
	}
}
