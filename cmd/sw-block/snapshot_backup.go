package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/seaweedfs/seaweed-block/core/ops"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	coresnapshot "github.com/seaweedfs/seaweed-block/core/snapshot"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type snapshotBackupAPI interface {
	Export(context.Context, string, string) (*control.SnapshotBackupRecord, error)
	Get(context.Context, string) (*control.SnapshotBackupRecord, error)
	List(context.Context) ([]*control.SnapshotBackupRecord, error)
	Import(context.Context, string) (*control.SnapshotRecord, error)
	Close() error
}

type snapshotBackupAPIConfig struct {
	Address        string
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string
	TokenFile      string
}

var snapshotBackupAPIFactory = newSnapshotBackupAPI

var (
	snapshotBackupIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	snapshotRecordIDPattern = regexp.MustCompile(`^snap-[0-9a-f]{32}$`)
	sha256Pattern           = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func runOpsSnapshotBackup(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "sw-block ops snapshot-backup: expected export|get|list|import")
		return ops.VolumeStatusExitInvalid
	}
	action := args[0]
	fs := flag.NewFlagSet("sw-block ops snapshot-backup "+action, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg snapshotBackupAPIConfig
	var backupID, snapshotID string
	var timeout time.Duration
	fs.StringVar(&cfg.Address, "api", "", "dedicated blockmaster mTLS SnapshotBackupService address")
	fs.StringVar(&cfg.CAFile, "ca", "", "CA certificate for SnapshotBackupService")
	fs.StringVar(&cfg.ClientCertFile, "client-cert", "", "mTLS client certificate for SnapshotBackupService")
	fs.StringVar(&cfg.ClientKeyFile, "client-key", "", "mTLS client private key for SnapshotBackupService")
	fs.StringVar(&cfg.TokenFile, "token-file", "", "file containing the backup-only bearer token")
	fs.StringVar(&backupID, "backup-id", "", "backup identity")
	fs.StringVar(&snapshotID, "snapshot-id", "", "ready snapshot identity for export")
	fs.DurationVar(&timeout, "timeout", 30*time.Minute, "operation timeout")
	if err := fs.Parse(args[1:]); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 || cfg.Address == "" || cfg.CAFile == "" || cfg.ClientCertFile == "" || cfg.ClientKeyFile == "" || cfg.TokenFile == "" || timeout <= 0 {
		fmt.Fprintln(stderr, "sw-block ops snapshot-backup: complete mTLS/token configuration and a positive timeout are required")
		return ops.VolumeStatusExitInvalid
	}
	switch action {
	case "export":
		if !snapshotBackupIDPattern.MatchString(backupID) || !snapshotRecordIDPattern.MatchString(snapshotID) {
			fmt.Fprintln(stderr, "sw-block ops snapshot-backup export: canonical --backup-id and --snapshot-id are required")
			return ops.VolumeStatusExitInvalid
		}
	case "get", "import":
		if !snapshotBackupIDPattern.MatchString(backupID) || snapshotID != "" {
			fmt.Fprintf(stderr, "sw-block ops snapshot-backup %s: canonical --backup-id is required and --snapshot-id is not accepted\n", action)
			return ops.VolumeStatusExitInvalid
		}
	case "list":
		if backupID != "" || snapshotID != "" {
			fmt.Fprintln(stderr, "sw-block ops snapshot-backup list: backup and snapshot filters are not supported")
			return ops.VolumeStatusExitInvalid
		}
	default:
		fmt.Fprintf(stderr, "sw-block ops snapshot-backup: unknown action %q\n", action)
		return ops.VolumeStatusExitInvalid
	}

	api, err := snapshotBackupAPIFactory(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops snapshot-backup: %v\n", err)
		return ops.VolumeStatusExitInvalid
	}
	defer api.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	switch action {
	case "export":
		record, err := api.Export(ctx, backupID, snapshotID)
		return printSnapshotBackupResult("export", record, err, backupID, snapshotID, stdout, stderr)
	case "get":
		record, err := api.Get(ctx, backupID)
		return printSnapshotBackupResult("get", record, err, backupID, "", stdout, stderr)
	case "list":
		records, err := api.List(ctx)
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops snapshot-backup list: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		for _, record := range records {
			if err := validateSnapshotBackupRecord(record, "", ""); err != nil {
				fmt.Fprintf(stderr, "sw-block ops snapshot-backup list: %v\n", err)
				return ops.VolumeStatusExitInvalid
			}
		}
		fmt.Fprintf(stdout, "backup_count=%d\n", len(records))
		for _, record := range records {
			printSnapshotBackupRecord(stdout, record)
		}
		return ops.VolumeStatusExitOK
	case "import":
		backup, err := api.Get(ctx, backupID)
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops snapshot-backup import: get backup: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		if err := validateSnapshotBackupRecord(backup, backupID, ""); err != nil {
			fmt.Fprintf(stderr, "sw-block ops snapshot-backup import: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		record, err := api.Import(ctx, backupID)
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops snapshot-backup import: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		if err := validateSnapshotRecord(record); err != nil {
			fmt.Fprintf(stderr, "sw-block ops snapshot-backup import: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		if !sameImportedSnapshot(record, backup.GetSnapshot()) {
			fmt.Fprintln(stderr, "sw-block ops snapshot-backup import: imported snapshot does not match the requested backup")
			return ops.VolumeStatusExitInvalid
		}
		fmt.Fprintf(stdout, "snapshot_backup_operation=import backup_id=%s snapshot_id=%s source_volume_id=%s state=%s\n", backupID, record.GetSnapshotId(), record.GetSourceVolumeId(), record.GetState())
		return ops.VolumeStatusExitOK
	}
	return ops.VolumeStatusExitInvalid
}

func printSnapshotBackupResult(action string, record *control.SnapshotBackupRecord, err error, backupID, snapshotID string, stdout, stderr io.Writer) int {
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops snapshot-backup %s: %v\n", action, err)
		return ops.VolumeStatusExitInvalid
	}
	if err := validateSnapshotBackupRecord(record, backupID, snapshotID); err != nil {
		fmt.Fprintf(stderr, "sw-block ops snapshot-backup %s: %v\n", action, err)
		return ops.VolumeStatusExitInvalid
	}
	fmt.Fprintf(stdout, "snapshot_backup_operation=%s ", action)
	printSnapshotBackupRecord(stdout, record)
	return ops.VolumeStatusExitOK
}

func validateSnapshotBackupRecord(record *control.SnapshotBackupRecord, expectedBackupID, expectedSnapshotID string) error {
	if record == nil || !snapshotBackupIDPattern.MatchString(record.GetBackupId()) || !snapshotRecordIDPattern.MatchString(record.GetSourceSnapshotId()) || record.GetState() != "ready" || record.GetArchiveBytes() <= 0 || !sha256Pattern.MatchString(record.GetArchiveSha256()) || !sha256Pattern.MatchString(record.GetManifestSha256()) || record.GetDestinationEvidence() != "archives/"+record.GetBackupId()+".sbbackup" {
		return fmt.Errorf("backup API returned an incomplete or invalid ready record")
	}
	if _, err := ptypes.Timestamp(record.GetCreatedAt()); err != nil {
		return fmt.Errorf("backup API returned an invalid creation timestamp")
	}
	if expectedBackupID != "" && record.GetBackupId() != expectedBackupID {
		return fmt.Errorf("backup API returned backup %q, expected %q", record.GetBackupId(), expectedBackupID)
	}
	if expectedSnapshotID != "" && record.GetSourceSnapshotId() != expectedSnapshotID {
		return fmt.Errorf("backup API returned snapshot %q, expected %q", record.GetSourceSnapshotId(), expectedSnapshotID)
	}
	snapshot := record.GetSnapshot()
	if err := validateSnapshotRecord(snapshot); err != nil || snapshot.GetSnapshotId() != record.GetSourceSnapshotId() || snapshot.GetArchiveBytes() != record.GetArchiveBytes() || snapshot.GetArchiveSha256() != record.GetArchiveSha256() {
		return fmt.Errorf("backup API returned inconsistent snapshot metadata")
	}
	return nil
}

func validateSnapshotRecord(record *control.SnapshotRecord) error {
	domain, err := snapshotRecordFromWire(record)
	if err != nil || coresnapshot.ValidatePortableRecord(domain) != nil {
		return fmt.Errorf("snapshot API returned an incomplete or invalid ready record")
	}
	return nil
}

func sameImportedSnapshot(got, want *control.SnapshotRecord) bool {
	gotRecord, gotErr := snapshotRecordFromWire(got)
	wantRecord, wantErr := snapshotRecordFromWire(want)
	return gotErr == nil && wantErr == nil && coresnapshot.SamePortableRecord(gotRecord, wantRecord)
}

func snapshotRecordFromWire(record *control.SnapshotRecord) (coresnapshot.Record, error) {
	if record == nil || uint64(record.GetBlockSize()) > uint64(^uint(0)>>1) {
		return coresnapshot.Record{}, fmt.Errorf("invalid snapshot record")
	}
	createdAt, err := ptypes.Timestamp(record.GetCreatedAt())
	if err != nil {
		return coresnapshot.Record{}, err
	}
	return coresnapshot.Record{
		SnapshotID:     record.GetSnapshotId(),
		Name:           record.GetName(),
		SourceVolumeID: record.GetSourceVolumeId(),
		CreatedAt:      createdAt,
		State:          record.GetState(),
		Frontier:       record.GetFrontier(),
		SizeBytes:      record.GetSizeBytes(),
		NumBlocks:      record.GetNumBlocks(),
		BlockSize:      int(record.GetBlockSize()),
		RecordCount:    record.GetRecordCount(),
		DataBytes:      record.GetDataBytes(),
		ArchiveBytes:   record.GetArchiveBytes(),
		ArchiveSHA256:  record.GetArchiveSha256(),
	}, nil
}

func printSnapshotBackupRecord(w io.Writer, record *control.SnapshotBackupRecord) {
	fmt.Fprintf(w, "backup_id=%s source_snapshot_id=%s state=%s archive_bytes=%d archive_sha256=%s manifest_sha256=%s destination_evidence=%s\n",
		record.GetBackupId(), record.GetSourceSnapshotId(), record.GetState(), record.GetArchiveBytes(), record.GetArchiveSha256(), record.GetManifestSha256(), record.GetDestinationEvidence())
}

type grpcSnapshotBackupAPI struct {
	client control.SnapshotBackupServiceClient
	token  string
	conn   *grpc.ClientConn
}

func newSnapshotBackupAPI(cfg snapshotBackupAPIConfig) (snapshotBackupAPI, error) {
	certificate, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load backup API client identity: %w", err)
	}
	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read backup API CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("backup API CA contains no certificates")
	}
	tokenBytes, err := os.ReadFile(cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read backup API token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return nil, fmt.Errorf("backup API token is empty")
	}
	conn, err := grpc.NewClient(cfg.Address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs: roots, Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
	})))
	if err != nil {
		return nil, fmt.Errorf("dial backup API: %w", err)
	}
	return &grpcSnapshotBackupAPI{client: control.NewSnapshotBackupServiceClient(conn), token: token, conn: conn}, nil
}

func (a *grpcSnapshotBackupAPI) authorized(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+a.token)
}

func (a *grpcSnapshotBackupAPI) Export(ctx context.Context, backupID, snapshotID string) (*control.SnapshotBackupRecord, error) {
	return a.client.ExportSnapshotBackup(a.authorized(ctx), &control.ExportSnapshotBackupRequest{BackupId: backupID, SnapshotId: snapshotID})
}

func (a *grpcSnapshotBackupAPI) Get(ctx context.Context, backupID string) (*control.SnapshotBackupRecord, error) {
	return a.client.GetSnapshotBackup(a.authorized(ctx), &control.GetSnapshotBackupRequest{BackupId: backupID})
}

func (a *grpcSnapshotBackupAPI) List(ctx context.Context) ([]*control.SnapshotBackupRecord, error) {
	response, err := a.client.ListSnapshotBackups(a.authorized(ctx), &control.ListSnapshotBackupsRequest{})
	if err != nil {
		return nil, err
	}
	return response.GetBackups(), nil
}

func (a *grpcSnapshotBackupAPI) Import(ctx context.Context, backupID string) (*control.SnapshotRecord, error) {
	return a.client.ImportSnapshotBackup(a.authorized(ctx), &control.ImportSnapshotBackupRequest{BackupId: backupID})
}

func (a *grpcSnapshotBackupAPI) Close() error {
	return a.conn.Close()
}
