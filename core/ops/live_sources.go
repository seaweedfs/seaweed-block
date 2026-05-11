package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os/exec"
	pathpkg "path"
	"runtime"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LiveVolumeStatusConfig struct {
	VolumeID        string
	MasterAddr      string
	StatusAddr      string
	ProductRevision string
	RunnerRevision  string
	Source          ReportSource
	HTTPClient      *http.Client
	RunCommand      func(context.Context, string, ...string) ([]byte, error)
}

func NewLiveVolumeStatusReportCollector(cfg LiveVolumeStatusConfig) VolumeStatusReportCollector {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	runCommand := cfg.RunCommand
	if runCommand == nil {
		runCommand = DefaultRunCommand
	}
	source := cfg.Source
	if source.Component == "" {
		source.Component = "sw-block ops status"
	}
	if source.Scenario == "" {
		source.Scenario = "live"
	}

	c := VolumeStatusReportCollector{
		Now:             func() time.Time { return time.Now().UTC() },
		Source:          source,
		ProductRevision: cfg.ProductRevision,
		RunnerRevision:  cfg.RunnerRevision,
	}
	if cfg.MasterAddr != "" {
		c.MasterStatus = func(ctx context.Context) (*control.StatusResponse, error) {
			return queryLiveMasterStatus(ctx, cfg.MasterAddr, cfg.VolumeID)
		}
	}
	if cfg.StatusAddr != "" {
		c.LocalStatus = func(ctx context.Context) (*hostvolume.StatusProjection, error) {
			var out hostvolume.StatusProjection
			if err := getStatusJSON(ctx, httpClient, cfg.StatusAddr, "/status", cfg.VolumeID, &out, false); err != nil {
				return nil, err
			}
			return &out, nil
		}
		c.Peers = func(ctx context.Context) ([]replication.ReplicaPeerStatus, error) {
			var out struct {
				Peers []replication.ReplicaPeerStatus
			}
			if err := getStatusJSON(ctx, httpClient, cfg.StatusAddr, "/status/peers", cfg.VolumeID, &out, true); err != nil {
				return nil, err
			}
			return out.Peers, nil
		}
		c.Durable = func(ctx context.Context) ([]durable.VolumeStatus, error) {
			var out struct {
				Volumes []durable.VolumeStatus
			}
			if err := getStatusJSON(ctx, httpClient, cfg.StatusAddr, "/status/durable", cfg.VolumeID, &out, true); err != nil {
				return nil, err
			}
			return out.Volumes, nil
		}
	}
	c.Residue = func(ctx context.Context) (ResidueReport, error) {
		return collectLocalResidue(ctx, runCommand)
	}
	return c
}

func queryLiveMasterStatus(ctx context.Context, masterAddr, volumeID string) (*control.StatusResponse, error) {
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial master: %w", err)
	}
	defer conn.Close()
	return control.NewEvidenceServiceClient(conn).QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: volumeID})
}

func getStatusJSON(ctx context.Context, client *http.Client, base, path, volumeID string, out any, allowNotFound bool) error {
	endpoint, err := statusEndpoint(base, path, volumeID)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if allowNotFound && resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", endpoint, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func statusEndpoint(base, path, volumeID string) (string, error) {
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	basePath := strings.TrimRight(u.Path, "/")
	if basePath == "" {
		u.Path = path
	} else {
		u.Path = pathpkg.Join(basePath, path)
		if strings.HasSuffix(path, "/") && !strings.HasSuffix(u.Path, "/") {
			u.Path += "/"
		}
	}
	q := u.Query()
	q.Set("volume", volumeID)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func collectLocalResidue(ctx context.Context, run func(context.Context, string, ...string) ([]byte, error)) (ResidueReport, error) {
	return collectLocalResidueForOS(ctx, run, runtime.GOOS)
}

func collectLocalResidueForOS(ctx context.Context, run func(context.Context, string, ...string) ([]byte, error), goos string) (ResidueReport, error) {
	var errs []string
	iscsi := []byte{}
	nvme := []byte{}
	unchecked := []string{"processes", "kubernetes", "storage_paths"}
	if goos == "windows" {
		unchecked = append(unchecked, "iscsi_sessions", "nvme_subsystems")
	} else {
		var err error
		iscsi, err = run(ctx, "iscsiadm", "-m", "session")
		if err != nil {
			if bytes.Contains(iscsi, []byte("No active sessions")) {
				err = nil
			} else if isCommandNotFound(err) {
				unchecked = append(unchecked, "iscsi_sessions")
			} else {
				errs = append(errs, fmt.Sprintf("iscsiadm session: %v", err))
			}
		}
		nvme, err = run(ctx, "nvme", "list-subsys", "-o", "json")
		if err != nil {
			if isCommandNotFound(err) {
				unchecked = append(unchecked, "nvme_subsystems")
			} else {
				errs = append(errs, fmt.Sprintf("nvme list-subsys: %v", err))
			}
		}
	}
	out := ResidueReport{
		HostInitiator: HostInitiatorResidue{
			ISCSISessions:  filterLines(string(iscsi), []string{"io.seaweedfs", "sw-block", "weedblock"}),
			NVMESubsystems: filterLines(string(nvme), []string{"seaweedfs", "sw-block", "weedblock"}),
		},
		Processes:    []string{},
		Kubernetes:   []string{},
		StoragePaths: []string{},
		Unchecked:    unchecked,
	}
	if len(errs) > 0 {
		return out, errors.New(strings.Join(errs, "; "))
	}
	return out, nil
}

func isCommandNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "executable file not found")
}

func filterLines(raw string, needles []string) []string {
	var out []string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		for _, needle := range needles {
			if strings.Contains(lower, strings.ToLower(needle)) {
				out = append(out, line)
				break
			}
		}
	}
	if out == nil {
		return []string{}
	}
	return out
}

// DefaultRunCommand executes a local read-only probe command and returns its
// combined output for residue collection.
func DefaultRunCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.CombinedOutput()
}
