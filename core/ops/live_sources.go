package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	pathpkg "path"
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
}

func NewLiveVolumeStatusReportCollector(cfg LiveVolumeStatusConfig) VolumeStatusReportCollector {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
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
	c.Residue = func(context.Context) (ResidueReport, error) {
		return ResidueReport{
			HostInitiator: HostInitiatorResidue{
				ISCSISessions:  []string{},
				NVMESubsystems: []string{},
			},
			Processes:    []string{},
			Kubernetes:   []string{},
			StoragePaths: []string{},
		}, errors.New("residue collection not implemented for live ops status")
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
