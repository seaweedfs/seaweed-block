package master

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

type WorkloadPlanPromotionProbeConfig struct {
	AckProfile          string
	RequiredLSNByVolume map[string]uint64
	ExternalStatus      bool
	HTTPClient          *http.Client
}

func NewWorkloadPlanPromotionEvidenceProvider(plans []lifecycle.BlockVolumeWorkloadPlan, cfg WorkloadPlanPromotionProbeConfig) PromotionEvidenceProvider {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 2 * time.Second}
	}
	statusAddrs := map[string]map[string]string{}
	for _, plan := range plans {
		for _, replica := range plan.Replicas {
			port := replica.ISCSIListenPort
			if plan.Protocol == "nvme" && replica.NVMeListenPort > 0 {
				port = replica.NVMeListenPort
			}
			if port <= 0 {
				continue
			}
			if statusAddrs[plan.VolumeID] == nil {
				statusAddrs[plan.VolumeID] = map[string]string{}
			}
			host := "127.0.0.1"
			if cfg.ExternalStatus {
				h, err := promotionProbeHostFromAddr(replica.DataAddr)
				if err != nil {
					continue
				}
				host = h
			}
			statusAddrs[plan.VolumeID][replica.ReplicaID] = fmt.Sprintf("%s:%d", host, port+20000)
		}
	}
	return &workloadPlanPromotionEvidenceProvider{
		ackProfile: cfg.AckProfile,
		required:   cloneRequiredLSNMap(cfg.RequiredLSNByVolume),
		status:     statusAddrs,
		client:     httpClient,
	}
}

type workloadPlanPromotionEvidenceProvider struct {
	ackProfile string
	required   map[string]uint64
	status     map[string]map[string]string
	client     *http.Client
}

func (p *workloadPlanPromotionEvidenceProvider) ProbePromotionCandidates(volumeID string, current authority.AuthorityBasis, candidates []authority.ReplicaCandidate) (PromotionProbeResult, error) {
	result := PromotionProbeResult{
		AckProfile: p.ackProfile,
		SyncAckLSN: p.required[volumeID],
		Candidates: make([]PromotionCandidateEvidence, 0, len(candidates)),
	}
	if current.ReplicaID != "" {
		result.CurrentKnown = true
		statusAddr := p.status[volumeID][current.ReplicaID]
		if statusAddr == "" {
			result.Current = PromotionCandidateEvidence{ReplicaID: current.ReplicaID, ProbeAddr: "missing"}
		} else if evidence, err := p.probeCurrent(volumeID, current.ReplicaID, statusAddr); err == nil {
			result.Current = evidence
		} else {
			result.Current = PromotionCandidateEvidence{ReplicaID: current.ReplicaID}
		}
	}
	for _, candidate := range candidates {
		statusAddr := p.status[volumeID][candidate.ReplicaID]
		if statusAddr == "" {
			result.Candidates = append(result.Candidates, PromotionCandidateEvidence{ReplicaID: candidate.ReplicaID, ProbeAddr: "missing"})
			continue
		}
		evidence, err := p.probeCandidate(volumeID, candidate.ReplicaID, statusAddr)
		if err != nil {
			result.Candidates = append(result.Candidates, PromotionCandidateEvidence{ReplicaID: candidate.ReplicaID})
			continue
		}
		result.Candidates = append(result.Candidates, evidence)
	}
	return result, nil
}

func (p *workloadPlanPromotionEvidenceProvider) probeCandidate(volumeID, replicaID, statusAddr string) (PromotionCandidateEvidence, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var local volume.StatusProjection
	if err := getPromotionProbeJSON(ctx, p.client, statusAddr, "/status", volumeID, &local); err != nil {
		return PromotionCandidateEvidence{ReplicaID: replicaID, ProbeAddr: statusAddr}, err
	}
	var durableBody struct {
		Volumes []durable.VolumeStatus
	}
	if err := getPromotionProbeJSON(ctx, p.client, statusAddr, "/status/durable", volumeID, &durableBody); err != nil {
		return PromotionCandidateEvidence{ReplicaID: replicaID, ProbeAddr: statusAddr}, err
	}
	var best durable.VolumeStatus
	for _, st := range durableBody.Volumes {
		if st.VolumeID != volumeID || st.ReplicaID != replicaID || !st.FrontierKnown {
			continue
		}
		if st.DurableLSN >= best.DurableLSN {
			best = st
		}
	}
	return PromotionCandidateEvidence{
		ReplicaID: replicaID,
		ProbeAddr: statusAddr,
		Ready: local.VolumeID == volumeID &&
			local.ReplicaID == replicaID &&
			strings.EqualFold(local.ReplicationRole, "replica_ready") &&
			best.FrontierKnown,
		DurableLSN: best.DurableLSN,
	}, nil
}

func (p *workloadPlanPromotionEvidenceProvider) probeCurrent(volumeID, replicaID, statusAddr string) (PromotionCandidateEvidence, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var local volume.StatusProjection
	if err := getPromotionProbeJSON(ctx, p.client, statusAddr, "/status", volumeID, &local); err != nil {
		return PromotionCandidateEvidence{ReplicaID: replicaID, ProbeAddr: statusAddr}, err
	}
	return PromotionCandidateEvidence{
		ReplicaID: replicaID,
		ProbeAddr: statusAddr,
		Ready: local.VolumeID == volumeID &&
			local.ReplicaID == replicaID &&
			strings.EqualFold(local.AuthorityRole, volume.AuthorityRolePrimary) &&
			local.Healthy &&
			local.FrontendPrimaryReady,
	}, nil
}

func getPromotionProbeJSON(ctx context.Context, client *http.Client, base, suffix, volumeID string, out any) error {
	endpoint, err := promotionProbeEndpoint(base, suffix, volumeID)
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
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", endpoint, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func promotionProbeEndpoint(base, suffix, volumeID string) (string, error) {
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(strings.TrimRight(u.Path, "/"), suffix)
	q := u.Query()
	q.Set("volume", volumeID)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func promotionProbeHostFromAddr(addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("node address %q is not host:port: %w", addr, err)
	}
	if host == "" {
		return "", fmt.Errorf("node address %q has empty host", addr)
	}
	if isLocalhostOrLoopbackHost(host) {
		return "", fmt.Errorf("node address %q is loopback; external promotion probes require non-loopback node addresses", addr)
	}
	return host, nil
}

func isLocalhostOrLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func cloneRequiredLSNMap(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
