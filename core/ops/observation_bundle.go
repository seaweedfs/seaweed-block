package ops

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ClusterEvidenceArtifact        = "cluster-evidence.json"
	ObservationReportHTMLArtifact  = "index.html"
	ObservationReportJSONArtifact  = ClusterEvidenceArtifact
	ObservationReportTextArtifact  = "summary.txt"
	ObservationReportJSONLArtifact = "timeline.jsonl"

	NodeLossRecoverySummaryArtifact = "node-loss-recovery-summary.txt"
	PrimaryFailureRecoveryArtifact  = "primary-failure-recovery.txt"
	ControlPlaneTimelineArtifact    = "control-plane-timeline.txt"
	KubeSystemPodsDeploysArtifact   = "kube-system-pods-deploys.txt"
)

type ObservationBundleOptions struct {
	Dir      string
	VolumeID string
}

func BuildObservationFromBundle(opts ObservationBundleOptions) (ClusterEvidence, error) {
	if strings.TrimSpace(opts.Dir) == "" {
		return ClusterEvidence{}, fmt.Errorf("bundle dir is required")
	}
	cluster := NewClusterEvidence(time.Time{})
	cluster.ProductRevision = "from-bundle"
	sourceLoaded := false

	productCluster, _, productErr := loadBestClusterEvidence(opts.Dir)
	if productErr == nil {
		cluster = filterObservationCluster(productCluster, opts.VolumeID)
		sourceLoaded = true
	} else {
		inventory, inventoryPath, inventoryErr := loadBestVolumeInventory(opts.Dir)
		if inventoryErr == nil {
			fromInventory, err := BuildObservationFromInventory(inventory, opts.VolumeID, inventoryPath)
			if err != nil {
				return cluster, err
			}
			cluster = fromInventory
			sourceLoaded = true
		} else if opts.VolumeID != "" {
			cluster = filterObservationCluster(cluster, opts.VolumeID)
		}
	}

	summary, summaryPath, _ := loadKeyValueArtifact(opts.Dir, NodeLossRecoverySummaryArtifact)
	if len(summary) > 0 {
		applyNodeLossSummary(&cluster, summary, summaryPath)
	}
	timeline, timelinePath, _ := loadTimelineArtifact(opts.Dir)
	if len(timeline) > 0 && len(cluster.Events) == 0 {
		cluster.Events = append(cluster.Events, timeline...)
		for i := range cluster.Events {
			if cluster.Events[i].EvidenceRef == "" {
				cluster.Events[i].EvidenceRef = timelinePath
			}
		}
	}
	if blocked, blockedVolume := buildImagePullBlockedEvidence(opts.Dir); blocked {
		cluster.Status = ObservationStatusBlocked
		sourceLoaded = true
		if len(cluster.Volumes) == 0 {
			if opts.VolumeID != "" {
				blockedVolume.VolumeID = opts.VolumeID
			}
			cluster.Volumes = append(cluster.Volumes, blockedVolume)
		}
	}
	if len(cluster.Volumes) == 0 {
		if opts.VolumeID == "" && sourceLoaded {
			return cluster, nil
		}
		if !sourceLoaded && productErr != nil {
			return cluster, productErr
		}
		return cluster, fmt.Errorf("volume %q not found in bundle", opts.VolumeID)
	}
	return cluster, nil
}

func BuildObservationFromInventory(inventory VolumeInventory, volumeID string, evidencePath string) (ClusterEvidence, error) {
	cluster := NewClusterEvidence(inventory.CapturedAt)
	cluster.ProductRevision = inventory.ProductRevision
	cluster.Status = observationStatusFromInventoryCode(ClassifyVolumeInventory(inventory))
	cluster.NonClaims = append(cluster.NonClaims, inventory.NonClaims...)
	for _, volume := range inventory.Volumes {
		if volumeID != "" && volume.VolumeID != volumeID {
			continue
		}
		cluster.Volumes = append(cluster.Volumes, observationVolumeFromInventory(volume, evidencePath))
	}
	if len(cluster.Volumes) == 0 {
		if volumeID == "" {
			return cluster, nil
		}
		return cluster, fmt.Errorf("volume %q not found in inventory", volumeID)
	}
	return cluster, nil
}

func RenderObservationExplainText(cluster ClusterEvidence) string {
	var b strings.Builder
	b.WriteString(RenderClusterEvidenceText(cluster))
	if len(cluster.Events) > 0 {
		b.WriteString("timeline:\n")
		events := append([]ClusterEvent(nil), cluster.Events...)
		sort.SliceStable(events, func(i, j int) bool {
			return events[i].EventID < events[j].EventID
		})
		for _, event := range events {
			fmt.Fprintf(&b, "- %s severity=%s reason=%s volume=%s replica=%s %s\n",
				event.Type,
				emptyAsDash(event.Severity),
				emptyAsDash(event.Reason),
				emptyAsDash(event.VolumeID),
				emptyAsDash(event.ReplicaID),
				event.Message)
		}
	}
	return b.String()
}

func loadBestVolumeInventory(root string) (VolumeInventory, string, error) {
	paths, err := findArtifactPaths(root, VolumeInventoryArtifact)
	if err != nil {
		return VolumeInventory{}, "", err
	}
	if len(paths) == 0 {
		return VolumeInventory{}, "", fmt.Errorf("%s not found under %s", VolumeInventoryArtifact, root)
	}
	sort.SliceStable(paths, func(i, j int) bool {
		return inventoryPathRank(paths[i]) < inventoryPathRank(paths[j])
	})
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		return VolumeInventory{}, paths[0], fmt.Errorf("read %s: %w", paths[0], err)
	}
	var inventory VolumeInventory
	if err := json.Unmarshal(raw, &inventory); err != nil {
		return VolumeInventory{}, paths[0], fmt.Errorf("decode %s: %w", paths[0], err)
	}
	return inventory, paths[0], nil
}

func loadBestClusterEvidence(root string) (ClusterEvidence, string, error) {
	paths, err := findArtifactPaths(root, ClusterEvidenceArtifact)
	if err != nil {
		return ClusterEvidence{}, "", err
	}
	if len(paths) == 0 {
		return ClusterEvidence{}, "", fmt.Errorf("%s not found under %s", ClusterEvidenceArtifact, root)
	}
	sort.SliceStable(paths, func(i, j int) bool {
		return clusterEvidencePathRank(paths[i]) < clusterEvidencePathRank(paths[j])
	})
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		return ClusterEvidence{}, paths[0], fmt.Errorf("read %s: %w", paths[0], err)
	}
	var cluster ClusterEvidence
	if err := json.Unmarshal(raw, &cluster); err != nil {
		return ClusterEvidence{}, paths[0], fmt.Errorf("decode %s: %w", paths[0], err)
	}
	return normalizeObservationCluster(cluster), paths[0], nil
}

func clusterEvidencePathRank(path string) int {
	path = filepath.ToSlash(path)
	switch {
	case strings.Contains(path, "product-observation"):
		return 0
	case strings.Contains(path, "/status/"):
		return 1
	default:
		return 10
	}
}

func normalizeObservationCluster(cluster ClusterEvidence) ClusterEvidence {
	if cluster.SchemaVersion == "" {
		cluster.SchemaVersion = ObservationSchemaVersion
	}
	if cluster.CapturedAt.IsZero() {
		cluster.CapturedAt = time.Now().UTC()
	} else {
		cluster.CapturedAt = cluster.CapturedAt.UTC()
	}
	if cluster.Status == "" {
		cluster.Status = ObservationStatusUnavailable
	}
	if len(cluster.NonClaims) == 0 {
		cluster.NonClaims = NewClusterEvidence(cluster.CapturedAt).NonClaims
	}
	return cluster
}

func filterObservationCluster(cluster ClusterEvidence, volumeID string) ClusterEvidence {
	cluster = normalizeObservationCluster(cluster)
	if strings.TrimSpace(volumeID) == "" {
		return cluster
	}
	filtered := cluster
	filtered.Volumes = nil
	for _, volume := range cluster.Volumes {
		if volume.VolumeID == volumeID {
			filtered.Volumes = append(filtered.Volumes, volume)
		}
	}
	filtered.Events = nil
	for _, event := range cluster.Events {
		if event.VolumeID == "" || event.VolumeID == volumeID {
			filtered.Events = append(filtered.Events, event)
		}
	}
	return filtered
}

func inventoryPathRank(path string) int {
	path = filepath.ToSlash(path)
	switch {
	case strings.Contains(path, "ops-inventory-reader-verified"):
		return 0
	case strings.Contains(path, "ops-inventory-after-primary-failure"):
		return 1
	case strings.Contains(path, "ops-inventory-before-primary-failure"):
		return 2
	default:
		return 10
	}
}

func observationVolumeFromInventory(volume VolumeInventoryVolume, evidencePath string) VolumeEvidence {
	out := VolumeEvidence{
		VolumeID:          volume.VolumeID,
		Namespace:         volume.Namespace,
		PVCName:           volume.PVCName,
		PVName:            volume.PVName,
		ReplicationFactor: volume.ReplicationFactor,
		DesiredReplicas:   volume.DesiredReplicas,
		ObservedReplicas:  volume.ObservedReplicas,
		Status:            observationStatusFromInventoryStatus(volume.Status),
		PrimaryReplica:    volume.PrimaryReplicaID,
		Epoch:             maxInventoryEpoch(volume.Replicas),
		EndpointVersion:   maxInventoryEndpointVersion(volume.Replicas),
		SupportBundleHint: evidencePath,
		Replicas:          make([]ReplicaEvidence, 0, len(volume.Replicas)),
	}
	if len(volume.Issues) > 0 {
		out.Reason = reasonFromIssueList(volume.Issues)
	}
	for _, replica := range volume.Replicas {
		ev := ReplicaEvidence{
			ReplicaID:            replica.ReplicaID,
			ServerID:             replica.ServerID,
			KubernetesNode:       replica.NodeName,
			Observed:             replica.Observed,
			Role:                 replica.AuthorityRole,
			ReplicationRole:      replica.ReplicationRole,
			DurableLatched:       replica.PromotionReadiness.FrontierCovered,
			DurableFrontierKnown: replica.PromotionReadiness.CandidateFrontierKnown,
			DurableFrontierLSN:   replica.PromotionReadiness.CandidateFrontierLSN,
			CandidateReady:       replica.PromotionReadiness.CandidateReady,
			CandidateReadyReason: replica.PromotionReadiness.Reason,
			FrontendProtocol:     replica.Protocol,
			FrontendAddr:         replica.FrontendAddress,
			StatusAddr:           replica.StatusAddress,
			StalePrimaryFenced:   containsIssuePrefix(replica.Issues, "stale_primary_frontend_ready=") || replica.AuthorityRole == "superseded",
			SupportBundlePath:    replica.SupportBundle,
		}
		if replica.ReplicaID == volume.PrimaryReplicaID {
			out.PrimaryNode = replica.NodeName
			out.PublishTarget = replica.FrontendAddress
			out.AckProfile = replica.AckProfile
			out.ClaimProfile = replica.PromotionReadiness.ClaimProfile
		}
		out.Replicas = append(out.Replicas, ev)
	}
	if len(out.NextActions) == 0 {
		out.NextActions = []string{"none"}
		if out.Status != ObservationStatusOK {
			out.NextActions = []string{"collect support bundle and inspect timeline"}
		}
	}
	return out
}

func applyNodeLossSummary(cluster *ClusterEvidence, summary map[string]string, evidencePath string) {
	volume := ensureObservationVolume(cluster, "")
	volume.AckProfile = defaultString(summary["ack_profile"], volume.AckProfile)
	volume.Status = ObservationStatusRecovering
	volume.Reason = ReasonPrimaryNodeLost
	volume.SupportBundleHint = evidencePath
	if summary["result"] == "promoted" && summary["reader_verified"] == "true" {
		volume.Status = ObservationStatusOK
	}
	if promoted := summary["promoted"]; promoted != "" {
		replicaID, node := splitReplicaNode(promoted)
		volume.PrimaryReplica = defaultString(replicaID, volume.PrimaryReplica)
		volume.PrimaryNode = defaultString(node, volume.PrimaryNode)
	}
	if after := summary["after_frontend"]; after != "" {
		volume.PublishTarget = after
	}
	volume.Conditions = append(volume.Conditions, ObservationCondition{
		Type:     "NodeLossRecovery",
		Status:   summary["result"],
		Reason:   ReasonPrimaryNodeLost,
		Severity: "info",
		Message: fmt.Sprintf("CSI target changed %s -> %s; reader_verified=%s; pod_recreate_used=%s",
			emptyAsDash(summary["before_frontend"]),
			emptyAsDash(summary["after_frontend"]),
			emptyAsDash(summary["reader_verified"]),
			emptyAsDash(summary["pod_recreate_used"])),
	})
	if summary["old_primary_stale_io_success_count"] == "0" {
		volume.Conditions = append(volume.Conditions, ObservationCondition{
			Type:     "StalePrimary",
			Status:   "false",
			Reason:   ReasonStalePrimaryFenced,
			Severity: "info",
			Message:  "old primary stale I/O success count is 0",
		})
	}
	volume.NextActions = []string{"none"}
	cluster.Volumes[0] = volume
}

func buildImagePullBlockedEvidence(root string) (bool, VolumeEvidence) {
	paths, err := findArtifactPaths(root, KubeSystemPodsDeploysArtifact)
	if err != nil || len(paths) == 0 {
		return false, VolumeEvidence{}
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		text := string(raw)
		if !(strings.Contains(text, "ImagePullBackOff") || strings.Contains(text, "ErrImagePull")) {
			continue
		}
		node := parseImagePullNode(text)
		volume := VolumeEvidence{
			VolumeID:          "unknown",
			ReplicationFactor: 3,
			Status:            ObservationStatusBlocked,
			Reason:            ReasonCSINodeImagePullFailed,
			Conditions: []ObservationCondition{{
				Type:     "Attach",
				Status:   "false",
				Reason:   ReasonCSINodeImagePullFailed,
				Severity: "error",
				Message:  fmt.Sprintf("pod kube-system/sw-block-csi-node waiting=ImagePullBackOff on node %s image sw-block-csi:local", emptyAsDash(node)),
			}},
			NextActions:       []string{"import sw-block-csi:local to the blocked node or use a registry reachable by all nodes"},
			SupportBundleHint: path,
		}
		return true, volume
	}
	return false, VolumeEvidence{}
}

func parseImagePullNode(text string) string {
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, "sw-block-csi-node") || !(strings.Contains(line, "ImagePullBackOff") || strings.Contains(line, "ErrImagePull")) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 7 {
			return fields[6]
		}
	}
	return Unavailable
}

func loadTimelineArtifact(root string) ([]ClusterEvent, string, error) {
	paths, err := findArtifactPaths(root, ControlPlaneTimelineArtifact)
	if err != nil || len(paths) == 0 {
		return nil, "", err
	}
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		return nil, paths[0], err
	}
	var events []ClusterEvent
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	ordinal := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		ordinal++
		fields := parseSpaceKeyValues(line)
		eventType := fields["event"]
		if eventType == "" {
			eventType = "unknown"
		}
		event := ClusterEvent{
			EventID:     fmt.Sprintf("%04d", ordinal),
			EventTime:   time.Unix(int64(ordinal), 0).UTC(),
			Type:        eventType,
			Severity:    "info",
			VolumeID:    defaultString(fields["volume"], parseVolumeIDFromEvidence(fields["evidence"])),
			ReplicaID:   defaultString(fields["replica"], fields["to"]),
			NodeName:    fields["node"],
			Message:     line,
			Reason:      reasonForTimelineEvent(eventType, fields),
			EvidenceRef: paths[0],
		}
		if primaryCount, err := strconv.ParseUint(fields["primary_count"], 10, 64); err == nil {
			event.NewValue = fmt.Sprintf("primary_count=%d", primaryCount)
		}
		events = append(events, event)
	}
	return events, paths[0], scanner.Err()
}

func loadKeyValueArtifact(root, name string) (map[string]string, string, error) {
	paths, err := findArtifactPaths(root, name)
	if err != nil || len(paths) == 0 {
		return nil, "", err
	}
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		return nil, paths[0], err
	}
	out := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		key, value, ok := strings.Cut(line, "=")
		if ok {
			out[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	return out, paths[0], scanner.Err()
}

func findArtifactPaths(root, name string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() == name {
			paths = append(paths, path)
		}
		return nil
	})
	sort.Strings(paths)
	return paths, err
}

func ensureObservationVolume(cluster *ClusterEvidence, volumeID string) VolumeEvidence {
	if len(cluster.Volumes) == 0 {
		if volumeID == "" {
			volumeID = "unknown"
		}
		cluster.Volumes = append(cluster.Volumes, VolumeEvidence{VolumeID: volumeID, Status: ObservationStatusUnavailable})
	}
	return cluster.Volumes[0]
}

func observationStatusFromInventoryStatus(status string) string {
	switch status {
	case "ok":
		return ObservationStatusOK
	case "invalid":
		return ObservationStatusInvalid
	case "unhealthy":
		return ObservationStatusDegraded
	default:
		return ObservationStatusUnavailable
	}
}

func observationStatusFromInventoryCode(code int) string {
	switch code {
	case VolumeStatusExitOK:
		return ObservationStatusOK
	case VolumeStatusExitInvalid:
		return ObservationStatusInvalid
	default:
		return ObservationStatusDegraded
	}
}

func reasonFromIssueList(issues []string) string {
	joined := strings.Join(issues, "\n")
	switch {
	case strings.Contains(joined, "status_endpoint_unreachable"):
		return ReasonStatusEndpointUnreachable
	case strings.Contains(joined, "generated_deployment_missing"):
		return ReasonGeneratedDeploymentMissing
	case strings.Contains(joined, "observed_replicas="):
		return ReasonObservedReplicasBelowDesired
	case strings.Contains(joined, "candidate_frontier_behind"):
		return ReasonCandidateFrontierBehind
	case strings.Contains(joined, "durable_frontier_missing"):
		return ReasonDurableFrontierMissing
	case strings.Contains(joined, "stale_primary"):
		return ReasonStalePrimaryFenced
	default:
		return "inventory_issue"
	}
}

func parseSpaceKeyValues(line string) map[string]string {
	out := map[string]string{}
	for _, token := range strings.Fields(line) {
		key, value, ok := strings.Cut(token, "=")
		if ok {
			out[key] = value
		}
	}
	return out
}

func reasonForTimelineEvent(eventType string, fields map[string]string) string {
	if reason := fields["reason"]; reason != "" {
		return reason
	}
	switch eventType {
	case "candidate_evaluated":
		if fields["candidate_ready"] == "true" {
			return ReasonCandidateCoversRequiredFrontier
		}
		return ReasonNoPromotionReadyCandidate
	case "authority_published":
		return ReasonCandidateCoversRequiredFrontier
	case "primary_failure_injected":
		return ReasonPrimaryNodeLost
	case "data_check":
		return "reader_checksum_passed"
	default:
		return ""
	}
}

func parseVolumeIDFromEvidence(evidence string) string {
	fields := parseSpaceKeyValues(evidence)
	return fields["volume"]
}

func splitReplicaNode(raw string) (string, string) {
	replica, node, ok := strings.Cut(raw, "@")
	if !ok {
		return raw, ""
	}
	return replica, node
}

func maxInventoryEpoch(replicas []VolumeInventoryReplica) uint64 {
	var max uint64
	for _, replica := range replicas {
		if replica.Epoch > max {
			max = replica.Epoch
		}
	}
	return max
}

func maxInventoryEndpointVersion(replicas []VolumeInventoryReplica) uint64 {
	var max uint64
	for _, replica := range replicas {
		if replica.EndpointVersion > max {
			max = replica.EndpointVersion
		}
	}
	return max
}
