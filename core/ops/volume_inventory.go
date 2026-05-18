package ops

import (
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"
	"time"

	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
)

const (
	// VolumeInventorySchemaVersion is the first multi-volume, replica-aware
	// inventory contract. It is read-only evidence, not a repair input.
	VolumeInventorySchemaVersion = "1.0"
)

type VolumeInventoryInput struct {
	CapturedAt       time.Time
	Source           ReportSource
	ProductRevision  string
	RunnerRevision   string
	CollectionErrors []string
	Volumes          []VolumeInventoryVolumeInput
}

type VolumeInventoryVolumeInput struct {
	VolumeID          string
	Namespace         string
	PVCName           string
	PVName            string
	ReplicationFactor int
	SupportBundle     string
	CollectionErrors  []string
	Residue           ResidueReport
	Issues            []string
	FailoverTimeline  []VolumeInventoryFailoverEvent
	Replicas          []VolumeInventoryReplicaInput
}

type VolumeInventoryReplicaInput struct {
	ReplicaID              string
	ServerID               string
	NodeName               string
	GeneratedDeployment    string
	LifecycleOwner         string
	OwnerReference         string
	Protocol               string
	FrontendAddress        string
	StatusAddress          string
	SupportBundle          string
	DataAddr               string
	CtrlAddr               string
	Observed               bool
	AuthorityRole          string
	Healthy                bool
	FrontendPrimaryReady   bool
	ReplicationRole        string
	Epoch                  uint64
	EndpointVersion        uint64
	ClaimProfile           string
	AckProfile             string
	DurableLatched         bool
	DurableOperational     bool
	RequiredFrontierLSN    uint64
	RequiredFrontierKnown  bool
	CandidateFrontierLSN   uint64
	CandidateFrontierKnown bool
	Residue                ResidueReport
	CollectionErrors       []string
	Issues                 []string
}

type VolumeInventory struct {
	SchemaVersion    string                  `json:"schema_version"`
	CapturedAt       time.Time               `json:"captured_at"`
	Source           ReportSource            `json:"source"`
	ProductRevision  string                  `json:"product_revision"`
	RunnerRevision   string                  `json:"runner_revision,omitempty"`
	Status           string                  `json:"status"`
	CollectionErrors []string                `json:"collection_errors"`
	Volumes          []VolumeInventoryVolume `json:"volumes"`
	NonClaims        []string                `json:"non_claims"`
}

type VolumeInventoryVolume struct {
	VolumeID                string                         `json:"volume_id"`
	Namespace               string                         `json:"namespace"`
	PVCName                 string                         `json:"pvc_name"`
	PVName                  string                         `json:"pv_name"`
	ReplicationFactor       int                            `json:"replication_factor"`
	DesiredReplicas         int                            `json:"desired_replicas"`
	ObservedReplicas        int                            `json:"observed_replicas"`
	PrimaryReplicaID        string                         `json:"primary_replica_id"`
	Protocols               []string                       `json:"protocols"`
	ProductRevision         string                         `json:"product_revision"`
	Status                  string                         `json:"status"`
	ReplicasOnDistinctNodes bool                           `json:"replicas_on_distinct_nodes"`
	FrontendsNonLoopback    bool                           `json:"frontends_non_loopback"`
	Residue                 ResidueReport                  `json:"residue"`
	Issues                  []string                       `json:"issues"`
	Unchecked               []string                       `json:"unchecked"`
	CollectionErrors        []string                       `json:"collection_errors"`
	SupportBundle           string                         `json:"support_bundle"`
	FailoverTimeline        []VolumeInventoryFailoverEvent `json:"failover_timeline,omitempty"`
	Replicas                []VolumeInventoryReplica       `json:"replicas"`
}

type VolumeInventoryFailoverEvent struct {
	Phase           string `json:"phase"`
	ReplicaID       string `json:"replica_id"`
	Role            string `json:"role"`
	Epoch           uint64 `json:"epoch"`
	EndpointVersion uint64 `json:"endpoint_version"`
	Status          string `json:"status"`
	Reason          string `json:"reason,omitempty"`
}

type VolumeInventoryReplica struct {
	ReplicaID            string                   `json:"replica_id"`
	ServerID             string                   `json:"server_id"`
	NodeName             string                   `json:"node_name"`
	GeneratedDeployment  string                   `json:"generated_deployment"`
	LifecycleOwner       string                   `json:"lifecycle_owner"`
	OwnerReference       string                   `json:"owner_reference"`
	Protocol             string                   `json:"protocol"`
	FrontendAddress      string                   `json:"frontend_address"`
	StatusAddress        string                   `json:"status_address"`
	SupportBundle        string                   `json:"support_bundle"`
	DataAddr             string                   `json:"data_addr"`
	CtrlAddr             string                   `json:"ctrl_addr"`
	Observed             bool                     `json:"observed"`
	Status               string                   `json:"status"`
	AuthorityRole        string                   `json:"authority_role"`
	Healthy              bool                     `json:"healthy"`
	FrontendPrimaryReady bool                     `json:"frontend_primary_ready"`
	ReplicationRole      string                   `json:"replication_role"`
	Epoch                uint64                   `json:"epoch"`
	EndpointVersion      uint64                   `json:"endpoint_version"`
	AckProfile           string                   `json:"ack_profile"`
	PromotionReadiness   PromotionReadinessReport `json:"promotion_readiness"`
	Residue              ResidueReport            `json:"residue"`
	Issues               []string                 `json:"issues"`
	CollectionErrors     []string                 `json:"collection_errors"`
}

func BuildVolumeInventory(in VolumeInventoryInput) VolumeInventory {
	capturedAt := in.CapturedAt
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	} else {
		capturedAt = capturedAt.UTC()
	}
	source := in.Source
	source.Component = explicitUnavailable(source.Component)

	out := VolumeInventory{
		SchemaVersion:    VolumeInventorySchemaVersion,
		CapturedAt:       capturedAt,
		Source:           source,
		ProductRevision:  explicitUnavailable(in.ProductRevision),
		RunnerRevision:   in.RunnerRevision,
		CollectionErrors: copyStringSlice(in.CollectionErrors),
		Volumes:          make([]VolumeInventoryVolume, 0, len(in.Volumes)),
		NonClaims: []string{
			"read-only-observation: inventory does not mutate product state",
			"single-cluster-alpha-scope: discovery is scoped to one alpha Kubernetes cluster",
			"best-effort-partial-discovery: missing inputs are reported as issues or unchecked evidence, not inferred as healthy",
			"no-mutating-admin: inventory is not repair, cleanup, failover, backup, or restore",
			"no-multi-node-scheduling: inventory observes placement, it does not schedule or rebalance replicas",
			"rf2-rf3-live-kubernetes-operation: non-claim unless a runner gate explicitly proves it",
		},
	}
	for _, volume := range in.Volumes {
		out.Volumes = append(out.Volumes, buildInventoryVolume(out.ProductRevision, volume))
	}
	sort.SliceStable(out.Volumes, func(i, j int) bool {
		return out.Volumes[i].VolumeID < out.Volumes[j].VolumeID
	})
	out.Status = inventoryExitLabel(ClassifyVolumeInventory(out))
	return out
}

func buildInventoryVolume(productRevision string, in VolumeInventoryVolumeInput) VolumeInventoryVolume {
	replicas := make([]VolumeInventoryReplica, 0, len(in.Replicas))
	for _, replica := range in.Replicas {
		replicas = append(replicas, buildInventoryReplica(replica))
	}
	sort.SliceStable(replicas, func(i, j int) bool {
		return replicas[i].ReplicaID < replicas[j].ReplicaID
	})

	desired := in.ReplicationFactor
	if desired == 0 {
		desired = len(replicas)
	}
	observed := 0
	primary := Unavailable
	primaryCount := 0
	protocolsSeen := map[string]bool{}
	for _, replica := range replicas {
		if replica.Observed {
			observed++
		}
		if replica.AuthorityRole == hostvolume.AuthorityRolePrimary {
			primary = replica.ReplicaID
			primaryCount++
		}
		if replica.Protocol != "" && replica.Protocol != Unavailable {
			protocolsSeen[replica.Protocol] = true
		}
	}
	protocols := make([]string, 0, len(protocolsSeen))
	for protocol := range protocolsSeen {
		protocols = append(protocols, protocol)
	}
	sort.Strings(protocols)

	volume := VolumeInventoryVolume{
		VolumeID:                explicitUnavailable(in.VolumeID),
		Namespace:               explicitUnavailable(in.Namespace),
		PVCName:                 explicitUnavailable(in.PVCName),
		PVName:                  explicitUnavailable(in.PVName),
		ReplicationFactor:       desired,
		DesiredReplicas:         desired,
		ObservedReplicas:        observed,
		PrimaryReplicaID:        primaryReplicaID(primary, primaryCount),
		Protocols:               protocols,
		ProductRevision:         productRevision,
		ReplicasOnDistinctNodes: replicasOnDistinctNodes(replicas, desired),
		FrontendsNonLoopback:    frontendsNonLoopback(replicas, desired),
		Residue:                 copyResidue(in.Residue),
		Unchecked:               copyStringSlice(in.Residue.Unchecked),
		CollectionErrors:        copyStringSlice(in.CollectionErrors),
		SupportBundle:           in.SupportBundle,
		FailoverTimeline:        copyFailoverTimeline(in.FailoverTimeline),
		Replicas:                replicas,
	}
	volume.Issues = append(copyStringSlice(in.Issues), volumeInventoryVolumeIssues(volume)...)
	volume.Status = inventoryIssueStatus(volume.Issues)
	return volume
}

func primaryReplicaID(primary string, count int) string {
	if count == 1 {
		return explicitUnavailable(primary)
	}
	return Unavailable
}

func buildInventoryReplica(in VolumeInventoryReplicaInput) VolumeInventoryReplica {
	replica := VolumeInventoryReplica{
		ReplicaID:            explicitUnavailable(in.ReplicaID),
		ServerID:             explicitUnavailable(in.ServerID),
		NodeName:             explicitUnavailable(in.NodeName),
		GeneratedDeployment:  explicitUnavailable(in.GeneratedDeployment),
		LifecycleOwner:       explicitUnavailable(in.LifecycleOwner),
		OwnerReference:       explicitUnavailable(in.OwnerReference),
		Protocol:             explicitUnavailable(in.Protocol),
		FrontendAddress:      explicitUnavailable(in.FrontendAddress),
		StatusAddress:        explicitUnavailable(in.StatusAddress),
		SupportBundle:        explicitUnavailable(in.SupportBundle),
		DataAddr:             explicitUnavailable(in.DataAddr),
		CtrlAddr:             explicitUnavailable(in.CtrlAddr),
		Observed:             in.Observed,
		AuthorityRole:        explicitUnavailable(in.AuthorityRole),
		Healthy:              in.Healthy,
		FrontendPrimaryReady: in.FrontendPrimaryReady,
		ReplicationRole:      explicitUnavailable(in.ReplicationRole),
		Epoch:                in.Epoch,
		EndpointVersion:      in.EndpointVersion,
		AckProfile:           explicitUnavailable(defaultString(in.AckProfile, PromotionAckProfileBestEffort)),
		Residue:              copyResidue(in.Residue),
		Issues:               copyStringSlice(in.Issues),
		CollectionErrors:     copyStringSlice(in.CollectionErrors),
	}
	replica.PromotionReadiness = EvaluatePromotionReadiness(PromotionReadinessInput{
		CandidateReplicaID:     replica.ReplicaID,
		ClaimProfile:           in.ClaimProfile,
		AckProfile:             replica.AckProfile,
		Observed:               replica.Observed,
		Reachable:              !containsIssuePrefix(replica.Issues, "status_endpoint_unavailable") && !containsIssuePrefix(replica.Issues, "status_endpoint_unreachable="),
		AuthorityRole:          replica.AuthorityRole,
		ReplicationRole:        replica.ReplicationRole,
		DurableLatched:         in.DurableLatched,
		DurableOperational:     in.DurableOperational,
		RequiredFrontierLSN:    in.RequiredFrontierLSN,
		RequiredFrontierKnown:  in.RequiredFrontierKnown,
		CandidateFrontierLSN:   in.CandidateFrontierLSN,
		CandidateFrontierKnown: in.CandidateFrontierKnown,
	})
	replica.Issues = append(replica.Issues, volumeInventoryReplicaIssues(replica)...)
	replica.Status = inventoryReplicaStatus(replica)
	return replica
}

func ClassifyVolumeInventory(in VolumeInventory) int {
	issues := VolumeInventoryIssues(in)
	if hasInvalidIssue(issues) {
		return VolumeStatusExitInvalid
	}
	if len(issues) > 0 {
		return VolumeStatusExitUnhealthy
	}
	return VolumeStatusExitOK
}

func VolumeInventoryIssues(in VolumeInventory) []string {
	var issues []string
	if in.SchemaVersion != VolumeInventorySchemaVersion {
		issues = append(issues, fmt.Sprintf("invalid: schema_version=%s want %s", in.SchemaVersion, VolumeInventorySchemaVersion))
	}
	if in.ProductRevision == "" || in.ProductRevision == Unavailable {
		issues = append(issues, "invalid: product_revision unavailable")
	}
	for _, errText := range in.CollectionErrors {
		if errText != "" {
			if strings.HasPrefix(errText, "kubernetes_unreachable:") {
				issues = append(issues, fmt.Sprintf("invalid: %s", errText))
				continue
			}
			issues = append(issues, fmt.Sprintf("collection_error: %s", errText))
		}
	}
	for _, volume := range in.Volumes {
		for _, issue := range volume.Issues {
			if strings.HasPrefix(issue, "invalid:") {
				issues = append(issues, fmt.Sprintf("invalid: volume %s %s", volume.VolumeID, strings.TrimSpace(strings.TrimPrefix(issue, "invalid:"))))
				continue
			}
			issues = append(issues, fmt.Sprintf("volume %s %s", volume.VolumeID, issue))
		}
	}
	return issues
}

func RenderVolumeInventorySummary(in VolumeInventory) string {
	issues := VolumeInventoryIssues(in)
	status := inventoryIssueStatus(issues)
	ok, unhealthy, invalid := 0, 0, 0
	for _, volume := range in.Volumes {
		switch volume.Status {
		case "ok":
			ok++
		case "invalid":
			invalid++
		default:
			unhealthy++
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "inventory_status: %s\n", status)
	fmt.Fprintf(&b, "schema_version: %s\n", in.SchemaVersion)
	if !in.CapturedAt.IsZero() {
		fmt.Fprintf(&b, "captured_at: %s\n", in.CapturedAt.UTC().Format("2006-01-02T15:04:05Z"))
	}
	fmt.Fprintf(&b, "source: component=%s host=%s scenario=%s\n", in.Source.Component, emptyAsDash(in.Source.Host), emptyAsDash(in.Source.Scenario))
	fmt.Fprintf(&b, "product_revision: %s\n", in.ProductRevision)
	if in.RunnerRevision != "" {
		fmt.Fprintf(&b, "runner_revision: %s\n", in.RunnerRevision)
	}
	fmt.Fprintf(&b, "volumes: total=%d ok=%d unhealthy=%d invalid=%d\n", len(in.Volumes), ok, unhealthy, invalid)
	for _, volume := range in.Volumes {
		fmt.Fprintf(&b, "volume: id=%s namespace=%s pvc=%s pv=%s rf=%d desired=%d observed=%d primary=%s status=%s protocols=%s replicas=%d\n",
			volume.VolumeID,
			volume.Namespace,
			volume.PVCName,
			emptyAsDash(volume.PVName),
			volume.ReplicationFactor,
			volume.DesiredReplicas,
			volume.ObservedReplicas,
			volume.PrimaryReplicaID,
			volume.Status,
			strings.Join(volume.Protocols, ","),
			len(volume.Replicas))
		fmt.Fprintf(&b, "eligibility: volume=%s replicas_on_distinct_nodes=%t frontends_non_loopback=%t\n",
			volume.VolumeID,
			volume.ReplicasOnDistinctNodes,
			volume.FrontendsNonLoopback)
		for _, replica := range volume.Replicas {
			fmt.Fprintf(&b, "replica: volume=%s replica=%s server=%s node=%s observed=%t status=%s lifecycle_owner=%s owner_ref=%s role=%s replication=%s healthy=%t epoch=%d endpoint_version=%d frontend=%s status_addr=%s support_bundle=%s\n",
				volume.VolumeID,
				replica.ReplicaID,
				replica.ServerID,
				replica.NodeName,
				replica.Observed,
				replica.Status,
				replica.LifecycleOwner,
				replica.OwnerReference,
				replica.AuthorityRole,
				replica.ReplicationRole,
				replica.Healthy,
				replica.Epoch,
				replica.EndpointVersion,
				replica.FrontendAddress,
				replica.StatusAddress,
				replica.SupportBundle)
			fmt.Fprintf(&b, "promotion: volume=%s replica=%s candidate_ready=%t reason=%s claim_profile=%s ack_profile=%s required_frontier_known=%t required_frontier_lsn=%d candidate_frontier_known=%t candidate_frontier_lsn=%d frontier_covered=%t\n",
				volume.VolumeID,
				replica.ReplicaID,
				replica.PromotionReadiness.CandidateReady,
				replica.PromotionReadiness.Reason,
				replica.PromotionReadiness.ClaimProfile,
				replica.PromotionReadiness.AckProfile,
				replica.PromotionReadiness.RequiredFrontierKnown,
				replica.PromotionReadiness.RequiredFrontierLSN,
				replica.PromotionReadiness.CandidateFrontierKnown,
				replica.PromotionReadiness.CandidateFrontierLSN,
				replica.PromotionReadiness.FrontierCovered)
		}
		for _, ev := range volume.FailoverTimeline {
			fmt.Fprintf(&b, "failover: volume=%s phase=%s replica=%s role=%s epoch=%d endpoint_version=%d status=%s reason=%s\n",
				volume.VolumeID,
				explicitUnavailable(ev.Phase),
				explicitUnavailable(ev.ReplicaID),
				explicitUnavailable(ev.Role),
				ev.Epoch,
				ev.EndpointVersion,
				explicitUnavailable(ev.Status),
				emptyAsDash(ev.Reason))
		}
	}
	if len(issues) == 0 {
		b.WriteString("issues: none\n")
		return b.String()
	}
	b.WriteString("issues:\n")
	for _, issue := range issues {
		fmt.Fprintf(&b, "- %s\n", issue)
	}
	return b.String()
}

func volumeInventoryVolumeIssues(volume VolumeInventoryVolume) []string {
	var issues []string
	if volume.VolumeID == "" || volume.VolumeID == Unavailable {
		issues = append(issues, "invalid: volume_id unavailable")
	}
	if volume.DesiredReplicas <= 0 {
		issues = append(issues, fmt.Sprintf("invalid: desired_replicas=%d", volume.DesiredReplicas))
	}
	if volume.ObservedReplicas < volume.DesiredReplicas {
		issues = append(issues, fmt.Sprintf("observed_replicas=%d desired_replicas=%d", volume.ObservedReplicas, volume.DesiredReplicas))
	}
	if volume.PrimaryReplicaID == Unavailable && volume.ObservedReplicas > 0 {
		issues = append(issues, "primary_replica_id unavailable")
	}
	var primaryReplicas []string
	for _, replica := range volume.Replicas {
		if replica.Observed && replica.AuthorityRole == hostvolume.AuthorityRolePrimary {
			primaryReplicas = append(primaryReplicas, replica.ReplicaID)
		}
	}
	if len(primaryReplicas) > 1 {
		sort.Strings(primaryReplicas)
		issues = append(issues, "conflicting_primary_replicas="+strings.Join(primaryReplicas, ","))
	}
	for _, errText := range volume.CollectionErrors {
		if errText != "" {
			issues = append(issues, fmt.Sprintf("collection_error: %s", errText))
		}
	}
	for _, replica := range volume.Replicas {
		switch replica.Status {
		case "missing":
			issues = append(issues, fmt.Sprintf("replica_slot_missing=%s", replica.ReplicaID))
		case "unhealthy", "invalid":
			issues = append(issues, fmt.Sprintf("replica_degraded=%s status=%s", replica.ReplicaID, replica.Status))
		}
		for _, issue := range replica.Issues {
			issues = append(issues, fmt.Sprintf("replica %s %s", replica.ReplicaID, issue))
		}
		if volume.DesiredReplicas > 1 &&
			replica.Observed &&
			replica.AuthorityRole != hostvolume.AuthorityRolePrimary &&
			!replica.PromotionReadiness.CandidateReady {
			issues = append(issues, fmt.Sprintf("candidate_not_promotion_ready=%s reason=%s ack_profile=%s",
				replica.ReplicaID,
				replica.PromotionReadiness.Reason,
				replica.PromotionReadiness.AckProfile))
		}
	}
	if volume.ObservedReplicas < volume.DesiredReplicas && len(volume.Replicas) == volume.ObservedReplicas {
		issues = append(issues, "replica_slot_missing=unknown")
	}
	return issues
}

func replicasOnDistinctNodes(replicas []VolumeInventoryReplica, desired int) bool {
	if desired <= 1 {
		return true
	}
	seen := map[string]struct{}{}
	observed := 0
	for _, replica := range replicas {
		if !replica.Observed {
			continue
		}
		node := strings.TrimSpace(replica.NodeName)
		if node == "" || node == Unavailable {
			return false
		}
		if _, ok := seen[node]; ok {
			return false
		}
		seen[node] = struct{}{}
		observed++
	}
	return observed >= desired
}

func frontendsNonLoopback(replicas []VolumeInventoryReplica, desired int) bool {
	if desired <= 0 {
		return false
	}
	observed := 0
	for _, replica := range replicas {
		if !replica.Observed {
			continue
		}
		observed++
		if !endpointHostNonLoopback(replica.FrontendAddress) {
			return false
		}
	}
	return observed >= desired
}

func endpointHostNonLoopback(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" || addr == Unavailable {
		return false
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if host == "" || strings.EqualFold(host, "localhost") {
		return false
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return !ip.IsLoopback() && !ip.IsUnspecified()
	}
	return isValidDNSHost(host)
}

func isValidDNSHost(host string) bool {
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, ch := range label {
			if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func volumeInventoryReplicaIssues(replica VolumeInventoryReplica) []string {
	var issues []string
	if replica.ReplicaID == "" || replica.ReplicaID == Unavailable {
		issues = append(issues, "invalid: replica_id unavailable")
	}
	if !replica.Observed {
		issues = append(issues, "missing")
		return issues
	}
	if replica.AuthorityRole == "" || replica.AuthorityRole == Unavailable {
		issues = append(issues, "authority_role unavailable")
	}
	if replica.ReplicationRole == "" || replica.ReplicationRole == Unavailable || replica.ReplicationRole == hostvolume.ReplicationRoleUnknown {
		issues = append(issues, "replication_role unavailable")
	}
	if replica.AuthorityRole == hostvolume.AuthorityRolePrimary && !replica.Healthy {
		issues = append(issues, "authority healthy=false")
	}
	if replica.AuthorityRole == hostvolume.AuthorityRolePrimary && !replica.FrontendPrimaryReady {
		issues = append(issues, "primary frontend_primary_ready=false")
	}
	if replica.AuthorityRole == hostvolume.AuthorityRolePrimary && replica.ReplicationRole != hostvolume.ReplicationRoleNone {
		issues = append(issues, fmt.Sprintf("primary replication_role=%s want %s",
			replica.ReplicationRole, hostvolume.ReplicationRoleNone))
	}
	if replica.AuthorityRole != "" &&
		replica.AuthorityRole != Unavailable &&
		replica.AuthorityRole != hostvolume.AuthorityRolePrimary &&
		replica.FrontendPrimaryReady {
		if replica.AuthorityRole == hostvolume.AuthorityRoleSuperseded {
			issues = append(issues, fmt.Sprintf("stale_primary_frontend_ready=true role=%s epoch=%d endpoint_version=%d",
				replica.AuthorityRole, replica.Epoch, replica.EndpointVersion))
		}
		issues = append(issues, fmt.Sprintf("non-primary authority_role=%s frontend_primary_ready=true", replica.AuthorityRole))
	}
	if replica.AuthorityRole != "" &&
		replica.AuthorityRole != Unavailable &&
		replica.AuthorityRole != hostvolume.AuthorityRolePrimary &&
		replica.ReplicationRole == hostvolume.ReplicationRoleNone {
		issues = append(issues, fmt.Sprintf("non-primary authority_role=%s replication_role=%s",
			replica.AuthorityRole, replica.ReplicationRole))
	}
	for _, errText := range replica.CollectionErrors {
		if errText != "" {
			issues = append(issues, fmt.Sprintf("collection_error: %s", errText))
		}
	}
	if n := len(replica.Residue.HostInitiator.ISCSISessions); n > 0 {
		issues = append(issues, fmt.Sprintf("residue iscsi_sessions=%d", n))
	}
	if n := len(replica.Residue.HostInitiator.NVMESubsystems); n > 0 {
		issues = append(issues, fmt.Sprintf("residue nvme_subsystems=%d", n))
	}
	if n := len(replica.Residue.Processes); n > 0 {
		issues = append(issues, fmt.Sprintf("residue processes=%d", n))
	}
	if n := len(replica.Residue.Kubernetes); n > 0 {
		issues = append(issues, fmt.Sprintf("residue kubernetes=%d", n))
	}
	if n := len(replica.Residue.StoragePaths); n > 0 {
		issues = append(issues, fmt.Sprintf("residue storage_paths=%d", n))
	}
	return issues
}

func inventoryReplicaStatus(replica VolumeInventoryReplica) string {
	if !replica.Observed {
		return "missing"
	}
	return inventoryIssueStatus(replica.Issues)
}

func inventoryIssueStatus(issues []string) string {
	if hasInvalidIssue(issues) {
		return "invalid"
	}
	if len(issues) > 0 {
		return "unhealthy"
	}
	return "ok"
}

func inventoryExitLabel(code int) string {
	switch code {
	case VolumeStatusExitOK:
		return "ok"
	case VolumeStatusExitUnhealthy:
		return "unhealthy"
	default:
		return "invalid"
	}
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func containsIssuePrefix(issues []string, prefix string) bool {
	for _, issue := range issues {
		if strings.HasPrefix(issue, prefix) {
			return true
		}
	}
	return false
}

func copyFailoverTimeline(in []VolumeInventoryFailoverEvent) []VolumeInventoryFailoverEvent {
	if len(in) == 0 {
		return nil
	}
	out := make([]VolumeInventoryFailoverEvent, len(in))
	copy(out, in)
	return out
}
