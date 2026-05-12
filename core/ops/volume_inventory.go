package ops

import (
	"fmt"
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
	Replicas          []VolumeInventoryReplicaInput
}

type VolumeInventoryReplicaInput struct {
	ReplicaID            string
	ServerID             string
	NodeName             string
	GeneratedDeployment  string
	Protocol             string
	FrontendAddress      string
	StatusAddress        string
	DataAddr             string
	CtrlAddr             string
	Observed             bool
	AuthorityRole        string
	Healthy              bool
	FrontendPrimaryReady bool
	ReplicationRole      string
	Epoch                uint64
	EndpointVersion      uint64
	Residue              ResidueReport
	CollectionErrors     []string
	Issues               []string
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
	VolumeID          string                   `json:"volume_id"`
	Namespace         string                   `json:"namespace"`
	PVCName           string                   `json:"pvc_name"`
	PVName            string                   `json:"pv_name"`
	ReplicationFactor int                      `json:"replication_factor"`
	DesiredReplicas   int                      `json:"desired_replicas"`
	ObservedReplicas  int                      `json:"observed_replicas"`
	PrimaryReplicaID  string                   `json:"primary_replica_id"`
	Protocols         []string                 `json:"protocols"`
	ProductRevision   string                   `json:"product_revision"`
	Status            string                   `json:"status"`
	Residue           ResidueReport            `json:"residue"`
	Issues            []string                 `json:"issues"`
	Unchecked         []string                 `json:"unchecked"`
	CollectionErrors  []string                 `json:"collection_errors"`
	SupportBundle     string                   `json:"support_bundle"`
	Replicas          []VolumeInventoryReplica `json:"replicas"`
}

type VolumeInventoryReplica struct {
	ReplicaID            string        `json:"replica_id"`
	ServerID             string        `json:"server_id"`
	NodeName             string        `json:"node_name"`
	GeneratedDeployment  string        `json:"generated_deployment"`
	Protocol             string        `json:"protocol"`
	FrontendAddress      string        `json:"frontend_address"`
	StatusAddress        string        `json:"status_address"`
	DataAddr             string        `json:"data_addr"`
	CtrlAddr             string        `json:"ctrl_addr"`
	Observed             bool          `json:"observed"`
	Status               string        `json:"status"`
	AuthorityRole        string        `json:"authority_role"`
	Healthy              bool          `json:"healthy"`
	FrontendPrimaryReady bool          `json:"frontend_primary_ready"`
	ReplicationRole      string        `json:"replication_role"`
	Epoch                uint64        `json:"epoch"`
	EndpointVersion      uint64        `json:"endpoint_version"`
	Residue              ResidueReport `json:"residue"`
	Issues               []string      `json:"issues"`
	CollectionErrors     []string      `json:"collection_errors"`
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
			"Read-only inventory; it does not mutate product state.",
			"Inventory is not repair, cleanup, failover, backup, or restore.",
			"RF=2/RF=3 live Kubernetes operation is claimed only when a runner gate explicitly proves it.",
			"Missing inputs are reported as issues or unchecked evidence, not inferred as healthy.",
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
	protocolsSeen := map[string]bool{}
	for _, replica := range replicas {
		if replica.Observed {
			observed++
		}
		if replica.AuthorityRole == hostvolume.AuthorityRolePrimary {
			primary = replica.ReplicaID
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
		VolumeID:          explicitUnavailable(in.VolumeID),
		Namespace:         explicitUnavailable(in.Namespace),
		PVCName:           explicitUnavailable(in.PVCName),
		PVName:            explicitUnavailable(in.PVName),
		ReplicationFactor: desired,
		DesiredReplicas:   desired,
		ObservedReplicas:  observed,
		PrimaryReplicaID:  primary,
		Protocols:         protocols,
		ProductRevision:   productRevision,
		Residue:           copyResidue(in.Residue),
		Unchecked:         copyStringSlice(in.Residue.Unchecked),
		CollectionErrors:  copyStringSlice(in.CollectionErrors),
		SupportBundle:     in.SupportBundle,
		Replicas:          replicas,
	}
	volume.Issues = volumeInventoryVolumeIssues(volume)
	volume.Status = inventoryIssueStatus(volume.Issues)
	return volume
}

func buildInventoryReplica(in VolumeInventoryReplicaInput) VolumeInventoryReplica {
	replica := VolumeInventoryReplica{
		ReplicaID:            explicitUnavailable(in.ReplicaID),
		ServerID:             explicitUnavailable(in.ServerID),
		NodeName:             explicitUnavailable(in.NodeName),
		GeneratedDeployment:  explicitUnavailable(in.GeneratedDeployment),
		Protocol:             explicitUnavailable(in.Protocol),
		FrontendAddress:      explicitUnavailable(in.FrontendAddress),
		StatusAddress:        explicitUnavailable(in.StatusAddress),
		DataAddr:             explicitUnavailable(in.DataAddr),
		CtrlAddr:             explicitUnavailable(in.CtrlAddr),
		Observed:             in.Observed,
		AuthorityRole:        explicitUnavailable(in.AuthorityRole),
		Healthy:              in.Healthy,
		FrontendPrimaryReady: in.FrontendPrimaryReady,
		ReplicationRole:      explicitUnavailable(in.ReplicationRole),
		Epoch:                in.Epoch,
		EndpointVersion:      in.EndpointVersion,
		Residue:              copyResidue(in.Residue),
		Issues:               copyStringSlice(in.Issues),
		CollectionErrors:     copyStringSlice(in.CollectionErrors),
	}
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
			issues = append(issues, fmt.Sprintf("collection_error: %s", errText))
		}
	}
	if len(in.Volumes) == 0 {
		issues = append(issues, "inventory has no volumes")
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
		for _, replica := range volume.Replicas {
			fmt.Fprintf(&b, "replica: volume=%s replica=%s server=%s node=%s observed=%t status=%s role=%s replication=%s healthy=%t epoch=%d endpoint_version=%d frontend=%s status_addr=%s\n",
				volume.VolumeID,
				replica.ReplicaID,
				replica.ServerID,
				replica.NodeName,
				replica.Observed,
				replica.Status,
				replica.AuthorityRole,
				replica.ReplicationRole,
				replica.Healthy,
				replica.Epoch,
				replica.EndpointVersion,
				replica.FrontendAddress,
				replica.StatusAddress)
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
	for _, errText := range volume.CollectionErrors {
		if errText != "" {
			issues = append(issues, fmt.Sprintf("collection_error: %s", errText))
		}
	}
	for _, replica := range volume.Replicas {
		for _, issue := range replica.Issues {
			issues = append(issues, fmt.Sprintf("replica %s %s", replica.ReplicaID, issue))
		}
	}
	return issues
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
