package ops

import (
	"fmt"
	"strings"

	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
)

const (
	// VolumeStatusExitOK means the report is valid and has no unsafe evidence.
	VolumeStatusExitOK = 0
	// VolumeStatusExitUnhealthy means the report is valid but contains
	// unhealthy, incomplete, or residue evidence an operator should inspect.
	VolumeStatusExitUnhealthy = 1
	// VolumeStatusExitInvalid means the report shape or identity is unusable.
	VolumeStatusExitInvalid = 2
)

// ClassifyVolumeStatusReport maps a report into the exit-code contract used by
// operator-facing status commands. It is intentionally conservative: unknown or
// unavailable health evidence is not a clean pass.
func ClassifyVolumeStatusReport(r VolumeStatusReport) int {
	issues := VolumeStatusReportIssues(r)
	for _, issue := range issues {
		if strings.HasPrefix(issue, "invalid:") {
			return VolumeStatusExitInvalid
		}
	}
	if len(issues) > 0 {
		return VolumeStatusExitUnhealthy
	}
	return VolumeStatusExitOK
}

// RenderVolumeStatusSummary converts the machine-readable report into stable
// plain text suitable for logs, artifacts, and bug reports.
func RenderVolumeStatusSummary(r VolumeStatusReport) string {
	issues := VolumeStatusReportIssues(r)
	status := "ok"
	switch ClassifyVolumeStatusReport(r) {
	case VolumeStatusExitInvalid:
		status = "invalid"
	case VolumeStatusExitUnhealthy:
		status = "unhealthy"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "status: %s\n", status)
	fmt.Fprintf(&b, "schema_version: %s\n", r.SchemaVersion)
	if !r.CapturedAt.IsZero() {
		fmt.Fprintf(&b, "captured_at: %s\n", r.CapturedAt.UTC().Format("2006-01-02T15:04:05Z"))
	}
	fmt.Fprintf(&b, "source: component=%s host=%s scenario=%s\n", r.Source.Component, emptyAsDash(r.Source.Host), emptyAsDash(r.Source.Scenario))
	fmt.Fprintf(&b, "product_revision: %s\n", r.ProductRevision)
	if r.RunnerRevision != "" {
		fmt.Fprintf(&b, "runner_revision: %s\n", r.RunnerRevision)
	}
	fmt.Fprintf(&b, "volume: id=%s replica=%s protocols=%s frontends=%d\n", r.Volume.VolumeID, r.Volume.ReplicaID, strings.Join(r.Volume.Protocols, ","), len(r.Volume.Frontends))
	for _, f := range r.Volume.Frontends {
		fmt.Fprintf(&b, "frontend: protocol=%s addr=%s iqn=%s nqn=%s lun=%d nsid=%d\n", f.Protocol, emptyAsDash(f.Addr), emptyAsDash(f.IQN), emptyAsDash(f.NQN), f.LUN, f.NSID)
	}
	fmt.Fprintf(&b, "authority: role=%s healthy=%t primary_ready=%t assigned=%t epoch=%d endpoint_version=%d\n", r.Authority.AuthorityRole, r.Authority.Healthy, r.Authority.FrontendPrimaryReady, r.Authority.Assigned, r.Authority.Epoch, r.Authority.EndpointVersion)
	fmt.Fprintf(&b, "replication: role=%s peers=%d\n", r.Replication.ReplicationRole, len(r.Replication.Peers))
	for _, p := range r.Replication.Peers {
		fmt.Fprintf(&b, "peer: replica=%s state=%s healthy=%t closed=%t probe_in_flight=%t epoch=%d endpoint_version=%d\n", p.ReplicaID, p.State, p.Healthy, p.Closed, p.ProbeInFlight, p.Epoch, p.EndpointVersion)
	}
	fmt.Fprintf(&b, "durable: entries=%d\n", len(r.Durable))
	for _, d := range r.Durable {
		fmt.Fprintf(&b, "durable_entry: impl=%s path=%s replica=%s latched=%t operational=%t closed=%t epoch=%d endpoint_version=%d\n", d.Impl, emptyAsDash(d.Path), d.ReplicaID, d.Latched, d.Operational, d.Closed, d.Epoch, d.EndpointVersion)
	}
	fmt.Fprintf(&b, "residue: iscsi_sessions=%d nvme_subsystems=%d processes=%d kubernetes=%d storage_paths=%d\n",
		len(r.Residue.HostInitiator.ISCSISessions),
		len(r.Residue.HostInitiator.NVMESubsystems),
		len(r.Residue.Processes),
		len(r.Residue.Kubernetes),
		len(r.Residue.StoragePaths))
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

// VolumeStatusReportIssues returns deterministic operator-facing reasons for a
// non-zero status classification.
func VolumeStatusReportIssues(r VolumeStatusReport) []string {
	var issues []string
	if r.SchemaVersion != VolumeStatusReportSchemaVersion {
		issues = append(issues, fmt.Sprintf("invalid: schema_version=%s want %s", r.SchemaVersion, VolumeStatusReportSchemaVersion))
	}
	if r.Volume.VolumeID == "" || r.Volume.VolumeID == Unavailable {
		issues = append(issues, "invalid: volume_id unavailable")
	}
	if r.Volume.ReplicaID == "" || r.Volume.ReplicaID == Unavailable {
		issues = append(issues, "invalid: replica_id unavailable")
	}
	if r.ProductRevision == "" || r.ProductRevision == Unavailable {
		issues = append(issues, "product_revision unavailable")
	}
	if r.Authority.AuthorityRole == "" || r.Authority.AuthorityRole == Unavailable {
		issues = append(issues, "authority_role unavailable")
	}
	if r.Authority.AuthorityRole == hostvolume.AuthorityRolePrimary && !r.Authority.Healthy {
		issues = append(issues, "authority healthy=false")
	}
	if r.Authority.AuthorityRole == hostvolume.AuthorityRolePrimary && !r.Authority.FrontendPrimaryReady {
		issues = append(issues, "primary frontend_primary_ready=false")
	}
	if r.Authority.AuthorityRole != "" &&
		r.Authority.AuthorityRole != Unavailable &&
		r.Authority.AuthorityRole != hostvolume.AuthorityRolePrimary &&
		r.Authority.FrontendPrimaryReady {
		issues = append(issues, fmt.Sprintf("non-primary authority_role=%s frontend_primary_ready=true", r.Authority.AuthorityRole))
	}
	switch r.Replication.ReplicationRole {
	case "", Unavailable, hostvolume.ReplicationRoleUnknown:
		issues = append(issues, "replication_role unavailable")
	case hostvolume.ReplicationRoleNone, hostvolume.ReplicationRoleReady:
	default:
		issues = append(issues, fmt.Sprintf("replication_role=%s", r.Replication.ReplicationRole))
	}
	if r.Authority.AuthorityRole == hostvolume.AuthorityRolePrimary &&
		r.Replication.ReplicationRole != hostvolume.ReplicationRoleNone {
		issues = append(issues, fmt.Sprintf("primary replication_role=%s want %s", r.Replication.ReplicationRole, hostvolume.ReplicationRoleNone))
	}
	if r.Authority.AuthorityRole != "" &&
		r.Authority.AuthorityRole != Unavailable &&
		r.Authority.AuthorityRole != hostvolume.AuthorityRolePrimary &&
		r.Replication.ReplicationRole == hostvolume.ReplicationRoleNone {
		issues = append(issues, fmt.Sprintf("non-primary authority_role=%s replication_role=%s", r.Authority.AuthorityRole, r.Replication.ReplicationRole))
	}
	for _, p := range r.Replication.Peers {
		if !p.Healthy {
			issues = append(issues, fmt.Sprintf("peer %s healthy=false state=%s", p.ReplicaID, p.State))
		}
		if p.Closed {
			issues = append(issues, fmt.Sprintf("peer %s closed=true", p.ReplicaID))
		}
		if p.ProbeInFlight {
			issues = append(issues, fmt.Sprintf("peer %s probe_in_flight=true", p.ReplicaID))
		}
	}
	for _, d := range r.Durable {
		if !d.Latched {
			issues = append(issues, fmt.Sprintf("durable %s/%s latched=false", d.VolumeID, d.ReplicaID))
		}
		if !d.Operational {
			issues = append(issues, fmt.Sprintf("durable %s/%s operational=false", d.VolumeID, d.ReplicaID))
		}
		if d.Closed {
			issues = append(issues, fmt.Sprintf("durable %s/%s closed=true", d.VolumeID, d.ReplicaID))
		}
	}
	if n := len(r.Residue.HostInitiator.ISCSISessions); n > 0 {
		issues = append(issues, fmt.Sprintf("residue iscsi_sessions=%d", n))
	}
	if n := len(r.Residue.HostInitiator.NVMESubsystems); n > 0 {
		issues = append(issues, fmt.Sprintf("residue nvme_subsystems=%d", n))
	}
	if n := len(r.Residue.Processes); n > 0 {
		issues = append(issues, fmt.Sprintf("residue processes=%d", n))
	}
	if n := len(r.Residue.Kubernetes); n > 0 {
		issues = append(issues, fmt.Sprintf("residue kubernetes=%d", n))
	}
	if n := len(r.Residue.StoragePaths); n > 0 {
		issues = append(issues, fmt.Sprintf("residue storage_paths=%d", n))
	}
	return issues
}

func emptyAsDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
