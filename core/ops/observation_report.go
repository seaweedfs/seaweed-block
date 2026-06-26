package ops

import (
	"fmt"
	"html"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func WriteObservationReportArtifacts(outDir string, cluster ClusterEvidence) error {
	if strings.TrimSpace(outDir) == "" {
		return fmt.Errorf("report out dir is required")
	}
	cluster = NormalizeObservationCluster(cluster)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	jsonRaw, err := MarshalObservationJSON(cluster)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, ObservationReportJSONArtifact), jsonRaw, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, ObservationReportTextArtifact), []byte(RenderObservationReportSummary(cluster)), 0o644); err != nil {
		return err
	}
	operatorRaw, err := MarshalObservationJSON(BuildOperatorFoundationSnapshot(cluster))
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, ObservationOperatorSnapshotArtifact), operatorRaw, 0o644); err != nil {
		return err
	}
	jsonl, err := RenderClusterEventsJSONL(cluster.Events)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, ObservationReportJSONLArtifact), []byte(jsonl), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, ObservationReportHTMLArtifact), []byte(RenderObservationReportHTML(cluster)), 0o644)
}

func RenderObservationReportSummary(cluster ClusterEvidence) string {
	cluster = NormalizeObservationCluster(cluster)
	var b strings.Builder
	fmt.Fprintf(&b, "sw-block report\n")
	fmt.Fprintf(&b, "status=%s\n", emptyAsDash(cluster.Status))
	fmt.Fprintf(&b, "captured_at=%s\n", cluster.CapturedAt.Format("2006-01-02T15:04:05Z07:00"))
	fmt.Fprintf(&b, "volumes=%d\n", len(cluster.Volumes))
	fmt.Fprintf(&b, "nodes=%d\n", len(cluster.Nodes))
	fmt.Fprintf(&b, "events=%d\n", len(cluster.Events))
	fmt.Fprintf(&b, "operator_snapshot=%s\n", ObservationOperatorSnapshotArtifact)
	if cluster.Cleanup != nil {
		for _, line := range cluster.Cleanup.ReportSummaryLines() {
			fmt.Fprintf(&b, "%s\n", line)
		}
	}
	if cluster.InstallDrift != nil {
		fmt.Fprintf(&b, "install_drift_status=%s reason=%s evidence=%s\n",
			emptyAsDash(cluster.InstallDrift.Status),
			emptyAsDash(cluster.InstallDrift.ReasonCode),
			emptyAsDash(cluster.InstallDrift.EvidenceRef))
		fmt.Fprintf(&b, "install_drift_chart current=%s desired=%s app_current=%s app_desired=%s\n",
			emptyAsDash(cluster.InstallDrift.CurrentChartVersion),
			emptyAsDash(cluster.InstallDrift.DesiredChartVersion),
			emptyAsDash(cluster.InstallDrift.CurrentAppVersion),
			emptyAsDash(cluster.InstallDrift.DesiredAppVersion))
		fmt.Fprintf(&b, "install_drift_image current=%s desired=%s csi_current=%s csi_desired=%s operator_current=%s operator_desired=%s\n",
			emptyAsDash(cluster.InstallDrift.CurrentImage),
			emptyAsDash(cluster.InstallDrift.DesiredImage),
			emptyAsDash(cluster.InstallDrift.CurrentCSIImage),
			emptyAsDash(cluster.InstallDrift.DesiredCSIImage),
			emptyAsDash(cluster.InstallDrift.CurrentOperatorImage),
			emptyAsDash(cluster.InstallDrift.DesiredOperatorImage))
	}
	supportRefs := supportBundleRefsFromCluster(cluster)
	for _, ref := range supportRefs {
		fmt.Fprintf(&b, "support_bundle_ref=%s\n", ref)
	}
	for _, step := range safeNextStepsFromCluster(cluster, supportRefs) {
		fmt.Fprintf(&b, "safe_next_step=%s mode=%s mutation_allowed=%t command=%q reason=%s\n",
			emptyAsDash(step.Type),
			emptyAsDash(step.Mode),
			step.MutationAllowed,
			step.Command,
			emptyAsDash(step.ReasonCode))
	}
	operatorSnapshot := BuildOperatorFoundationSnapshot(cluster)
	for _, condition := range operatorSnapshot.Cluster.Conditions {
		fmt.Fprintf(&b, "cluster_condition=%s status=%s reason=%s severity=%s\n",
			emptyAsDash(condition.Type),
			emptyAsDash(condition.Status),
			emptyAsDash(condition.Reason),
			emptyAsDash(condition.Severity))
	}
	for _, node := range operatorSnapshot.Cluster.Nodes {
		fmt.Fprintf(&b, "node=%s k8s=%s status=%s reason=%s ready=%t schedulable=%t missing_images=%s\n",
			emptyAsDash(node.Name),
			emptyAsDash(node.KubernetesNode),
			emptyAsDash(node.Status),
			emptyAsDash(node.ReasonCode),
			node.Ready,
			node.Schedulable,
			emptyAsDash(strings.Join(node.MissingImages, ",")))
	}
	renderedManaged := map[string]bool{}
	for _, volume := range cluster.Volumes {
		fmt.Fprintf(&b, "volume=%s status=%s pvc=%s/%s primary=%s@%s frontend=%s rf=%d ack=%s\n",
			emptyAsDash(volume.VolumeID),
			emptyAsDash(volume.Status),
			emptyAsDash(volume.Namespace),
			emptyAsDash(volume.PVCName),
			emptyAsDash(volume.PrimaryReplica),
			emptyAsDash(volume.PrimaryNode),
			emptyAsDash(volume.PublishTarget),
			volume.ReplicationFactor,
			emptyAsDash(volume.AckProfile))
		managed := managedProjectionForVolume(cluster.ManagedVolumes, volume.VolumeID)
		renderManagedProjectionSummary(&b, managed)
		renderedManaged[managedProjectionKey(managed)] = true
	}
	for _, managed := range cluster.ManagedVolumes {
		if renderedManaged[managedProjectionKey(managed)] {
			continue
		}
		renderManagedProjectionSummary(&b, managed)
	}
	fmt.Fprintf(&b, "read_only=true\n")
	return b.String()
}

func renderManagedProjectionSummary(b *strings.Builder, managed ManagedVolumeProjection) {
	fmt.Fprintf(b, "managed_volume=%s status=%s reason=%s\n",
		emptyAsDash(managed.VolumeID),
		emptyAsDash(managed.Status),
		emptyAsDash(managed.ReasonCode))
	fmt.Fprintf(b, "managed_volume_authority=%s primary=%s publish_target=%s epoch=%d endpoint_version=%d\n",
		emptyAsDash(managed.VolumeID),
		emptyAsDash(managed.PrimaryReplicaID),
		emptyAsDash(managed.PublishTarget),
		managed.AuthorityEpoch,
		managed.AuthorityEndpointVersion)
	for _, condition := range managed.Conditions {
		fmt.Fprintf(b, "managed_volume_condition=%s status=%s reason=%s severity=%s\n",
			emptyAsDash(condition.Type),
			emptyAsDash(condition.Status),
			emptyAsDash(condition.Reason),
			emptyAsDash(condition.Severity))
	}
	if managed.DeleteSafety != nil {
		fmt.Fprintf(b, "managed_volume_delete_safety=%s state=%s decision=%s reason=%s release_allowed=%t action=%s\n",
			emptyAsDash(managed.VolumeID),
			emptyAsDash(managed.DeleteSafety.State),
			emptyAsDash(managed.DeleteSafety.Decision),
			emptyAsDash(managed.DeleteSafety.Reason),
			managed.DeleteSafety.FinalizerReleaseAllowed,
			emptyAsDash(managed.DeleteSafety.ActionType))
		if managed.DeleteSafety.SafeNextAction != "" {
			fmt.Fprintf(b, "managed_volume_delete_safety_safe_next_action=%s %s\n",
				emptyAsDash(managed.VolumeID),
				managed.DeleteSafety.SafeNextAction)
		}
	}
	for _, returned := range managed.ReplicaReintegrations {
		fmt.Fprintf(b, "managed_volume_returned_replica=%s replica=%s state=%s reason=%s frontend_fenced=%t ack_eligibility_known=%t ack_eligible=%t durable_frontier_known=%t durable_lsn=%d required_frontier_known=%t required_lsn=%d\n",
			emptyAsDash(managed.VolumeID),
			emptyAsDash(returned.ReplicaID),
			emptyAsDash(returned.State),
			emptyAsDash(returned.ReasonCode),
			returned.FrontendFenced,
			returned.AckEligibilityKnown,
			returned.AckEligible,
			returned.DurableFrontierKnown,
			returned.DurableFrontierLSN,
			returned.RequiredFrontierKnown,
			returned.RequiredFrontierLSN)
	}
	for _, preflight := range ReturnedReplicaExecutorPreflights(managed) {
		fmt.Fprintf(b, "managed_volume_executor_preflight=%s target=%s decision=%s reason=%s mode=%s executor=%s mutation_allowed=%t ack_eligibility_known=%t required_lsn=%d durable_lsn=%d\n",
			emptyAsDash(preflight.ActionType),
			emptyAsDash(preflight.ReplicaID),
			emptyAsDash(preflight.Decision),
			emptyAsDash(preflight.Reason),
			emptyAsDash(preflight.Mode),
			emptyAsDash(preflight.OwnerExecutor),
			preflight.MutationAllowed,
			preflight.AckEligibilityKnown,
			preflight.RequiredFrontierLSN,
			preflight.DurableFrontierLSN)
	}
	for _, contract := range ReturnedReplicaExecutorContracts(managed) {
		fmt.Fprintf(b, "managed_volume_executor_contract=%s target=%s decision=%s reason=%s executor=%s execution_enabled=%t mutation_allowed=%t allowed_mutation=%s terminal_evidence=%s\n",
			emptyAsDash(contract.ActionType),
			emptyAsDash(contract.ReplicaID),
			emptyAsDash(contract.Decision),
			emptyAsDash(contract.Reason),
			emptyAsDash(contract.OwnerExecutor),
			contract.ExecutionEnabled,
			contract.MutationAllowed,
			emptyAsDash(strings.Join(contract.AllowedMutationClass, ",")),
			emptyAsDash(strings.Join(contract.TerminalEvidenceRequired, ",")))
	}
	for _, action := range managed.Actions {
		fmt.Fprintf(b, "managed_volume_action=%s mode=%s side_effect=%s executor=%s decision=%s",
			emptyAsDash(action.Type),
			emptyAsDash(action.Mode),
			emptyAsDash(action.SideEffectClass),
			emptyAsDash(action.OwnerExecutor),
			emptyAsDash(action.Decision))
		if action.DecisionReason != "" {
			fmt.Fprintf(b, " reason=%s", action.DecisionReason)
		}
		b.WriteByte('\n')
		if action.EvidenceRequired != "" {
			fmt.Fprintf(b, "managed_volume_action_evidence_required=%s %s\n",
				emptyAsDash(action.Type),
				action.EvidenceRequired)
		}
	}
}

func managedProjectionKey(managed ManagedVolumeProjection) string {
	return defaultString(managed.VolumeID, managed.PVCName)
}

func RenderObservationReportHTML(cluster ClusterEvidence) string {
	cluster = NormalizeObservationCluster(cluster)
	events := append([]ClusterEvent(nil), cluster.Events...)
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].EventTime.Before(events[j].EventTime)
	})
	if len(events) > 25 {
		events = events[len(events)-25:]
	}

	var b strings.Builder
	b.WriteString("<!doctype html><html><head><meta charset=\"utf-8\">")
	b.WriteString("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")
	b.WriteString("<title>sw-block status report</title>")
	b.WriteString("<style>")
	b.WriteString("body{font-family:ui-sans-serif,Segoe UI,Arial,sans-serif;margin:0;background:#f7f4ee;color:#15120d}")
	b.WriteString("header{padding:28px 34px;background:#172019;color:#f6f0e4}")
	b.WriteString("main{padding:24px 34px;display:grid;gap:22px}")
	b.WriteString(".cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}")
	b.WriteString(".card,section{background:#fffaf0;border:1px solid #d9cdb8;border-radius:14px;padding:16px;box-shadow:0 1px 0 #eee}")
	b.WriteString(".label{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:#6f6558}.value{font-size:24px;font-weight:700;margin-top:4px}")
	b.WriteString("table{width:100%;border-collapse:collapse}th,td{padding:9px 8px;border-bottom:1px solid #eadfcd;text-align:left;font-size:14px;vertical-align:top}")
	b.WriteString("th{color:#5b5145;font-size:12px;text-transform:uppercase;letter-spacing:.05em}.ok{color:#12613a}.bad{color:#9b2c1d}code{background:#efe3cf;padding:2px 5px;border-radius:5px}")
	b.WriteString("</style></head><body>")
	fmt.Fprintf(&b, "<header><h1>sw-block read-only status</h1><p>status=%s captured=%s product=%s</p><p>This report is observation-only. It does not promote, repair, delete, or mutate Kubernetes resources.</p></header>",
		esc(emptyAsDash(cluster.Status)), esc(cluster.CapturedAt.Format("2006-01-02T15:04:05Z07:00")), esc(emptyAsDash(cluster.ProductRevision)))
	b.WriteString("<main>")
	b.WriteString("<div class=\"cards\">")
	reportCard(&b, "Volumes", fmt.Sprintf("%d", len(cluster.Volumes)), "")
	reportCard(&b, "Nodes", fmt.Sprintf("%d", len(cluster.Nodes)), "")
	reportCard(&b, "Events", fmt.Sprintf("%d", len(cluster.Events)), "")
	reportCard(&b, "Read Only", "true", "ok")
	b.WriteString("</div>")

	if cluster.Cleanup != nil {
		row := cluster.Cleanup.ReportRow()
		b.WriteString("<section><h2>Lifecycle Cleanup</h2><table><thead><tr><th>Status</th><th>K8s</th><th>iSCSI</th><th>Multipath</th><th>Processes</th><th>HostPath</th><th>Failures</th><th>Evidence</th></tr></thead><tbody>")
		fmt.Fprintf(&b, "<tr><td class=\"%s\">%s</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%s</td></tr>",
			row.StatusClass,
			esc(row.Status),
			row.KubernetesResidueCount,
			row.ISCSIResidueCount,
			row.MultipathResidueCount,
			row.ProcessResidueCount,
			row.HostPathResidueCount,
			row.FailureCount,
			esc(row.EvidenceRef))
		b.WriteString("</tbody></table></section>")
	}

	if cluster.InstallDrift != nil {
		b.WriteString("<section><h2>Install Drift</h2><table><thead><tr><th>Status</th><th>Reason</th><th>Chart</th><th>App</th><th>Image</th><th>CSI Image</th><th>Operator Image</th><th>Evidence</th></tr></thead><tbody>")
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%s</td><td>%s -> %s</td><td>%s -> %s</td><td>%s -> %s</td><td>%s -> %s</td><td>%s -> %s</td><td>%s</td></tr>",
			esc(emptyAsDash(cluster.InstallDrift.Status)),
			esc(emptyAsDash(cluster.InstallDrift.ReasonCode)),
			esc(emptyAsDash(cluster.InstallDrift.CurrentChartVersion)),
			esc(emptyAsDash(cluster.InstallDrift.DesiredChartVersion)),
			esc(emptyAsDash(cluster.InstallDrift.CurrentAppVersion)),
			esc(emptyAsDash(cluster.InstallDrift.DesiredAppVersion)),
			esc(emptyAsDash(cluster.InstallDrift.CurrentImage)),
			esc(emptyAsDash(cluster.InstallDrift.DesiredImage)),
			esc(emptyAsDash(cluster.InstallDrift.CurrentCSIImage)),
			esc(emptyAsDash(cluster.InstallDrift.DesiredCSIImage)),
			esc(emptyAsDash(cluster.InstallDrift.CurrentOperatorImage)),
			esc(emptyAsDash(cluster.InstallDrift.DesiredOperatorImage)),
			esc(emptyAsDash(cluster.InstallDrift.EvidenceRef)))
		b.WriteString("</tbody></table></section>")
	}

	supportRefs := supportBundleRefsFromCluster(cluster)
	if len(supportRefs) > 0 {
		b.WriteString("<section><h2>Support Evidence</h2><table><thead><tr><th>Evidence Ref</th></tr></thead><tbody>")
		for _, ref := range supportRefs {
			fmt.Fprintf(&b, "<tr><td><code>%s</code></td></tr>", esc(ref))
		}
		b.WriteString("</tbody></table></section>")
	}
	if steps := safeNextStepsFromCluster(cluster, supportRefs); len(steps) > 0 {
		b.WriteString("<section><h2>Safe Next Steps</h2><table><thead><tr><th>Type</th><th>Mode</th><th>Mutation</th><th>Reason</th><th>Command</th></tr></thead><tbody>")
		for _, step := range steps {
			fmt.Fprintf(&b, "<tr><td>%s</td><td>%s</td><td>%t</td><td>%s</td><td><code>%s</code></td></tr>",
				esc(step.Type), esc(step.Mode), step.MutationAllowed, esc(emptyAsDash(step.ReasonCode)), esc(step.Command))
		}
		b.WriteString("</tbody></table></section>")
	}

	b.WriteString("<section><h2>Managed Volumes</h2><table><thead><tr><th>Volume</th><th>Status</th><th>Reason</th><th>Conditions</th><th>Safe Actions</th></tr></thead><tbody>")
	for _, managed := range cluster.ManagedVolumes {
		class := "ok"
		if managed.Status != ManagedVolumeStatusReady && managed.Status != ManagedVolumeStatusRecovered {
			class = "bad"
		}
		fmt.Fprintf(&b, "<tr><td><code>%s</code></td><td class=\"%s\">%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
			esc(emptyAsDash(managed.VolumeID)),
			class,
			esc(emptyAsDash(managed.Status)),
			esc(emptyAsDash(managed.ReasonCode)),
			esc(managedConditionSummary(managed.Conditions)),
			esc(managedActionSummary(managed.Actions)))
	}
	b.WriteString("</tbody></table></section>")

	b.WriteString("<section><h2>Volumes</h2><table><thead><tr><th>Volume</th><th>PVC</th><th>Status</th><th>RF</th><th>Primary</th><th>Frontend</th><th>Reason</th></tr></thead><tbody>")
	for _, volume := range cluster.Volumes {
		class := "ok"
		if volume.Status != ObservationStatusOK {
			class = "bad"
		}
		fmt.Fprintf(&b, "<tr><td><code>%s</code></td><td>%s/%s</td><td class=\"%s\">%s</td><td>%d</td><td>%s@%s</td><td>%s</td><td>%s</td></tr>",
			esc(emptyAsDash(volume.VolumeID)), esc(emptyAsDash(volume.Namespace)), esc(emptyAsDash(volume.PVCName)),
			class, esc(emptyAsDash(volume.Status)), volume.ReplicationFactor,
			esc(emptyAsDash(volume.PrimaryReplica)), esc(emptyAsDash(volume.PrimaryNode)),
			esc(emptyAsDash(volume.PublishTarget)), esc(emptyAsDash(volume.Reason)))
	}
	b.WriteString("</tbody></table></section>")

	b.WriteString("<section><h2>Managed Volume Conditions</h2><table><thead><tr><th>Volume</th><th>Type</th><th>Status</th><th>Severity</th><th>Reason</th><th>Evidence</th><th>Message</th></tr></thead><tbody>")
	for _, managed := range cluster.ManagedVolumes {
		for _, condition := range managed.Conditions {
			class := "ok"
			if condition.Status != "True" || condition.Severity == "warning" || condition.Severity == "error" {
				class = "bad"
			}
			fmt.Fprintf(&b, "<tr><td><code>%s</code></td><td>%s</td><td class=\"%s\">%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
				esc(emptyAsDash(managed.VolumeID)),
				esc(emptyAsDash(condition.Type)),
				class,
				esc(emptyAsDash(condition.Status)),
				esc(emptyAsDash(condition.Severity)),
				esc(emptyAsDash(condition.Reason)),
				esc(strings.Join(condition.EvidenceRefs, ", ")),
				esc(emptyAsDash(condition.Message)))
		}
	}
	b.WriteString("</tbody></table></section>")

	b.WriteString("<section><h2>Nodes</h2><table><thead><tr><th>Node</th><th>IP</th><th>Ready</th><th>Schedulable</th><th>Replicas</th><th>Missing Images</th></tr></thead><tbody>")
	for _, node := range cluster.Nodes {
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%s</td><td>%t</td><td>%t</td><td>%d</td><td>%s</td></tr>",
			esc(emptyAsDash(node.NodeName)), esc(emptyAsDash(node.InternalIP)), node.Ready, node.Schedulable, node.ReplicaCount, esc(strings.Join(node.MissingImages, ", ")))
	}
	b.WriteString("</tbody></table></section>")

	b.WriteString("<section><h2>Recent Timeline</h2><table><thead><tr><th>Time</th><th>Type</th><th>Reason</th><th>Volume</th><th>Replica</th><th>Message</th></tr></thead><tbody>")
	for _, event := range events {
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
			esc(event.EventTime.Format("15:04:05")), esc(emptyAsDash(event.Type)), esc(emptyAsDash(event.Reason)),
			esc(emptyAsDash(event.VolumeID)), esc(emptyAsDash(event.ReplicaID)), esc(emptyAsDash(event.Message)))
	}
	b.WriteString("</tbody></table></section>")
	b.WriteString("<section><h2>Machine Artifacts</h2><p><code>cluster-evidence.json</code>, <code>timeline.jsonl</code>, and <code>summary.txt</code> are generated next to this page.</p></section>")
	b.WriteString("</main></body></html>\n")
	return b.String()
}

func reportCard(b *strings.Builder, label, value, class string) {
	if class != "" {
		fmt.Fprintf(b, "<div class=\"card\"><div class=\"label\">%s</div><div class=\"value %s\">%s</div></div>", esc(label), class, esc(value))
		return
	}
	fmt.Fprintf(b, "<div class=\"card\"><div class=\"label\">%s</div><div class=\"value\">%s</div></div>", esc(label), esc(value))
}

func managedConditionSummary(conditions []ObservationCondition) string {
	if len(conditions) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		parts = append(parts, fmt.Sprintf("%s=%s/%s", emptyAsDash(condition.Type), emptyAsDash(condition.Status), emptyAsDash(condition.Reason)))
	}
	return strings.Join(parts, "; ")
}

func managedActionSummary(actions []ManagedVolumeAction) string {
	if len(actions) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(actions))
	for _, action := range actions {
		parts = append(parts, fmt.Sprintf("%s(%s)", emptyAsDash(action.Type), emptyAsDash(action.Mode)))
	}
	return strings.Join(parts, "; ")
}

func managedProjectionForVolume(managed []ManagedVolumeProjection, volumeID string) ManagedVolumeProjection {
	for _, projection := range managed {
		if projection.VolumeID == volumeID {
			return projection
		}
	}
	if len(managed) == 1 && volumeID == "" {
		return managed[0]
	}
	return ManagedVolumeProjection{VolumeID: volumeID, Status: ManagedVolumeStatusUnknown}
}

func esc(value string) string {
	return html.EscapeString(value)
}
