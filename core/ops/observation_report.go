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
	cluster = normalizeObservationCluster(cluster)
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
	cluster = normalizeObservationCluster(cluster)
	var b strings.Builder
	fmt.Fprintf(&b, "sw-block report\n")
	fmt.Fprintf(&b, "status=%s\n", emptyAsDash(cluster.Status))
	fmt.Fprintf(&b, "captured_at=%s\n", cluster.CapturedAt.Format("2006-01-02T15:04:05Z07:00"))
	fmt.Fprintf(&b, "volumes=%d\n", len(cluster.Volumes))
	fmt.Fprintf(&b, "nodes=%d\n", len(cluster.Nodes))
	fmt.Fprintf(&b, "events=%d\n", len(cluster.Events))
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
	}
	fmt.Fprintf(&b, "read_only=true\n")
	return b.String()
}

func RenderObservationReportHTML(cluster ClusterEvidence) string {
	cluster = normalizeObservationCluster(cluster)
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

func esc(value string) string {
	return html.EscapeString(value)
}
