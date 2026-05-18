package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/internal/buildinfo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

var (
	opsStatusRunCommand    = ops.DefaultRunCommand
	opsInventoryRunCommand = ops.DefaultRunCommand
)

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
	if args[0] == "--version" || args[0] == "version" {
		fmt.Fprintln(stdout, buildinfo.Version("sw-block"))
		return ops.VolumeStatusExitOK
	}
	if args[0] != "ops" {
		fmt.Fprintf(stderr, "sw-block: unknown command %q\n", args[0])
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
	if len(args) < 2 {
		fmt.Fprintln(stderr, "sw-block: expected subcommand ops status|inventory|list|cluster|volumes|describe|timeline|explain|report")
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
	switch args[1] {
	case "status":
		return runOpsStatus(args[2:], stdout, stderr)
	case "inventory", "list":
		return runOpsInventory(args[2:], stdout, stderr)
	case "cluster":
		return runOpsCluster(args[2:], stdout, stderr)
	case "volumes":
		return runOpsVolumes(args[2:], stdout, stderr)
	case "describe":
		return runOpsDescribe(args[2:], stdout, stderr)
	case "timeline":
		return runOpsTimeline(args[2:], stdout, stderr)
	case "explain":
		return runOpsExplain(args[2:], stdout, stderr)
	case "report":
		return runOpsReport(args[2:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "sw-block: unknown ops subcommand %q\n", args[1])
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
}

func runOpsStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		volumeID        string
		masterAddr      string
		statusAddr      string
		outDir          string
		productRevision string
		runnerRevision  string
		timeout         time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id to inspect")
	fs.StringVar(&masterAddr, "master", "", "blockmaster gRPC address for read-only QueryVolumeStatus")
	fs.StringVar(&statusAddr, "status-addr", "", "blockvolume loopback status address or URL")
	fs.StringVar(&outDir, "out", "", "directory for volume-status-report.json, volume-status-summary.txt, and ops-status-bundle.json")
	fs.StringVar(&productRevision, "product-revision", "", "product revision label to include in the report")
	fs.StringVar(&runnerRevision, "runner-revision", "", "runner revision label to include in the report")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "collection timeout")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if volumeID == "" || outDir == "" {
		fmt.Fprintln(stderr, "sw-block ops status: --volume and --out are required")
		return ops.VolumeStatusExitInvalid
	}
	if masterAddr == "" || statusAddr == "" {
		fmt.Fprintln(stderr, "sw-block ops status: --master and --status-addr are both required for a clean live report")
		return ops.VolumeStatusExitInvalid
	}
	if productRevision == "" {
		productRevision = buildinfo.Version("sw-block")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	collector := ops.NewLiveVolumeStatusReportCollector(ops.LiveVolumeStatusConfig{
		VolumeID:        volumeID,
		MasterAddr:      masterAddr,
		StatusAddr:      statusAddr,
		ProductRevision: productRevision,
		RunnerRevision:  runnerRevision,
		RunCommand:      opsStatusRunCommand,
	})
	report, code, err := ops.WriteVolumeStatusArtifacts(ctx, outDir, collector)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops status: %v\n", err)
	}
	fmt.Fprint(stdout, ops.RenderVolumeStatusSummary(report))
	if code != ops.VolumeStatusExitInvalid {
		fmt.Fprintf(stdout, "artifacts: %s %s %s\n", ops.VolumeStatusReportArtifact, ops.VolumeStatusSummaryArtifact, ops.OpsStatusBundleArtifact)
	}
	return code
}

func runOpsInventory(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops inventory", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		namespace         string
		masterAddr        string
		outDir            string
		productRevision   string
		runnerRevision    string
		claimProfile      string
		requiredFrontiers requiredFrontierFlags
		timeout           time.Duration
	)
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace to inspect once live discovery is enabled")
	fs.StringVar(&masterAddr, "master", "", "optional blockmaster gRPC address; when set, collect per-replica ops status bundles for replicas with --status-addr")
	fs.StringVar(&outDir, "out", "", "directory for volume-inventory.json, volume-inventory-summary.txt, and ops-inventory-bundle.json")
	fs.StringVar(&productRevision, "product-revision", "", "product revision label to include in the inventory")
	fs.StringVar(&runnerRevision, "runner-revision", "", "runner revision label to include in the inventory")
	fs.StringVar(&claimProfile, "claim-profile", "", "promotion-readiness claim profile: beta-recovery (default), controlled-best-effort-demo, or stage2-iscsi-alua-multipath")
	fs.Var(&requiredFrontiers, "required-frontier", "mounted-writer required frontier as volume_id=lsn; repeat for multiple volumes")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "collection timeout")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if outDir == "" {
		fmt.Fprintln(stderr, "sw-block ops inventory: --out is required")
		return ops.VolumeStatusExitInvalid
	}
	if productRevision == "" {
		productRevision = buildinfo.Version("sw-block")
	}
	if !ops.PromotionClaimProfileAccepted(claimProfile) {
		fmt.Fprintf(stderr, "sw-block ops inventory: --claim-profile=%q invalid; want %q, %q, or %q\n", claimProfile, ops.PromotionClaimBetaRecovery, ops.PromotionClaimControlledBestEffortDemo, ops.PromotionClaimStage2ISCSIALUAMultipath)
		return ops.VolumeStatusExitInvalid
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	collector := ops.NewKubernetesVolumeInventoryCollector(ops.KubernetesInventoryConfig{
		Namespace:         namespace,
		MasterAddr:        masterAddr,
		StatusBundleRoot:  outDir,
		ProductRevision:   productRevision,
		RunnerRevision:    runnerRevision,
		ClaimProfile:      claimProfile,
		RequiredFrontiers: requiredFrontiers.values,
		RunCommand:        opsInventoryRunCommand,
	})
	inventory, code, err := ops.WriteVolumeInventoryArtifacts(ctx, outDir, collector)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops inventory: %v\n", err)
	}
	fmt.Fprint(stdout, ops.RenderVolumeInventorySummary(inventory))
	if code != ops.VolumeStatusExitInvalid {
		fmt.Fprintf(stdout, "artifacts: %s %s %s\n", ops.VolumeInventoryArtifact, ops.VolumeInventorySummaryArtifact, ops.OpsInventoryBundleArtifact)
	}
	return code
}

func runOpsCluster(args []string, stdout, stderr io.Writer) int {
	cluster, out, code := loadObservationCluster("sw-block ops cluster", args, stderr)
	if code != ops.VolumeStatusExitOK {
		return code
	}
	if out == "json" {
		raw, err := ops.MarshalObservationJSON(cluster)
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops cluster: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		_, _ = stdout.Write(raw)
		return ops.VolumeStatusExitOK
	}
	fmt.Fprint(stdout, ops.RenderClusterEvidenceText(cluster))
	return ops.VolumeStatusExitOK
}

func runOpsVolumes(args []string, stdout, stderr io.Writer) int {
	cluster, out, code := loadObservationCluster("sw-block ops volumes", args, stderr)
	if code != ops.VolumeStatusExitOK {
		return code
	}
	if out == "json" {
		raw, err := ops.MarshalObservationJSON(cluster.Volumes)
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops volumes: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		_, _ = stdout.Write(raw)
		return ops.VolumeStatusExitOK
	}
	for _, volume := range cluster.Volumes {
		fmt.Fprintf(stdout, "volume %s status=%s rf=%d primary=%s node=%s frontend=%s\n",
			emptyCLI(volume.VolumeID),
			emptyCLI(volume.Status),
			volume.ReplicationFactor,
			emptyCLI(volume.PrimaryReplica),
			emptyCLI(volume.PrimaryNode),
			emptyCLI(volume.PublishTarget))
	}
	if len(cluster.Volumes) == 0 {
		fmt.Fprintln(stdout, "volumes: none")
	}
	return ops.VolumeStatusExitOK
}

func runOpsDescribe(args []string, stdout, stderr io.Writer) int {
	cluster, out, code := loadObservationVolume("sw-block ops describe", args, stderr)
	if code != ops.VolumeStatusExitOK {
		return code
	}
	switch out {
	case "json":
		raw, err := ops.MarshalObservationJSON(cluster)
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops describe: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		_, _ = stdout.Write(raw)
	default:
		fmt.Fprint(stdout, ops.RenderClusterEvidenceText(cluster))
	}
	return ops.VolumeStatusExitOK
}

func loadObservationCluster(command string, args []string, stderr io.Writer) (ops.ClusterEvidence, string, int) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		fromBundle       string
		namespace        string
		masterAddr       string
		masterAPIAddr    string
		out              string
		outDir           string
		productRevision  string
		claimProfile     string
		requiredFrontier requiredFrontierFlags
		timeout          time.Duration
	)
	fs.StringVar(&fromBundle, "from-bundle", "", "existing inventory/support bundle directory to explain")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace for live read-only inventory")
	fs.StringVar(&masterAddr, "master", "", "optional blockmaster gRPC address for live per-replica status evidence")
	fs.StringVar(&masterAPIAddr, "master-api", "", "optional blockmaster gRPC address for ClusterEvidenceService read-only snapshot")
	fs.StringVar(&out, "o", "text", "output format: text or json")
	fs.StringVar(&outDir, "out", "", "optional directory for nested live status evidence")
	fs.StringVar(&productRevision, "product-revision", "", "product revision label for live evidence")
	fs.StringVar(&claimProfile, "claim-profile", "", "promotion-readiness claim profile for live evidence")
	fs.Var(&requiredFrontier, "required-frontier", "required frontier as volume_id=lsn; repeat for multiple volumes")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "live collection timeout")
	if err := fs.Parse(args); err != nil {
		return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "%s: unexpected args %s\n", command, strings.Join(fs.Args(), " "))
		return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
	}
	var (
		cluster ops.ClusterEvidence
		err     error
	)
	if fromBundle != "" {
		cluster, err = ops.BuildObservationFromBundle(ops.ObservationBundleOptions{Dir: fromBundle})
	} else if masterAPIAddr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cluster, err = readMasterClusterEvidence(ctx, masterAPIAddr)
	} else {
		if productRevision == "" {
			productRevision = buildinfo.Version("sw-block")
		}
		if !ops.PromotionClaimProfileAccepted(claimProfile) {
			fmt.Fprintf(stderr, "%s: --claim-profile=%q invalid; want %q, %q, or %q\n", command, claimProfile, ops.PromotionClaimBetaRecovery, ops.PromotionClaimControlledBestEffortDemo, ops.PromotionClaimStage2ISCSIALUAMultipath)
			return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		collector := ops.NewKubernetesVolumeInventoryCollector(ops.KubernetesInventoryConfig{
			Namespace:         namespace,
			MasterAddr:        masterAddr,
			StatusBundleRoot:  outDir,
			ProductRevision:   productRevision,
			ClaimProfile:      claimProfile,
			RequiredFrontiers: requiredFrontier.values,
			RunCommand:        opsInventoryRunCommand,
		})
		inventory, collectErr := collector.Collect(ctx)
		if collectErr != nil {
			inventory.CollectionErrors = append(inventory.CollectionErrors, strings.Split(collectErr.Error(), "\n")...)
		}
		cluster, err = ops.BuildObservationFromInventory(inventory, "", outDir)
	}
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", command, err)
		return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
	}
	return cluster, out, ops.VolumeStatusExitOK
}

func readMasterClusterEvidence(ctx context.Context, masterAddr string) (ops.ClusterEvidence, error) {
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return ops.ClusterEvidence{}, err
	}
	defer conn.Close()
	resp, err := control.NewClusterEvidenceServiceClient(conn).GetClusterStatus(ctx, &control.GetClusterStatusRequest{})
	if err != nil {
		return ops.ClusterEvidence{}, err
	}
	return clusterEvidenceFromWire(resp), nil
}

func clusterEvidenceFromWire(resp *control.ClusterStatusResponse) ops.ClusterEvidence {
	if resp == nil {
		return ops.ClusterEvidence{}
	}
	out := ops.ClusterEvidence{
		SchemaVersion:   resp.GetSchemaVersion(),
		ProductRevision: resp.GetProductRevision(),
		Status:          resp.GetStatus(),
		Conditions:      conditionsFromWire(resp.GetConditions()),
		NonClaims:       append([]string(nil), resp.GetNonClaims()...),
	}
	if ts := resp.GetCapturedAt(); ts != nil && ts.IsValid() {
		out.CapturedAt = ts.AsTime()
	}
	for _, node := range resp.GetNodes() {
		out.Nodes = append(out.Nodes, nodeEvidenceFromWire(node))
	}
	for _, volume := range resp.GetVolumes() {
		out.Volumes = append(out.Volumes, volumeEvidenceFromWire(volume))
	}
	for _, event := range resp.GetEvents() {
		out.Events = append(out.Events, clusterEventFromWire(event))
	}
	return out
}

func nodeEvidenceFromWire(node *control.NodeEvidence) ops.NodeEvidence {
	if node == nil {
		return ops.NodeEvidence{}
	}
	out := ops.NodeEvidence{
		NodeName:       node.GetNodeName(),
		KubernetesNode: node.GetKubernetesNode(),
		PhysicalHost:   node.GetPhysicalHost(),
		InternalIP:     node.GetInternalIp(),
		Schedulable:    node.GetSchedulable(),
		Ready:          node.GetReady(),
		ReplicaCount:   int(node.GetReplicaCount()),
		RequiredImages: append([]string(nil), node.GetRequiredImages()...),
		MissingImages:  append([]string(nil), node.GetMissingImages()...),
		Conditions:     conditionsFromWire(node.GetConditions()),
	}
	if ts := node.GetLastHeartbeatAt(); ts != nil && ts.IsValid() {
		out.LastHeartbeatAt = ts.AsTime()
	}
	return out
}

func volumeEvidenceFromWire(volume *control.VolumeEvidence) ops.VolumeEvidence {
	if volume == nil {
		return ops.VolumeEvidence{}
	}
	out := ops.VolumeEvidence{
		VolumeID:          volume.GetVolumeId(),
		Namespace:         volume.GetNamespace(),
		PVCName:           volume.GetPvcName(),
		PVName:            volume.GetPvName(),
		ReplicationFactor: int(volume.GetReplicationFactor()),
		AckProfile:        volume.GetAckProfile(),
		ClaimProfile:      volume.GetClaimProfile(),
		DesiredReplicas:   int(volume.GetDesiredReplicas()),
		ObservedReplicas:  int(volume.GetObservedReplicas()),
		Status:            volume.GetStatus(),
		Reason:            volume.GetReason(),
		PrimaryReplica:    volume.GetPrimaryReplica(),
		PrimaryNode:       volume.GetPrimaryNode(),
		PublishTarget:     volume.GetPublishTarget(),
		Epoch:             volume.GetEpoch(),
		EndpointVersion:   volume.GetEndpointVersion(),
		Conditions:        conditionsFromWire(volume.GetConditions()),
		NextActions:       append([]string(nil), volume.GetNextActions()...),
		SupportBundleHint: volume.GetSupportBundleHint(),
	}
	for _, replica := range volume.GetReplicas() {
		out.Replicas = append(out.Replicas, replicaEvidenceFromWire(replica))
	}
	return out
}

func replicaEvidenceFromWire(replica *control.ReplicaEvidence) ops.ReplicaEvidence {
	if replica == nil {
		return ops.ReplicaEvidence{}
	}
	return ops.ReplicaEvidence{
		ReplicaID:            replica.GetReplicaId(),
		ServerID:             replica.GetServerId(),
		KubernetesNode:       replica.GetKubernetesNode(),
		PhysicalHost:         replica.GetPhysicalHost(),
		Observed:             replica.GetObserved(),
		Role:                 replica.GetRole(),
		ReplicationRole:      replica.GetReplicationRole(),
		DurableLatched:       replica.GetDurableLatched(),
		DurableFrontierKnown: replica.GetDurableFrontierKnown(),
		DurableFrontierLSN:   replica.GetDurableFrontierLsn(),
		CandidateReady:       replica.GetCandidateReady(),
		CandidateReadyReason: replica.GetCandidateReadyReason(),
		FrontendProtocol:     replica.GetFrontendProtocol(),
		FrontendAddr:         replica.GetFrontendAddr(),
		StatusAddr:           replica.GetStatusAddr(),
		StalePrimaryFenced:   replica.GetStalePrimaryFenced(),
		Conditions:           conditionsFromWire(replica.GetConditions()),
		SupportBundlePath:    replica.GetSupportBundlePath(),
	}
}

func conditionsFromWire(in []*control.ObservationCondition) []ops.ObservationCondition {
	out := make([]ops.ObservationCondition, 0, len(in))
	for _, condition := range in {
		if condition == nil {
			continue
		}
		out = append(out, ops.ObservationCondition{
			Type:     condition.GetType(),
			Status:   condition.GetStatus(),
			Reason:   condition.GetReason(),
			Severity: condition.GetSeverity(),
			Message:  condition.GetMessage(),
		})
	}
	return out
}

func clusterEventFromWire(event *control.ClusterEvent) ops.ClusterEvent {
	if event == nil {
		return ops.ClusterEvent{}
	}
	out := ops.ClusterEvent{
		EventID:         event.GetEventId(),
		VolumeID:        event.GetVolumeId(),
		ReplicaID:       event.GetReplicaId(),
		NodeName:        event.GetNodeName(),
		Type:            event.GetEventType(),
		Severity:        event.GetSeverity(),
		Message:         event.GetMessage(),
		Reason:          event.GetReasonCode(),
		OldValue:        event.GetOldValue(),
		NewValue:        event.GetNewValue(),
		Epoch:           event.GetEpoch(),
		EndpointVersion: event.GetEndpointVersion(),
		CorrelationID:   event.GetCorrelationId(),
		EvidenceRef:     event.GetEvidenceRef(),
	}
	if ts := event.GetEventTime(); ts != nil && ts.IsValid() {
		out.EventTime = ts.AsTime()
	}
	return out
}

func runOpsTimeline(args []string, stdout, stderr io.Writer) int {
	cluster, out, code := loadObservationVolume("sw-block ops timeline", args, stderr)
	if code != ops.VolumeStatusExitOK {
		return code
	}
	switch out {
	case "jsonl":
		jsonl, err := ops.RenderClusterEventsJSONL(cluster.Events)
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops timeline: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		fmt.Fprint(stdout, jsonl)
	case "json":
		raw, err := ops.MarshalObservationJSON(cluster.Events)
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops timeline: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		_, _ = stdout.Write(raw)
	default:
		for _, event := range cluster.Events {
			fmt.Fprintf(stdout, "%s severity=%s reason=%s volume=%s replica=%s %s\n",
				event.Type,
				emptyCLI(event.Severity),
				emptyCLI(event.Reason),
				emptyCLI(event.VolumeID),
				emptyCLI(event.ReplicaID),
				event.Message)
		}
	}
	return ops.VolumeStatusExitOK
}

func runOpsExplain(args []string, stdout, stderr io.Writer) int {
	cluster, _, code := loadObservationVolume("sw-block ops explain", args, stderr)
	if code != ops.VolumeStatusExitOK {
		return code
	}
	fmt.Fprint(stdout, ops.RenderObservationExplainText(cluster))
	return ops.VolumeStatusExitOK
}

func runOpsReport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		fromBundle       string
		namespace        string
		masterAddr       string
		masterAPIAddr    string
		outDir           string
		evidenceOutDir   string
		productRevision  string
		claimProfile     string
		requiredFrontier requiredFrontierFlags
		timeout          time.Duration
	)
	fs.StringVar(&fromBundle, "from-bundle", "", "existing inventory/support bundle directory to render")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace for live read-only inventory")
	fs.StringVar(&masterAddr, "master", "", "optional blockmaster gRPC address for live per-replica status evidence")
	fs.StringVar(&masterAPIAddr, "master-api", "", "optional blockmaster gRPC address for ClusterEvidenceService read-only snapshot")
	fs.StringVar(&outDir, "out", "", "directory for index.html, cluster-evidence.json, timeline.jsonl, and summary.txt")
	fs.StringVar(&evidenceOutDir, "evidence-out", "", "optional directory for nested live status evidence")
	fs.StringVar(&productRevision, "product-revision", "", "product revision label for live evidence")
	fs.StringVar(&claimProfile, "claim-profile", "", "promotion-readiness claim profile for live evidence")
	fs.Var(&requiredFrontier, "required-frontier", "required frontier as volume_id=lsn; repeat for multiple volumes")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "live collection timeout")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops report: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	if outDir == "" {
		fmt.Fprintln(stderr, "sw-block ops report: --out is required")
		return ops.VolumeStatusExitInvalid
	}

	var (
		cluster ops.ClusterEvidence
		err     error
	)
	switch {
	case fromBundle != "":
		cluster, err = ops.BuildObservationFromBundle(ops.ObservationBundleOptions{Dir: fromBundle})
	case masterAPIAddr != "":
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cluster, err = readMasterClusterEvidence(ctx, masterAPIAddr)
	default:
		if productRevision == "" {
			productRevision = buildinfo.Version("sw-block")
		}
		if !ops.PromotionClaimProfileAccepted(claimProfile) {
			fmt.Fprintf(stderr, "sw-block ops report: --claim-profile=%q invalid; want %q, %q, or %q\n", claimProfile, ops.PromotionClaimBetaRecovery, ops.PromotionClaimControlledBestEffortDemo, ops.PromotionClaimStage2ISCSIALUAMultipath)
			return ops.VolumeStatusExitInvalid
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		collector := ops.NewKubernetesVolumeInventoryCollector(ops.KubernetesInventoryConfig{
			Namespace:         namespace,
			MasterAddr:        masterAddr,
			StatusBundleRoot:  evidenceOutDir,
			ProductRevision:   productRevision,
			ClaimProfile:      claimProfile,
			RequiredFrontiers: requiredFrontier.values,
			RunCommand:        opsInventoryRunCommand,
		})
		inventory, collectErr := collector.Collect(ctx)
		if collectErr != nil {
			inventory.CollectionErrors = append(inventory.CollectionErrors, strings.Split(collectErr.Error(), "\n")...)
		}
		cluster, err = ops.BuildObservationFromInventory(inventory, "", evidenceOutDir)
	}
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops report: %v\n", err)
		return ops.VolumeStatusExitInvalid
	}
	if err := ops.WriteObservationReportArtifacts(outDir, cluster); err != nil {
		fmt.Fprintf(stderr, "sw-block ops report: %v\n", err)
		return ops.VolumeStatusExitInvalid
	}
	fmt.Fprintf(stdout, "report_status=ok\n")
	fmt.Fprintf(stdout, "report_dir=%s\n", outDir)
	fmt.Fprintf(stdout, "html=%s\n", ops.ObservationReportHTMLArtifact)
	fmt.Fprintf(stdout, "cluster_evidence=%s\n", ops.ObservationReportJSONArtifact)
	fmt.Fprintf(stdout, "timeline=%s\n", ops.ObservationReportJSONLArtifact)
	fmt.Fprintf(stdout, "summary=%s\n", ops.ObservationReportTextArtifact)
	fmt.Fprintf(stdout, "read_only=true\n")
	return ops.VolumeStatusExitOK
}

func loadObservationVolume(command string, args []string, stderr io.Writer) (ops.ClusterEvidence, string, int) {
	if len(args) == 0 || args[0] != "volume" {
		fmt.Fprintf(stderr, "%s: expected volume [--from-bundle <dir>|--namespace <ns>] <volume-id>\n", command)
		return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
	}
	fs := flag.NewFlagSet(command+" volume", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		fromBundle       string
		namespace        string
		masterAddr       string
		out              string
		outDir           string
		productRevision  string
		claimProfile     string
		requiredFrontier requiredFrontierFlags
		timeout          time.Duration
	)
	fs.StringVar(&fromBundle, "from-bundle", "", "existing inventory/support bundle directory to explain")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace for live read-only inventory")
	fs.StringVar(&masterAddr, "master", "", "optional blockmaster gRPC address for live per-replica status evidence")
	fs.StringVar(&out, "o", "text", "output format: text, json, or jsonl where supported")
	fs.StringVar(&outDir, "out", "", "optional directory for nested live status evidence")
	fs.StringVar(&productRevision, "product-revision", "", "product revision label for live evidence")
	fs.StringVar(&claimProfile, "claim-profile", "", "promotion-readiness claim profile for live evidence")
	fs.Var(&requiredFrontier, "required-frontier", "required frontier as volume_id=lsn; repeat for multiple volumes")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "live collection timeout")
	if err := fs.Parse(normalizeObservationVolumeArgs(args[1:])); err != nil {
		return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
	}
	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(stderr, "%s: <volume-id> is required\n", command)
		return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
	}
	volumeID := remaining[0]
	var (
		cluster ops.ClusterEvidence
		err     error
	)
	if fromBundle != "" {
		cluster, err = ops.BuildObservationFromBundle(ops.ObservationBundleOptions{
			Dir:      fromBundle,
			VolumeID: volumeID,
		})
	} else {
		if productRevision == "" {
			productRevision = buildinfo.Version("sw-block")
		}
		if !ops.PromotionClaimProfileAccepted(claimProfile) {
			fmt.Fprintf(stderr, "%s: --claim-profile=%q invalid; want %q, %q, or %q\n", command, claimProfile, ops.PromotionClaimBetaRecovery, ops.PromotionClaimControlledBestEffortDemo, ops.PromotionClaimStage2ISCSIALUAMultipath)
			return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		collector := ops.NewKubernetesVolumeInventoryCollector(ops.KubernetesInventoryConfig{
			Namespace:         namespace,
			MasterAddr:        masterAddr,
			StatusBundleRoot:  outDir,
			ProductRevision:   productRevision,
			ClaimProfile:      claimProfile,
			RequiredFrontiers: requiredFrontier.values,
			RunCommand:        opsInventoryRunCommand,
		})
		inventory, collectErr := collector.Collect(ctx)
		if collectErr != nil {
			inventory.CollectionErrors = append(inventory.CollectionErrors, strings.Split(collectErr.Error(), "\n")...)
		}
		cluster, err = ops.BuildObservationFromInventory(inventory, volumeID, outDir)
	}
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", command, err)
		return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
	}
	return cluster, out, ops.VolumeStatusExitOK
}

func normalizeObservationVolumeArgs(args []string) []string {
	valueFlags := map[string]bool{
		"--from-bundle":       true,
		"--namespace":         true,
		"--master":            true,
		"--out":               true,
		"-o":                  true,
		"--product-revision":  true,
		"--claim-profile":     true,
		"--required-frontier": true,
		"--timeout":           true,
	}
	var volumeID string
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if valueFlags[arg] {
			out = append(out, arg)
			if i+1 < len(args) {
				i++
				out = append(out, args[i])
			}
			continue
		}
		if strings.HasPrefix(arg, "-") {
			out = append(out, arg)
			continue
		}
		if volumeID == "" {
			volumeID = arg
			continue
		}
		out = append(out, arg)
	}
	if volumeID != "" {
		out = append(out, volumeID)
	}
	return out
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  sw-block --version")
	fmt.Fprintln(w, "  sw-block ops status --volume <id> --master <addr> --status-addr <addr|url> --out <dir>")
	fmt.Fprintln(w, "  sw-block ops inventory --namespace <ns> [--master <addr>] --out <dir>")
	fmt.Fprintln(w, "  sw-block ops cluster --namespace <ns> [--master <addr>|--master-api <addr>] [-o json]")
	fmt.Fprintln(w, "  sw-block ops volumes --namespace <ns> [--master <addr>] [-o json]")
	fmt.Fprintln(w, "  sw-block ops describe volume --from-bundle <dir> <volume-id> [-o json]")
	fmt.Fprintln(w, "  sw-block ops describe volume <volume-id> --namespace <ns> [--master <addr>] [--out <dir>]")
	fmt.Fprintln(w, "  sw-block ops timeline volume --from-bundle <dir> <volume-id> [-o jsonl]")
	fmt.Fprintln(w, "  sw-block ops explain volume --from-bundle <dir> <volume-id>")
	fmt.Fprintln(w, "  sw-block ops explain volume <volume-id> --namespace <ns> [--master <addr>] [--out <dir>]")
	fmt.Fprintln(w, "  sw-block ops report --from-bundle <dir> --out <dir>")
	fmt.Fprintln(w, "  sw-block ops report --master-api <addr> --out <dir>")
}

func emptyCLI(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

type requiredFrontierFlags struct {
	values map[string]uint64
}

func (f *requiredFrontierFlags) String() string {
	if f == nil || len(f.values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(f.values))
	for volumeID, lsn := range f.values {
		parts = append(parts, fmt.Sprintf("%s=%d", volumeID, lsn))
	}
	return strings.Join(parts, ",")
}

func (f *requiredFrontierFlags) Set(raw string) error {
	volumeID, lsnText, ok := strings.Cut(raw, "=")
	volumeID = strings.TrimSpace(volumeID)
	lsnText = strings.TrimSpace(lsnText)
	if !ok || volumeID == "" || lsnText == "" {
		return fmt.Errorf("expected volume_id=lsn")
	}
	lsn, err := strconv.ParseUint(lsnText, 10, 64)
	if err != nil {
		return fmt.Errorf("parse required frontier lsn: %w", err)
	}
	if f.values == nil {
		f.values = map[string]uint64{}
	}
	f.values[volumeID] = lsn
	return nil
}
