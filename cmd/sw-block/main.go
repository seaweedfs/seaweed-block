package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/internal/buildinfo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

var (
	opsStatusRunCommand             = ops.DefaultRunCommand
	opsInventoryRunCommand          = ops.DefaultRunCommand
	opsGenerateHelmValuesRunCommand = ops.DefaultRunCommand
	opsNodeEvidenceEnricherFactory  = func() (ops.OperatorNodeEvidenceEnricher, error) {
		return ops.NewInClusterKubernetesStatusClient()
	}
	opsOperatorStatusWriterFactory = func() (ops.OperatorStatusWriter, error) {
		return ops.NewInClusterKubernetesStatusClient()
	}
	opsLifecycleOwnerClientFactory = func() (ops.LifecycleOwnerClient, ops.OperatorEventSink, error) {
		client, err := ops.NewInClusterKubernetesStatusClient()
		if err != nil {
			return nil, nil, err
		}
		client.EventComponent = "sw-block-lifecycle-owner"
		return client, client, nil
	}
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
		fmt.Fprintln(stderr, "sw-block: expected subcommand ops status|inventory|list|cluster|volumes|describe|timeline|explain|report|dashboard|generate-helm-values|operator-status|lifecycle-owner")
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
	case "dashboard":
		return runOpsDashboard(args[2:], stdout, stderr)
	case "generate-helm-values":
		return runOpsGenerateHelmValues(args[2:], stdout, stderr)
	case "operator-status":
		return runOpsOperatorStatus(args[2:], stdout, stderr)
	case "lifecycle-owner":
		return runOpsLifecycleOwner(args[2:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "sw-block: unknown ops subcommand %q\n", args[1])
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
}

func runOpsLifecycleOwner(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops lifecycle-owner", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dryRun    bool
		namespace string
		interval  time.Duration
	)
	fs.BoolVar(&dryRun, "dry-run", false, "evaluate lifecycle-owner reconciliation without patching finalizers or emitting Events")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace containing SwBlockVolume objects")
	fs.DurationVar(&interval, "interval", 0, "repeat lifecycle-owner reconciliation at this interval; 0 runs once")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops lifecycle-owner: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	runOnce := func() int {
		client, events, err := opsLifecycleOwnerClientFactory()
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops lifecycle-owner: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		mode := "finalizer_mutation"
		if dryRun {
			mode = "dry_run"
			events = nil
		}
		result, err := (ops.LifecycleOwnerReconciler{
			Namespace: namespace,
			Client:    client,
			EventSink: events,
			DryRun:    dryRun,
		}).Reconcile(context.Background())
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops lifecycle-owner: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		fmt.Fprintf(stdout, "lifecycle_owner=%s namespace=%s volumes=%d finalizer_patches=%d finalizer_added=%d finalizer_held=%d finalizer_released=%d events=%d mutation_allowed=%t\n",
			mode,
			namespace,
			result.VolumeCount,
			result.FinalizerPatchCount,
			result.FinalizerAddedCount,
			result.FinalizerHeldCount,
			result.FinalizerReleasedCount,
			result.EventCount,
			!dryRun)
		return ops.VolumeStatusExitOK
	}
	if interval <= 0 {
		return runOnce()
	}
	for {
		code := runOnce()
		if code != ops.VolumeStatusExitOK {
			fmt.Fprintf(stderr, "sw-block ops lifecycle-owner: iteration failed exit=%d; retrying in %s\n", code, interval)
		}
		time.Sleep(interval)
	}
}

func runOpsOperatorStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops operator-status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dryRun          bool
		fromBundle      string
		cleanupSummary  string
		namespace       string
		masterAddr      string
		masterAPIAddr   string
		outDir          string
		productRevision string
		claimProfile    string
		clusterName     string
		timeout         time.Duration
		interval        time.Duration
	)
	fs.BoolVar(&dryRun, "dry-run", false, "render the status projection without writing Kubernetes CRD status")
	fs.StringVar(&fromBundle, "from-bundle", "", "existing inventory/support bundle directory to project")
	fs.StringVar(&cleanupSummary, "cleanup-summary", "", "cleanup-summary.txt evidence to project into delete-safety status")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace for live read-only inventory")
	fs.StringVar(&masterAddr, "master", "", "optional blockmaster gRPC address for live per-replica status evidence")
	fs.StringVar(&masterAPIAddr, "master-api", "", "optional blockmaster gRPC address for ClusterEvidenceService read-only snapshot")
	fs.StringVar(&outDir, "out", "", "optional directory for nested live status evidence")
	fs.StringVar(&productRevision, "product-revision", "", "product revision label for live evidence")
	fs.StringVar(&claimProfile, "claim-profile", "", "promotion-readiness claim profile for live evidence")
	fs.StringVar(&clusterName, "cluster-name", ops.DefaultSwBlockClusterName, "SwBlockCluster object name")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "live collection timeout")
	fs.DurationVar(&interval, "interval", 0, "repeat dry-run projection at this interval; 0 runs once")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops operator-status: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	runOnce := func() int {
		clusterArgs := []string{"--namespace", namespace, "--timeout", timeout.String()}
		if fromBundle != "" {
			clusterArgs = append(clusterArgs, "--from-bundle", fromBundle)
		}
		if masterAddr != "" {
			clusterArgs = append(clusterArgs, "--master", masterAddr)
		}
		if masterAPIAddr != "" {
			clusterArgs = append(clusterArgs, "--master-api", masterAPIAddr)
		}
		if outDir != "" {
			clusterArgs = append(clusterArgs, "--out", outDir)
		}
		if productRevision != "" {
			clusterArgs = append(clusterArgs, "--product-revision", productRevision)
		}
		if claimProfile != "" {
			clusterArgs = append(clusterArgs, "--claim-profile", claimProfile)
		}
		cluster, _, code := loadObservationCluster("sw-block ops operator-status", clusterArgs, stderr)
		if code != ops.VolumeStatusExitOK {
			return code
		}
		if cleanupSummary != "" {
			cleanup, err := ops.LoadCleanupEvidenceSummary(cleanupSummary)
			if err != nil {
				fmt.Fprintf(stderr, "sw-block ops operator-status: %v\n", err)
				return ops.VolumeStatusExitInvalid
			}
			cluster.Cleanup = cleanup
		}
		mode := "write_status"
		var writer ops.OperatorStatusWriter
		var events ops.OperatorEventSink
		var volumes ops.OperatorSwBlockVolumeSource
		if dryRun {
			mode = "dry_run"
			writer = &operatorStatusDryRunWriter{}
			events = &operatorStatusDryRunEventSink{}
		} else {
			var err error
			writer, err = opsOperatorStatusWriterFactory()
			if err != nil {
				fmt.Fprintf(stderr, "sw-block ops operator-status: %v\n", err)
				return ops.VolumeStatusExitInvalid
			}
			events, _ = writer.(ops.OperatorEventSink)
			volumes, _ = writer.(ops.OperatorSwBlockVolumeSource)
		}
		result, err := (ops.OperatorStatusReconciler{
			Namespace:   namespace,
			ClusterName: clusterName,
			Source:      operatorStatusClusterSource{cluster: cluster},
			Writer:      writer,
			Volumes:     volumes,
			EventSink:   events,
		}).Reconcile(context.Background())
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops operator-status: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		fmt.Fprintf(stdout, "operator_status=%s cluster=%s/%s volumes=%d events=%d finalizer_patches=%d mutation_allowed=false\n",
			mode,
			result.ClusterRef.Namespace,
			result.ClusterRef.Name,
			len(result.VolumeRefs),
			result.EventCount,
			result.FinalizerPatchCount)
		if dryWriter, ok := writer.(*operatorStatusDryRunWriter); ok {
			fmt.Fprintf(stdout, "cluster_status volumes=%d ready=%d blocked=%d stale=%d\n",
				dryWriter.cluster.VolumeCount,
				dryWriter.cluster.ReadyVolumeCount,
				dryWriter.cluster.BlockedVolumeCount,
				dryWriter.cluster.StaleVolumeCount)
			for _, volume := range dryWriter.volumes {
				fmt.Fprintf(stdout, "volume_status name=%s volume_id=%s pvc=%s status=%s reason=%s\n",
					volume.ref.Name,
					emptyCLI(volume.status.VolumeID),
					emptyCLI(volume.status.PVCName),
					emptyCLI(volume.status.Status),
					emptyCLI(volume.status.ReasonCode))
			}
		}
		return ops.VolumeStatusExitOK
	}
	if interval <= 0 {
		return runOnce()
	}
	for {
		code := runOnce()
		if code != ops.VolumeStatusExitOK {
			fmt.Fprintf(stderr, "sw-block ops operator-status: iteration failed exit=%d; retrying in %s\n", code, interval)
		}
		time.Sleep(interval)
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
	cluster, err = enrichLiveObservationCluster(namespace, timeout, fromBundle == "", cluster)
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", command, err)
		return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
	}
	return ops.NormalizeObservationCluster(cluster), out, ops.VolumeStatusExitOK
}

func enrichLiveObservationCluster(namespace string, timeout time.Duration, live bool, cluster ops.ClusterEvidence) (ops.ClusterEvidence, error) {
	if !live || os.Getenv("KUBERNETES_SERVICE_HOST") == "" {
		return cluster, nil
	}
	enricher, err := opsNodeEvidenceEnricherFactory()
	if err != nil {
		return cluster, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cluster, err = enricher.EnrichNodeEvidence(ctx, liveNodeEvidenceNamespace(namespace), cluster)
	if err != nil {
		return cluster, fmt.Errorf("enrich node evidence: %w", err)
	}
	return cluster, nil
}

func liveNodeEvidenceNamespace(namespace string) string {
	if namespace := strings.TrimSpace(os.Getenv("SW_BLOCK_HELM_NAMESPACE")); namespace != "" {
		return namespace
	}
	if namespace = strings.TrimSpace(namespace); namespace != "" && namespace != "default" {
		return namespace
	}
	return "kube-system"
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
	cluster, err = enrichLiveObservationCluster(namespace, timeout, fromBundle == "", cluster)
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
	fmt.Fprintf(stdout, "operator_snapshot=%s\n", ops.ObservationOperatorSnapshotArtifact)
	fmt.Fprintf(stdout, "summary=%s\n", ops.ObservationReportTextArtifact)
	fmt.Fprintf(stdout, "read_only=true\n")
	return ops.VolumeStatusExitOK
}

func runOpsDashboard(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops dashboard", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		fromBundle       string
		namespace        string
		masterAddr       string
		masterAPIAddr    string
		evidenceOutDir   string
		productRevision  string
		claimProfile     string
		listenAddr       string
		requiredFrontier requiredFrontierFlags
		timeout          time.Duration
		serveDuration    time.Duration
	)
	fs.StringVar(&fromBundle, "from-bundle", "", "existing inventory/support bundle directory to serve")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace for live read-only inventory")
	fs.StringVar(&masterAddr, "master", "", "optional blockmaster gRPC address for live per-replica status evidence")
	fs.StringVar(&masterAPIAddr, "master-api", "", "optional blockmaster gRPC address for ClusterEvidenceService read-only snapshot")
	fs.StringVar(&evidenceOutDir, "evidence-out", "", "optional directory for nested live status evidence")
	fs.StringVar(&productRevision, "product-revision", "", "product revision label for live evidence")
	fs.StringVar(&claimProfile, "claim-profile", "", "promotion-readiness claim profile for live evidence")
	fs.StringVar(&listenAddr, "listen", "127.0.0.1:9334", "dashboard listen address; keep loopback for alpha")
	fs.Var(&requiredFrontier, "required-frontier", "required frontier as volume_id=lsn; repeat for multiple volumes")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "live collection timeout")
	fs.DurationVar(&serveDuration, "serve-duration", 0, "optional test duration; 0 serves until interrupted")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops dashboard: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}

	cluster, err := loadDashboardCluster(dashboardClusterOptions{
		FromBundle:        fromBundle,
		Namespace:         namespace,
		MasterAddr:        masterAddr,
		MasterAPIAddr:     masterAPIAddr,
		EvidenceOutDir:    evidenceOutDir,
		ProductRevision:   productRevision,
		ClaimProfile:      claimProfile,
		RequiredFrontiers: requiredFrontier.values,
		Timeout:           timeout,
	})
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops dashboard: %v\n", err)
		return ops.VolumeStatusExitInvalid
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops dashboard: listen %s: %v\n", listenAddr, err)
		return ops.VolumeStatusExitInvalid
	}
	defer ln.Close()

	server := &http.Server{
		Handler:           ops.NewObservationDashboardHandler(cluster),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(ln)
	}()
	fmt.Fprintf(stdout, "dashboard_status=ok\n")
	fmt.Fprintf(stdout, "url=http://%s/\n", ln.Addr().String())
	fmt.Fprintf(stdout, "cluster_evidence=%s\n", ops.ObservationReportJSONArtifact)
	fmt.Fprintf(stdout, "timeline=%s\n", ops.ObservationReportJSONLArtifact)
	fmt.Fprintf(stdout, "operator_snapshot=%s\n", ops.ObservationOperatorSnapshotArtifact)
	fmt.Fprintf(stdout, "summary=%s\n", ops.ObservationReportTextArtifact)
	fmt.Fprintf(stdout, "read_only=true\n")

	if serveDuration > 0 {
		timer := time.NewTimer(serveDuration)
		defer timer.Stop()
		select {
		case err := <-errCh:
			if err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(stderr, "sw-block ops dashboard: %v\n", err)
				return ops.VolumeStatusExitInvalid
			}
			return ops.VolumeStatusExitOK
		case <-timer.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				fmt.Fprintf(stderr, "sw-block ops dashboard: shutdown: %v\n", err)
				return ops.VolumeStatusExitInvalid
			}
			if err := <-errCh; err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(stderr, "sw-block ops dashboard: %v\n", err)
				return ops.VolumeStatusExitInvalid
			}
			return ops.VolumeStatusExitOK
		}
	}

	if err := <-errCh; err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(stderr, "sw-block ops dashboard: %v\n", err)
		return ops.VolumeStatusExitInvalid
	}
	return ops.VolumeStatusExitOK
}

type dashboardClusterOptions struct {
	FromBundle        string
	Namespace         string
	MasterAddr        string
	MasterAPIAddr     string
	EvidenceOutDir    string
	ProductRevision   string
	ClaimProfile      string
	RequiredFrontiers map[string]uint64
	Timeout           time.Duration
}

func loadDashboardCluster(options dashboardClusterOptions) (ops.ClusterEvidence, error) {
	var (
		cluster ops.ClusterEvidence
		err     error
		live    bool
	)
	switch {
	case options.FromBundle != "":
		cluster, err = ops.BuildObservationFromBundle(ops.ObservationBundleOptions{Dir: options.FromBundle})
	case options.MasterAPIAddr != "":
		ctx, cancel := context.WithTimeout(context.Background(), options.Timeout)
		defer cancel()
		cluster, err = readMasterClusterEvidence(ctx, options.MasterAPIAddr)
		live = true
	default:
		productRevision := options.ProductRevision
		if productRevision == "" {
			productRevision = buildinfo.Version("sw-block")
		}
		if !ops.PromotionClaimProfileAccepted(options.ClaimProfile) {
			return ops.ClusterEvidence{}, fmt.Errorf("--claim-profile=%q invalid; want %q, %q, or %q", options.ClaimProfile, ops.PromotionClaimBetaRecovery, ops.PromotionClaimControlledBestEffortDemo, ops.PromotionClaimStage2ISCSIALUAMultipath)
		}
		ctx, cancel := context.WithTimeout(context.Background(), options.Timeout)
		defer cancel()
		collector := ops.NewKubernetesVolumeInventoryCollector(ops.KubernetesInventoryConfig{
			Namespace:         options.Namespace,
			MasterAddr:        options.MasterAddr,
			StatusBundleRoot:  options.EvidenceOutDir,
			ProductRevision:   productRevision,
			ClaimProfile:      options.ClaimProfile,
			RequiredFrontiers: options.RequiredFrontiers,
			RunCommand:        opsInventoryRunCommand,
		})
		inventory, collectErr := collector.Collect(ctx)
		if collectErr != nil {
			inventory.CollectionErrors = append(inventory.CollectionErrors, strings.Split(collectErr.Error(), "\n")...)
		}
		cluster, err = ops.BuildObservationFromInventory(inventory, "", options.EvidenceOutDir)
		live = true
	}
	if err != nil {
		return ops.ClusterEvidence{}, err
	}
	return enrichLiveObservationCluster(options.Namespace, options.Timeout, live, cluster)
}

type helmValuesFile struct {
	Image           helmValuesImage        `yaml:"image"`
	CSIImage        helmValuesImage        `yaml:"csiImage"`
	AppNamespace    string                 `yaml:"appNamespace"`
	Blockmaster     *helmValuesBlockmaster `yaml:"blockmaster,omitempty"`
	StorageClass    helmValuesStorageClass `yaml:"storageClass"`
	Replication     helmValuesReplication  `yaml:"replication"`
	Network         helmValuesNetwork      `yaml:"network"`
	Restart         *helmValuesRestart     `yaml:"restartPersistence,omitempty"`
	Compat          helmValuesCompat       `yaml:"compat"`
	CHAP            helmValuesCHAP         `yaml:"chap"`
	Stage2Multipath helmValuesEnabled      `yaml:"stage2Multipath"`
	BlockNodes      []helmValuesBlockNode  `yaml:"blockNodes"`
}

type helmValuesImage struct {
	Repository string `yaml:"repository"`
	Tag        string `yaml:"tag"`
	Digest     string `yaml:"digest"`
}

type helmValuesBlockmaster struct {
	StateHostPath string `yaml:"stateHostPath,omitempty"`
}

type helmValuesRestart struct {
	Mode          string `yaml:"mode,omitempty"`
	StateHostPath string `yaml:"stateHostPath,omitempty"`
}

type helmValuesStorageClass struct {
	Create            bool   `yaml:"create"`
	Name              string `yaml:"name"`
	ReplicationFactor int    `yaml:"replicationFactor"`
	Protocol          string `yaml:"protocol"`
}

type helmValuesReplication struct {
	AckProfile             string `yaml:"ackProfile"`
	ExpectedSlotsPerVolume int    `yaml:"expectedSlotsPerVolume"`
}

type helmValuesNetwork struct {
	ExternalISCSI                bool `yaml:"externalISCSI"`
	ExternalStatus               bool `yaml:"externalStatus"`
	RejectLoopbackPublishTargets bool `yaml:"rejectLoopbackPublishTargets"`
}

type helmValuesCompat struct {
	LauncherDurableImplFlag    bool `yaml:"launcherDurableImplFlag"`
	LauncherReplicationAckFlag bool `yaml:"launcherReplicationAckFlag"`
	LauncherRejectLoopbackFlag bool `yaml:"launcherRejectLoopbackFlag"`
}

type helmValuesCHAP struct {
	Enabled    bool   `yaml:"enabled"`
	Create     bool   `yaml:"create"`
	SecretName string `yaml:"secretName"`
	Username   string `yaml:"username"`
	Secret     string `yaml:"secret"`
}

type helmValuesEnabled struct {
	Enabled bool `yaml:"enabled"`
}

type helmValuesBlockNode struct {
	Name           string `yaml:"name"`
	KubernetesNode string `yaml:"kubernetesNode"`
	InternalIP     string `yaml:"internalIP"`
	DataPort       int    `yaml:"dataPort"`
	ControlPort    int    `yaml:"controlPort"`
	Pool           string `yaml:"pool"`
}

type kubernetesReadyNode struct {
	Name       string
	InternalIP string
}

func runOpsGenerateHelmValues(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops generate-helm-values", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		outPath            string
		kubeconfig         string
		image              string
		csiImage           string
		replicationFactor  int
		ackProfile         string
		storageClass       string
		appNamespace       string
		targetNode         string
		nodeLimit          int
		chapSecretName     string
		chapUsername       string
		chapSecret         string
		stage2Multipath    bool
		restartPersistence string
		stateHostPath      string
		timeout            time.Duration
	)
	fs.StringVar(&outPath, "out", "", "output Helm values.yaml path")
	fs.StringVar(&outPath, "o", "", "output Helm values.yaml path")
	fs.StringVar(&kubeconfig, "kubeconfig", "", "optional kubeconfig path passed to kubectl")
	fs.StringVar(&image, "image", "ghcr.io/seaweedfs/seaweed-block:alpha", "sw-block image reference")
	fs.StringVar(&csiImage, "csi-image", "ghcr.io/seaweedfs/seaweed-block-csi:alpha", "sw-block CSI image reference")
	fs.IntVar(&replicationFactor, "replication-factor", 1, "storage class replication factor")
	fs.StringVar(&ackProfile, "ack-profile", "best-effort", "replication ACK profile: best-effort, sync-quorum, or sync-all")
	fs.StringVar(&storageClass, "storageclass", "sw-block-dynamic", "StorageClass name")
	fs.StringVar(&appNamespace, "app-namespace", "default", "default application namespace")
	fs.StringVar(&targetNode, "target-node", "", "optional Kubernetes node name to select for single-node values")
	fs.IntVar(&nodeLimit, "node-limit", 0, "optional maximum selected Ready node count")
	fs.StringVar(&chapSecretName, "chap-secret-name", "sw-block-iscsi-chap", "iSCSI CHAP Secret name")
	fs.StringVar(&chapUsername, "chap-username", "sw-block", "iSCSI CHAP username")
	fs.StringVar(&chapSecret, "chap-secret", "", "iSCSI CHAP shared secret; generated when needed and omitted")
	fs.BoolVar(&stage2Multipath, "stage2-multipath", false, "enable Stage 2 multipath chart values")
	fs.StringVar(&restartPersistence, "restart-persistence", "ephemeral", "restart persistence mode: ephemeral or hostpath")
	fs.StringVar(&stateHostPath, "state-hostpath", "/var/lib/sw-block", "hostPath base used when --restart-persistence=hostpath")
	fs.DurationVar(&timeout, "timeout", 10*time.Second, "kubectl discovery timeout")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	if outPath == "" {
		fmt.Fprintln(stderr, "sw-block ops generate-helm-values: --out is required")
		return ops.VolumeStatusExitInvalid
	}
	if replicationFactor < 1 {
		fmt.Fprintln(stderr, "sw-block ops generate-helm-values: --replication-factor must be >= 1")
		return ops.VolumeStatusExitInvalid
	}
	if !helmValuesAckProfileAccepted(ackProfile) {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: --ack-profile=%q invalid; want best-effort, sync-quorum, or sync-all\n", ackProfile)
		return ops.VolumeStatusExitInvalid
	}
	if !helmValuesRestartPersistenceAccepted(restartPersistence) {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: --restart-persistence=%q invalid; want ephemeral or hostpath\n", restartPersistence)
		return ops.VolumeStatusExitInvalid
	}
	if restartPersistence == "hostpath" && strings.TrimSpace(stateHostPath) == "" {
		fmt.Fprintln(stderr, "sw-block ops generate-helm-values: --state-hostpath is required when --restart-persistence=hostpath")
		return ops.VolumeStatusExitInvalid
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	kubectlArgs := []string{}
	if kubeconfig != "" {
		kubectlArgs = append(kubectlArgs, "--kubeconfig", kubeconfig)
	}
	kubectlArgs = append(kubectlArgs, "get", "nodes", "-o", "wide", "--no-headers")
	rawNodes, err := opsGenerateHelmValuesRunCommand(ctx, "kubectl", kubectlArgs...)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: kubectl node discovery failed: %v\n%s", err, rawNodes)
		return ops.VolumeStatusExitInvalid
	}
	discovered, selected, err := selectHelmValuesNodes(string(rawNodes), targetNode, nodeLimit)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: %v\n", err)
		return ops.VolumeStatusExitInvalid
	}
	if replicationFactor > len(selected) {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: --replication-factor=%d requires at least %d selected Ready nodes; selected=%d\n", replicationFactor, replicationFactor, len(selected))
		return ops.VolumeStatusExitInvalid
	}

	multiNode := len(selected) > 1
	if multiNode && chapSecret == "" {
		chapSecret = generateHelmValuesSecret()
	}
	values := helmValuesFile{
		Image:        parseHelmValuesImage(image),
		CSIImage:     parseHelmValuesImage(csiImage),
		AppNamespace: appNamespace,
		StorageClass: helmValuesStorageClass{
			Create:            true,
			Name:              storageClass,
			ReplicationFactor: replicationFactor,
			Protocol:          "iscsi",
		},
		Replication: helmValuesReplication{
			AckProfile:             ackProfile,
			ExpectedSlotsPerVolume: replicationFactor,
		},
		Network: helmValuesNetwork{
			ExternalISCSI:                multiNode,
			ExternalStatus:               multiNode,
			RejectLoopbackPublishTargets: multiNode,
		},
		Compat: helmValuesCompat{
			LauncherRejectLoopbackFlag: false,
		},
		CHAP: helmValuesCHAP{
			Enabled:    multiNode,
			Create:     multiNode,
			SecretName: chapSecretName,
			Username:   chapUsername,
			Secret:     chapSecret,
		},
		Stage2Multipath: helmValuesEnabled{Enabled: stage2Multipath},
		BlockNodes:      make([]helmValuesBlockNode, 0, len(selected)),
	}
	values.Restart = &helmValuesRestart{Mode: restartPersistence}
	if restartPersistence == "hostpath" {
		values.Blockmaster = &helmValuesBlockmaster{StateHostPath: stateHostPath}
		values.Restart.StateHostPath = stateHostPath
	}
	for i, node := range selected {
		ip := node.InternalIP
		if !multiNode {
			ip = "127.0.0.1"
		}
		values.BlockNodes = append(values.BlockNodes, helmValuesBlockNode{
			Name:           node.Name,
			KubernetesNode: node.Name,
			InternalIP:     ip,
			DataPort:       19101 + (i * 2),
			ControlPort:    19102 + (i * 2),
			Pool:           "default",
		})
	}
	rawValues, err := yaml.Marshal(values)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: marshal values: %v\n", err)
		return ops.VolumeStatusExitInvalid
	}
	if dir := filepath.Dir(outPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintf(stderr, "sw-block ops generate-helm-values: create output dir: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
	}
	if err := os.WriteFile(outPath, rawValues, 0o600); err != nil {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: write %s: %v\n", outPath, err)
		return ops.VolumeStatusExitInvalid
	}

	networkMode := "loopback"
	if multiNode {
		networkMode = "external-iscsi"
	}
	fmt.Fprintln(stdout, "helm_values_status=ok")
	fmt.Fprintf(stdout, "values_file=%s\n", outPath)
	fmt.Fprintf(stdout, "network_mode=%s\n", networkMode)
	fmt.Fprintf(stdout, "ready_kubernetes_nodes=%d\n", len(selected))
	fmt.Fprintf(stdout, "discovered_kubernetes_nodes=%d\n", len(discovered))
	fmt.Fprintf(stdout, "target_node=%s\n", emptyCLI(targetNode))
	fmt.Fprintf(stdout, "node_limit=%s\n", emptyCLI(strconv.Itoa(nodeLimit)))
	fmt.Fprintf(stdout, "external_iscsi=%t\n", multiNode)
	fmt.Fprintf(stdout, "chap_enabled=%t\n", multiNode)
	fmt.Fprintf(stdout, "replication_factor=%d\n", replicationFactor)
	fmt.Fprintf(stdout, "ack_profile=%s\n", ackProfile)
	fmt.Fprintf(stdout, "restart_persistence_mode=%s\n", restartPersistence)
	if restartPersistence == "hostpath" {
		fmt.Fprintf(stdout, "state_hostpath=%s\n", stateHostPath)
	}
	return ops.VolumeStatusExitOK
}

func helmValuesAckProfileAccepted(value string) bool {
	switch value {
	case "best-effort", "sync-quorum", "sync-all":
		return true
	default:
		return false
	}
}

func helmValuesRestartPersistenceAccepted(value string) bool {
	switch value {
	case "ephemeral", "hostpath":
		return true
	default:
		return false
	}
}

func selectHelmValuesNodes(raw, targetNode string, nodeLimit int) ([]kubernetesReadyNode, []kubernetesReadyNode, error) {
	discovered := parseKubectlWideReadyNodes(raw)
	if len(discovered) == 0 {
		return nil, nil, fmt.Errorf("no Ready schedulable Kubernetes nodes with non-loopback InternalIP found")
	}
	selected := make([]kubernetesReadyNode, 0, len(discovered))
	for _, node := range discovered {
		if targetNode != "" && node.Name != targetNode {
			continue
		}
		selected = append(selected, node)
	}
	if targetNode != "" && len(selected) == 0 {
		return discovered, nil, fmt.Errorf("--target-node=%q is not a Ready schedulable node with non-loopback InternalIP", targetNode)
	}
	if nodeLimit > 0 && len(selected) > nodeLimit {
		selected = selected[:nodeLimit]
	}
	if len(selected) == 0 {
		return discovered, nil, fmt.Errorf("no Kubernetes nodes selected")
	}
	return discovered, selected, nil
}

func parseKubectlWideReadyNodes(raw string) []kubernetesReadyNode {
	var nodes []kubernetesReadyNode
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		status := fields[1]
		internalIP := fields[5]
		if !kubectlNodeStatusReadySchedulable(status) || helmValuesIPUnsafe(internalIP) {
			continue
		}
		nodes = append(nodes, kubernetesReadyNode{
			Name:       fields[0],
			InternalIP: internalIP,
		})
	}
	return nodes
}

func kubectlNodeStatusReadySchedulable(status string) bool {
	ready := false
	for _, part := range strings.Split(status, ",") {
		switch strings.TrimSpace(part) {
		case "Ready":
			ready = true
		case "SchedulingDisabled":
			return false
		}
	}
	return ready
}

func helmValuesIPUnsafe(ip string) bool {
	return ip == "" || ip == "<none>" || ip == "0.0.0.0" || ip == "::1" || strings.HasPrefix(ip, "127.")
}

func parseHelmValuesImage(ref string) helmValuesImage {
	repository := ref
	tag := "latest"
	digest := ""
	if before, after, ok := strings.Cut(ref, "@"); ok {
		repository = before
		digest = after
	}
	lastSlash := strings.LastIndex(repository, "/")
	lastColon := strings.LastIndex(repository, ":")
	if lastColon > lastSlash {
		tag = repository[lastColon+1:]
		repository = repository[:lastColon]
	}
	return helmValuesImage{Repository: repository, Tag: tag, Digest: digest}
}

func generateHelmValuesSecret() string {
	var b [18]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("sw-block-%d", time.Now().UnixNano())
	}
	return "sw-block-" + hex.EncodeToString(b[:])
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
	cluster, err = enrichLiveObservationCluster(namespace, timeout, fromBundle == "", cluster)
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
	fmt.Fprintln(w, "  sw-block ops dashboard --from-bundle <dir> [--listen 127.0.0.1:9334]")
	fmt.Fprintln(w, "  sw-block ops dashboard --master-api <addr> [--listen 127.0.0.1:9334]")
	fmt.Fprintln(w, "  sw-block ops generate-helm-values --out values.yaml [--target-node <node>] [--replication-factor <n>]")
	fmt.Fprintln(w, "      [--restart-persistence ephemeral|hostpath] [--state-hostpath /var/lib/sw-block] [--timeout 10s]")
	fmt.Fprintln(w, "  sw-block ops operator-status --dry-run [--master-api <addr>|--from-bundle <dir>] [--cleanup-summary <file>] [--interval 30s]")
	fmt.Fprintln(w, "  sw-block ops lifecycle-owner [--dry-run] [--namespace <ns>] [--interval 30s]")
}

func emptyCLI(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

type operatorStatusClusterSource struct {
	cluster ops.ClusterEvidence
}

func (s operatorStatusClusterSource) ClusterEvidence(context.Context) (ops.ClusterEvidence, error) {
	return s.cluster, nil
}

type operatorStatusDryRunWriter struct {
	cluster ops.SwBlockClusterCRDStatus
	volumes []operatorStatusDryRunVolumeWrite
}

type operatorStatusDryRunVolumeWrite struct {
	ref    ops.OperatorObjectRef
	status ops.SwBlockVolumeCRDStatus
}

func (w *operatorStatusDryRunWriter) WriteClusterStatus(_ context.Context, _ ops.OperatorObjectRef, status ops.SwBlockClusterCRDStatus) error {
	w.cluster = status
	return nil
}

func (w *operatorStatusDryRunWriter) WriteVolumeStatus(_ context.Context, ref ops.OperatorObjectRef, status ops.SwBlockVolumeCRDStatus) error {
	w.volumes = append(w.volumes, operatorStatusDryRunVolumeWrite{ref: ref, status: status})
	return nil
}

type operatorStatusDryRunEventSink struct {
	events []ops.OperatorKubernetesEvent
}

func (s *operatorStatusDryRunEventSink) EmitEvent(_ context.Context, event ops.OperatorKubernetesEvent) error {
	s.events = append(s.events, event)
	return nil
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
