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
	opsSwBlockVolumeSourceFactory = func() (ops.OperatorSwBlockVolumeSource, error) {
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
	opsAuthorityExecutorClientFactory = func() (ops.AuthorityExecutorClient, error) {
		return ops.NewInClusterKubernetesStatusClient()
	}
	opsRebuildTargetOwnerClientFactory = func() (ops.RebuildTargetOwnerClient, error) {
		return ops.NewInClusterKubernetesStatusClient()
	}
	opsFailbackTargetOwnerClientFactory = func() (ops.FailbackTargetOwnerClient, error) {
		return ops.NewInClusterKubernetesStatusClient()
	}
	opsFrontendPublicationTargetOwnerClientFactory = func() (ops.FrontendPublicationTargetOwnerClient, error) {
		return ops.NewInClusterKubernetesStatusClient()
	}
	opsFrontendPublicationExecutorClientFactory = func() (ops.FrontendPublicationExecutorClient, error) {
		return ops.NewInClusterKubernetesStatusClient()
	}
	opsFailbackExecutorClientFactory = func() (ops.FailbackExecutorClient, error) {
		return ops.NewInClusterKubernetesStatusClient()
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
		fmt.Fprintln(stderr, "sw-block: expected subcommand ops status|inventory|list|cluster|volumes|describe|timeline|explain|report|dashboard|generate-helm-values|operator-status|lifecycle-owner|authority-executor|rebuild-target-owner|failback-target-owner|failback-executor|frontend-publication-target-owner|frontend-publication-executor")
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
	case "authority-executor":
		return runOpsAuthorityExecutor(args[2:], stdout, stderr)
	case "rebuild-target-owner":
		return runOpsRebuildTargetOwner(args[2:], stdout, stderr)
	case "failback-target-owner":
		return runOpsFailbackTargetOwner(args[2:], stdout, stderr)
	case "failback-executor":
		return runOpsFailbackExecutor(args[2:], stdout, stderr)
	case "frontend-publication-target-owner":
		return runOpsFrontendPublicationTargetOwner(args[2:], stdout, stderr)
	case "frontend-publication-executor":
		return runOpsFrontendPublicationExecutor(args[2:], stdout, stderr)
	case "snapshot-backup":
		return runOpsSnapshotBackup(args[2:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "sw-block: unknown ops subcommand %q\n", args[1])
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
}

func runOpsFrontendPublicationExecutor(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops frontend-publication-executor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dryRun                 bool
		namespace              string
		enableExecution        bool
		executionPolicyEnabled bool
		runtimeURL             string
		interval               time.Duration
	)
	fs.BoolVar(&dryRun, "dry-run", false, "evaluate frontend publication targets without writing status")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace containing SwBlockFrontendPublication objects")
	fs.BoolVar(&enableExecution, "enable-execution", false, "request frontend publication runtime execution for enabled SwBlockFrontendPublication targets")
	fs.BoolVar(&executionPolicyEnabled, "execution-policy", false, "allow frontend publication runtime execution; must be set with --enable-execution")
	fs.StringVar(&runtimeURL, "frontend-publication-runtime-url", "", "HTTP runtime endpoint for frontend publication execution")
	fs.DurationVar(&interval, "interval", 0, "repeat frontend-publication-executor reconciliation at this interval; 0 runs once")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops frontend-publication-executor: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	if strings.TrimSpace(runtimeURL) != "" && !enableExecution {
		fmt.Fprintln(stderr, "sw-block ops frontend-publication-executor: --frontend-publication-runtime-url requires --enable-execution")
		return ops.VolumeStatusExitInvalid
	}
	runOnce := func() int {
		client, err := opsFrontendPublicationExecutorClientFactory()
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops frontend-publication-executor: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		result, err := (ops.FrontendPublicationExecutorReconciler{
			Namespace:              namespace,
			Client:                 client,
			DryRun:                 dryRun,
			ExecutionRequested:     enableExecution,
			ExecutionPolicyEnabled: executionPolicyEnabled,
			Runtime:                frontendPublicationRuntimeFromURL(runtimeURL),
		}).Reconcile(context.Background())
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops frontend-publication-executor: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		mode := "write_status"
		if dryRun {
			mode = "dry_run"
		}
		statusMutationAllowed := !dryRun && result.StatusWriteCount > 0
		fmt.Fprintf(stdout, "frontend_publication_executor=%s namespace=%s targets=%d status_writes=%d invalid_targets=%d frontend_publication_attempts=%d failback_attempts=%d status_mutation_allowed=%t frontend_publication_mutation_allowed=false mutation_allowed=%t storage_mutation_allowed=%t\n",
			mode,
			namespace,
			result.TargetCount,
			result.StatusWriteCount,
			result.InvalidTargetCount,
			result.FrontendPublicationAttempts,
			result.FailbackAttempts,
			statusMutationAllowed,
			statusMutationAllowed,
			result.StorageMutationAllowed)
		return ops.VolumeStatusExitOK
	}
	if interval <= 0 {
		return runOnce()
	}
	for {
		code := runOnce()
		if code != ops.VolumeStatusExitOK {
			fmt.Fprintf(stderr, "sw-block ops frontend-publication-executor: iteration failed exit=%d; retrying in %s\n", code, interval)
		}
		time.Sleep(interval)
	}
}

func frontendPublicationRuntimeFromURL(runtimeURL string) ops.FrontendPublicationRuntime {
	if strings.TrimSpace(runtimeURL) == "" {
		return nil
	}
	return ops.NewHTTPFrontendPublicationRuntime(runtimeURL, nil)
}

func runOpsFailbackExecutor(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops failback-executor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dryRun                 bool
		namespace              string
		enableExecution        bool
		executionPolicyEnabled bool
		failbackRuntimeURL     string
		failbackRuntimeGRPC    string
		interval               time.Duration
	)
	fs.BoolVar(&dryRun, "dry-run", false, "evaluate failback targets without writing SwBlockReplicaFailback status")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace containing SwBlockReplicaFailback objects")
	fs.BoolVar(&enableExecution, "enable-execution", false, "request failback runtime execution")
	fs.BoolVar(&executionPolicyEnabled, "execution-policy", false, "allow failback executor to evaluate execution; default is disabled")
	fs.StringVar(&failbackRuntimeURL, "failback-runtime-url", "", "HTTP endpoint for failback runtime execution; empty uses target runtimeEndpoint when execution is enabled")
	fs.StringVar(&failbackRuntimeGRPC, "failback-runtime-grpc-addr", "", "gRPC blockmaster FailbackService address for failback runtime execution")
	fs.DurationVar(&interval, "interval", 0, "repeat failback-executor reconciliation at this interval; 0 runs once")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops failback-executor: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	if strings.TrimSpace(failbackRuntimeURL) != "" && !enableExecution {
		fmt.Fprintf(stderr, "sw-block ops failback-executor: --failback-runtime-url requires --enable-execution reason=unsupported_runtime_without_execution failback_attempts=0\n")
		return ops.VolumeStatusExitInvalid
	}
	if strings.TrimSpace(failbackRuntimeGRPC) != "" && !enableExecution {
		fmt.Fprintf(stderr, "sw-block ops failback-executor: --failback-runtime-grpc-addr requires --enable-execution reason=unsupported_runtime_without_execution failback_attempts=0\n")
		return ops.VolumeStatusExitInvalid
	}
	if strings.TrimSpace(failbackRuntimeURL) != "" && strings.TrimSpace(failbackRuntimeGRPC) != "" {
		fmt.Fprintf(stderr, "sw-block ops failback-executor: --failback-runtime-url and --failback-runtime-grpc-addr are mutually exclusive reason=ambiguous_runtime failback_attempts=0\n")
		return ops.VolumeStatusExitInvalid
	}
	if enableExecution && !executionPolicyEnabled {
		fmt.Fprintf(stderr, "sw-block ops failback-executor: failback execution is disabled by product policy reason=%s failback_attempts=0 authority_mutation_allowed=false frontend_publication_allowed=false storage_mutation_allowed=false\n", ops.AuthorityExecutorFailbackReasonDisabled)
		return ops.VolumeStatusExitInvalid
	}
	runOnce := func() int {
		client, err := opsFailbackExecutorClientFactory()
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops failback-executor: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		var runtime ops.FailbackRuntime
		if strings.TrimSpace(failbackRuntimeURL) != "" {
			runtime = ops.NewHTTPFailbackRuntime(failbackRuntimeURL, nil)
		} else if strings.TrimSpace(failbackRuntimeGRPC) != "" {
			runtime = ops.NewGRPCFailbackRuntime(failbackRuntimeGRPC)
		}
		result, err := (ops.FailbackExecutorReconciler{
			Namespace:              namespace,
			Client:                 client,
			Runtime:                runtime,
			DryRun:                 dryRun,
			ExecutionRequested:     enableExecution,
			ExecutionPolicyEnabled: executionPolicyEnabled,
		}).Reconcile(context.Background())
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops failback-executor: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		mode := "write_status"
		if dryRun {
			mode = "dry_run"
		}
		fmt.Fprintf(stdout, "failback_executor=%s namespace=%s targets=%d status_writes=%d invalid_targets=%d failback_attempts=%d execution_requested=%t execution_policy_enabled=%t status_mutation_allowed=%t authority_mutation_allowed=%t frontend_publication_allowed=%t mutation_allowed=%t storage_mutation_allowed=%t\n",
			mode,
			namespace,
			result.TargetCount,
			result.StatusWriteCount,
			result.InvalidTargetCount,
			result.FailbackAttempts,
			enableExecution,
			executionPolicyEnabled,
			!dryRun && result.StatusWriteCount > 0,
			result.AuthorityMutationAllowed,
			result.FrontendPublicationAllowed,
			!dryRun && result.StatusWriteCount > 0,
			result.StorageMutationAllowed)
		return ops.VolumeStatusExitOK
	}
	if interval <= 0 {
		return runOnce()
	}
	for {
		code := runOnce()
		if code != ops.VolumeStatusExitOK {
			fmt.Fprintf(stderr, "sw-block ops failback-executor: iteration failed exit=%d; retrying in %s\n", code, interval)
		}
		time.Sleep(interval)
	}
}

func runOpsFrontendPublicationTargetOwner(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops frontend-publication-target-owner", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dryRun                  bool
		namespace               string
		activateTargets         bool
		activationPolicyEnabled bool
		runtimeEndpoint         string
		interval                time.Duration
	)
	fs.BoolVar(&dryRun, "dry-run", false, "evaluate frontend publication target planning without creating SwBlockFrontendPublication objects")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace containing SwBlockReplicaEligibility and SwBlockReplicaFailback objects")
	fs.BoolVar(&activateTargets, "activate-targets", false, "create enabled frontend publication targets for explicit execution")
	fs.BoolVar(&activationPolicyEnabled, "activation-policy", false, "allow frontend publication target activation; must be set with --activate-targets")
	fs.StringVar(&runtimeEndpoint, "runtime-endpoint", "", "frontend publication runtime endpoint to stamp on activated targets")
	fs.DurationVar(&interval, "interval", 0, "repeat frontend-publication-target-owner reconciliation at this interval; 0 runs once")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops frontend-publication-target-owner: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	runOnce := func() int {
		client, err := opsFrontendPublicationTargetOwnerClientFactory()
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops frontend-publication-target-owner: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		result, err := (ops.FrontendPublicationTargetOwnerReconciler{
			Namespace:               namespace,
			Client:                  client,
			DryRun:                  dryRun,
			ActivateTargets:         activateTargets,
			ActivationPolicyEnabled: activationPolicyEnabled,
			RuntimeEndpoint:         runtimeEndpoint,
		}).Reconcile(context.Background())
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops frontend-publication-target-owner: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		mode := "target_mutation"
		if dryRun {
			mode = "dry_run"
		}
		fmt.Fprintf(stdout, "frontend_publication_target_owner=%s namespace=%s eligibilities=%d ready_eligibilities=%d failbacks=%d terminal_failbacks=%d targets_planned=%d targets_existing=%d targets_created=%d invalid_eligibilities=%d invalid_failbacks=%d frontend_publication_attempts=%d failback_attempts=%d mutation_allowed=%t storage_mutation_allowed=%t\n",
			mode,
			namespace,
			result.EligibilityCount,
			result.ReadyEligibilityCount,
			result.FailbackCount,
			result.TerminalFailbackCount,
			result.TargetPlannedCount,
			result.TargetExistingCount,
			result.TargetCreateCount,
			result.InvalidEligibilityCount,
			result.InvalidFailbackCount,
			result.FrontendPublicationAttempts,
			result.FailbackAttempts,
			!dryRun && result.TargetCreateCount > 0,
			result.StorageMutationAllowed)
		return ops.VolumeStatusExitOK
	}
	if interval <= 0 {
		return runOnce()
	}
	for {
		code := runOnce()
		if code != ops.VolumeStatusExitOK {
			fmt.Fprintf(stderr, "sw-block ops frontend-publication-target-owner: iteration failed exit=%d; retrying in %s\n", code, interval)
		}
		time.Sleep(interval)
	}
}

func runOpsRebuildTargetOwner(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops rebuild-target-owner", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dryRun    bool
		namespace string
		interval  time.Duration
	)
	fs.BoolVar(&dryRun, "dry-run", false, "evaluate rebuild target planning without creating SwBlockReplicaRebuild objects")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace containing SwBlockVolume objects")
	fs.DurationVar(&interval, "interval", 0, "repeat rebuild-target-owner reconciliation at this interval; 0 runs once")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops rebuild-target-owner: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	runOnce := func() int {
		client, err := opsRebuildTargetOwnerClientFactory()
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops rebuild-target-owner: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		result, err := (ops.RebuildTargetOwnerReconciler{
			Namespace: namespace,
			Client:    client,
			DryRun:    dryRun,
		}).Reconcile(context.Background())
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops rebuild-target-owner: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		mode := "target_mutation"
		if dryRun {
			mode = "dry_run"
		}
		fmt.Fprintf(stdout, "rebuild_target_owner=%s namespace=%s volumes=%d contracts=%d targets_planned=%d targets_existing=%d targets_created=%d invalid_contracts=%d runtime_target_ready=%d runtime_target_missing=%d mutation_allowed=%t storage_mutation_allowed=false frontend_publication_allowed=false failback_allowed=false\n",
			mode,
			namespace,
			result.VolumeCount,
			result.ContractCount,
			result.TargetPlannedCount,
			result.TargetExistingCount,
			result.TargetCreateCount,
			result.InvalidContractCount,
			result.RuntimeTargetReadyCount,
			result.RuntimeTargetMissingCount,
			!dryRun && result.TargetCreateCount > 0)
		return ops.VolumeStatusExitOK
	}
	if interval <= 0 {
		return runOnce()
	}
	for {
		code := runOnce()
		if code != ops.VolumeStatusExitOK {
			fmt.Fprintf(stderr, "sw-block ops rebuild-target-owner: iteration failed exit=%d; retrying in %s\n", code, interval)
		}
		time.Sleep(interval)
	}
}

func runOpsFailbackTargetOwner(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops failback-target-owner", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dryRun                  bool
		namespace               string
		interval                time.Duration
		activateTargets         bool
		activationPolicyEnabled bool
		runtimeEndpoint         string
	)
	fs.BoolVar(&dryRun, "dry-run", false, "evaluate failback target planning without creating SwBlockReplicaFailback objects")
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace containing SwBlockVolume objects")
	fs.DurationVar(&interval, "interval", 0, "repeat failback-target-owner reconciliation at this interval; 0 runs once")
	fs.BoolVar(&activateTargets, "activate-targets", false, "create failback targets with failbackDecision=enabled")
	fs.BoolVar(&activationPolicyEnabled, "activation-policy", false, "allow failback target activation when --activate-targets is set")
	fs.StringVar(&runtimeEndpoint, "runtime-endpoint", "", "failback runtime endpoint to stamp on activated targets")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops failback-target-owner: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	runOnce := func() int {
		client, err := opsFailbackTargetOwnerClientFactory()
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops failback-target-owner: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		result, err := (ops.FailbackTargetOwnerReconciler{
			Namespace:               namespace,
			Client:                  client,
			DryRun:                  dryRun,
			ActivateTargets:         activateTargets,
			ActivationPolicyEnabled: activationPolicyEnabled,
			RuntimeEndpoint:         runtimeEndpoint,
		}).Reconcile(context.Background())
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops failback-target-owner: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		mode := "target_mutation"
		if dryRun {
			mode = "dry_run"
		}
		fmt.Fprintf(stdout, "failback_target_owner=%s namespace=%s volumes=%d contracts=%d targets_planned=%d targets_existing=%d targets_created=%d invalid_contracts=%d authority_facts_missing=%d terminal_evidence_ready=%d terminal_evidence_missing=%d activate_targets=%t activation_policy=%t failback_attempts=%d mutation_allowed=%t storage_mutation_allowed=%t frontend_publication_allowed=%t\n",
			mode,
			namespace,
			result.VolumeCount,
			result.ContractCount,
			result.TargetPlannedCount,
			result.TargetExistingCount,
			result.TargetCreateCount,
			result.InvalidContractCount,
			result.AuthorityFactsMissing,
			result.TerminalEvidenceReady,
			result.TerminalEvidenceMissing,
			activateTargets,
			activationPolicyEnabled,
			result.FailbackAttempts,
			!dryRun && result.TargetCreateCount > 0,
			result.StorageMutationAllowed,
			result.FrontendPublicationAllowed)
		return ops.VolumeStatusExitOK
	}
	if interval <= 0 {
		return runOnce()
	}
	for {
		code := runOnce()
		if code != ops.VolumeStatusExitOK {
			fmt.Fprintf(stderr, "sw-block ops failback-target-owner: iteration failed exit=%d; retrying in %s\n", code, interval)
		}
		time.Sleep(interval)
	}
}

func runOpsAuthorityExecutor(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops authority-executor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		namespace              string
		enableExecution        bool
		executionPolicyEnabled bool
		allowedMutationClass   string
		rebuildRuntimeURL      string
		interval               time.Duration
	)
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace containing SwBlockVolume objects")
	fs.BoolVar(&enableExecution, "enable-execution", false, "request returned-replica executor mutation")
	fs.BoolVar(&executionPolicyEnabled, "execution-policy", false, "allow authority executor to evaluate execution; still blocked until ACK mutation target exists")
	fs.StringVar(&allowedMutationClass, "allowed-mutation-class", ops.AuthorityExecutorAllowedMutationAckEligibility, "supported values: ack_eligibility, rebuild_traffic")
	fs.StringVar(&rebuildRuntimeURL, "rebuild-runtime-url", "", "HTTP endpoint for rebuild_traffic runtime execution; empty preserves planned-only status")
	fs.DurationVar(&interval, "interval", 0, "repeat authority-executor reconciliation at this interval; 0 runs once")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "sw-block ops authority-executor: unexpected args %s\n", strings.Join(fs.Args(), " "))
		return ops.VolumeStatusExitInvalid
	}
	if allowedMutationClass != ops.AuthorityExecutorAllowedMutationAckEligibility && allowedMutationClass != ops.AuthorityExecutorAllowedMutationRebuildTraffic {
		fmt.Fprintf(stderr, "sw-block ops authority-executor: unsupported mutation class %q reason=unsupported_mutation_class mutation_attempts=0 ack_eligibility_mutation_attempts=0 rebuild_progress_mutation_attempts=0\n", allowedMutationClass)
		return ops.VolumeStatusExitInvalid
	}
	if strings.TrimSpace(rebuildRuntimeURL) != "" && allowedMutationClass != ops.AuthorityExecutorAllowedMutationRebuildTraffic {
		fmt.Fprintf(stderr, "sw-block ops authority-executor: --rebuild-runtime-url requires --allowed-mutation-class rebuild_traffic reason=unsupported_runtime_mutation_class mutation_attempts=0 ack_eligibility_mutation_attempts=0 rebuild_progress_mutation_attempts=0\n")
		return ops.VolumeStatusExitInvalid
	}
	if enableExecution && !executionPolicyEnabled {
		fmt.Fprintf(stderr, "sw-block ops authority-executor: returned-replica execution is disabled by product policy reason=%s mutation_attempts=0 ack_eligibility_mutation_attempts=0 rebuild_progress_mutation_attempts=0\n", ops.AuthorityExecutorBlockedPolicyDisabled)
		return ops.VolumeStatusExitInvalid
	}
	runOnce := func() int {
		client, err := opsAuthorityExecutorClientFactory()
		if err != nil {
			fmt.Fprintf(stderr, "sw-block ops authority-executor: %v\n", err)
			return ops.VolumeStatusExitInvalid
		}
		var rebuildRuntime ops.AuthorityRebuildRuntime
		if strings.TrimSpace(rebuildRuntimeURL) != "" {
			rebuildRuntime = ops.NewHTTPAuthorityRebuildRuntime(rebuildRuntimeURL, nil)
		}
		result, err := (ops.AuthorityExecutorReconciler{
			Namespace:              namespace,
			Client:                 client,
			RebuildRuntime:         rebuildRuntime,
			ExecutionRequested:     enableExecution,
			ExecutionPolicyEnabled: executionPolicyEnabled,
			AllowedMutationClass:   allowedMutationClass,
		}).Reconcile(context.Background())
		if err != nil {
			if result.BlockedReason != "" {
				fmt.Fprintf(stderr, "sw-block ops authority-executor: %v reason=%s mutation_attempts=%d ack_eligibility_mutation_attempts=%d rebuild_progress_mutation_attempts=%d\n",
					err, result.BlockedReason, result.MutationAttemptCount, result.AckEligibilityMutationAttempts, result.RebuildProgressMutationAttempts)
			} else {
				fmt.Fprintf(stderr, "sw-block ops authority-executor: %v\n", err)
			}
			return ops.VolumeStatusExitInvalid
		}
		status := "disabled"
		if enableExecution && result.MutationAttemptCount > 0 && result.BlockedReason != "" {
			status = "partial"
		} else if enableExecution && result.MutationAttemptCount > 0 {
			status = "executed"
		} else if enableExecution && result.BlockedReason != "" {
			status = "blocked"
		}
		fmt.Fprintf(stdout, "authority_executor=%s namespace=%s volumes=%d contracts=%d disabled_contracts=%d blocked_contracts=%d terminal_evidence_required=%d terminal_evidence_missing=%d ack_eligibility_target_missing=%d rebuild_target_missing=%d allowed_mutation_class=%s execution_requested=%t execution_policy_enabled=%t mutation_attempts=%d ack_eligibility_mutation_attempts=%d rebuild_progress_mutation_attempts=%d mutation_allowed=%t storage_mutation_allowed=false\n",
			status,
			namespace,
			result.VolumeCount,
			result.ContractCount,
			result.DisabledContractCount,
			result.BlockedContractCount,
			result.TerminalEvidenceRequiredCount,
			result.TerminalEvidenceMissingCount,
			result.AckEligibilityTargetMissingCount,
			result.RebuildTargetMissingCount,
			allowedMutationClass,
			enableExecution,
			executionPolicyEnabled,
			result.MutationAttemptCount,
			result.AckEligibilityMutationAttempts,
			result.RebuildProgressMutationAttempts,
			result.MutationAttemptCount > 0)
		return ops.VolumeStatusExitOK
	}
	if interval <= 0 {
		return runOnce()
	}
	for {
		code := runOnce()
		if code != ops.VolumeStatusExitOK {
			fmt.Fprintf(stderr, "sw-block ops authority-executor: iteration failed exit=%d; retrying in %s\n", code, interval)
		}
		time.Sleep(interval)
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
		cleanupSummary   string
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
	fs.StringVar(&cleanupSummary, "cleanup-summary", "", "cleanup-summary.txt evidence to project into delete-safety status")
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
	cluster, err = applyCleanupSummaryProjection(namespace, cleanupSummary, cluster)
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

func applyCleanupSummaryProjection(namespace, cleanupSummary string, cluster ops.ClusterEvidence) (ops.ClusterEvidence, error) {
	if cleanupSummary == "" {
		return cluster, nil
	}
	cleanup, err := ops.LoadCleanupEvidenceSummary(cleanupSummary)
	if err != nil {
		return ops.ClusterEvidence{}, err
	}
	cluster.Cleanup = cleanup
	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" {
		return cluster, nil
	}
	client, err := opsSwBlockVolumeSourceFactory()
	if err != nil {
		return ops.ClusterEvidence{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listNamespace := strings.TrimSpace(namespace)
	if listNamespace == "" {
		listNamespace = "default"
	}
	volumes, err := client.ListSwBlockVolumes(ctx, listNamespace)
	if err != nil {
		return ops.ClusterEvidence{}, err
	}
	return ops.ProjectSwBlockVolumeDeleteSafety(cluster, volumes), nil
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
		NodeName:             node.GetNodeName(),
		KubernetesNode:       node.GetKubernetesNode(),
		PhysicalHost:         node.GetPhysicalHost(),
		InternalIP:           node.GetInternalIp(),
		FrontendIP:           node.GetFrontendIp(),
		FrontendNetworkClass: node.GetFrontendNetworkClass(),
		Schedulable:          node.GetSchedulable(),
		Ready:                node.GetReady(),
		ReplicaCount:         int(node.GetReplicaCount()),
		RequiredImages:       append([]string(nil), node.GetRequiredImages()...),
		MissingImages:        append([]string(nil), node.GetMissingImages()...),
		Conditions:           conditionsFromWire(node.GetConditions()),
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
		FrontendTransport:    replica.GetFrontendTransport(),
		FrontendAddr:         replica.GetFrontendAddr(),
		FrontendNQN:          replica.GetFrontendNqn(),
		FrontendNSID:         replica.GetFrontendNsid(),
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
		cleanupSummary   string
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
	fs.StringVar(&cleanupSummary, "cleanup-summary", "", "cleanup-summary.txt evidence to project into delete-safety status")
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
	cluster, err = applyCleanupSummaryProjection(namespace, cleanupSummary, cluster)
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
		cleanupSummary   string
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
	fs.StringVar(&cleanupSummary, "cleanup-summary", "", "cleanup-summary.txt evidence to project into delete-safety status")
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
		CleanupSummary:    cleanupSummary,
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
	CleanupSummary    string
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
	cluster, err = applyCleanupSummaryProjection(options.Namespace, options.CleanupSummary, cluster)
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
	NVMeTransport     string `yaml:"nvmeTransport"`
}

type helmValuesReplication struct {
	AckProfile             string `yaml:"ackProfile"`
	ExpectedSlotsPerVolume int    `yaml:"expectedSlotsPerVolume"`
}

type helmValuesNetwork struct {
	ExternalISCSI                bool `yaml:"externalISCSI"`
	ExternalNVMe                 bool `yaml:"externalNVMe"`
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
	Name                 string `yaml:"name"`
	KubernetesNode       string `yaml:"kubernetesNode"`
	InternalIP           string `yaml:"internalIP"`
	ManagementIP         string `yaml:"managementIP,omitempty"`
	FrontendIP           string `yaml:"frontendIP,omitempty"`
	FrontendNetworkClass string `yaml:"frontendNetworkClass,omitempty"`
	DataPort             int    `yaml:"dataPort"`
	ControlPort          int    `yaml:"controlPort"`
	Pool                 string `yaml:"pool"`
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
		protocol           string
		nvmeTransport      string
		appNamespace       string
		targetNode         string
		nodeLimit          int
		chapSecretName     string
		chapUsername       string
		chapSecret         string
		stage2Multipath    bool
		restartPersistence string
		stateHostPath      string
		frontendIPMapRaw   string
		frontendClass      string
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
	fs.StringVar(&protocol, "protocol", "iscsi", "StorageClass frontend protocol: iscsi or nvme")
	fs.StringVar(&nvmeTransport, "nvme-transport", "tcp", "NVMe fabric transport: tcp or rdma")
	fs.StringVar(&appNamespace, "app-namespace", "default", "default application namespace")
	fs.StringVar(&targetNode, "target-node", "", "optional Kubernetes node name to select for single-node values")
	fs.IntVar(&nodeLimit, "node-limit", 0, "optional maximum selected Ready node count")
	fs.StringVar(&chapSecretName, "chap-secret-name", "sw-block-iscsi-chap", "iSCSI CHAP Secret name")
	fs.StringVar(&chapUsername, "chap-username", "sw-block", "iSCSI CHAP username")
	fs.StringVar(&chapSecret, "chap-secret", "", "iSCSI CHAP shared secret; generated when needed and omitted")
	fs.BoolVar(&stage2Multipath, "stage2-multipath", false, "enable Stage 2 multipath chart values")
	fs.StringVar(&restartPersistence, "restart-persistence", "ephemeral", "restart persistence mode: ephemeral or hostpath")
	fs.StringVar(&stateHostPath, "state-hostpath", "/var/lib/sw-block", "hostPath base used when --restart-persistence=hostpath")
	fs.StringVar(&frontendIPMapRaw, "frontend-ip-map", "", "optional comma-separated Kubernetes node to frontend/data-plane IP map, for example m01=10.0.0.181,m02=10.0.0.184")
	fs.StringVar(&frontendClass, "frontend-network-class", "", "network class for --frontend-ip-map values: management_lan, 100gbe_tcp, or 100gbe_roce")
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
	if !helmValuesProtocolAccepted(protocol) {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: --protocol=%q invalid; want iscsi or nvme\n", protocol)
		return ops.VolumeStatusExitInvalid
	}
	if !helmValuesNVMeTransportAccepted(nvmeTransport) {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: --nvme-transport=%q invalid; want tcp or rdma\n", nvmeTransport)
		return ops.VolumeStatusExitInvalid
	}
	if protocol != "nvme" && nvmeTransport != "tcp" {
		fmt.Fprintln(stderr, "sw-block ops generate-helm-values: --nvme-transport=rdma requires --protocol=nvme")
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
	frontendIPMap, err := parseHelmValuesNodeIPMap(frontendIPMapRaw)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops generate-helm-values: %v\n", err)
		return ops.VolumeStatusExitInvalid
	}
	if len(frontendIPMap) > 0 {
		if strings.TrimSpace(frontendClass) == "" {
			fmt.Fprintln(stderr, "sw-block ops generate-helm-values: --frontend-network-class is required when --frontend-ip-map is set")
			return ops.VolumeStatusExitInvalid
		}
		if !helmValuesFrontendNetworkClassAccepted(frontendClass) {
			fmt.Fprintf(stderr, "sw-block ops generate-helm-values: --frontend-network-class=%q invalid; want management_lan, 100gbe_tcp, or 100gbe_roce\n", frontendClass)
			return ops.VolumeStatusExitInvalid
		}
	} else if strings.TrimSpace(frontendClass) != "" {
		fmt.Fprintln(stderr, "sw-block ops generate-helm-values: --frontend-network-class requires --frontend-ip-map")
		return ops.VolumeStatusExitInvalid
	}
	if nvmeTransport == "rdma" && (len(frontendIPMap) == 0 || frontendClass != "100gbe_roce") {
		fmt.Fprintln(stderr, "sw-block ops generate-helm-values: NVMe/RDMA requires --frontend-ip-map and --frontend-network-class=100gbe_roce")
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
	externalISCSI := multiNode && protocol == "iscsi"
	externalNVMe := protocol == "nvme" && (multiNode || nvmeTransport == "rdma")
	if externalISCSI && chapSecret == "" {
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
			Protocol:          protocol,
			NVMeTransport:     nvmeTransport,
		},
		Replication: helmValuesReplication{
			AckProfile:             ackProfile,
			ExpectedSlotsPerVolume: replicationFactor,
		},
		Network: helmValuesNetwork{
			ExternalISCSI:                externalISCSI,
			ExternalNVMe:                 externalNVMe,
			ExternalStatus:               multiNode || nvmeTransport == "rdma",
			RejectLoopbackPublishTargets: multiNode || nvmeTransport == "rdma",
		},
		Compat: helmValuesCompat{
			LauncherRejectLoopbackFlag: false,
		},
		CHAP: helmValuesCHAP{
			Enabled:    externalISCSI,
			Create:     externalISCSI,
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
		frontendIP := ""
		if len(frontendIPMap) > 0 {
			var ok bool
			frontendIP, ok = frontendIPMap[node.Name]
			if !ok {
				fmt.Fprintf(stderr, "sw-block ops generate-helm-values: --frontend-ip-map missing selected node %q\n", node.Name)
				return ops.VolumeStatusExitInvalid
			}
		}
		values.BlockNodes = append(values.BlockNodes, helmValuesBlockNode{
			Name:                 node.Name,
			KubernetesNode:       node.Name,
			InternalIP:           ip,
			ManagementIP:         node.InternalIP,
			FrontendIP:           frontendIP,
			FrontendNetworkClass: frontendClass,
			DataPort:             19101 + (i * 2),
			ControlPort:          19102 + (i * 2),
			Pool:                 "default",
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
	if externalISCSI {
		networkMode = "external-iscsi"
	} else if externalNVMe {
		networkMode = "external-nvme"
	}
	fmt.Fprintln(stdout, "helm_values_status=ok")
	fmt.Fprintf(stdout, "values_file=%s\n", outPath)
	fmt.Fprintf(stdout, "network_mode=%s\n", networkMode)
	fmt.Fprintf(stdout, "ready_kubernetes_nodes=%d\n", len(selected))
	fmt.Fprintf(stdout, "discovered_kubernetes_nodes=%d\n", len(discovered))
	fmt.Fprintf(stdout, "target_node=%s\n", emptyCLI(targetNode))
	fmt.Fprintf(stdout, "node_limit=%s\n", emptyCLI(strconv.Itoa(nodeLimit)))
	fmt.Fprintf(stdout, "external_iscsi=%t\n", externalISCSI)
	fmt.Fprintf(stdout, "external_nvme=%t\n", externalNVMe)
	fmt.Fprintf(stdout, "frontend_ip_map=%s\n", emptyCLI(frontendIPMapRaw))
	fmt.Fprintf(stdout, "frontend_network_class=%s\n", emptyCLI(frontendClass))
	fmt.Fprintf(stdout, "chap_enabled=%t\n", externalISCSI)
	fmt.Fprintf(stdout, "protocol=%s\n", protocol)
	fmt.Fprintf(stdout, "nvme_transport=%s\n", nvmeTransport)
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

func helmValuesProtocolAccepted(value string) bool {
	switch value {
	case "iscsi", "nvme":
		return true
	default:
		return false
	}
}

func helmValuesNVMeTransportAccepted(value string) bool {
	return value == "tcp" || value == "rdma"
}

func helmValuesRestartPersistenceAccepted(value string) bool {
	switch value {
	case "ephemeral", "hostpath":
		return true
	default:
		return false
	}
}

func helmValuesFrontendNetworkClassAccepted(value string) bool {
	switch value {
	case "management_lan", "100gbe_tcp", "100gbe_roce":
		return true
	default:
		return false
	}
}

func parseHelmValuesNodeIPMap(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		node, ip, ok := strings.Cut(part, "=")
		node = strings.TrimSpace(node)
		ip = strings.TrimSpace(ip)
		if !ok || node == "" || ip == "" {
			return nil, fmt.Errorf("--frontend-ip-map entry %q must be node=ip", part)
		}
		if helmValuesIPUnsafe(ip) {
			return nil, fmt.Errorf("--frontend-ip-map node %q has unsafe IP %q", node, ip)
		}
		if _, exists := out[node]; exists {
			return nil, fmt.Errorf("--frontend-ip-map duplicates node %q", node)
		}
		out[node] = ip
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
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
		cleanupSummary   string
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
	fs.StringVar(&cleanupSummary, "cleanup-summary", "", "cleanup-summary.txt evidence to project into delete-safety status")
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
		if err != nil && cleanupSummary != "" && strings.Contains(err.Error(), "not found in inventory") {
			cluster, err = ops.BuildObservationFromInventory(inventory, "", outDir)
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", command, err)
		return ops.ClusterEvidence{}, "", ops.VolumeStatusExitInvalid
	}
	cluster, err = applyCleanupSummaryProjection(namespace, cleanupSummary, cluster)
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
		"--cleanup-summary":   true,
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
	fmt.Fprintln(w, "  sw-block ops generate-helm-values --out values.yaml [--target-node <node>] [--replication-factor <n>] [--protocol iscsi|nvme] [--nvme-transport tcp|rdma]")
	fmt.Fprintln(w, "      [--restart-persistence ephemeral|hostpath] [--state-hostpath /var/lib/sw-block] [--timeout 10s]")
	fmt.Fprintln(w, "  sw-block ops operator-status --dry-run [--master-api <addr>|--from-bundle <dir>] [--cleanup-summary <file>] [--interval 30s]")
	fmt.Fprintln(w, "  sw-block ops lifecycle-owner [--dry-run] [--namespace <ns>] [--interval 30s]")
	fmt.Fprintln(w, "  sw-block ops authority-executor [--namespace <ns>] [--allowed-mutation-class ack_eligibility|rebuild_traffic] [--interval 30s]")
	fmt.Fprintln(w, "  sw-block ops rebuild-target-owner [--dry-run] [--namespace <ns>] [--interval 30s]")
	fmt.Fprintln(w, "  sw-block ops failback-target-owner [--dry-run] [--namespace <ns>] [--interval 30s] [--activate-targets --activation-policy --runtime-endpoint <addr>]")
	fmt.Fprintln(w, "  sw-block ops failback-executor [--dry-run] [--namespace <ns>] [--enable-execution] [--execution-policy] [--failback-runtime-url <url>] [--interval 30s]")
	fmt.Fprintln(w, "  sw-block ops frontend-publication-target-owner [--dry-run] [--namespace <ns>] [--interval 30s] [--activate-targets --activation-policy --runtime-endpoint <url>]")
	fmt.Fprintln(w, "  sw-block ops frontend-publication-executor [--dry-run] [--namespace <ns>] [--enable-execution] [--execution-policy] [--frontend-publication-runtime-url <url>] [--interval 30s]")
	fmt.Fprintln(w, "  sw-block ops snapshot-backup export|get|list|import --api <addr> --ca <file> --client-cert <file> --client-key <file> --token-file <file> [--backup-id <id>] [--snapshot-id <id>]")
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
