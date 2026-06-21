package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOpsStatusWritesArtifactsAndReturnsClean(t *testing.T) {
	oldRunCommand := opsStatusRunCommand
	opsStatusRunCommand = cleanCmdResidueCommand
	defer func() { opsStatusRunCommand = oldRunCommand }()

	masterAddr, closeMaster := startCmdFakeMaster(t)
	defer closeMaster()
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			writeCmdJSON(t, w, hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:        "v1",
					ReplicaID:       "r1",
					Epoch:           7,
					EndpointVersion: 2,
					Healthy:         true,
				},
				FrontendPrimaryReady: true,
				AuthorityRole:        hostvolume.AuthorityRolePrimary,
				ReplicationRole:      hostvolume.ReplicationRoleNone,
			})
		case "/status/peers":
			writeCmdJSON(t, w, struct {
				Peers []replication.ReplicaPeerStatus
			}{Peers: []replication.ReplicaPeerStatus{{ReplicaID: "r2", State: "healthy", Epoch: 7, EndpointVersion: 2}}})
		case "/status/durable":
			writeCmdJSON(t, w, struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{VolumeID: "v1", ReplicaID: "r1", Latched: true, Operational: true}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer statusServer.Close()

	outDir := os.Getenv("SW_BLOCK_OPS_STATUS_CLI_ARTIFACT_DIR")
	if outDir == "" {
		outDir = t.TempDir()
	} else if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("create artifact dir: %v", err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"ops", "status",
		"--volume", "v1",
		"--master", masterAddr,
		"--status-addr", statusServer.URL,
		"--out", outDir,
		"--product-revision", "product-rev",
		"--runner-revision", "runner-rev",
	}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, name := range []string{ops.VolumeStatusReportArtifact, ops.VolumeStatusSummaryArtifact} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
	if !strings.Contains(stdout.String(), "status: ok") {
		t.Fatalf("stdout missing summary:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), ops.OpsStatusBundleArtifact) {
		t.Fatalf("stdout missing bundle artifact:\n%s", stdout.String())
	}
	raw, err := os.ReadFile(filepath.Join(outDir, ops.VolumeStatusReportArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var report ops.VolumeStatusReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	if report.Volume.VolumeID != "v1" || len(report.Volume.Frontends) != 1 {
		t.Fatalf("report mismatch: %+v", report)
	}
	if len(report.CollectionErrors) != 0 {
		t.Fatalf("unexpected collection errors: %+v", report.CollectionErrors)
	}
	rawBundle, err := os.ReadFile(filepath.Join(outDir, ops.OpsStatusBundleArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var bundle ops.OpsStatusBundle
	if err := json.Unmarshal(rawBundle, &bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Command != "sw-block ops status" || bundle.VolumeID != "v1" || bundle.ProductRevision != "product-rev" || bundle.RunnerRevision != "runner-rev" {
		t.Fatalf("bundle mismatch: %+v", bundle)
	}
	if bundle.ExitCode != ops.VolumeStatusExitOK || bundle.Status != "ok" {
		t.Fatalf("bundle classification mismatch: %+v", bundle)
	}
	if bundle.CollectionErrors == nil || bundle.Unchecked == nil || len(bundle.NonClaims) == 0 {
		t.Fatalf("bundle should include stable arrays and non-claims: %+v", bundle)
	}
}

func TestOpsInventoryWritesEmptyClusterArtifacts(t *testing.T) {
	oldRunCommand := opsInventoryRunCommand
	opsInventoryRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl -n default get pvc -o json":                          `{"items":[]}`,
		"kubectl get pv -o json":                                      `{"items":[]}`,
		"kubectl -n default get deploy -l app=sw-blockvolume -o json": `{"items":[]}`,
	})
	defer func() { opsInventoryRunCommand = oldRunCommand }()

	outDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"ops", "inventory",
		"--namespace", "default",
		"--out", outDir,
		"--product-revision", "product-rev",
		"--runner-revision", "runner-rev",
	}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, name := range []string{ops.VolumeInventoryArtifact, ops.VolumeInventorySummaryArtifact, ops.OpsInventoryBundleArtifact} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
	if !strings.Contains(stdout.String(), "inventory_status: ok") ||
		!strings.Contains(stdout.String(), "volumes: total=0") ||
		!strings.Contains(stdout.String(), ops.OpsInventoryBundleArtifact) {
		t.Fatalf("stdout missing inventory evidence:\n%s", stdout.String())
	}

	raw, err := os.ReadFile(filepath.Join(outDir, ops.VolumeInventoryArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var inventory ops.VolumeInventory
	if err := json.Unmarshal(raw, &inventory); err != nil {
		t.Fatal(err)
	}
	if inventory.ProductRevision != "product-rev" || inventory.RunnerRevision != "runner-rev" || len(inventory.Volumes) != 0 {
		t.Fatalf("inventory mismatch: %+v", inventory)
	}
	rawBundle, err := os.ReadFile(filepath.Join(outDir, ops.OpsInventoryBundleArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var bundle ops.OpsInventoryBundle
	if err := json.Unmarshal(rawBundle, &bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Command != "sw-block ops inventory" || bundle.Status != "ok" || bundle.VolumeCount != 0 {
		t.Fatalf("bundle mismatch: %+v", bundle)
	}
}

func TestOpsListAliasesInventory(t *testing.T) {
	oldRunCommand := opsInventoryRunCommand
	opsInventoryRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl -n default get pvc -o json":                          `{"items":[]}`,
		"kubectl get pv -o json":                                      `{"items":[]}`,
		"kubectl -n default get deploy -l app=sw-blockvolume -o json": `{"items":[]}`,
	})
	defer func() { opsInventoryRunCommand = oldRunCommand }()

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "list", "--out", t.TempDir(), "--product-revision", "product-rev"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "inventory_status: ok") {
		t.Fatalf("stdout=%s", stdout.String())
	}
}

func TestOpsInventoryClusterUnreachableReturnsInvalidWithBundle(t *testing.T) {
	oldRunCommand := opsInventoryRunCommand
	opsInventoryRunCommand = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("The connection to the server 127.0.0.1:6443 was refused\n"), errors.New("exit status 1")
	}
	defer func() { opsInventoryRunCommand = oldRunCommand }()

	outDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "inventory", "--out", outDir, "--product-revision", "product-rev"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stderr.String(), "kubernetes_unreachable") ||
		!strings.Contains(stdout.String(), "inventory_status: invalid") {
		t.Fatalf("stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	raw, err := os.ReadFile(filepath.Join(outDir, ops.OpsInventoryBundleArtifact))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "kubernetes_unreachable") {
		t.Fatalf("bundle missing unreachable evidence:\n%s", raw)
	}
}

func TestOpsInventoryMissingOutReturnsInvalid(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "inventory"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want %d", code, ops.VolumeStatusExitInvalid)
	}
	if !strings.Contains(stderr.String(), "--out is required") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

func TestOpsOperatorStatusDryRunFromBundle(t *testing.T) {
	dir := t.TempDir()
	cluster := ops.NewClusterEvidence(time.Date(2026, 6, 2, 23, 30, 0, 0, time.UTC))
	cluster.ManagedVolumes = []ops.ManagedVolumeProjection{ops.ProjectManagedVolume(ops.ManagedVolumeFacts{
		VolumeID: "pvc-operator",
		PVCName:  "demo-pvc",
		PVC:      &ops.PVCFact{Phase: "Bound"},
		Authority: &ops.AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "192.168.1.184:3260",
		},
		Replicas: []ops.ReplicaFact{{
			ReplicaID:      "r1",
			KubernetesNode: "m02",
			Role:           "primary",
			Observed:       true,
		}},
		CSIStages: []ops.CSIStageFact{{NodeName: "m02", Target: "192.168.1.184:3260"}},
		Workload:  &ops.WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
	})}
	raw, err := ops.MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ops.ClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "operator-status", "--dry-run", "--from-bundle", dir, "--namespace", "kube-system"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"operator_status=dry_run cluster=kube-system/sw-block volumes=1",
		"mutation_allowed=false",
		"cluster_status volumes=1 ready=1 blocked=0 stale=0",
		"volume_status name=demo-pvc volume_id=pvc-operator pvc=demo-pvc status=ready reason=first_volume_verified",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

func TestOpsOperatorStatusWritesCRDStatusWhenDryRunDisabled(t *testing.T) {
	dir := t.TempDir()
	cluster := ops.NewClusterEvidence(time.Date(2026, 6, 3, 10, 30, 0, 0, time.UTC))
	cluster.ManagedVolumes = []ops.ManagedVolumeProjection{ops.ProjectManagedVolume(ops.ManagedVolumeFacts{
		VolumeID: "pvc-write",
		PVCName:  "write-pvc",
		PVC:      &ops.PVCFact{Phase: "Bound"},
		Authority: &ops.AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "192.168.1.184:3260",
		},
		Replicas: []ops.ReplicaFact{{
			ReplicaID:      "r1",
			KubernetesNode: "m02",
			Role:           "primary",
			Observed:       true,
		}},
		CSIStages: []ops.CSIStageFact{{NodeName: "m02", Target: "192.168.1.184:3260"}},
		Workload:  &ops.WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
	})}
	raw, err := ops.MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ops.ClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	writer := &operatorStatusTestWriter{}
	oldFactory := opsOperatorStatusWriterFactory
	opsOperatorStatusWriterFactory = func() (ops.OperatorStatusWriter, error) {
		return writer, nil
	}
	t.Cleanup(func() { opsOperatorStatusWriterFactory = oldFactory })

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "operator-status", "--from-bundle", t.TempDir()}, &stdout, &stderr)
	if code == ops.VolumeStatusExitOK {
		t.Fatalf("empty bundle unexpectedly succeeded stdout=%s stderr=%s", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"ops", "operator-status", "--from-bundle", dir, "--namespace", "kube-system"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if writer.cluster.VolumeCount != 1 || len(writer.volumes) != 1 {
		t.Fatalf("writer cluster=%+v volumes=%+v", writer.cluster, writer.volumes)
	}
	if writer.volumes[0].ref.Name != "write-pvc" || writer.volumes[0].status.Status != ops.ManagedVolumeStatusReady {
		t.Fatalf("volume write=%+v", writer.volumes[0])
	}
	out := stdout.String()
	for _, want := range []string{
		"operator_status=write_status cluster=kube-system/sw-block volumes=1",
		"mutation_allowed=false",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

func TestOpsLifecycleOwnerDryRunDoesNotPatch(t *testing.T) {
	client := &lifecycleOwnerTestClient{
		volumes: []ops.SwBlockVolumeObject{{
			Ref: ops.OperatorObjectRef{
				APIVersion: ops.SwBlockVolumeAPIVersion,
				Kind:       ops.SwBlockVolumeKind,
				Namespace:  "kube-system",
				Name:       "demo-volume",
			},
		}},
	}
	oldFactory := opsLifecycleOwnerClientFactory
	opsLifecycleOwnerClientFactory = func() (ops.LifecycleOwnerClient, ops.OperatorEventSink, error) {
		return client, client, nil
	}
	t.Cleanup(func() { opsLifecycleOwnerClientFactory = oldFactory })

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "lifecycle-owner", "--dry-run", "--namespace", "kube-system"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(client.patches) != 0 || len(client.events) != 0 {
		t.Fatalf("dry-run patched/events: patches=%+v events=%+v", client.patches, client.events)
	}
	out := stdout.String()
	for _, want := range []string{
		"lifecycle_owner=dry_run namespace=kube-system volumes=1",
		"finalizer_patches=0",
		"finalizer_added=1",
		"events=0",
		"mutation_allowed=false",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

func TestOpsLifecycleOwnerWritesProtectionFinalizer(t *testing.T) {
	client := &lifecycleOwnerTestClient{
		volumes: []ops.SwBlockVolumeObject{{
			Ref: ops.OperatorObjectRef{
				APIVersion: ops.SwBlockVolumeAPIVersion,
				Kind:       ops.SwBlockVolumeKind,
				Namespace:  "kube-system",
				Name:       "demo-volume",
			},
			Finalizers: []string{"example.com/foreign"},
		}},
	}
	oldFactory := opsLifecycleOwnerClientFactory
	opsLifecycleOwnerClientFactory = func() (ops.LifecycleOwnerClient, ops.OperatorEventSink, error) {
		return client, client, nil
	}
	t.Cleanup(func() { opsLifecycleOwnerClientFactory = oldFactory })

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "lifecycle-owner", "--namespace", "kube-system"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(client.patches) != 1 {
		t.Fatalf("patches=%+v", client.patches)
	}
	wantFinalizers := []string{"example.com/foreign", ops.SwBlockVolumeFinalizerName}
	if fmt.Sprint(client.patches[0].finalizers) != fmt.Sprint(wantFinalizers) {
		t.Fatalf("finalizers=%+v want %+v", client.patches[0].finalizers, wantFinalizers)
	}
	if len(client.events) != 1 || client.events[0].Reason != ops.ReasonDeleteFinalizerAdded {
		t.Fatalf("events=%+v", client.events)
	}
	out := stdout.String()
	for _, want := range []string{
		"lifecycle_owner=finalizer_mutation namespace=kube-system volumes=1",
		"finalizer_patches=1",
		"finalizer_added=1",
		"events=1",
		"mutation_allowed=true",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

func TestOpsLifecycleOwnerReleasesProtectionFinalizerWhenDeleteSafetyAllows(t *testing.T) {
	deletingAt := time.Date(2026, 6, 15, 2, 0, 0, 0, time.UTC)
	client := &lifecycleOwnerTestClient{
		volumes: []ops.SwBlockVolumeObject{{
			Ref: ops.OperatorObjectRef{
				APIVersion: ops.SwBlockVolumeAPIVersion,
				Kind:       ops.SwBlockVolumeKind,
				Namespace:  "kube-system",
				Name:       "demo-volume",
			},
			Finalizers:        []string{"example.com/foreign", ops.SwBlockVolumeFinalizerName},
			DeletionTimestamp: &deletingAt,
			Status: ops.SwBlockVolumeCRDStatus{DeleteSafety: &ops.SwBlockVolumeCRDDeleteSafety{
				Decision:                ops.ManagedVolumeActionDecisionAllowed,
				State:                   ops.DeleteSafetyStateReleasable,
				Reason:                  ops.ReasonDeleteFinalizerReleasable,
				FinalizerReleaseAllowed: true,
			}},
		}},
	}
	oldFactory := opsLifecycleOwnerClientFactory
	opsLifecycleOwnerClientFactory = func() (ops.LifecycleOwnerClient, ops.OperatorEventSink, error) {
		return client, client, nil
	}
	t.Cleanup(func() { opsLifecycleOwnerClientFactory = oldFactory })

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "lifecycle-owner", "--namespace", "kube-system"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(client.patches) != 1 {
		t.Fatalf("patches=%+v", client.patches)
	}
	if got, want := fmt.Sprint(client.patches[0].finalizers), fmt.Sprint([]string{"example.com/foreign"}); got != want {
		t.Fatalf("finalizers=%s want %s", got, want)
	}
	out := stdout.String()
	for _, want := range []string{
		"finalizer_patches=1",
		"finalizer_released=1",
		"events=1",
		"mutation_allowed=true",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

func TestOpsClusterMasterAPIUsesSharedInClusterNodeEvidenceEnrichment(t *testing.T) {
	masterAddr, closeMaster := startCmdFakeMaster(t)
	defer closeMaster()
	oldFactory := opsNodeEvidenceEnricherFactory
	opsNodeEvidenceEnricherFactory = func() (ops.OperatorNodeEvidenceEnricher, error) {
		return fakeNodeEvidenceEnricher{}, nil
	}
	t.Cleanup(func() { opsNodeEvidenceEnricherFactory = oldFactory })
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "cluster", "--master-api", masterAddr, "-o", "json"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	out := stdout.String()
	for _, want := range []string{
		`"kubernetes_node": "m02"`,
		`"ready": false`,
		`"reason": "node_not_ready"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

func TestOpsReportMasterAPIUsesSharedInClusterNodeEvidenceEnrichment(t *testing.T) {
	masterAddr, closeMaster := startCmdFakeMaster(t)
	defer closeMaster()
	oldFactory := opsNodeEvidenceEnricherFactory
	opsNodeEvidenceEnricherFactory = func() (ops.OperatorNodeEvidenceEnricher, error) {
		return fakeNodeEvidenceEnricher{}, nil
	}
	t.Cleanup(func() { opsNodeEvidenceEnricherFactory = oldFactory })
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")

	outDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "report", "--master-api", masterAddr, "--out", outDir}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	raw, err := os.ReadFile(filepath.Join(outDir, ops.ObservationReportJSONArtifact))
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	for _, want := range []string{
		`"kubernetes_node": "m02"`,
		`"ready": false`,
		`"reason": "node_not_ready"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("report missing %q:\n%s", want, out)
		}
	}
}

func TestLoadObservationVolumeUsesSharedInClusterNodeEvidenceEnrichment(t *testing.T) {
	oldFactory := opsNodeEvidenceEnricherFactory
	opsNodeEvidenceEnricherFactory = func() (ops.OperatorNodeEvidenceEnricher, error) {
		return fakeNodeEvidenceEnricher{}, nil
	}
	t.Cleanup(func() { opsNodeEvidenceEnricherFactory = oldFactory })
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	oldRunCommand := opsInventoryRunCommand
	opsInventoryRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl -n default get pvc -o json":                          cmdSinglePVCListJSON,
		"kubectl get pv -o json":                                      cmdSinglePVListJSON,
		"kubectl -n default get deploy -l app=sw-blockvolume -o json": cmdSingleDeploymentListJSON,
	})
	t.Cleanup(func() { opsInventoryRunCommand = oldRunCommand })

	var stderr bytes.Buffer
	cluster, _, code := loadObservationVolume("sw-block ops explain", []string{"volume", "--namespace", "default", "--product-revision", "product-rev", "pvc-live"}, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s cluster=%+v", code, stderr.String(), cluster)
	}
	if len(cluster.Nodes) != 1 || cluster.Nodes[0].KubernetesNode != "m02" {
		t.Fatalf("cluster nodes=%+v", cluster.Nodes)
	}
	if !cmdConditionReason(cluster.Nodes[0].Conditions, ops.ReasonNodeNotReady) {
		t.Fatalf("volume loader did not enrich node evidence: %+v", cluster.Nodes[0])
	}
}

func TestOpsExplainProjectsDeletingCRFromCleanupSummary(t *testing.T) {
	oldInventoryRun := opsInventoryRunCommand
	opsInventoryRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl -n default get pvc -o json":                          `{"items":[]}`,
		"kubectl get pv -o json":                                      `{"items":[]}`,
		"kubectl -n default get deploy -l app=sw-blockvolume -o json": `{"items":[]}`,
	})
	t.Cleanup(func() { opsInventoryRunCommand = oldInventoryRun })

	oldNodeFactory := opsNodeEvidenceEnricherFactory
	opsNodeEvidenceEnricherFactory = func() (ops.OperatorNodeEvidenceEnricher, error) {
		return fakeNodeEvidenceEnricher{}, nil
	}
	t.Cleanup(func() { opsNodeEvidenceEnricherFactory = oldNodeFactory })

	deletingAt := time.Date(2026, 6, 17, 8, 0, 0, 0, time.UTC)
	oldVolumeSourceFactory := opsSwBlockVolumeSourceFactory
	opsSwBlockVolumeSourceFactory = func() (ops.OperatorSwBlockVolumeSource, error) {
		return &lifecycleOwnerTestClient{volumes: []ops.SwBlockVolumeObject{{
			Ref: ops.OperatorObjectRef{
				Namespace: "default",
				Name:      "delete-pvc",
			},
			Finalizers:        []string{ops.SwBlockVolumeFinalizerName},
			DeletionTimestamp: &deletingAt,
			Spec:              ops.SwBlockVolumeSpec{PVCName: "delete-pvc"},
			Status: ops.SwBlockVolumeCRDStatus{
				VolumeID: "pvc-delete",
				PVCName:  "delete-pvc",
			},
		}}}, nil
	}
	t.Cleanup(func() { opsSwBlockVolumeSourceFactory = oldVolumeSourceFactory })
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")

	cleanupSummary := filepath.Join(t.TempDir(), "cleanup-summary.txt")
	cleanupObservedAt := time.Now().UTC().Format(time.RFC3339)
	if err := os.WriteFile(cleanupSummary, []byte(strings.Join([]string{
		"cleanup_status=failed",
		"iscsi_residue_count=1",
		"failure_count=1",
		"reason_codes=iscsi_node_records_present",
		"cleanup_observed_at=" + cleanupObservedAt,
	}, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "explain", "volume", "--namespace", "default", "--cleanup-summary", cleanupSummary, "delete-pvc"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"managed_volume pvc-delete status=blocked reason=iscsi_node_records_present",
		"managed_volume_delete_safety state=blocked decision=rejected reason=iscsi_node_records_present",
		"managed_volume_action safe_k8s.release_swblockvolume_finalizer mode=dry_run",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("explain missing %q:\n%s", want, out)
		}
	}
}

func TestOpsDashboardMasterAPIUsesSharedInClusterNodeEvidenceEnrichment(t *testing.T) {
	masterAddr, closeMaster := startCmdFakeMaster(t)
	defer closeMaster()
	oldFactory := opsNodeEvidenceEnricherFactory
	opsNodeEvidenceEnricherFactory = func() (ops.OperatorNodeEvidenceEnricher, error) {
		return fakeNodeEvidenceEnricher{}, nil
	}
	t.Cleanup(func() { opsNodeEvidenceEnricherFactory = oldFactory })
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")

	addr := freeTCPAddr(t)
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run([]string{
			"ops", "dashboard",
			"--master-api", masterAddr,
			"--listen", addr,
			"--serve-duration", "1500ms",
		}, &stdout, &stderr)
	}()

	body := waitForHTTPContains(t, "http://"+addr+"/cluster-evidence.json", `"reason": "node_not_ready"`)
	if !strings.Contains(body, `"kubernetes_node": "m02"`) || !strings.Contains(body, `"ready": false`) {
		t.Fatalf("dashboard cluster evidence missing enriched node:\n%s", body)
	}
	select {
	case code := <-done:
		if code != ops.VolumeStatusExitOK {
			t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("dashboard command did not stop; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestLiveNodeEvidenceUsesHelmNamespaceWhenAppNamespaceDefaults(t *testing.T) {
	recorder := &recordingNodeEvidenceEnricher{}
	oldFactory := opsNodeEvidenceEnricherFactory
	opsNodeEvidenceEnricherFactory = func() (ops.OperatorNodeEvidenceEnricher, error) {
		return recorder, nil
	}
	t.Cleanup(func() { opsNodeEvidenceEnricherFactory = oldFactory })
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")

	_, err := enrichLiveObservationCluster("default", time.Second, true, ops.ClusterEvidence{})
	if err != nil {
		t.Fatalf("enrich: %v", err)
	}
	if recorder.namespace != "kube-system" {
		t.Fatalf("node evidence namespace=%q want kube-system", recorder.namespace)
	}
}

func TestLiveNodeEvidenceNamespaceHonorsHelmNamespaceOverride(t *testing.T) {
	recorder := &recordingNodeEvidenceEnricher{}
	oldFactory := opsNodeEvidenceEnricherFactory
	opsNodeEvidenceEnricherFactory = func() (ops.OperatorNodeEvidenceEnricher, error) {
		return recorder, nil
	}
	t.Cleanup(func() { opsNodeEvidenceEnricherFactory = oldFactory })
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("SW_BLOCK_HELM_NAMESPACE", "sw-system")

	_, err := enrichLiveObservationCluster("default", time.Second, true, ops.ClusterEvidence{})
	if err != nil {
		t.Fatalf("enrich: %v", err)
	}
	if recorder.namespace != "sw-system" {
		t.Fatalf("node evidence namespace=%q want sw-system", recorder.namespace)
	}
}

func TestLiveNodeEvidenceUsesExplicitNonDefaultNamespace(t *testing.T) {
	recorder := &recordingNodeEvidenceEnricher{}
	oldFactory := opsNodeEvidenceEnricherFactory
	opsNodeEvidenceEnricherFactory = func() (ops.OperatorNodeEvidenceEnricher, error) {
		return recorder, nil
	}
	t.Cleanup(func() { opsNodeEvidenceEnricherFactory = oldFactory })
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")

	_, err := enrichLiveObservationCluster("sw-system", time.Second, true, ops.ClusterEvidence{})
	if err != nil {
		t.Fatalf("enrich: %v", err)
	}
	if recorder.namespace != "sw-system" {
		t.Fatalf("node evidence namespace=%q want sw-system", recorder.namespace)
	}
}

type fakeNodeEvidenceEnricher struct{}

func (fakeNodeEvidenceEnricher) EnrichNodeEvidence(_ context.Context, _ string, cluster ops.ClusterEvidence) (ops.ClusterEvidence, error) {
	cluster.Nodes = []ops.NodeEvidence{{
		NodeName:       "m02",
		KubernetesNode: "m02",
		Ready:          false,
		Schedulable:    true,
		Conditions: []ops.ObservationCondition{{
			Type:     ops.ConditionReady,
			Status:   "Unknown",
			Reason:   ops.ReasonNodeNotReady,
			Severity: "warning",
			Message:  "Kubernetes node Ready condition is not True",
		}},
	}}
	return cluster, nil
}

type recordingNodeEvidenceEnricher struct {
	namespace string
}

func (r *recordingNodeEvidenceEnricher) EnrichNodeEvidence(_ context.Context, namespace string, cluster ops.ClusterEvidence) (ops.ClusterEvidence, error) {
	r.namespace = namespace
	return cluster, nil
}

func cmdConditionReason(conditions []ops.ObservationCondition, reason string) bool {
	for _, condition := range conditions {
		if condition.Reason == reason {
			return true
		}
	}
	return false
}

type operatorStatusTestWriter struct {
	cluster ops.SwBlockClusterCRDStatus
	volumes []operatorStatusTestVolumeWrite
}

type operatorStatusTestVolumeWrite struct {
	ref    ops.OperatorObjectRef
	status ops.SwBlockVolumeCRDStatus
}

func (w *operatorStatusTestWriter) WriteClusterStatus(_ context.Context, _ ops.OperatorObjectRef, status ops.SwBlockClusterCRDStatus) error {
	w.cluster = status
	return nil
}

func (w *operatorStatusTestWriter) WriteVolumeStatus(_ context.Context, ref ops.OperatorObjectRef, status ops.SwBlockVolumeCRDStatus) error {
	w.volumes = append(w.volumes, operatorStatusTestVolumeWrite{ref: ref, status: status})
	return nil
}

type lifecycleOwnerTestClient struct {
	volumes []ops.SwBlockVolumeObject
	patches []lifecycleOwnerTestPatch
	events  []ops.OperatorKubernetesEvent
}

type lifecycleOwnerTestPatch struct {
	ref        ops.OperatorObjectRef
	finalizers []string
}

func (c *lifecycleOwnerTestClient) ListSwBlockVolumes(_ context.Context, _ string) ([]ops.SwBlockVolumeObject, error) {
	return append([]ops.SwBlockVolumeObject(nil), c.volumes...), nil
}

func (c *lifecycleOwnerTestClient) PatchSwBlockVolumeFinalizers(_ context.Context, ref ops.OperatorObjectRef, finalizers []string) error {
	c.patches = append(c.patches, lifecycleOwnerTestPatch{
		ref:        ref,
		finalizers: append([]string(nil), finalizers...),
	})
	return nil
}

func (c *lifecycleOwnerTestClient) EmitEvent(_ context.Context, event ops.OperatorKubernetesEvent) error {
	c.events = append(c.events, event)
	return nil
}

func TestOpsInventoryRejectsBadRequiredFrontierFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "inventory", "--out", t.TempDir(), "--required-frontier", "pvc-a=not-a-number"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want %d", code, ops.VolumeStatusExitInvalid)
	}
	if !strings.Contains(stderr.String(), "parse required frontier lsn") {
		t.Fatalf("stderr=%s stdout=%s", stderr.String(), stdout.String())
	}
}

func TestOpsInventoryRejectsBadClaimProfile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "inventory", "--out", t.TempDir(), "--claim-profile", "wishful-ha"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want %d", code, ops.VolumeStatusExitInvalid)
	}
	if !strings.Contains(stderr.String(), "--claim-profile") {
		t.Fatalf("stderr=%s stdout=%s", stderr.String(), stdout.String())
	}
	if !strings.Contains(stderr.String(), ops.PromotionClaimStage2ISCSIALUAMultipath) {
		t.Fatalf("stderr should list stage2 profile: %s", stderr.String())
	}
}

func TestOpsInventoryAcceptsStage2ClaimProfile(t *testing.T) {
	oldRunCommand := opsInventoryRunCommand
	opsInventoryRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl -n default get pvc -o json":                          `{"items":[]}`,
		"kubectl get pv -o json":                                      `{"items":[]}`,
		"kubectl -n default get deploy -l app=sw-blockvolume -o json": `{"items":[]}`,
	})
	defer func() { opsInventoryRunCommand = oldRunCommand }()

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "inventory", "--out", t.TempDir(), "--claim-profile", ops.PromotionClaimStage2ISCSIALUAMultipath}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d want %d stderr=%s stdout=%s", code, ops.VolumeStatusExitOK, stderr.String(), stdout.String())
	}
	if strings.Contains(stderr.String(), "--claim-profile") {
		t.Fatalf("stage2 claim profile should pass validation: stderr=%s stdout=%s", stderr.String(), stdout.String())
	}
}

func TestOpsGenerateHelmValuesSingleNodeFromKubernetes(t *testing.T) {
	oldRunCommand := opsGenerateHelmValuesRunCommand
	opsGenerateHelmValuesRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl get nodes -o wide --no-headers": cmdHelmNodeWide,
	})
	defer func() { opsGenerateHelmValuesRunCommand = oldRunCommand }()

	outPath := filepath.Join(t.TempDir(), "values.yaml")
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"ops", "generate-helm-values",
		"--out", outPath,
		"--target-node", "m02",
		"--node-limit", "1",
		"--image", "ghcr.io/seaweedfs/seaweed-block:sha-test",
		"--csi-image", "ghcr.io/seaweedfs/seaweed-block-csi:sha-test",
	}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"helm_values_status=ok",
		"network_mode=loopback",
		"ready_kubernetes_nodes=1",
		"discovered_kubernetes_nodes=3",
		"target_node=m02",
		"restart_persistence_mode=ephemeral",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	values, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"repository: ghcr.io/seaweedfs/seaweed-block",
		"tag: sha-test",
		"externalISCSI: false",
		"externalStatus: false",
		"rejectLoopbackPublishTargets: false",
		"restartPersistence:",
		"mode: ephemeral",
		"expectedSlotsPerVolume: 1",
		"enabled: false",
		"name: m02",
		"kubernetesNode: m02",
		"internalIP: 127.0.0.1",
		"dataPort: 19101",
		"controlPort: 19102",
		"launcherDurableImplFlag: false",
		"launcherReplicationAckFlag: false",
	} {
		if !strings.Contains(string(values), want) {
			t.Fatalf("values missing %q:\n%s", want, values)
		}
	}
}

func TestOpsGenerateHelmValuesMultiNodeExternalISCSI(t *testing.T) {
	oldRunCommand := opsGenerateHelmValuesRunCommand
	opsGenerateHelmValuesRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl get nodes -o wide --no-headers": cmdHelmNodeWide,
	})
	defer func() { opsGenerateHelmValuesRunCommand = oldRunCommand }()

	outPath := filepath.Join(t.TempDir(), "values.yaml")
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"ops", "generate-helm-values",
		"--out", outPath,
		"--replication-factor", "3",
		"--ack-profile", "sync-quorum",
		"--chap-secret", "fixed-chap-secret",
		"--stage2-multipath",
	}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"network_mode=external-iscsi",
		"ready_kubernetes_nodes=3",
		"external_iscsi=true",
		"chap_enabled=true",
		"ack_profile=sync-quorum",
		"restart_persistence_mode=ephemeral",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	values, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"replicationFactor: 3",
		"ackProfile: sync-quorum",
		"expectedSlotsPerVolume: 3",
		"externalISCSI: true",
		"externalStatus: true",
		"rejectLoopbackPublishTargets: true",
		"restartPersistence:",
		"mode: ephemeral",
		"enabled: true",
		"secret: fixed-chap-secret",
		"name: m01",
		"internalIP: 192.168.1.181",
		"dataPort: 19101",
		"controlPort: 19102",
		"name: m02",
		"internalIP: 192.168.1.184",
		"dataPort: 19103",
		"controlPort: 19104",
		"name: tp01",
		"internalIP: 192.168.1.188",
		"dataPort: 19105",
		"controlPort: 19106",
		"launcherDurableImplFlag: false",
		"launcherReplicationAckFlag: false",
	} {
		if !strings.Contains(string(values), want) {
			t.Fatalf("values missing %q:\n%s", want, values)
		}
	}
	if strings.Contains(string(values), "dataPort: 3260") {
		t.Fatalf("values must not assign dataPort 3260 because it collides with iSCSI listener port:\n%s", values)
	}
}

func TestOpsGenerateHelmValuesRestartPersistenceHostPath(t *testing.T) {
	oldRunCommand := opsGenerateHelmValuesRunCommand
	opsGenerateHelmValuesRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl get nodes -o wide --no-headers": cmdHelmNodeWide,
	})
	defer func() { opsGenerateHelmValuesRunCommand = oldRunCommand }()

	outPath := filepath.Join(t.TempDir(), "values.yaml")
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"ops", "generate-helm-values",
		"--out", outPath,
		"--target-node", "m02",
		"--node-limit", "1",
		"--restart-persistence", "hostpath",
		"--state-hostpath", "/var/lib/sw-block",
	}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"restart_persistence_mode=hostpath",
		"state_hostpath=/var/lib/sw-block",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	values, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"blockmaster:",
		"stateHostPath: /var/lib/sw-block",
		"restartPersistence:",
		"mode: hostpath",
	} {
		if !strings.Contains(string(values), want) {
			t.Fatalf("values missing %q:\n%s", want, values)
		}
	}
}

func TestOpsGenerateHelmValuesRejectsUnknownRestartPersistence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"ops", "generate-helm-values",
		"--out", filepath.Join(t.TempDir(), "values.yaml"),
		"--restart-persistence", "durable-magic",
	}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want invalid stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--restart-persistence=\"durable-magic\" invalid") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

func TestOpsGenerateHelmValuesRejectsRFAboveSelectedNodes(t *testing.T) {
	oldRunCommand := opsGenerateHelmValuesRunCommand
	opsGenerateHelmValuesRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl get nodes -o wide --no-headers": cmdHelmNodeWide,
	})
	defer func() { opsGenerateHelmValuesRunCommand = oldRunCommand }()

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"ops", "generate-helm-values",
		"--out", filepath.Join(t.TempDir(), "values.yaml"),
		"--target-node", "m02",
		"--replication-factor", "3",
	}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want invalid stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "requires at least 3 selected Ready nodes; selected=1") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

func TestOpsDescribeVolumeFromBundle(t *testing.T) {
	dir := writeCmdObservationBundle(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "describe", "volume", "--from-bundle", dir, "pvc-observed"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"cluster status=degraded volumes=1",
		"volume pvc-observed status=ok rf=3 ack=sync-quorum",
		"primary r2 on m02 frontend=192.168.1.184:3260",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestOpsDescribeVolumeFromBundlePrefersProductClusterEvidence(t *testing.T) {
	dir := writeCmdProductClusterBundle(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "describe", "volume", "--from-bundle", dir, "pvc-product"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"cluster status=ok volumes=1 nodes=1",
		"volume pvc-product status=ok rf=3 ack=sync-quorum",
		"primary r2 on m02 frontend=192.168.1.184:3260",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "pvc-observed") {
		t.Fatalf("describe should prefer product cluster evidence over fallback inventory:\n%s", stdout.String())
	}
}

func TestOpsReportFromBundleWritesStaticReadOnlyArtifacts(t *testing.T) {
	dir := writeCmdProductClusterBundle(t)
	outDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "report", "--from-bundle", dir, "--out", outDir}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, name := range []string{
		ops.ObservationReportHTMLArtifact,
		ops.ObservationReportJSONArtifact,
		ops.ObservationReportJSONLArtifact,
		ops.ObservationOperatorSnapshotArtifact,
		ops.ObservationReportTextArtifact,
	} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			t.Fatalf("missing report artifact %s: %v", name, err)
		}
	}
	html, err := os.ReadFile(filepath.Join(outDir, ops.ObservationReportHTMLArtifact))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"sw-block read-only status",
		"pvc-product",
		"192.168.1.184:3260",
		"This report is observation-only",
	} {
		if !strings.Contains(string(html), want) {
			t.Fatalf("html missing %q:\n%s", want, html)
		}
	}
	summary, err := os.ReadFile(filepath.Join(outDir, ops.ObservationReportTextArtifact))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(summary), "read_only=true") || !strings.Contains(string(summary), "volume=pvc-product") {
		t.Fatalf("summary missing report evidence:\n%s", summary)
	}
	if !strings.Contains(stdout.String(), "report_status=ok") ||
		!strings.Contains(stdout.String(), "html=index.html") ||
		!strings.Contains(stdout.String(), "operator_snapshot=operator-snapshot.json") {
		t.Fatalf("stdout missing report paths:\n%s", stdout.String())
	}
}

func TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard(t *testing.T) {
	dir := writeCmdReturnedReplicaBundle(t)
	outDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "report", "--from-bundle", dir, "--out", outDir}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("report exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	summary, err := os.ReadFile(filepath.Join(outDir, ops.ObservationReportTextArtifact))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"managed_volume_returned_replica=pvc-returned replica=r1 state=fenced reason=returned_replica_frontend_fenced",
		"managed_volume_executor_preflight=authority.reintegrate_returned_replica target=r1 decision=ready reason=preconditions_satisfied mode=dry_run executor=authority_recovery_executor mutation_allowed=false required_lsn=4241 durable_lsn=4241",
		"managed_volume_action=authority.reintegrate_returned_replica mode=dry_run side_effect=authority_mutating executor=authority_recovery_executor decision=allowed",
	} {
		if !strings.Contains(string(summary), want) {
			t.Fatalf("report summary missing %q:\n%s", want, summary)
		}
	}
	snapshot, err := os.ReadFile(filepath.Join(outDir, ops.ObservationOperatorSnapshotArtifact))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"replica_reintegrations": [`,
		`"executor_preflights": [`,
		`"state": "fenced"`,
		`"reason_code": "returned_replica_frontend_fenced"`,
		`"reason": "preconditions_satisfied"`,
		`"type": "authority.reintegrate_returned_replica"`,
	} {
		if !strings.Contains(string(snapshot), want) {
			t.Fatalf("operator snapshot missing %q:\n%s", want, snapshot)
		}
	}

	writer := &operatorStatusTestWriter{}
	oldFactory := opsOperatorStatusWriterFactory
	opsOperatorStatusWriterFactory = func() (ops.OperatorStatusWriter, error) {
		return writer, nil
	}
	t.Cleanup(func() { opsOperatorStatusWriterFactory = oldFactory })
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"ops", "operator-status", "--from-bundle", dir, "--namespace", "kube-system"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("operator-status exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if len(writer.volumes) != 1 {
		t.Fatalf("operator-status writes=%+v", writer.volumes)
	}
	returned := writer.volumes[0].status.ReplicaReintegrations
	if len(returned) != 1 || returned[0].ReplicaID != "r1" || returned[0].State != ops.ReturnedReplicaStateFenced {
		t.Fatalf("CRD returned replicas=%+v", returned)
	}
	foundReintegrate := false
	for _, action := range writer.volumes[0].status.AllowedActions {
		if action.Type == ops.ManagedVolumeActionReintegrateReturned {
			foundReintegrate = true
		}
	}
	if !foundReintegrate {
		t.Fatalf("CRD actions=%+v", writer.volumes[0].status.AllowedActions)
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"ops", "explain", "volume", "--from-bundle", dir, "pvc-returned"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("explain exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"managed_volume_returned_replica=pvc-returned replica=r1 state=fenced reason=returned_replica_frontend_fenced",
		"managed_volume_executor_preflight authority.reintegrate_returned_replica target=r1 decision=ready reason=preconditions_satisfied mode=dry_run executor=authority_recovery_executor mutation_allowed=false required_lsn=4241 durable_lsn=4241",
		"managed_volume_action authority.reintegrate_returned_replica mode=dry_run",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("explain missing %q:\n%s", want, stdout.String())
		}
	}

	addr := freeTCPAddr(t)
	stdout.Reset()
	stderr.Reset()
	done := make(chan int, 1)
	go func() {
		done <- run([]string{
			"ops", "dashboard",
			"--from-bundle", dir,
			"--listen", addr,
			"--serve-duration", "500ms",
		}, &stdout, &stderr)
	}()
	body := waitForHTTPContains(t, "http://"+addr+"/operator-snapshot.json", `"replica_reintegrations": [`)
	for _, want := range []string{
		`"reason_code": "returned_replica_frontend_fenced"`,
		`"executor_preflights": [`,
		`"reason": "preconditions_satisfied"`,
		`"type": "authority.reintegrate_returned_replica"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard operator snapshot missing %q:\n%s", want, body)
		}
	}
	select {
	case code := <-done:
		if code != ops.VolumeStatusExitOK {
			t.Fatalf("dashboard exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("dashboard command did not stop; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestOpsReportFromBundleAllowsEmptyClusterEvidence(t *testing.T) {
	dir := t.TempDir()
	productDir := filepath.Join(dir, "demo", "product-observation")
	if err := os.MkdirAll(productDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cluster := ops.NewClusterEvidence(time.Date(2026, 5, 17, 21, 0, 0, 0, time.UTC))
	raw, err := ops.MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(productDir, ops.ClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	outDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "report", "--from-bundle", dir, "--out", outDir}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	summary, err := os.ReadFile(filepath.Join(outDir, ops.ObservationReportTextArtifact))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(summary), "volumes=0") || !strings.Contains(string(summary), "read_only=true") {
		t.Fatalf("summary missing empty-cluster evidence:\n%s", summary)
	}
}

func TestOpsReportFromBundleSkipsCorruptClusterEvidenceCandidate(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "a-stale", "status"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a-stale", "status", ops.ClusterEvidenceArtifact), []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	validDir := filepath.Join(dir, "z-restart")
	if err := os.MkdirAll(validDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cluster := ops.NewClusterEvidence(time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC))
	cluster.Volumes = []ops.VolumeEvidence{{
		VolumeID:          "pvc-valid",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 1,
		Status:            ops.ObservationStatusOK,
		PrimaryReplica:    "r1",
		PrimaryNode:       "m02",
		PublishTarget:     "192.168.1.184:3260",
	}}
	raw, err := ops.MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(validDir, ops.RestartClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	outDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "report", "--from-bundle", dir, "--out", outDir}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	summary, err := os.ReadFile(filepath.Join(outDir, ops.ObservationReportTextArtifact))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(summary), "volume=pvc-valid") || strings.Contains(string(summary), "not-json") {
		t.Fatalf("summary did not use valid evidence:\n%s", summary)
	}
	snapshot, err := os.ReadFile(filepath.Join(outDir, ops.ObservationOperatorSnapshotArtifact))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(snapshot), `"volume_id": "pvc-valid"`) || !strings.Contains(string(snapshot), `"read_only": true`) {
		t.Fatalf("operator snapshot missing valid read-only evidence:\n%s", snapshot)
	}
}

func TestOpsDashboardFromBundleServesReadOnlyHTTP(t *testing.T) {
	dir := writeCmdProductClusterBundle(t)
	addr := freeTCPAddr(t)
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run([]string{
			"ops", "dashboard",
			"--from-bundle", dir,
			"--listen", addr,
			"--serve-duration", "500ms",
		}, &stdout, &stderr)
	}()

	body := waitForHTTPContains(t, "http://"+addr+"/summary.txt", "managed_volume=pvc-product")
	if !strings.Contains(body, "read_only=true") {
		t.Fatalf("summary missing read_only=true:\n%s", body)
	}
	snapshot := waitForHTTPContains(t, "http://"+addr+"/operator-snapshot.json", `"read_only": true`)
	if !strings.Contains(snapshot, `"mutation_allowed": false`) {
		t.Fatalf("operator snapshot missing read-only boundary:\n%s", snapshot)
	}
	postResp, err := http.Post("http://"+addr+"/", "application/json", strings.NewReader(`{"action":"promote"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer postResp.Body.Close()
	if postResp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("post status=%d", postResp.StatusCode)
	}

	select {
	case code := <-done:
		if code != ops.VolumeStatusExitOK {
			t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("dashboard command did not stop; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "dashboard_status=ok") ||
		!strings.Contains(stdout.String(), "read_only=true") {
		t.Fatalf("stdout missing dashboard summary:\n%s", stdout.String())
	}
}

func TestOpsDashboardMasterAPIServesLiveClusterEvidence(t *testing.T) {
	masterAddr, closeMaster := startCmdFakeMaster(t)
	defer closeMaster()
	addr := freeTCPAddr(t)
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run([]string{
			"ops", "dashboard",
			"--master-api", masterAddr,
			"--listen", addr,
			"--serve-duration", "1500ms",
		}, &stdout, &stderr)
	}()

	body := waitForHTTPContains(t, "http://"+addr+"/cluster-evidence.json", `"event_type": "csi_reattach_observed"`)
	if !strings.Contains(body, `"managed_volumes"`) {
		t.Fatalf("cluster evidence missing managed_volumes:\n%s", body)
	}
	snapshot := waitForHTTPContains(t, "http://"+addr+"/operator-snapshot.json", `"read_only": true`)
	if !strings.Contains(snapshot, `"api_version": "block.seaweedfs.com/v1alpha1"`) {
		t.Fatalf("operator snapshot missing api version:\n%s", snapshot)
	}

	select {
	case code := <-done:
		if code != ops.VolumeStatusExitOK {
			t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("dashboard command did not stop; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestOpsTimelineVolumeFromBundleJSONL(t *testing.T) {
	dir := writeCmdObservationBundle(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "timeline", "volume", "--from-bundle", dir, "-o", "jsonl", "pvc-observed"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), `"event_type":"authority_published"`) ||
		!strings.Contains(stdout.String(), `"reason_code":"candidate_covers_required_frontier"`) {
		t.Fatalf("stdout missing timeline jsonl:\n%s", stdout.String())
	}
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitForHTTPContains(t *testing.T, url, want string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusOK && strings.Contains(string(body), want) {
			return string(body)
		}
		lastErr = fmt.Errorf("status=%d body=%s", resp.StatusCode, body)
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s containing %q: %v", url, want, lastErr)
	return ""
}

func TestOpsExplainVolumeFromBundleBlockedImagePull(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "demo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "demo", ops.KubeSystemPodsDeploysArtifact), []byte(`NAME READY STATUS RESTARTS AGE IP NODE
pod/sw-block-csi-node-n948t 0/2 Init:ErrImagePull 0 2m3s 192.168.1.184 m02
deployment.apps/sw-block-csi-controller 1/1 1 1 2m3s block-csi sw-block-csi:local
`), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "explain", "volume", "--from-bundle", dir, "pvc-blocked"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"volume pvc-blocked status=blocked rf=3 reason=csi_node_image_pull_failed",
		"waiting=ImagePullBackOff on node m02 image sw-block-csi:local",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestOpsDescribeVolumeLiveInventory(t *testing.T) {
	oldRunCommand := opsInventoryRunCommand
	opsInventoryRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl -n default get pvc -o json":                          cmdSinglePVCListJSON,
		"kubectl get pv -o json":                                      cmdSinglePVListJSON,
		"kubectl -n default get deploy -l app=sw-blockvolume -o json": cmdSingleDeploymentListJSON,
	})
	defer func() { opsInventoryRunCommand = oldRunCommand }()

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "describe", "volume", "pvc-live", "--namespace", "default", "--product-revision", "product-rev"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		"cluster status=ok volumes=1 nodes=0",
		"volume pvc-live status=ok rf=1 ack=best-effort",
		"pvc default/app-live",
		"primary r1 on m02 frontend=127.0.0.1:3260",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestOpsClusterAndVolumesLiveInventory(t *testing.T) {
	oldRunCommand := opsInventoryRunCommand
	opsInventoryRunCommand = fixtureCmdKubectl(map[string]string{
		"kubectl -n default get pvc -o json":                          cmdSinglePVCListJSON,
		"kubectl get pv -o json":                                      cmdSinglePVListJSON,
		"kubectl -n default get deploy -l app=sw-blockvolume -o json": cmdSingleDeploymentListJSON,
	})
	defer func() { opsInventoryRunCommand = oldRunCommand }()

	var clusterOut, clusterErr bytes.Buffer
	code := run([]string{"ops", "cluster", "--namespace", "default", "--product-revision", "product-rev"}, &clusterOut, &clusterErr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("cluster exit=%d stderr=%s stdout=%s", code, clusterErr.String(), clusterOut.String())
	}
	if !strings.Contains(clusterOut.String(), "cluster status=ok volumes=1 nodes=0") ||
		!strings.Contains(clusterOut.String(), "volume pvc-live status=ok rf=1 ack=best-effort") {
		t.Fatalf("cluster output missing evidence:\n%s", clusterOut.String())
	}

	var volumesOut, volumesErr bytes.Buffer
	code = run([]string{"ops", "volumes", "--namespace", "default", "--product-revision", "product-rev"}, &volumesOut, &volumesErr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("volumes exit=%d stderr=%s stdout=%s", code, volumesErr.String(), volumesOut.String())
	}
	if !strings.Contains(volumesOut.String(), "volume pvc-live status=ok rf=1 primary=r1 node=m02 frontend=127.0.0.1:3260") {
		t.Fatalf("volumes output missing evidence:\n%s", volumesOut.String())
	}
}

func cleanCmdResidueCommand(_ context.Context, name string, args ...string) ([]byte, error) {
	switch name {
	case "iscsiadm":
		return []byte("iscsiadm: No active sessions.\n"), errors.New("exit status 21")
	case "nvme":
		return []byte(`{"Subsystems":[]}`), nil
	case "ps", "tasklist":
		return []byte("PID ARGS\n1 unrelated\n"), nil
	default:
		return nil, fmt.Errorf("unexpected command %s", name)
	}
}

const cmdHelmNodeWide = `m01    Ready                      worker          10d   v1.34.4+k3s1   192.168.1.181   <none>        Ubuntu 24.04   6.8.0   containerd://2.0.0
m02    Ready                      control-plane   10d   v1.34.4+k3s1   192.168.1.184   <none>        Ubuntu 24.04   6.8.0   containerd://2.0.0
tp01   Ready                      worker          10d   v1.34.4+k3s1   192.168.1.188   <none>        Ubuntu 24.04   6.8.0   containerd://2.0.0
bad1   Ready,SchedulingDisabled   worker          10d   v1.34.4+k3s1   192.168.1.199   <none>        Ubuntu 24.04   6.8.0   containerd://2.0.0
bad2   NotReady                   worker          10d   v1.34.4+k3s1   192.168.1.200   <none>        Ubuntu 24.04   6.8.0   containerd://2.0.0
bad3   Ready                      worker          10d   v1.34.4+k3s1   127.0.0.1       <none>        Ubuntu 24.04   6.8.0   containerd://2.0.0
`

const cmdSinglePVCListJSON = `{
  "items": [
    {"metadata":{"name":"app-live","namespace":"default","uid":"uid-live"},"spec":{"volumeName":"pvc-live","storageClassName":"sw-block-dynamic"},"status":{"phase":"Bound"}}
  ]
}`

const cmdSinglePVListJSON = `{
  "items": [
    {"metadata":{"name":"pvc-live"},"spec":{"claimRef":{"namespace":"default","name":"app-live","uid":"uid-live"},"csi":{"driver":"block.csi.seaweedfs.com","volumeHandle":"pvc-live"}}}
  ]
}`

const cmdSingleDeploymentListJSON = `{
  "items": [
    {
      "metadata":{
        "name":"sw-blockvolume-pvc-live-r1",
        "namespace":"default",
        "labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-live","sw-block.seaweedfs.com/replica":"r1"},
        "ownerReferences":[{"kind":"PersistentVolumeClaim","name":"app-live","uid":"uid-live"}]
      },
      "spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"m02"},"containers":[{"name":"blockvolume","args":["--server-id=m02","--volume-id=pvc-live","--replica-id=r1","--data-addr=127.0.0.1:19101","--ctrl-addr=127.0.0.1:19102","--status-addr=127.0.0.1:23260","--iscsi-listen=127.0.0.1:3260","--iscsi-iqn=iqn.2026-05.io.seaweedfs:pvc-live"]}]}}},
      "status":{"replicas":1,"readyReplicas":1}
    }
  ]
}`

func writeCmdObservationBundle(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	inventoryDir := filepath.Join(dir, "demo", "ops-inventory-reader-verified")
	if err := os.MkdirAll(inventoryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	inventory := ops.BuildVolumeInventory(ops.VolumeInventoryInput{
		ProductRevision: "product-rev",
		Volumes: []ops.VolumeInventoryVolumeInput{{
			VolumeID:          "pvc-observed",
			Namespace:         "default",
			PVCName:           "sw-block-demo-pvc",
			PVName:            "pvc-observed",
			ReplicationFactor: 3,
			Replicas: []ops.VolumeInventoryReplicaInput{
				{
					ReplicaID:              "r2",
					ServerID:               "node-loss-r2",
					NodeName:               "m02",
					Observed:               true,
					Protocol:               "iscsi",
					FrontendAddress:        "192.168.1.184:3260",
					AuthorityRole:          "primary",
					ReplicationRole:        "none",
					Healthy:                true,
					FrontendPrimaryReady:   true,
					Epoch:                  2,
					EndpointVersion:        1,
					AckProfile:             ops.PromotionAckProfileSyncQuorum,
					ClaimProfile:           ops.PromotionClaimBetaRecovery,
					DurableLatched:         true,
					DurableOperational:     true,
					RequiredFrontierKnown:  true,
					RequiredFrontierLSN:    52,
					CandidateFrontierKnown: true,
					CandidateFrontierLSN:   52,
				},
				{
					ReplicaID:              "r3",
					ServerID:               "node-loss-r3",
					NodeName:               "tp01",
					Observed:               true,
					Protocol:               "iscsi",
					FrontendAddress:        "192.168.1.188:3260",
					AuthorityRole:          "unknown",
					ReplicationRole:        "replica_ready",
					AckProfile:             ops.PromotionAckProfileSyncQuorum,
					ClaimProfile:           ops.PromotionClaimBetaRecovery,
					DurableLatched:         true,
					DurableOperational:     true,
					RequiredFrontierKnown:  true,
					RequiredFrontierLSN:    52,
					CandidateFrontierKnown: true,
					CandidateFrontierLSN:   52,
				},
			},
		}},
	})
	raw, err := ops.MarshalObservationJSON(inventory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inventoryDir, ops.VolumeInventoryArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "demo", ops.NodeLossRecoverySummaryArtifact), []byte(strings.Join([]string{
		"result=promoted",
		"ack_profile=sync-quorum",
		"promoted=r2@m02",
		"before_frontend=192.168.1.181:3260",
		"after_frontend=192.168.1.184:3260",
		"reader_verified=true",
		"pod_recreate_used=true",
		"old_primary_stale_io_success_count=0",
	}, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "demo", ops.ControlPlaneTimelineArtifact), []byte("event=authority_published from=r1 to=r2 primary=r2 primary_count=1 volume=pvc-observed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func writeCmdProductClusterBundle(t *testing.T) string {
	t.Helper()
	dir := writeCmdObservationBundle(t)
	productDir := filepath.Join(dir, "demo", "product-observation")
	if err := os.MkdirAll(productDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cluster := ops.NewClusterEvidence(time.Date(2026, 5, 17, 20, 0, 0, 0, time.UTC))
	cluster.ProductRevision = "product-cluster-rev"
	cluster.Status = ops.ObservationStatusOK
	cluster.Nodes = []ops.NodeEvidence{{
		NodeName:      "m02",
		InternalIP:    "192.168.1.184",
		Schedulable:   true,
		Ready:         true,
		ReplicaCount:  1,
		MissingImages: nil,
	}}
	cluster.Volumes = []ops.VolumeEvidence{{
		VolumeID:          "pvc-product",
		Namespace:         "default",
		PVCName:           "sw-block-example-pvc",
		ReplicationFactor: 3,
		AckProfile:        ops.PromotionAckProfileSyncQuorum,
		DesiredReplicas:   3,
		ObservedReplicas:  3,
		Status:            ops.ObservationStatusOK,
		PrimaryReplica:    "r2",
		PrimaryNode:       "m02",
		PublishTarget:     "192.168.1.184:3260",
		Replicas: []ops.ReplicaEvidence{{
			ReplicaID:      "r2",
			KubernetesNode: "m02",
			Observed:       true,
			Role:           "primary",
			FrontendAddr:   "192.168.1.184:3260",
		}},
	}}
	cluster.Events = []ops.ClusterEvent{{
		EventID:   "master-1",
		EventTime: time.Date(2026, 5, 17, 20, 1, 0, 0, time.UTC),
		VolumeID:  "pvc-product",
		ReplicaID: "r2",
		Type:      ops.EventTypeCSIReattachObserved,
		Severity:  "info",
		Reason:    ops.EventTypeCSIReattachObserved,
		Message:   "CSI staged volume on node",
		NewValue:  "192.168.1.184:3260",
	}}
	raw, err := ops.MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(productDir, ops.ClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func writeCmdReturnedReplicaBundle(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	productDir := filepath.Join(dir, "demo", "product-observation")
	if err := os.MkdirAll(productDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cluster := ops.NewClusterEvidence(time.Date(2026, 6, 19, 8, 40, 0, 0, time.UTC))
	cluster.ProductRevision = "phase46-test"
	cluster.Status = ops.ObservationStatusRecovering
	cluster.Volumes = []ops.VolumeEvidence{{
		VolumeID:              "pvc-returned",
		Namespace:             "default",
		PVCName:               "returned-pvc",
		ReplicationFactor:     2,
		Status:                ops.ObservationStatusRecovering,
		PrimaryReplica:        "r2",
		PrimaryNode:           "m02",
		PublishTarget:         "192.168.1.184:3260",
		Epoch:                 2,
		EndpointVersion:       1,
		RequiredFrontierKnown: true,
		RequiredFrontierLSN:   4241,
		Replicas: []ops.ReplicaEvidence{{
			ReplicaID:            "r1",
			KubernetesNode:       "m01",
			Observed:             true,
			Role:                 "previous_primary",
			ReplicationRole:      "replica_ready",
			Healthy:              false,
			FrontendPrimaryReady: false,
			DurableFrontierKnown: true,
			DurableFrontierLSN:   4241,
			StalePrimaryFenced:   true,
			SupportBundlePath:    "returned-replica-summary.txt",
		}, {
			ReplicaID:            "r2",
			KubernetesNode:       "m02",
			Observed:             true,
			Role:                 "primary",
			Healthy:              true,
			FrontendPrimaryReady: true,
			ReplicationRole:      "none",
			FrontendAddr:         "192.168.1.184:3260",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   4241,
		}},
	}}
	raw, err := ops.MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(productDir, ops.ClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func fixtureCmdKubectl(outputs map[string]string) func(context.Context, string, ...string) ([]byte, error) {
	return func(_ context.Context, name string, args ...string) ([]byte, error) {
		key := strings.TrimSpace(name + " " + strings.Join(args, " "))
		out, ok := outputs[key]
		if !ok {
			return nil, fmt.Errorf("unexpected command %q", key)
		}
		return []byte(out), nil
	}
}

func TestOpsStatusMissingRequiredArgsReturnsInvalid(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "status", "--volume", "v1"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want %d", code, ops.VolumeStatusExitInvalid)
	}
	if !strings.Contains(stderr.String(), "--volume and --out are required") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

func TestOpsStatusRequiresMasterAndStatusAddr(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "status", "--volume", "v1", "--out", t.TempDir(), "--status-addr", "127.0.0.1:1"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want %d", code, ops.VolumeStatusExitInvalid)
	}
	if !strings.Contains(stderr.String(), "--master and --status-addr are both required") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

func TestOpsClusterReadsMasterAPIProductEvents(t *testing.T) {
	masterAddr, closeMaster := startCmdFakeMaster(t)
	defer closeMaster()

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "cluster", "--master-api", masterAddr, "-o", "json"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	var cluster ops.ClusterEvidence
	if err := json.Unmarshal(stdout.Bytes(), &cluster); err != nil {
		t.Fatalf("json: %v\n%s", err, stdout.String())
	}
	if len(cluster.Events) != 1 || cluster.Events[0].Type != ops.EventTypeCSIReattachObserved {
		t.Fatalf("events=%+v", cluster.Events)
	}
	if len(cluster.Volumes) != 1 || cluster.Volumes[0].VolumeID != "v1" || cluster.Volumes[0].PrimaryReplica != "r2" {
		t.Fatalf("volumes=%+v", cluster.Volumes)
	}
	if len(cluster.ManagedVolumes) != 1 || cluster.ManagedVolumes[0].VolumeID != "v1" {
		t.Fatalf("managed_volumes=%+v", cluster.ManagedVolumes)
	}
	if cluster.ManagedVolumes[0].States.Authority != ops.ManagedVolumeAuthorityPrimaryAvailable {
		t.Fatalf("managed volume=%+v", cluster.ManagedVolumes[0])
	}
}

func TestOpsClusterMasterAPIRejectsUnexpectedArgs(t *testing.T) {
	masterAddr, closeMaster := startCmdFakeMaster(t)
	defer closeMaster()

	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "cluster", "--master-api", masterAddr, "extra"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want invalid stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

type cmdFakeEvidenceServer struct {
	control.UnimplementedEvidenceServiceServer
}

func (cmdFakeEvidenceServer) QueryVolumeStatus(context.Context, *control.StatusRequest) (*control.StatusResponse, error) {
	return &control.StatusResponse{
		VolumeId:        "v1",
		ReplicaId:       "r1",
		Epoch:           7,
		EndpointVersion: 2,
		Assigned:        true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.io.seaweedfs:v1", Lun: 0},
		},
	}, nil
}

type cmdFakeClusterEvidenceServer struct {
	control.UnimplementedClusterEvidenceServiceServer
}

func (cmdFakeClusterEvidenceServer) GetClusterStatus(context.Context, *control.GetClusterStatusRequest) (*control.ClusterStatusResponse, error) {
	return &control.ClusterStatusResponse{
		SchemaVersion:   ops.ObservationSchemaVersion,
		CapturedAt:      timestamppb.Now(),
		ProductRevision: "test-rev",
		Status:          ops.ObservationStatusOK,
		Volumes: []*control.VolumeEvidence{{
			VolumeId:          "v1",
			ReplicationFactor: 3,
			DesiredReplicas:   3,
			ObservedReplicas:  3,
			Status:            ops.ObservationStatusOK,
			PrimaryReplica:    "r2",
			PrimaryNode:       "m02",
			PublishTarget:     "192.168.1.184:3260",
			Epoch:             2,
			EndpointVersion:   1,
		}},
		Events: []*control.ClusterEvent{{
			EventId:         "master-1",
			EventTime:       timestamppb.Now(),
			VolumeId:        "v1",
			ReplicaId:       "r2",
			NodeName:        "m02",
			EventType:       ops.EventTypeCSIReattachObserved,
			Severity:        "info",
			Message:         "CSI staged volume on node",
			ReasonCode:      ops.EventTypeCSIReattachObserved,
			NewValue:        "192.168.1.184:3260",
			Epoch:           2,
			EndpointVersion: 1,
			EvidenceRef:     "csi-node",
		}},
	}, nil
}

func startCmdFakeMaster(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	control.RegisterEvidenceServiceServer(srv, cmdFakeEvidenceServer{})
	control.RegisterClusterEvidenceServiceServer(srv, cmdFakeClusterEvidenceServer{})
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), func() {
		srv.Stop()
		_ = ln.Close()
	}
}

func writeCmdJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatal(err)
	}
}
