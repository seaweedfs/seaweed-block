package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
