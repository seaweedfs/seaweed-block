package ops

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestObservationBundle_RendersNodeLossRecoveryStory(t *testing.T) {
	dir := t.TempDir()
	inventoryDir := filepath.Join(dir, "demo", "ops-inventory-reader-verified")
	if err := os.MkdirAll(inventoryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 16, 16, 3, 6, 0, time.UTC),
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{{
			VolumeID:          "pvc-node-loss",
			Namespace:         "default",
			PVCName:           "sw-block-demo-pvc",
			PVName:            "pvc-node-loss",
			ReplicationFactor: 3,
			Replicas: []VolumeInventoryReplicaInput{
				{
					ReplicaID:       "r1",
					ServerID:        "node-loss-r1",
					NodeName:        "m01",
					Observed:        true,
					Protocol:        "iscsi",
					FrontendAddress: "192.168.1.181:3260",
					AuthorityRole:   "unavailable",
					ReplicationRole: "unavailable",
					AckProfile:      PromotionAckProfileSyncQuorum,
					Issues:          []string{"status_endpoint_unreachable=192.168.1.181:23260"},
				},
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
					AckProfile:             PromotionAckProfileSyncQuorum,
					ClaimProfile:           PromotionClaimBetaRecovery,
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
					AckProfile:             PromotionAckProfileSyncQuorum,
					ClaimProfile:           PromotionClaimBetaRecovery,
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
	mustWriteInventory(t, inventoryDir, inventory)
	mustWrite(t, filepath.Join(dir, "demo", NodeLossRecoverySummaryArtifact), strings.Join([]string{
		"node_loss_recovery_summary_version=1",
		"result=promoted",
		"ack_profile=sync-quorum",
		"before_primary=r1@m01",
		"failed=r1@m01",
		"promoted=r2@m02",
		"before_frontend=192.168.1.181:3260",
		"after_frontend=192.168.1.184:3260",
		"pod_recreate_used=true",
		"reader_verified=true",
		"data_check_after_node_loss=reader_checksum_passed",
		"old_primary_stale_io_success_count=0",
	}, "\n"))
	mustWrite(t, filepath.Join(dir, "demo", ControlPlaneTimelineArtifact), strings.Join([]string{
		"event=primary_observed replica=r1 volume=pvc-node-loss",
		"event=candidate_evaluated replica=r2 candidate_ready=true reason=promotion_ready volume=pvc-node-loss",
		"event=authority_published from=r1 to=r2 primary=r2 primary_count=1 volume=pvc-node-loss",
		"event=data_check reader_verified=true result=reader_checksum_passed volume=pvc-node-loss",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir, VolumeID: "pvc-node-loss"})
	if err != nil {
		t.Fatal(err)
	}
	text := RenderObservationExplainText(cluster)
	for _, want := range []string{
		"volume pvc-node-loss status=ok rf=3 ack=sync-quorum",
		"primary r2 on m02 frontend=192.168.1.184:3260",
		"CSI target changed 192.168.1.181:3260 -> 192.168.1.184:3260",
		"old primary stale I/O success count is 0",
		"timeline:",
		"authority_published severity=info reason=candidate_covers_required_frontier volume=pvc-node-loss replica=r2",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}
	jsonl, err := RenderClusterEventsJSONL(cluster.Events)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jsonl, `"event_type":"authority_published"`) || !strings.Contains(jsonl, `"reason_code":"candidate_covers_required_frontier"`) {
		t.Fatalf("jsonl missing event evidence:\n%s", jsonl)
	}
}

func TestObservationBundle_RendersImagePullBlockedStoryWithoutInventory(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "demo", KubeSystemPodsDeploysArtifact), `NAME READY STATUS RESTARTS AGE IP NODE
pod/sw-block-csi-node-n948t 0/2 Init:ErrImagePull 0 2m3s 192.168.1.184 m02
deployment.apps/sw-block-csi-controller 1/1 1 1 2m3s block-csi sw-block-csi:local`)

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir, VolumeID: "pvc-blocked"})
	if err != nil {
		t.Fatal(err)
	}
	text := RenderClusterEvidenceText(cluster)
	for _, want := range []string{
		"cluster status=blocked volumes=1 nodes=0",
		"volume pvc-blocked status=blocked rf=3 reason=csi_node_image_pull_failed",
		"waiting=ImagePullBackOff on node m02 image sw-block-csi:local",
		"next action: import sw-block-csi:local to the blocked node",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}
}

func TestObservationBundle_ManagedVolumeUsesPrimaryFailureArtifactHints(t *testing.T) {
	dir := t.TempDir()
	productDir := filepath.Join(dir, "demo", "product-observation")
	if err := os.MkdirAll(productDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cluster := NewClusterEvidence(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:          "pvc-stage2",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 3,
		PrimaryReplica:    "r2",
		PublishTarget:     "192.168.1.184:3261",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r2",
			KubernetesNode: "m02",
			Role:           "primary",
			Observed:       true,
			FrontendAddr:   "192.168.1.184:3261",
		}},
	}}
	raw, err := MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(productDir, ClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, "demo", PrimaryFailureRecoveryArtifact), strings.Join([]string{
		"promoted_replica=r2",
		"data_check_after_failover=mounted_workload_checksum_passed",
		"pod_recreate_used=false",
		"old_primary_stale_io_success_count=0",
		"transparent_failover_claimed=true",
	}, "\n"))

	out, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.ManagedVolumes) != 1 {
		t.Fatalf("managed_volumes=%+v", out.ManagedVolumes)
	}
	managed := out.ManagedVolumes[0]
	if managed.Status != ManagedVolumeStatusRecovered || managed.ReasonCode != ReasonTransparentHostPathRecovered {
		t.Fatalf("managed=%+v", managed)
	}
}

func TestObservationBundle_CarriesCleanupEvidenceIntoReportSurfaces(t *testing.T) {
	dir := t.TempDir()
	writeProductClusterEvidence(t, dir, []VolumeEvidence{healthyObservationVolume()})
	mustWrite(t, filepath.Join(dir, "demo", ObservationCleanupSummaryArtifact), strings.Join([]string{
		"cleanup_status=failed",
		"k8s_residue_count=1",
		"iscsi_residue_count=2",
		"multipath_residue_count=3",
		"process_residue_count=4",
		"hostpath_residue_count=5",
		"failure_count=2",
		"failed_phase=collect_and_cleanup",
		"reason_codes=kubernetes_sw_block_resources_present,multipath_maps_present",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if cluster.Cleanup == nil {
		t.Fatalf("missing cleanup evidence")
	}
	if cluster.Cleanup.Status != "failed" || cluster.Cleanup.KubernetesResidueCount != 1 || cluster.Cleanup.MultipathResidueCount != 3 {
		t.Fatalf("cleanup evidence=%+v", cluster.Cleanup)
	}
	if len(cluster.Cleanup.ReasonCodes) != 2 || cluster.Cleanup.ReasonCodes[1] != "multipath_maps_present" {
		t.Fatalf("cleanup reason codes=%+v", cluster.Cleanup.ReasonCodes)
	}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"cleanup_status=failed",
		"k8s_residue_count=1",
		"iscsi_residue_count=2",
		"multipath_residue_count=3",
		"process_residue_count=4",
		"hostpath_residue_count=5",
		"failure_count=2",
		"failed_phase=collect_and_cleanup",
		"cleanup_evidence=",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.Cleanup == nil || snapshot.Cluster.Cleanup.Status != "failed" || snapshot.Cluster.Cleanup.FailedPhase != "collect_and_cleanup" {
		t.Fatalf("operator cleanup evidence=%+v", snapshot.Cluster.Cleanup)
	}
}

func TestObservationBundle_D6ReplayGate_FirstVolumeBlockedAndRecovery(t *testing.T) {
	cases := []struct {
		name       string
		build      func(t *testing.T, dir string)
		wantStatus string
		wantReason string
		wantEvent  string
	}{
		{
			name: "first-volume",
			build: func(t *testing.T, dir string) {
				writeProductClusterEvidence(t, dir, []VolumeEvidence{healthyObservationVolume()})
			},
			wantStatus: ManagedVolumeStatusReady,
			wantReason: ReasonFirstVolumeVerified,
			wantEvent:  "Normal",
		},
		{
			name: "blocked-image-pull",
			build: func(t *testing.T, dir string) {
				mustWrite(t, filepath.Join(dir, "demo", KubeSystemPodsDeploysArtifact), `NAME READY STATUS RESTARTS AGE IP NODE
pod/sw-block-csi-node-n948t 0/2 Init:ErrImagePull 0 2m3s 192.168.1.184 m02
deployment.apps/sw-block-csi-controller 1/1 1 1 2m3s block-csi sw-block-csi:local`)
			},
			wantStatus: ManagedVolumeStatusBlocked,
			wantReason: ReasonCSINodeImagePullFailed,
			wantEvent:  "Warning",
		},
		{
			name: "stage2-recovery",
			build: func(t *testing.T, dir string) {
				writeProductClusterEvidence(t, dir, []VolumeEvidence{{
					VolumeID:          "pvc-stage2",
					Namespace:         "default",
					PVCName:           "demo-pvc",
					ReplicationFactor: 3,
					PrimaryReplica:    "r2",
					PublishTarget:     "192.168.1.184:3261",
					Replicas: []ReplicaEvidence{{
						ReplicaID:      "r2",
						KubernetesNode: "m02",
						Role:           "primary",
						Observed:       true,
						FrontendAddr:   "192.168.1.184:3261",
					}},
				}})
				mustWrite(t, filepath.Join(dir, "demo", PrimaryFailureRecoveryArtifact), strings.Join([]string{
					"promoted_replica=r2",
					"data_check_after_failover=mounted_workload_checksum_passed",
					"pod_recreate_used=false",
					"old_primary_stale_io_success_count=0",
					"transparent_failover_claimed=true",
				}, "\n"))
			},
			wantStatus: ManagedVolumeStatusRecovered,
			wantReason: ReasonTransparentHostPathRecovered,
			wantEvent:  "Normal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.build(t, dir)
			opts := ObservationBundleOptions{Dir: dir}
			if tc.name == "blocked-image-pull" {
				opts.VolumeID = "pvc-blocked"
			}
			cluster, err := BuildObservationFromBundle(opts)
			if err != nil {
				t.Fatal(err)
			}
			if len(cluster.ManagedVolumes) != 1 {
				t.Fatalf("managed_volumes=%+v", cluster.ManagedVolumes)
			}
			managed := cluster.ManagedVolumes[0]
			if managed.Status != tc.wantStatus || managed.ReasonCode != tc.wantReason {
				t.Fatalf("managed=%+v", managed)
			}
			reportDir := filepath.Join(dir, "report")
			if err := WriteObservationReportArtifacts(reportDir, cluster); err != nil {
				t.Fatal(err)
			}
			for _, artifact := range []string{
				ObservationReportHTMLArtifact,
				ObservationReportJSONArtifact,
				ObservationReportJSONLArtifact,
				ObservationOperatorSnapshotArtifact,
				ObservationReportTextArtifact,
			} {
				if _, err := os.Stat(filepath.Join(reportDir, artifact)); err != nil {
					t.Fatalf("missing report artifact %s: %v", artifact, err)
				}
			}
			explain := RenderObservationExplainText(cluster)
			if !strings.Contains(explain, "managed_volume_condition") {
				t.Fatalf("explain missing condition:\n%s", explain)
			}
			contract := ManagedVolumeOperatorContractFromProjection(managed)
			if len(contract.Events) == 0 || contract.Events[0].Type != tc.wantEvent {
				t.Fatalf("contract events=%+v", contract.Events)
			}
			for _, action := range contract.AllowedActions {
				if action.MutationAllowed {
					t.Fatalf("mutation allowed in replay contract: %+v", action)
				}
			}
		})
	}
}

func TestObservationBundle_DashboardReplayGate_FirstVolumeBlockedAndRecovery(t *testing.T) {
	cases := []struct {
		name       string
		build      func(t *testing.T, dir string)
		wantHTML   string
		wantJSON   string
		wantText   string
		wantStatus string
	}{
		{
			name: "first-volume",
			build: func(t *testing.T, dir string) {
				writeProductClusterEvidence(t, dir, []VolumeEvidence{healthyObservationVolume()})
			},
			wantHTML:   "Managed Volumes",
			wantJSON:   `"reason_code": "first_volume_verified"`,
			wantText:   "managed_volume=pvc-healthy status=ready reason=first_volume_verified",
			wantStatus: ManagedVolumeStatusReady,
		},
		{
			name: "blocked-image-pull",
			build: func(t *testing.T, dir string) {
				mustWrite(t, filepath.Join(dir, "demo", KubeSystemPodsDeploysArtifact), `NAME READY STATUS RESTARTS AGE IP NODE
pod/sw-block-csi-node-n948t 0/2 Init:ErrImagePull 0 2m3s 192.168.1.184 m02
deployment.apps/sw-block-csi-controller 1/1 1 1 2m3s block-csi sw-block-csi:local`)
			},
			wantHTML:   ReasonCSINodeImagePullFailed,
			wantJSON:   `"reason_code": "csi_node_image_pull_failed"`,
			wantText:   "managed_volume=pvc-blocked status=blocked reason=csi_node_image_pull_failed",
			wantStatus: ManagedVolumeStatusBlocked,
		},
		{
			name: "stage2-recovery",
			build: func(t *testing.T, dir string) {
				writeProductClusterEvidence(t, dir, []VolumeEvidence{{
					VolumeID:          "pvc-stage2",
					Namespace:         "default",
					PVCName:           "demo-pvc",
					ReplicationFactor: 3,
					PrimaryReplica:    "r2",
					PublishTarget:     "192.168.1.184:3261",
					Replicas: []ReplicaEvidence{{
						ReplicaID:      "r2",
						KubernetesNode: "m02",
						Role:           "primary",
						Observed:       true,
						FrontendAddr:   "192.168.1.184:3261",
					}},
				}})
				mustWrite(t, filepath.Join(dir, "demo", PrimaryFailureRecoveryArtifact), strings.Join([]string{
					"promoted_replica=r2",
					"data_check_after_failover=mounted_workload_checksum_passed",
					"pod_recreate_used=false",
					"old_primary_stale_io_success_count=0",
					"transparent_failover_claimed=true",
				}, "\n"))
			},
			wantHTML:   ReasonTransparentHostPathRecovered,
			wantJSON:   `"reason_code": "transparent_host_path_recovered"`,
			wantText:   "managed_volume=pvc-stage2 status=recovered reason=transparent_host_path_recovered",
			wantStatus: ManagedVolumeStatusRecovered,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.build(t, dir)
			opts := ObservationBundleOptions{Dir: dir}
			if tc.name == "blocked-image-pull" {
				opts.VolumeID = "pvc-blocked"
			}
			cluster, err := BuildObservationFromBundle(opts)
			if err != nil {
				t.Fatal(err)
			}
			if len(cluster.ManagedVolumes) != 1 || cluster.ManagedVolumes[0].Status != tc.wantStatus {
				t.Fatalf("managed_volumes=%+v", cluster.ManagedVolumes)
			}

			server := httptest.NewServer(NewObservationDashboardHandler(cluster))
			defer server.Close()
			assertDashboardEndpointContains(t, server.URL+"/", tc.wantHTML)
			assertDashboardEndpointContains(t, server.URL+"/"+ObservationReportJSONArtifact, tc.wantJSON)
			assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"read_only": true`)
			assertDashboardEndpointContains(t, server.URL+"/"+ObservationReportTextArtifact, tc.wantText)

			resp, err := http.Post(server.URL+"/", "application/json", strings.NewReader(`{"action":"repair"}`))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusMethodNotAllowed || !strings.Contains(string(body), "read-only dashboard") {
				t.Fatalf("mutation response status=%d body=%s", resp.StatusCode, body)
			}
		})
	}
}

func assertDashboardEndpointContains(t *testing.T, url, want string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status=%d body=%s", url, resp.StatusCode, body)
	}
	if !strings.Contains(string(body), want) {
		t.Fatalf("%s missing %q:\n%s", url, want, body)
	}
}

func writeProductClusterEvidence(t *testing.T, dir string, volumes []VolumeEvidence) {
	t.Helper()
	productDir := filepath.Join(dir, "demo", "product-observation")
	if err := os.MkdirAll(productDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cluster := NewClusterEvidence(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))
	cluster.Volumes = volumes
	raw, err := MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(productDir, ClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustWriteInventory(t *testing.T, dir string, inventory VolumeInventory) {
	t.Helper()
	raw, err := MarshalObservationJSON(inventory)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, VolumeInventoryArtifact), string(raw))
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}
