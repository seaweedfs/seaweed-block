package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestG15d_K8sBlockStack_HasLauncherButNoPrecreatedBlockvolume(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "g15d", "block-stack.yaml")
	for _, want := range []string{
		"--launcher-loop-interval=100ms",
		"--launcher-manifest-dir=/manifests",
		"__NODE_NAME__",
		"pools:",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("block-stack missing %q", want)
		}
	}
	if strings.Contains(body, "name: sw-blockvolume-r1") || strings.Contains(body, "/usr/local/bin/blockvolume") {
		t.Fatalf("G15d block stack must not precreate blockvolume workloads:\n%s", body)
	}
}

func TestG15d_K8sCSIController_IncludesExternalProvisioner(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "g15d", "csi-controller.yaml")
	for _, want := range []string{
		"name: csi-provisioner",
		"registry.k8s.io/sig-storage/csi-provisioner:",
		"name: csi-attacher",
		"--csi-address=/csi/csi.sock",
		"--extra-create-metadata=true",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("csi-controller missing %q", want)
		}
	}
}

func TestAlphaK8sCSIController_IncludesCreateMetadata(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "alpha", "csi-controller.yaml")
	if !strings.Contains(body, "--extra-create-metadata=true") {
		t.Fatalf("alpha csi-controller must enable PVC metadata propagation:\n%s", body)
	}
}

func TestG15d_K8sDynamicPVC_UsesStorageClassNoPrecreatedPV(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "g15d", "dynamic-pvc-pod.yaml")
	for _, want := range []string{
		"kind: StorageClass",
		"provisioner: block.csi.seaweedfs.com",
		"name: sw-block-dynamic-v1",
		"sha256sum -c /data/payload.sha256",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dynamic-pvc-pod missing %q", want)
		}
	}
	if strings.Contains(body, "kind: PersistentVolume\n") {
		t.Fatalf("dynamic PVC scenario must not precreate PV:\n%s", body)
	}
}

func TestG15d_K8sRunner_AppliesLauncherGeneratedManifest(t *testing.T) {
	body := g15dReadFile(t, "scripts", "run-g15d-k8s-dynamic.sh")
	for _, want := range []string{
		"generated-blockvolume.yaml",
		"SW_BLOCK_LAUNCHER_PVC_OWNER_REF",
		"SW_BLOCK_ISCSI_CHAP_USERNAME",
		"SW_BLOCK_ISCSI_CHAP_SECRET",
		"SW_BLOCK_FRONTEND_PROTOCOL",
		"csi.storage.k8s.io/node-stage-secret-name",
		`sw-block.seaweedfs.com/protocol: \"nvme\"`,
		`protocol: \"nvme\"`,
		"frontend_protocol=$FRONTEND_PROTOCOL",
		"expected_git_revision=${EXPECTED_GIT_REVISION:-unknown}",
		"blockmaster.version.txt",
		"blockcsi.version.txt",
		"blockvolume.version.txt",
		"kube-system-imageids.txt",
		"image revision mismatch",
		"delete-storageclass-before-apply.log",
		"storageclass.live.yaml",
		"live StorageClass missing NVMe protocol parameter",
		"lifecycle-volumes.json",
		"verified generated blockvolume frontend protocol=nvme",
		"generated blockvolume manifest rendered iSCSI args while frontend_protocol=nvme",
		"--launcher-iscsi-chap-secret-name",
		"BLOCKVOLUME_NAMESPACE=\"kube-system\"",
		"--kubernetes-pvc-uid-lookup",
		"--launcher-pvc-owner-ref",
		"apply iSCSI CHAP Secret",
		"kubectl apply -f \"$ARTIFACT_DIR/generated-blockvolume.yaml\"",
		"kubectl -n \"$BLOCKVOLUME_NAMESPACE\" wait --for=condition=available deploy -l app=sw-blockvolume",
		"kubectl -n \"$BLOCKVOLUME_NAMESPACE\" logs -l sw-block.seaweedfs.com/volume",
		"kubectl -n \"$NAMESPACE\" delete pvc sw-block-dynamic-v1",
		"wait for launcher manifest cleanup after DeleteVolume",
		"delete generated blockvolume Deployment after manifest cleanup",
		"wait for Kubernetes GC to delete PVC-owned blockvolume Deployment",
		"iscsi-sessions.after-delete.txt",
		"nvme-list-subsys.after-delete.json",
		"PASS: dynamic PVC create/delete completed checksum write/read and cleanup",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("runner missing %q", want)
		}
	}
}

func TestG15d_K8sRunner_NVMeProtocolWrapper(t *testing.T) {
	body := g15dReadFile(t, "scripts", "run-k8s-alpha-nvme.sh")
	for _, want := range []string{
		"SW_BLOCK_FRONTEND_PROTOCOL=nvme",
		"SW_BLOCK_LAUNCHER_PVC_OWNER_REF",
		"run-alpha-k8s-dynamic.sh",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("nvme alpha wrapper missing %q:\n%s", want, body)
		}
	}
}

func TestAlphaCSINode_LoadsBothFrontendKernelModules(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "alpha", "csi-node.yaml")
	for _, want := range []string{"modprobe iscsi_tcp", "modprobe nvme_tcp", "modprobe dm_multipath", "modprobe scsi_dh_alua", "/run/udev"} {
		if !strings.Contains(body, want) {
			t.Fatalf("alpha csi-node missing %q", want)
		}
	}
}

func TestAlphaCSIDriver_AppliesFilesystemGroupOwnership(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "alpha", "csi-driver.yaml")
	for _, want := range []string{
		"kind: CSIDriver",
		"name: block.csi.seaweedfs.com",
		"fsGroupPolicy: File",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("alpha CSIDriver missing %q", want)
		}
	}
}

func TestAlphaScripts_Stage2MultipathIsOptIn(t *testing.T) {
	for _, script := range []string{"install-k8s-alpha.sh", "run-alpha-app-demo.sh"} {
		t.Run(script, func(t *testing.T) {
			body := g15dReadFile(t, "scripts", script)
			for _, want := range []string{
				"SW_BLOCK_STAGE2_MULTIPATH",
				"--stage2-multipath",
				"stage2_multipath=$STAGE2_MULTIPATH",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s missing %q", script, want)
				}
			}
		})
	}
}

func TestNodeLoss_AlphaScriptsCanRenderExplicitMultiNodeSpecs(t *testing.T) {
	for _, script := range []string{"install-k8s-alpha.sh", "run-alpha-app-demo.sh"} {
		t.Run(script, func(t *testing.T) {
			body := g15dReadFile(t, "scripts", script)
			for _, want := range []string{
				"SW_BLOCK_ALPHA_NODE_SPECS",
				"render_cluster_spec_node_specs",
				"server_id|kubernetes_node|host_or_ip|pool_id",
				"host_or_ip must be non-loopback",
				"data_addr: ${host}:${data_port}",
				"ctrl_addr: ${host}:${ctrl_port}",
				"kubernetes.io/hostname: ${node_name}",
				"expected_slots_per_volume=$EXPECTED_SLOTS_PER_VOLUME",
				"node_specs=${NODE_SPECS:-<single-node>}",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s missing %q", script, want)
				}
			}
		})
	}
}

func TestNodeLoss_AlphaScriptsExternalISCSIIsOptInWithCHAPSecret(t *testing.T) {
	for _, script := range []string{"install-k8s-alpha.sh", "run-alpha-app-demo.sh"} {
		t.Run(script, func(t *testing.T) {
			body := g15dReadFile(t, "scripts", script)
			for _, want := range []string{
				"SW_BLOCK_LAUNCHER_EXTERNAL_ISCSI",
				"SW_BLOCK_ISCSI_CHAP_SECRET_NAME",
				"--launcher-external-iscsi",
				"--launcher-iscsi-chap-secret-name=",
				"launcher_external_iscsi=$LAUNCHER_EXTERNAL_ISCSI",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s missing %q", script, want)
				}
			}
		})
	}
}

func TestNodeLoss_AlphaScriptsCanRejectLoopbackPublishTargets(t *testing.T) {
	for _, script := range []string{"install-k8s-alpha.sh", "run-alpha-app-demo.sh"} {
		t.Run(script, func(t *testing.T) {
			body := g15dReadFile(t, "scripts", script)
			for _, want := range []string{
				"SW_BLOCK_REJECT_LOOPBACK_PUBLISH_TARGETS",
				"--reject-loopback-publish-targets",
				"reject_loopback_publish_targets=$REJECT_LOOPBACK_PUBLISH_TARGETS",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s missing %q", script, want)
				}
			}
		})
	}
}

func TestNodeLoss_AlphaScriptsCanExposeExternalStatusForPromotionProbes(t *testing.T) {
	for _, script := range []string{"install-k8s-alpha.sh", "run-alpha-app-demo.sh"} {
		t.Run(script, func(t *testing.T) {
			body := g15dReadFile(t, "scripts", script)
			for _, want := range []string{
				"SW_BLOCK_LAUNCHER_EXTERNAL_STATUS",
				"SW_BLOCK_LAUNCHER_EXTERNAL_STATUS requires SW_BLOCK_LAUNCHER_EXTERNAL_ISCSI=1",
				"--launcher-external-status",
				"launcher_external_status=",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s missing %q", script, want)
				}
			}
		})
	}
}

func TestNodeLoss_AppDemoDeliversCHAPSecretToCSI(t *testing.T) {
	body := g15dReadFile(t, "scripts", "run-alpha-app-demo.sh")
	for _, want := range []string{
		"SW_BLOCK_ISCSI_CHAP_USERNAME",
		"SW_BLOCK_ISCSI_CHAP_SECRET",
		"inject_node_stage_secret_into_storageclass",
		"csi.storage.k8s.io/node-stage-secret-name",
		"csi.storage.k8s.io/node-stage-secret-namespace",
		"apply iSCSI CHAP Secret",
		"--from-literal=chapUsername",
		"--from-literal=chapSecret",
		"delete secret \"$CHAP_SECRET_NAME\"",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("run-alpha-app-demo.sh missing %q", want)
		}
	}
}

func TestNodeLoss_InstallAppliesCHAPSecretForExternalISCSI(t *testing.T) {
	body := g15dReadFile(t, "scripts", "install-k8s-alpha.sh")
	for _, want := range []string{
		"SW_BLOCK_ISCSI_CHAP_USERNAME",
		"SW_BLOCK_ISCSI_CHAP_SECRET",
		"SW_BLOCK_APP_NAMESPACE",
		"SW_BLOCK_LAUNCHER_EXTERNAL_ISCSI requires SW_BLOCK_ISCSI_CHAP_USERNAME",
		"apply iSCSI CHAP Secret",
		"--from-literal=chapUsername",
		"--from-literal=chapSecret",
		"chap_enabled=$([[ -n \"$CHAP_SECRET\" ]] && echo 1 || echo 0)",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("install-k8s-alpha.sh missing %q", want)
		}
	}
}

func TestStage2ISCSIALUAMultipathScenarioPinsOptInAndClaimBoundary(t *testing.T) {
	body := g15dReadFile(t, "testops", "scenarios", "stage2-iscsi-alua-multipath-baseline-chain.yaml")
	for _, want := range []string{
		"name: stage2-iscsi-alua-multipath-baseline-chain",
		"multipath-prereq.txt",
		"module_dm_multipath=loaded",
		"module_scsi_dh_alua=loaded",
		"SW_BLOCK_STAGE2_MULTIPATH=1",
		"SW_BLOCK_ALPHA_LOGICAL_SERVERS=3",
		"SW_BLOCK_ALPHA_EXPECTED_SLOTS_PER_VOLUME=3",
		"SW_BLOCK_ALPHA_REPLICATION_ACK=sync-quorum",
		"demo-app-pvc-writer-hold.yaml",
		"SW_BLOCK_DEMO_STOP_AFTER=writer-verified",
		"--stage2-multipath",
		"--replica-id=r1",
		"--replica-id=r2",
		"--replica-id=r3",
		"unique-iscsi-iqns.txt",
		"multipath=true",
		"staged transport=iscsi .* portals=.*127.0.0.1:3260",
		"staged transport=iscsi .* portals=.*127.0.0.1:3261",
		"staged transport=iscsi .* portals=.*127.0.0.1:3262",
		"controlled-stop-writer-verified.txt",
		"phase=writer-verified",
		"iscsi-sessions.writer-mounted.txt",
		"multipath.writer-mounted.txt",
		"sg-rtpg.writer-mounted.txt",
		"asymmetric access state",
		"transparent_failover_claimed=false",
		"next_required_gate=mounted_writer_primary_failure_without_pod_recreate",
		"bounded-waits.txt",
		"multipath -f",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("stage2 multipath scenario missing %q", want)
		}
	}
}

func TestStage2ISCSIALUAMultipathFailoverScenarioUsesMountedWriter(t *testing.T) {
	body := g15dReadFile(t, "testops", "scenarios", "stage2-iscsi-alua-multipath-failover-chain.yaml")
	for _, want := range []string{
		"name: stage2-iscsi-alua-multipath-failover-chain",
		"SW_BLOCK_STAGE2_MULTIPATH=1",
		"SW_BLOCK_ALPHA_LOGICAL_SERVERS=3",
		"SW_BLOCK_ALPHA_REPLICATION_ACK=sync-quorum",
		"SW_BLOCK_DEMO_STOP_AFTER=writer-verified",
		"SW_BLOCK_DEMO_KEEP_ON_STOP=1",
		"pod_recreate_used=false",
		"before_primary_replica=",
		"target_deployment=",
		"scale-primary-zero.log",
		"post_failure_primary_count=1",
		"timeout 180s kubectl -n default exec sw-block-demo-writer",
		"mounted_workload_checksum_passed",
		"data_check_after_failover=mounted_workload_checksum_passed",
		"multipath-before.txt",
		"multipath-after.txt",
		"sg-inq.txt",
		"sg-vpd83.txt",
		"sg-rtpg.before.txt",
		"sg-rtpg.after.txt",
		"old_primary_stale_io_success_count=0",
		"bounded_waits=pass",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("stage2 multipath failover scenario missing %q", want)
		}
	}
	if strings.Contains(body, "apply-reader") || strings.Contains(body, "demo-app-reader") || strings.Contains(body, "data_check_after_failover=reader") {
		t.Fatalf("stage2 mounted failover scenario must not use reader pod recreate")
	}
}

func TestAlphaAppDemo_WriterVerifiedStopCapturesMountedHostPathEvidence(t *testing.T) {
	body := g15dReadFile(t, "scripts", "run-alpha-app-demo.sh")
	for _, want := range []string{
		`"$DEMO_STOP_AFTER" != "writer-verified"`,
		`"$DEMO_STOP_AFTER" == "writer-verified"`,
		`capture_stage2_host_path_evidence "writer-mounted"`,
		"iscsi-sessions.${label}.txt",
		"multipath.${label}.txt",
		"sg-rtpg.${label}.txt",
		"controlled-stop-writer-verified.txt",
		"ops-inventory-writer-verified",
		"phase=writer-verified-failed",
		"writer_verified=false",
		"resources-kept: true",
		"trap - EXIT",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("run-alpha-app-demo.sh missing writer-verified host-path evidence hook %q", want)
		}
	}
}

func TestFailureBundleScenarioAssertsProcessHygieneAfterCleanup(t *testing.T) {
	body := g15dReadFile(t, "testops", "scenarios", "light-use-first-volume-failure-bundle-chain.yaml")
	finalIdx := strings.Index(body, "- name: final_asserts")
	cleanupIdx := strings.Index(body, "- name: collect_and_cleanup")
	if finalIdx < 0 || cleanupIdx < 0 || finalIdx > cleanupIdx {
		t.Fatalf("failure-bundle scenario must have final_asserts before collect_and_cleanup")
	}
	finalSection := body[finalIdx:cleanupIdx]
	if strings.Contains(finalSection, "assert_no_processes") {
		t.Fatalf("final_asserts must not assert process hygiene before cleanup; controlled stop leaves live pods:\n%s", finalSection)
	}
	cleanupSection := body[cleanupIdx:]
	for _, want := range []string{
		"scripts/uninstall-k8s-alpha.sh",
		"action: assert_no_processes",
		"blockmaster,blockvolume,blockcsi,iscsi-target",
	} {
		if !strings.Contains(cleanupSection, want) {
			t.Fatalf("cleanup section missing %q", want)
		}
	}
}

func TestAlphaScripts_OwnerRefModeInjectsBothLauncherAndCSILookupFlags(t *testing.T) {
	for _, script := range []string{"run-g15d-k8s-dynamic.sh", "run-alpha-app-demo.sh"} {
		t.Run(script, func(t *testing.T) {
			body := g15dReadFile(t, "scripts", script)
			for _, want := range []string{
				"SW_BLOCK_LAUNCHER_PVC_OWNER_REF",
				"--launcher-pvc-owner-ref",
				"--kubernetes-pvc-uid-lookup",
				"BLOCKVOLUME_NAMESPACE=\"$NAMESPACE\"",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s missing %q", script, want)
				}
			}
		})
	}
}

func TestPublicAlphaWrappers_DefaultToPVCOwnerReferenceCleanup(t *testing.T) {
	for _, script := range []string{"run-k8s-alpha.sh", "run-k8s-demo.sh"} {
		t.Run(script, func(t *testing.T) {
			body := g15dReadFile(t, "scripts", script)
			if !strings.Contains(body, `SW_BLOCK_LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}"`) {
				t.Fatalf("%s must default SW_BLOCK_LAUNCHER_PVC_OWNER_REF to 1", script)
			}
		})
	}
}

func TestInstallAlpha_DefaultsToPVCOwnerReferenceCleanup(t *testing.T) {
	body := g15dReadFile(t, "scripts", "install-k8s-alpha.sh")
	for _, want := range []string{
		`LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}"`,
		"--launcher-pvc-owner-ref",
		"--kubernetes-pvc-uid-lookup",
		`log "apply RBAC"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("install script missing %q", want)
		}
	}
}

func TestPhase20ActivationScriptPinsInstallToOperateLoop(t *testing.T) {
	body := g15dReadFile(t, "scripts", "activate-k8s-alpha.sh")
	for _, want := range []string{
		"scripts/preflight-k8s-alpha.sh",
		"scripts/build-alpha-images.sh",
		"scripts/install-k8s-alpha.sh",
		"SW_BLOCK_ACTIVATION_IMAGE_MODE",
		"SW_BLOCK_ACTIVATION_IMAGE_MODE must be local or published",
		"--local-k3s",
		"--ghcr",
		"ready_schedulable_node_count",
		"first_ready_node_with_internal_ip",
		"DAY1_NETWORK_MODE=\"external-iscsi\"",
		"SW_BLOCK_ALPHA_NODE_SPECS=\"$ALPHA_NODE_SPECS\"",
		"SW_BLOCK_LAUNCHER_EXTERNAL_ISCSI=\"$LAUNCHER_EXTERNAL_ISCSI\"",
		"SW_BLOCK_LAUNCHER_EXTERNAL_STATUS=\"$LAUNCHER_EXTERNAL_STATUS\"",
		"SW_BLOCK_ISCSI_CHAP_USERNAME=\"$CHAP_USERNAME\"",
		"csi.storage.k8s.io/node-stage-secret-name",
		"day1_network_mode=$DAY1_NETWORK_MODE",
		"phase_skip=build reason=published-images",
		"published-images.env",
		"image_digest=",
		"csi_image_digest=",
		"collect_failure_diagnostics",
		"diagnostics/failure-context.txt",
		"sw-blockmaster.previous.log",
		"activation_blocker=image_flag_mismatch",
		"remediation=republish the image from this commit or use matching sha-<commit> image tags",
		"ghcr.io/seaweedfs/seaweed-block:alpha",
		"ghcr.io/seaweedfs/seaweed-block-csi:alpha",
		"deploy/k8s/alpha/storageclass.yaml",
		"storageclass.rendered.yaml",
		"activation-summary.txt",
		"activation_status=$status",
		`write_summary "ok"`,
		"failed_phase=",
		"image_mode=$IMAGE_MODE",
		"master_ready_replicas=",
		"csi_controller_ready_replicas=",
		"csi_node_ready=",
		"storageclass_provider=",
		"next_create_volume=kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml",
		"next_status=kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333",
		"non_claims=alpha_only,no_backup_restore,no_upgrade_safety,no_mutating_dashboard_actions,no_broad_performance_slo",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("activation script missing %q", want)
		}
	}
}

func TestPhase20ActivationScenarioPinsDay1InstallGate(t *testing.T) {
	body := g15dReadFile(t, "testops", "scenarios", "activation-day1-install-chain.yaml")
	for _, want := range []string{
		"name: activation-day1-install-chain",
		"scripts/activate-k8s-alpha.sh",
		"activation-summary.txt",
		"^activation_status=ok$",
		"^master_ready_replicas=1$",
		"^csi_controller_ready_replicas=1$",
		"^csi_node_ready=[1-9][0-9]*/[1-9][0-9]*$",
		"^storageclass_provider=block.csi.seaweedfs.com$",
		"next_create_volume=kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml",
		"readiness.txt",
		"scripts/uninstall-k8s-alpha.sh",
		"delete-storageclass.log",
		`pattern: "sw-block-dynamic"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("activation scenario missing %q", want)
		}
	}
}

func TestPhase20FirstVolumeScriptPinsUserLoop(t *testing.T) {
	body := g15dReadFile(t, "scripts", "run-basic-app-example.sh")
	for _, want := range []string{
		"examples/kubernetes/basic-app",
		"sw-block-example-pvc",
		"sw-block-example-writer",
		"sw-block-example-reader",
		"first-volume-summary.txt",
		"writer_verified=$writer_ok",
		"reader_verified=$reader_ok",
		"status_evidence=status/cluster-evidence.json,status/inventory",
		"cluster_evidence=status/cluster-evidence.json",
		"inventory_bundle=status/inventory",
		"cleanup_status=$CLEANUP_STATUS",
		"failed_phase=$FAILED_PHASE",
		"SW_BLOCK_BASIC_APP_CLEANUP",
		"inject_node_stage_secret_into_storageclass",
		"csi.storage.k8s.io/node-stage-secret-name",
		"node_stage_secret=",
		"SW_BLOCK_CLI",
		"command -v sw-block",
		"sw_block_cmd ops cluster --master-api",
		"sw_block_cmd ops inventory --namespace",
		"PASS: basic app PVC writer/reader loop complete",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("basic app wrapper missing %q", want)
		}
	}
}

func TestPhase20FirstVolumeScenarioPinsPublishedUserGate(t *testing.T) {
	body := g15dReadFile(t, "testops", "scenarios", "activation-day1-first-volume-chain.yaml")
	for _, want := range []string{
		"name: activation-day1-first-volume-chain",
		"SW_BLOCK_ACTIVATION_IMAGE_MODE",
		"published",
		"ghcr.io/seaweedfs/seaweed-block:sha-",
		"scripts/activate-k8s-alpha.sh",
		"scripts/run-basic-app-example.sh",
		"env.SW_BLOCK_BASIC_APP_CLEANUP",
		"first-volume-summary.txt",
		"^first_volume_status=ok$",
		"^pvc=sw-block-example-pvc$",
		"^writer_verified=true$",
		"^reader_verified=true$",
		"^cleanup_status=ok$",
		"status/cluster-evidence.json",
		"status/inventory/volume-inventory-summary.txt",
		"kubectl -n default delete pvc sw-block-example-pvc",
		"SW_BLOCK_UNINSTALL_DELETE_ALL_BLOCKVOLUMES",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("first-volume scenario missing %q", want)
		}
	}
}

func TestPhase20AlphaStorageClassManifestIsDynamicPVCDefault(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "alpha", "storageclass.yaml")
	for _, want := range []string{
		"kind: StorageClass",
		"name: sw-block-dynamic",
		"provisioner: block.csi.seaweedfs.com",
		`replicationFactor: "1"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("alpha storageclass missing %q", want)
		}
	}
	uninstall := g15dReadFile(t, "scripts", "uninstall-k8s-alpha.sh")
	for _, want := range []string{
		`STORAGECLASS_NAME="${SW_BLOCK_STORAGECLASS_NAME:-sw-block-dynamic}"`,
		"delete alpha StorageClass",
		`kubectl delete storageclass "$STORAGECLASS_NAME"`,
		`storageclasses.storage.k8s.io \"$STORAGECLASS_NAME\" not found`,
	} {
		if !strings.Contains(uninstall, want) {
			t.Fatalf("uninstall missing storageclass cleanup contract %q", want)
		}
	}
}

func TestAlphaBlockStack_EnablesProductOwnedLauncherApply(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "alpha", "block-stack.yaml")
	for _, want := range []string{
		"serviceAccountName: sw-block-master",
		"hostNetwork: true",
		"dnsPolicy: ClusterFirstWithHostNet",
		"--launcher-kubernetes-apply",
		"--launcher-status",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("alpha block stack missing %q", want)
		}
	}
	rbac := g15dReadFile(t, "deploy", "k8s", "alpha", "rbac.yaml")
	for _, want := range []string{
		"name: sw-block-master",
		"name: sw-block-master-launcher",
		`resources: ["deployments"]`,
		`verbs: ["get", "list", "watch", "create", "delete", "update", "patch"]`,
	} {
		if !strings.Contains(rbac, want) {
			t.Fatalf("alpha rbac missing %q", want)
		}
	}
}

func TestAlphaRBAC_MasterLauncherCanReconcilePVCOwnerNamespaces(t *testing.T) {
	rbac := g15dReadFile(t, "deploy", "k8s", "alpha", "rbac.yaml")
	for _, want := range []string{
		"kind: ClusterRole",
		"kind: ClusterRoleBinding",
		"name: sw-block-master-launcher",
		`resources: ["deployments"]`,
		`verbs: ["get", "list", "watch", "create", "delete", "update", "patch"]`,
		"kind: ServiceAccount",
		"name: sw-block-master",
		"kind: ClusterRole",
		"name: sw-block-master-launcher",
	} {
		if !strings.Contains(rbac, want) {
			t.Fatalf("alpha rbac missing %q", want)
		}
	}
	if strings.Contains(rbac, "kind: Role\nmetadata:\n  name: sw-block-master-launcher") {
		t.Fatalf("master launcher RBAC must not be namespace-local; PVC-owned blockvolumes are reconciled in app namespaces")
	}
}

func TestAlphaUninstall_DoesNotParseUnrenderedBlockStackTemplate(t *testing.T) {
	body := g15dReadFile(t, "scripts", "uninstall-k8s-alpha.sh")
	if strings.Contains(body, `kubectl delete -f "$ROOT/deploy/k8s/alpha/block-stack.yaml"`) {
		t.Fatalf("uninstall must not parse unrendered block-stack.yaml template")
	}
	for _, want := range []string{
		"delete deploy/sw-blockmaster",
		"delete svc/blockmaster",
		"delete configmap/sw-block-cluster-spec",
		"delete secret \"$CHAP_SECRET_NAME\"",
		"delete-block-stack.log",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("uninstall script missing %q", want)
		}
	}
}

func TestApplyAlphaBlockvolumes_WaitsForGeneratedManifestInsteadOfKubeSystem(t *testing.T) {
	body := g15dReadFile(t, "scripts", "apply-k8s-alpha-blockvolumes.sh")
	if !strings.Contains(body, `kubectl wait -f "$ARTIFACT_DIR/generated-blockvolume.yaml" --for=condition=available`) {
		t.Fatalf("apply script must wait using generated manifest:\n%s", body)
	}
	if strings.Contains(body, "kubectl -n kube-system wait --for=condition=available deploy -l app=sw-blockvolume") {
		t.Fatalf("apply script must not hardcode kube-system generated workloads:\n%s", body)
	}
}

func TestAlphaAppDemo_CanRestartCSINodeBeforeReader(t *testing.T) {
	body := g15dReadFile(t, "scripts", "run-alpha-app-demo.sh")
	for _, want := range []string{
		"SW_BLOCK_RESTART_CSI_NODE_BEFORE_READER",
		"SW_BLOCK_DEMO_APP_MANIFEST",
		"rollout restart ds/sw-block-csi-node",
		"restart-csi-node-status.log",
		"wait_pod_log_contains sw-block-demo-writer",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("app demo script missing %q", want)
		}
	}

	wrapper := g15dReadFile(t, "scripts", "run-k8s-csi-node-restart.sh")
	if !strings.Contains(wrapper, "demo-app-pvc-writer-hold.yaml") {
		t.Fatalf("restart wrapper must use mounted-writer manifest:\n%s", wrapper)
	}
	if !strings.Contains(wrapper, "export SW_BLOCK_RESTART_CSI_NODE_BEFORE_READER=1") {
		t.Fatalf("restart wrapper must enable restart mode:\n%s", wrapper)
	}
}

func g15dReadFile(t *testing.T, parts ...string) string {
	t.Helper()
	root := g15bRepoRoot(t)
	path := filepath.Join(append([]string{root}, parts...)...)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}
