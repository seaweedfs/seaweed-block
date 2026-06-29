package ops

import "testing"

func TestManagedVolumeProjection_HealthyFirstVolumeReady(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:          "pvc-a",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		PVName:            "pvc-a",
		StorageClass:      "sw-block",
		ReplicationFactor: 1,
		AckProfile:        "best-effort",
		PVC: &PVCFact{
			Phase: "Bound",
		},
		Authority: &AuthorityFact{
			PrimaryReplica:  "r1",
			PublishTarget:   "127.0.0.1:3260",
			Epoch:           1,
			EndpointVersion: 1,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r1",
			KubernetesNode:       "m02",
			Role:                 "primary",
			Observed:             true,
			DurableFrontierKnown: true,
			DurableFrontierLSN:   7,
			FrontendAddr:         "127.0.0.1:3260",
		}},
		CSIStages: []CSIStageFact{{
			NodeName: "m02",
			Target:   "127.0.0.1:3260",
		}},
		Workload: &WorkloadCheckFact{
			WriterVerified: true,
			ReaderVerified: true,
		},
	})

	if projection.Status != ManagedVolumeStatusReady {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.States.Kubernetes != ManagedVolumeKubernetesBound ||
		projection.States.Authority != ManagedVolumeAuthorityPrimaryAvailable ||
		projection.States.Workload != ManagedVolumeWorkloadVerified {
		t.Fatalf("states=%+v", projection.States)
	}
	if projection.PrimaryReplicaID != "r1" ||
		projection.PublishTarget != "127.0.0.1:3260" ||
		projection.AuthorityEpoch != 1 ||
		projection.AuthorityEndpointVersion != 1 {
		t.Fatalf("authority facts primary=%s target=%s epoch=%d endpoint=%d",
			projection.PrimaryReplicaID,
			projection.PublishTarget,
			projection.AuthorityEpoch,
			projection.AuthorityEndpointVersion)
	}
	if len(projection.Actions) != 1 || projection.Actions[0].Type != ManagedVolumeActionCollectBundle {
		t.Fatalf("actions=%+v", projection.Actions)
	}
	if projection.Actions[0].SideEffectClass != ManagedVolumeSideEffectObserve {
		t.Fatalf("action side effect=%s", projection.Actions[0].SideEffectClass)
	}
}

func TestManagedVolumeProjection_ProjectsNVMeMultipathIdentity(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-nvme",
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "127.0.0.1:4420",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:        "r1",
			Observed:         true,
			Role:             "primary",
			FrontendProtocol: "nvme",
			FrontendAddr:     "127.0.0.1:4421",
			FrontendNQN:      "nqn.2026-05.io.seaweedfs:pvc-nvme",
			FrontendNSID:     1,
		}, {
			ReplicaID:        "r2",
			Observed:         true,
			Role:             "replica",
			FrontendProtocol: "nvme",
			FrontendAddr:     "127.0.0.1:4420",
			FrontendNQN:      "nqn.2026-05.io.seaweedfs:pvc-nvme",
			FrontendNSID:     1,
		}},
		HostPaths: []HostPathFact{{
			Protocol: "nvme",
			ANAState: "optimized",
		}},
	})

	if projection.NVMe == nil {
		t.Fatalf("missing nvme status: %+v", projection)
	}
	if projection.NVMe.NQN != "nqn.2026-05.io.seaweedfs:pvc-nvme" ||
		projection.NVMe.NSID != 1 ||
		projection.NVMe.NVMeAddr != "127.0.0.1:4420" ||
		projection.NVMe.PathCount != 2 ||
		!projection.NVMe.MultipathObserved ||
		projection.NVMe.ANAState != "optimized" ||
		projection.NVMe.ReasonCode != "" {
		t.Fatalf("nvme=%+v", projection.NVMe)
	}
	if got := projection.NVMe.NVMeAddrs; len(got) != 2 || got[0] != "127.0.0.1:4420" || got[1] != "127.0.0.1:4421" {
		t.Fatalf("nvme addrs=%v", got)
	}
}

func TestManagedVolumeProjection_FlagsNVMePathIdentityMismatch(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-nvme",
		Replicas: []ReplicaFact{{
			ReplicaID:        "r1",
			Observed:         true,
			FrontendProtocol: "nvme",
			FrontendAddr:     "127.0.0.1:4420",
			FrontendNQN:      "nqn.2026-05.io.seaweedfs:pvc-a",
			FrontendNSID:     1,
		}, {
			ReplicaID:        "r2",
			Observed:         true,
			FrontendProtocol: "nvme",
			FrontendAddr:     "127.0.0.1:4421",
			FrontendNQN:      "nqn.2026-05.io.seaweedfs:pvc-b",
			FrontendNSID:     1,
		}},
	})

	if projection.NVMe == nil || projection.NVMe.ReasonCode != ReasonNVMePathIdentityMismatch {
		t.Fatalf("nvme=%+v", projection.NVMe)
	}
	if projection.Status != ManagedVolumeStatusBlocked || projection.ReasonCode != ReasonNVMePathIdentityMismatch {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
}

func TestManagedVolumeProjection_BlocksMissingNVMeMultipath(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:          "pvc-nvme",
		ReplicationFactor: 2,
		PVC: &PVCFact{
			Phase: "Bound",
		},
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "127.0.0.1:4420",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:        "r1",
			Observed:         true,
			Role:             "primary",
			FrontendProtocol: "nvme",
			FrontendAddr:     "127.0.0.1:4420",
			FrontendNQN:      "nqn.2026-05.io.seaweedfs:pvc-nvme",
			FrontendNSID:     1,
		}},
		CSIStages: []CSIStageFact{{
			NodeName: "m02",
			Target:   "/var/lib/kubelet/plugins/kubernetes.io/csi",
		}},
		Workload: &WorkloadCheckFact{
			WriterVerified: true,
			ReaderVerified: true,
		},
	})

	if projection.NVMe == nil || projection.NVMe.ReasonCode != ReasonNVMeMultipathPathMissing {
		t.Fatalf("nvme=%+v", projection.NVMe)
	}
	if projection.Status != ManagedVolumeStatusBlocked || projection.ReasonCode != ReasonNVMeMultipathPathMissing {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
}

func TestManagedVolumeProjection_BlocksMissingNVMeMultipathFromDesiredReplicas(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:        "pvc-nvme",
		DesiredReplicas: 2,
		Authority: &AuthorityFact{
			PrimaryReplica: "r2",
			PublishTarget:  "127.0.0.1:4421",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:        "r2",
			Observed:         true,
			Role:             "primary",
			FrontendProtocol: "nvme",
			FrontendAddr:     "127.0.0.1:4421",
			FrontendNQN:      "nqn.2026-05.io.seaweedfs:pvc-nvme",
			FrontendNSID:     1,
		}},
	})

	if projection.NVMe == nil || projection.NVMe.PathCount != 1 || projection.NVMe.ReasonCode != ReasonNVMeMultipathPathMissing {
		t.Fatalf("nvme=%+v", projection.NVMe)
	}
	if projection.Status != ManagedVolumeStatusBlocked || projection.ReasonCode != ReasonNVMeMultipathPathMissing {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
}

func TestManagedVolumeProjection_LoopbackCrossNodeBlocked(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:          "pvc-a",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 1,
		PVC: &PVCFact{
			Phase: "Bound",
		},
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "127.0.0.1:3260",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Role:           "primary",
			Observed:       true,
			FrontendAddr:   "127.0.0.1:3260",
		}},
		CSIStages: []CSIStageFact{{
			NodeName: "m02",
			Target:   "127.0.0.1:3260",
		}},
		PodMounts: []PodMountFact{{
			PodName:  "writer",
			NodeName: "m02",
			Phase:    "Pending",
		}},
	})

	if projection.Status != ManagedVolumeStatusBlocked {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.ReasonCode != ReasonPublishTargetLoopbackCrossNode {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if !hasManagedVolumeAction(projection.Actions, ManagedVolumeActionCollectBundle) {
		t.Fatalf("missing collect bundle action: %+v", projection.Actions)
	}
	if !hasManagedVolumeAction(projection.Actions, ManagedVolumeActionReinstallExternalISCSI) {
		t.Fatalf("missing reinstall external iscsi action: %+v", projection.Actions)
	}
	for _, action := range projection.Actions {
		if action.Mode != ManagedVolumeActionModeDryRun && action.SideEffectClass != ManagedVolumeSideEffectObserve {
			t.Fatalf("unexpected executable action: %+v", action)
		}
	}
}

func TestManagedVolumeProjection_PVCPendingBlocked(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-pending",
		PVC: &PVCFact{
			Phase: "Pending",
		},
	})

	if projection.Status != ManagedVolumeStatusBlocked {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.ReasonCode != ReasonPVCUnbound {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if !hasManagedVolumeAction(projection.Actions, ManagedVolumeActionWaitForPVCBound) {
		t.Fatalf("missing wait action: %+v", projection.Actions)
	}
}

func TestManagedVolumeProjection_WriterFailedMountBlocked(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-mount",
		PVC: &PVCFact{
			Phase: "Bound",
		},
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "192.168.1.181:3260",
		},
		PodMounts: []PodMountFact{{
			PodName:  "writer",
			NodeName: "m02",
			Phase:    "Pending",
			Reason:   ReasonWriterMountFailed,
			Message:  "MountVolume.MountDevice failed",
		}},
	})

	if projection.Status != ManagedVolumeStatusBlocked {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.ReasonCode != ReasonWriterMountFailed {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if !hasManagedVolumeAction(projection.Actions, ManagedVolumeActionCollectBundle) {
		t.Fatalf("missing collect bundle action: %+v", projection.Actions)
	}
}

func TestManagedVolumeProjection_CSINodeImagePullBlocked(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-image",
		PVC: &PVCFact{
			Phase: "Bound",
		},
		KubernetesNodes: []KubernetesNodeFact{{
			NodeName:     "m02",
			Ready:        true,
			CSINodeReady: false,
			Reason:       ReasonCSINodeImagePullFailed,
			Message:      "sw-block-csi-node ImagePullBackOff",
		}},
	})

	if projection.Status != ManagedVolumeStatusBlocked {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.ReasonCode != ReasonCSINodeImagePullFailed {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if !hasManagedVolumeAction(projection.Actions, ManagedVolumeActionImportCSIImage) {
		t.Fatalf("missing image import action: %+v", projection.Actions)
	}
	for _, action := range projection.Actions {
		if action.Type == ManagedVolumeActionImportCSIImage && action.Mode != ManagedVolumeActionModeDryRun {
			t.Fatalf("image action must be dry-run: %+v", action)
		}
	}
}

func TestManagedVolumeProjection_TransparentISCSIALUARecovered(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:          "pvc-stage2",
		ReplicationFactor: 3,
		AckProfile:        "sync-quorum",
		PVC: &PVCFact{
			Phase: "Bound",
		},
		Authority: &AuthorityFact{
			PrimaryReplica:  "r2",
			PreviousPrimary: "r1",
			PublishTarget:   "192.168.1.184:3261",
			Epoch:           2,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:          "r1",
			KubernetesNode:     "m01",
			Role:               "unavailable",
			Observed:           false,
			StalePrimaryFenced: true,
		}, {
			ReplicaID:      "r2",
			KubernetesNode: "m02",
			Role:           "primary",
			Observed:       true,
			FrontendAddr:   "192.168.1.184:3261",
		}, {
			ReplicaID:      "r3",
			KubernetesNode: "tp01",
			Role:           "replica",
			Observed:       true,
		}},
		HostPaths: []HostPathFact{{
			NodeName:       "m02",
			Protocol:       "iscsi",
			Target:         "192.168.1.184:3261",
			State:          HostPathStateActiveOptimized,
			MultipathReady: true,
			StaleFenced:    true,
		}},
		Workload: &WorkloadCheckFact{
			WriterVerified: true,
			ReaderVerified: true,
			SamePodUID:     true,
		},
	})

	if projection.Status != ManagedVolumeStatusRecovered {
		t.Fatalf("status=%s reason=%s states=%+v", projection.Status, projection.ReasonCode, projection.States)
	}
	if projection.ReasonCode != ReasonTransparentHostPathRecovered {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if projection.States.HostPath != ManagedVolumeHostPathTransparentReady {
		t.Fatalf("host path=%s", projection.States.HostPath)
	}
}

func TestManagedVolumeProjection_MissingMultipathBlocksTransparentClaim(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-stage2",
		PVC: &PVCFact{
			Phase: "Bound",
		},
		Authority: &AuthorityFact{
			PrimaryReplica:  "r2",
			PreviousPrimary: "r1",
			PublishTarget:   "192.168.1.184:3261",
		},
		HostPaths: []HostPathFact{{
			NodeName:       "m02",
			Protocol:       "iscsi",
			Target:         "192.168.1.184:3261",
			State:          HostPathStateSinglePath,
			MultipathReady: false,
		}},
		Workload: &WorkloadCheckFact{
			WriterVerified: true,
			ReaderVerified: true,
			SamePodUID:     true,
		},
	})

	if projection.Status != ManagedVolumeStatusBlocked {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.ReasonCode != ReasonHostPathNotMultipathed {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if !hasManagedVolumeAction(projection.Actions, ManagedVolumeActionInspectHostPath) {
		t.Fatalf("missing host path action: %+v", projection.Actions)
	}
}

func TestManagedVolumeProjection_NVMeANASeamDoesNotInferTransparentRecovery(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-nvme",
		PVC: &PVCFact{
			Phase: "Bound",
		},
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "10.0.0.1:4420",
		},
		HostPaths: []HostPathFact{{
			NodeName: "m02",
			Protocol: "nvme",
			Target:   "nqn.2026-05.io.seaweedfs:pvc-nvme",
			State:    HostPathStateANAOptimized,
			ANAState: "optimized",
		}},
	})

	if projection.States.HostPath != ManagedVolumeHostPathReady {
		t.Fatalf("host path=%s", projection.States.HostPath)
	}
	if projection.Status == ManagedVolumeStatusRecovered {
		t.Fatalf("nvme schema seam must not infer recovered without workload/failover evidence: %+v", projection)
	}
}

func TestManagedVolumeProjection_FactOrderIndependent(t *testing.T) {
	base := ManagedVolumeFacts{
		VolumeID: "pvc-order",
		PVC:      &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica:  "r2",
			PreviousPrimary: "r1",
			PublishTarget:   "192.168.1.184:3260",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:          "r1",
			KubernetesNode:     "m01",
			Role:               "unavailable",
			Observed:           false,
			StalePrimaryFenced: true,
		}, {
			ReplicaID:      "r2",
			KubernetesNode: "m02",
			Role:           "primary",
			Observed:       true,
			FrontendAddr:   "192.168.1.184:3260",
		}},
		CSIStages: []CSIStageFact{{
			NodeName: "m02",
			Target:   "192.168.1.184:3260",
			Reattach: true,
		}},
		Workload: &WorkloadCheckFact{ReaderVerified: true},
	}
	reordered := base
	reordered.Replicas = []ReplicaFact{base.Replicas[1], base.Replicas[0]}

	first := ProjectManagedVolume(base)
	second := ProjectManagedVolume(reordered)

	if first.Status != second.Status || first.ReasonCode != second.ReasonCode || first.States != second.States {
		t.Fatalf("projection depends on fact order:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func TestManagedVolumeProjection_ActionInvariantRefs(t *testing.T) {
	blocked := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-loopback",
		PVC:      &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "127.0.0.1:3260",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Role:           "primary",
			Observed:       true,
		}},
		CSIStages: []CSIStageFact{{NodeName: "m02", Target: "127.0.0.1:3260"}},
	})

	action := findManagedVolumeAction(blocked.Actions, ManagedVolumeActionReinstallExternalISCSI)
	if action == nil {
		t.Fatalf("missing reinstall action: %+v", blocked.Actions)
	}
	if !stringSliceContains(action.InvariantRefs, "INV-K8S-NONLOOPBACK-001") {
		t.Fatalf("action invariant refs=%+v", action.InvariantRefs)
	}
	if action.OwnerExecutor != "installer_or_operator" || action.SideEffectClass != ManagedVolumeSideEffectSafeK8S || action.Mode != ManagedVolumeActionModeDryRun {
		t.Fatalf("action boundary=%+v", action)
	}
	if action.Decision != ManagedVolumeActionDecisionAllowed {
		t.Fatalf("action decision=%s reason=%s missing=%v", action.Decision, action.DecisionReason, action.MissingFacts)
	}
	if action.EvidenceRequired != "loopback_cross_node_evidence" {
		t.Fatalf("evidence_required=%q", action.EvidenceRequired)
	}
}

func TestManagedVolumeProjection_NonClaimsDerivedFromFacts(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-stage2",
		PVC:      &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica: "r2",
			PublishTarget:  "192.168.1.184:3261",
		},
		HostPaths: []HostPathFact{{
			Protocol:       "iscsi",
			State:          HostPathStateActiveOptimized,
			MultipathReady: true,
			StaleFenced:    true,
		}},
		Workload: &WorkloadCheckFact{
			WriterVerified: true,
			ReaderVerified: true,
			SamePodUID:     false,
		},
	})

	if projection.Status == ManagedVolumeStatusRecovered {
		t.Fatalf("must not claim transparent recovery without same pod UID: %+v", projection)
	}
	if !stringSliceContains(projection.NonClaims, NonClaimTransparentFailover) {
		t.Fatalf("non claims=%+v", projection.NonClaims)
	}
}

func TestManagedVolumeProjection_NodeLossReattachRecovered(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:          "pvc-a",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 3,
		AckProfile:        "sync-quorum",
		PVC: &PVCFact{
			Phase: "Bound",
		},
		Authority: &AuthorityFact{
			PrimaryReplica:        "r2",
			PreviousPrimary:       "r1",
			PublishTarget:         "192.168.1.184:3260",
			Epoch:                 2,
			EndpointVersion:       1,
			RequiredFrontierKnown: true,
			RequiredFrontierLSN:   52,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:          "r1",
			KubernetesNode:     "m01",
			Role:               "unavailable",
			Observed:           false,
			StalePrimaryFenced: true,
		}, {
			ReplicaID:            "r2",
			KubernetesNode:       "m02",
			Role:                 "primary",
			Observed:             true,
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendAddr:         "192.168.1.184:3260",
		}, {
			ReplicaID:            "r3",
			KubernetesNode:       "tp01",
			Role:                 "replica",
			Observed:             true,
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
		}},
		CSIStages: []CSIStageFact{{
			NodeName:        "m02",
			Target:          "192.168.1.184:3260",
			Epoch:           2,
			EndpointVersion: 1,
			Reattach:        true,
		}},
		Workload: &WorkloadCheckFact{
			ReaderVerified: true,
		},
	})

	if projection.Status != ManagedVolumeStatusRecovered {
		t.Fatalf("status=%s reason=%s states=%+v", projection.Status, projection.ReasonCode, projection.States)
	}
	if projection.ReasonCode != ReasonCSIReattachRecovered {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if projection.States.Recovery != ManagedVolumeRecoveryRecovered {
		t.Fatalf("recovery=%s", projection.States.Recovery)
	}
	if len(projection.Actions) != 1 || projection.Actions[0].Type != ManagedVolumeActionCollectBundle {
		t.Fatalf("actions=%+v", projection.Actions)
	}
}

func TestManagedVolumeProjection_ReturnedPreviousPrimaryStaysFrontendFenced(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:          "pvc-returned",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 3,
		AckProfile:        "sync-quorum",
		PVC:               &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica:        "r2",
			PreviousPrimary:       "r1",
			PublishTarget:         "192.168.1.184:3260",
			Epoch:                 2,
			EndpointVersion:       9,
			RequiredFrontierKnown: true,
			RequiredFrontierLSN:   52,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r1",
			KubernetesNode:       "m01",
			Observed:             true,
			Role:                 "replica",
			ReplicationRole:      "ready",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			Healthy:              false,
			FrontendPrimaryReady: false,
			AckEligibilityKnown:  true,
			AckEligible:          false,
			StalePrimaryFenced:   true,
		}, {
			ReplicaID:            "r2",
			KubernetesNode:       "m02",
			Observed:             true,
			Role:                 "primary",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendAddr:         "192.168.1.184:3260",
		}},
		EvidenceRefs: []string{"returned-replica-summary.txt"},
	})

	if len(projection.ReplicaReintegrations) != 1 {
		t.Fatalf("returned replicas=%+v", projection.ReplicaReintegrations)
	}
	returned := projection.ReplicaReintegrations[0]
	if returned.ReplicaID != "r1" || returned.State != ReturnedReplicaStateFenced || returned.ReasonCode != ReasonReturnedReplicaFrontendFenced {
		t.Fatalf("returned projection=%+v", returned)
	}
	if !returned.FrontendFenced || returned.FrontendPrimaryReady || returned.AckEligible {
		t.Fatalf("returned replica must be fenced and not ack eligible: %+v", returned)
	}
	action := findManagedVolumeAction(projection.Actions, ManagedVolumeActionReintegrateReturned)
	if action == nil {
		t.Fatalf("missing reintegrate action: %+v", projection.Actions)
	}
	if action.Decision != ManagedVolumeActionDecisionAllowed || action.DecisionReason != "" {
		t.Fatalf("reintegrate action must be dry-run admitted only after fencing/frontier evidence: %+v", action)
	}
	if hasManagedVolumeAction(projection.Actions, ManagedVolumeActionFailbackReturned) {
		t.Fatalf("failback action must not appear before ACK eligibility is recorded: %+v", projection.Actions)
	}
}

func TestManagedVolumeProjection_ReturnedReplicaFailbackActionAfterAckEligibility(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-returned",
		PVC:      &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica:        "r2",
			PreviousPrimary:       "r1",
			RequiredFrontierKnown: true,
			RequiredFrontierLSN:   52,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r1",
			Observed:             true,
			Role:                 "replica",
			ReplicationRole:      "ready",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendPrimaryReady: false,
			AckEligibilityKnown:  true,
			AckEligible:          true,
			StalePrimaryFenced:   true,
		}, {
			ReplicaID:            "r2",
			KubernetesNode:       "m02",
			Observed:             true,
			Role:                 "primary",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendAddr:         "192.168.1.184:3260",
		}},
		EvidenceRefs: []string{"returned-replica-summary.txt"},
	})

	action := findManagedVolumeAction(projection.Actions, ManagedVolumeActionFailbackReturned)
	if action == nil {
		t.Fatalf("missing failback action after ACK eligibility: %+v", projection.Actions)
	}
	if action.Decision != ManagedVolumeActionDecisionRejected || action.DecisionReason != ManagedVolumeActionRejectDisabled {
		t.Fatalf("failback action must stay policy-disabled: %+v", action)
	}
}

func TestManagedVolumeProjection_ReturnedReplicaFrontendReadyBlocksVolume(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-unsafe-returned",
		PVC:      &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica:  "r2",
			PreviousPrimary: "r1",
			PublishTarget:   "192.168.1.184:3260",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r1",
			Observed:             true,
			Role:                 "replica",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendPrimaryReady: true,
		}, {
			ReplicaID: "r2",
			Observed:  true,
			Role:      "primary",
		}},
	})

	if projection.Status != ManagedVolumeStatusBlocked {
		t.Fatalf("status=%s reason=%s returned=%+v", projection.Status, projection.ReasonCode, projection.ReplicaReintegrations)
	}
	if projection.ReasonCode != ReasonReturnedReplicaUnsafeFrontend {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	ready := findObservationCondition(projection.Conditions, ConditionReady)
	if ready == nil || ready.Status == "True" {
		t.Fatalf("unsafe returned replica must not emit Ready=True: %+v", projection.Conditions)
	}
}

func TestManagedVolumeProjection_InvalidDualPrimaryBeatsReady(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-a",
		PVC: &PVCFact{
			Phase: "Bound",
		},
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "192.168.1.181:3260",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Role:           "primary",
			Observed:       true,
			FrontendAddr:   "192.168.1.181:3260",
		}, {
			ReplicaID:      "r2",
			KubernetesNode: "m02",
			Role:           "primary",
			Observed:       true,
			FrontendAddr:   "192.168.1.184:3260",
		}},
		CSIStages: []CSIStageFact{{
			NodeName: "m02",
			Target:   "192.168.1.181:3260",
		}},
		Workload: &WorkloadCheckFact{
			WriterVerified: true,
			ReaderVerified: true,
		},
	})

	if projection.Status != ManagedVolumeStatusInvalid {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.ReasonCode != ReasonMultiplePrimariesObserved {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if !hasManagedVolumeAction(projection.Actions, ManagedVolumeActionCollectBundle) {
		t.Fatalf("missing collect bundle action: %+v", projection.Actions)
	}
	if hasManagedVolumeAction(projection.Actions, ManagedVolumeActionRequestPromotion) {
		t.Fatalf("invalid state must not recommend promotion: %+v", projection.Actions)
	}
}

func hasManagedVolumeAction(actions []ManagedVolumeAction, actionType string) bool {
	return findManagedVolumeAction(actions, actionType) != nil
}

func findManagedVolumeAction(actions []ManagedVolumeAction, actionType string) *ManagedVolumeAction {
	for i := range actions {
		if actions[i].Type == actionType {
			return &actions[i]
		}
	}
	return nil
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
