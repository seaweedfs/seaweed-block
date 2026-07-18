package lifecycle

import (
	"fmt"
	"sort"
)

// BlockVolumeWorkloadPlan is launcher input. It describes which blockvolume
// daemons should exist for one desired volume. It is deliberately not
// authority-shaped: no epoch, endpoint_version, primary, ready, or healthy.
type BlockVolumeWorkloadPlan struct {
	VolumeID      string
	SizeBytes     uint64
	Protocol      string
	NVMeTransport string
	PVCName       string
	PVCNamespace  string
	PVCUID        string
	Replicas      []BlockVolumeReplicaWorkload
}

type BlockVolumeReplicaWorkload struct {
	ServerID             string
	KubernetesNodeName   string
	PoolID               string
	ReplicaID            string
	Source               string
	PortAssignmentPinned bool
	DataAddr             string
	CtrlAddr             string
	ISCSIListenPort      int
	ISCSIQualifiedName   string
	NVMeListenPort       int
	NVMeSubsystemNQN     string
	NVMeNSID             int
}

// PlanBlockVolumeWorkloads converts desired lifecycle state plus placement
// intent into blockvolume daemon workload intent. It does not verify
// observation, choose primary, or publish authority.
func PlanBlockVolumeWorkloads(volume VolumeRecord, placement PlacementIntent, nodes []NodeRegistration, cfg WorkloadPlanConfig) (BlockVolumeWorkloadPlan, error) {
	volume.Spec = normalizeVolumeSpec(volume.Spec)
	if err := validateSpec(volume.Spec); err != nil {
		return BlockVolumeWorkloadPlan{}, err
	}
	if placement.VolumeID != volume.Spec.VolumeID {
		return BlockVolumeWorkloadPlan{}, fmt.Errorf("%w: placement volume %q != desired volume %q", ErrInvalidVolumeSpec, placement.VolumeID, volume.Spec.VolumeID)
	}
	if err := validatePlacementIntent(placement); err != nil {
		return BlockVolumeWorkloadPlan{}, err
	}
	if cfg.ISCSIPortBase == 0 {
		cfg.ISCSIPortBase = 3260
	}
	if cfg.IQNPrefix == "" {
		cfg.IQNPrefix = "iqn.2026-05.io.seaweedfs"
	}
	if cfg.NVMePortBase == 0 {
		cfg.NVMePortBase = 4420
	}
	if cfg.NQNPrefix == "" {
		cfg.NQNPrefix = "nqn.2026-05.io.seaweedfs"
	}
	protocol := volume.Spec.Protocol
	if protocol == "" {
		protocol = "iscsi"
	}
	nodeByID := make(map[string]NodeRegistration, len(nodes))
	for _, node := range nodes {
		nodeByID[node.ServerID] = node
	}

	out := BlockVolumeWorkloadPlan{
		VolumeID:      volume.Spec.VolumeID,
		SizeBytes:     volume.Spec.SizeBytes,
		Protocol:      protocol,
		NVMeTransport: volume.Spec.FrontendTransport,
		PVCName:       volume.Spec.PVCName,
		PVCNamespace:  volume.Spec.PVCNamespace,
		PVCUID:        volume.Spec.PVCUID,
		Replicas:      make([]BlockVolumeReplicaWorkload, 0, len(placement.Slots)),
	}
	for i, slot := range placement.Slots {
		node, ok := nodeByID[slot.ServerID]
		if !ok {
			return BlockVolumeWorkloadPlan{}, fmt.Errorf("%w: placement server %q has no node inventory", ErrInvalidNodeRegistration, slot.ServerID)
		}
		replicaID := slot.ReplicaID
		if replicaID == "" {
			replicaID = fmt.Sprintf("r%d", i+1)
		}
		dataAddr, ctrlAddr := nodePlacementAddrs(node)
		portAssignmentPinned := false
		iscsiListenPort := cfg.ISCSIPortBase + i
		nvmeListenPort := cfg.NVMePortBase + i
		if slot.DataAddr != "" {
			dataAddr = slot.DataAddr
			portAssignmentPinned = true
		}
		if slot.CtrlAddr != "" {
			ctrlAddr = slot.CtrlAddr
			portAssignmentPinned = true
		}
		if slot.ISCSIListenPort != 0 {
			iscsiListenPort = slot.ISCSIListenPort
			portAssignmentPinned = true
		}
		if slot.NVMeListenPort != 0 {
			nvmeListenPort = slot.NVMeListenPort
			portAssignmentPinned = true
		}
		out.Replicas = append(out.Replicas, BlockVolumeReplicaWorkload{
			ServerID:             slot.ServerID,
			KubernetesNodeName:   kubernetesNodeName(node),
			PoolID:               slot.PoolID,
			ReplicaID:            replicaID,
			Source:               slot.Source,
			PortAssignmentPinned: portAssignmentPinned,
			DataAddr:             dataAddr,
			CtrlAddr:             ctrlAddr,
			ISCSIListenPort:      iscsiListenPort,
			ISCSIQualifiedName:   fmt.Sprintf("%s:%s", cfg.IQNPrefix, volume.Spec.VolumeID),
			NVMeListenPort:       nvmeListenPort,
			NVMeSubsystemNQN:     fmt.Sprintf("%s:%s", cfg.NQNPrefix, volume.Spec.VolumeID),
			NVMeNSID:             1,
		})
	}
	return out, nil
}

type WorkloadPlanConfig struct {
	ISCSIPortBase int
	IQNPrefix     string
	NVMePortBase  int
	NQNPrefix     string
}

func SortWorkloadPlans(plans []BlockVolumeWorkloadPlan) {
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].VolumeID < plans[j].VolumeID
	})
}

func kubernetesNodeName(node NodeRegistration) string {
	if node.Labels != nil {
		if name := node.Labels[KubernetesNodeNameLabel]; name != "" {
			return name
		}
	}
	return node.ServerID
}
