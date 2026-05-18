package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const seaweedBlockCSIDriver = "block.csi.seaweedfs.com"

type KubernetesInventoryConfig struct {
	Namespace         string
	MasterAddr        string
	StatusBundleRoot  string
	ProductRevision   string
	RunnerRevision    string
	ClaimProfile      string
	RequiredFrontiers map[string]uint64
	RunCommand        func(context.Context, string, ...string) ([]byte, error)
}

func NewKubernetesVolumeInventoryCollector(cfg KubernetesInventoryConfig) VolumeInventoryCollector {
	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	if cfg.RunCommand == nil {
		cfg.RunCommand = DefaultRunCommand
	}
	return VolumeInventoryCollectorFunc(func(ctx context.Context) (VolumeInventory, error) {
		return collectKubernetesVolumeInventory(ctx, cfg)
	})
}

func collectKubernetesVolumeInventory(ctx context.Context, cfg KubernetesInventoryConfig) (VolumeInventory, error) {
	pvcRaw, err := cfg.RunCommand(ctx, "kubectl", "-n", cfg.Namespace, "get", "pvc", "-o", "json")
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("kubernetes_unreachable: list pvc namespace=%s: %w", cfg.Namespace, err)
	}
	pvRaw, err := cfg.RunCommand(ctx, "kubectl", "get", "pv", "-o", "json")
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("kubernetes_unreachable: list pv: %w", err)
	}
	deployRaw, err := cfg.RunCommand(ctx, "kubectl", "-n", cfg.Namespace, "get", "deploy", "-l", "app=sw-blockvolume", "-o", "json")
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("kubernetes_unreachable: list blockvolume deployments namespace=%s: %w", cfg.Namespace, err)
	}

	pvcs, err := decodeK8sList[k8sPVC](pvcRaw)
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("parse pvc list: %w", err)
	}
	pvs, err := decodeK8sList[k8sPV](pvRaw)
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("parse pv list: %w", err)
	}
	deploys, err := decodeK8sList[k8sDeployment](deployRaw)
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("parse blockvolume deployment list: %w", err)
	}

	pvByClaim := map[string]k8sPV{}
	for _, pv := range pvs {
		key := claimKey(pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
		if key != "/" {
			pvByClaim[key] = pv
		}
	}
	replicasByVolume := map[string][]VolumeInventoryReplicaInput{}
	for _, deploy := range deploys {
		replica := replicaFromDeployment(deploy)
		volumeID := deploy.Metadata.Labels["sw-block.seaweedfs.com/volume"]
		if volumeID == "" {
			volumeID = argValue(deploymentArgs(deploy), "--volume-id")
		}
		if volumeID == "" {
			continue
		}
		replicasByVolume[volumeID] = append(replicasByVolume[volumeID], replica)
	}
	processReplicasByVolume := localBlockvolumeProcessReplicas(ctx, cfg.RunCommand)

	volumes := make([]VolumeInventoryVolumeInput, 0, len(pvcs))
	claimedVolumeIDs := map[string]bool{}
	for _, pvc := range pvcs {
		pv, hasPV := pvByClaim[claimKey(pvc.Metadata.Namespace, pvc.Metadata.Name)]
		if !isSeaweedBlockPVC(pvc, pv, hasPV) {
			continue
		}
		volumeID := pvc.Spec.VolumeName
		pvName := pvc.Spec.VolumeName
		replicationFactor := max(1, len(replicasByVolume[volumeID]))
		var replicationFactorIssue string
		if hasPV {
			pvName = pv.Metadata.Name
			if pv.Spec.CSI.VolumeHandle != "" {
				volumeID = pv.Spec.CSI.VolumeHandle
			}
			replicationFactor, replicationFactorIssue = replicationFactorFromVolumeAttributes(pv.Spec.CSI.VolumeAttributes, replicationFactor)
		}
		volume := VolumeInventoryVolumeInput{
			VolumeID:          volumeID,
			Namespace:         pvc.Metadata.Namespace,
			PVCName:           pvc.Metadata.Name,
			PVName:            pvName,
			ReplicationFactor: replicationFactor,
			SupportBundle:     "volumes/" + volumeID,
			Replicas:          replicasByVolume[volumeID],
		}
		if replicationFactorIssue != "" {
			volume.Issues = append(volume.Issues, replicationFactorIssue)
		}
		if len(volume.Replicas) == 0 {
			volume.Issues = append(volume.Issues, "generated_deployment_missing")
		}
		applyRequiredFrontierEvidence(&volume, cfg.RequiredFrontiers)
		applyClaimProfile(&volume, cfg.ClaimProfile)
		volumes = append(volumes, volume)
		claimedVolumeIDs[volumeID] = true
	}
	for volumeID, replicas := range replicasByVolume {
		if claimedVolumeIDs[volumeID] {
			continue
		}
		orphan := VolumeInventoryVolumeInput{
			VolumeID:          volumeID,
			Namespace:         cfg.Namespace,
			PVCName:           Unavailable,
			PVName:            Unavailable,
			ReplicationFactor: max(1, len(replicas)),
			SupportBundle:     "volumes/" + volumeID,
			Replicas:          replicas,
			Issues: []string{
				"orphan-blockvolume-deploy=" + strings.Join(replicaDeploymentNames(replicas), ","),
				"heartbeat-without-placement=" + strings.Join(replicaServerIDs(replicas), ",") + " state=unadmitted-by-master reason=no-matching-pvc-or-pv",
			},
		}
		applyClaimProfile(&orphan, cfg.ClaimProfile)
		volumes = append(volumes, orphan)
	}
	for volumeID, replicas := range processReplicasByVolume {
		if claimedVolumeIDs[volumeID] || len(replicasByVolume[volumeID]) > 0 {
			continue
		}
		orphan := VolumeInventoryVolumeInput{
			VolumeID:          volumeID,
			Namespace:         cfg.Namespace,
			PVCName:           Unavailable,
			PVName:            Unavailable,
			ReplicationFactor: max(1, len(replicas)),
			SupportBundle:     "volumes/" + volumeID,
			Replicas:          replicas,
			Issues: []string{
				"blockvolume-process-without-placement=" + strings.Join(replicaServerIDs(replicas), ","),
				"heartbeat-without-placement=" + strings.Join(replicaServerIDs(replicas), ",") + " state=unadmitted-by-master reason=local-process-without-pvc-or-pv",
			},
		}
		applyClaimProfile(&orphan, cfg.ClaimProfile)
		volumes = append(volumes, orphan)
	}
	volumes = collectKubernetesReplicaStatusBundles(ctx, cfg, volumes)

	return BuildVolumeInventory(VolumeInventoryInput{
		Source:          ReportSource{Component: "sw-block ops inventory", Scenario: "namespace=" + cfg.Namespace},
		ProductRevision: cfg.ProductRevision,
		RunnerRevision:  cfg.RunnerRevision,
		Volumes:         volumes,
	}), nil
}

func applyRequiredFrontierEvidence(volume *VolumeInventoryVolumeInput, required map[string]uint64) {
	if volume == nil || len(required) == 0 {
		return
	}
	lsn, ok := required[volume.VolumeID]
	if !ok {
		return
	}
	for i := range volume.Replicas {
		volume.Replicas[i].RequiredFrontierKnown = true
		volume.Replicas[i].RequiredFrontierLSN = lsn
	}
}

func applyClaimProfile(volume *VolumeInventoryVolumeInput, claimProfile string) {
	if volume == nil || claimProfile == "" {
		return
	}
	for i := range volume.Replicas {
		volume.Replicas[i].ClaimProfile = claimProfile
	}
}

func localBlockvolumeProcessReplicas(ctx context.Context, run func(context.Context, string, ...string) ([]byte, error)) map[string][]VolumeInventoryReplicaInput {
	out := map[string][]VolumeInventoryReplicaInput{}
	if run == nil {
		return out
	}
	raw, err := run(ctx, "ps", "-eo", "args")
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(raw), "\n") {
		replica, volumeID, ok := replicaFromProcessArgs(line)
		if !ok {
			continue
		}
		out[volumeID] = append(out[volumeID], replica)
	}
	return out
}

func replicaFromProcessArgs(line string) (VolumeInventoryReplicaInput, string, bool) {
	line = strings.TrimSpace(line)
	if line == "" || !strings.Contains(line, "blockvolume") || !strings.Contains(line, "--volume-id") {
		return VolumeInventoryReplicaInput{}, "", false
	}
	args := strings.Fields(line)
	volumeID := argValue(args, "--volume-id")
	replicaID := argValue(args, "--replica-id")
	serverID := argValue(args, "--server-id")
	if volumeID == "" || replicaID == "" || serverID == "" {
		return VolumeInventoryReplicaInput{}, "", false
	}
	protocol := "iscsi"
	frontend := argValue(args, "--iscsi-listen")
	if nvme := argValue(args, "--nvme-listen"); nvme != "" {
		protocol = "nvme"
		frontend = nvme
	}
	return VolumeInventoryReplicaInput{
		ReplicaID:           replicaID,
		ServerID:            serverID,
		NodeName:            serverID,
		GeneratedDeployment: Unavailable,
		Protocol:            protocol,
		FrontendAddress:     frontend,
		StatusAddress:       argValue(args, "--status-addr"),
		AckProfile:          defaultString(argValue(args, "--replication-ack"), PromotionAckProfileBestEffort),
		DataAddr:            argValue(args, "--data-addr"),
		CtrlAddr:            argValue(args, "--ctrl-addr"),
		Observed:            true,
		Issues:              []string{"local_process_without_kubernetes_placement"},
	}, volumeID, true
}

func replicaDeploymentNames(replicas []VolumeInventoryReplicaInput) []string {
	names := make([]string, 0, len(replicas))
	for _, replica := range replicas {
		names = append(names, explicitUnavailable(replica.GeneratedDeployment))
	}
	return names
}

func replicaServerIDs(replicas []VolumeInventoryReplicaInput) []string {
	ids := make([]string, 0, len(replicas))
	for _, replica := range replicas {
		ids = append(ids, explicitUnavailable(replica.ServerID))
	}
	return ids
}

func collectKubernetesReplicaStatusBundles(ctx context.Context, cfg KubernetesInventoryConfig, volumes []VolumeInventoryVolumeInput) []VolumeInventoryVolumeInput {
	if cfg.MasterAddr == "" || cfg.StatusBundleRoot == "" {
		return volumes
	}
	for vi := range volumes {
		volumeID := volumes[vi].VolumeID
		for ri := range volumes[vi].Replicas {
			replica := &volumes[vi].Replicas[ri]
			if strings.TrimSpace(replica.StatusAddress) == "" {
				replica.Issues = append(replica.Issues, "status_endpoint_unavailable")
				replica.CollectionErrors = append(replica.CollectionErrors, "ops_status: status_address unavailable")
				continue
			}
			relBundle := filepath.ToSlash(filepath.Join("volumes", safePathSegment(volumeID), safePathSegment(replica.ReplicaID)))
			replica.SupportBundle = relBundle
			bundleDir := filepath.Join(cfg.StatusBundleRoot, filepath.FromSlash(relBundle))
			statusAddr, cleanup, statusAddrErr := statusAddressForKubernetesInventory(ctx, cfg, bundleDir, *replica)
			if statusAddrErr != nil {
				replica.Issues = append(replica.Issues, "status_endpoint_unreachable="+replica.StatusAddress)
				replica.CollectionErrors = append(replica.CollectionErrors, prefixErrorMessages("ops_status", statusAddrErr)...)
				continue
			}
			report, code, err := WriteVolumeStatusArtifacts(ctx, bundleDir, NewLiveVolumeStatusReportCollector(LiveVolumeStatusConfig{
				VolumeID:        volumeID,
				MasterAddr:      cfg.MasterAddr,
				StatusAddr:      statusAddr,
				ProductRevision: cfg.ProductRevision,
				RunnerRevision:  cfg.RunnerRevision,
				Source:          ReportSource{Component: "sw-block ops inventory", Scenario: "replica-status"},
				RunCommand:      cfg.RunCommand,
			}))
			if cleanup != nil {
				cleanup()
			}
			applyStatusReportToInventoryReplica(replica, report)
			if code != VolumeStatusExitOK {
				replica.Issues = append(replica.Issues, opsStatusInventoryIssue(code, report))
			}
			if err != nil {
				replica.Issues = append(replica.Issues, "status_endpoint_unreachable="+replica.StatusAddress)
				replica.CollectionErrors = append(replica.CollectionErrors, prefixErrorMessages("ops_status", err)...)
			}
		}
	}
	return volumes
}

func applyStatusReportToInventoryReplica(replica *VolumeInventoryReplicaInput, report VolumeStatusReport) {
	if replica == nil || report.SchemaVersion == "" {
		return
	}
	replica.AuthorityRole = report.Authority.AuthorityRole
	replica.Healthy = report.Authority.Healthy
	replica.FrontendPrimaryReady = report.Authority.FrontendPrimaryReady
	replica.ReplicationRole = report.Replication.ReplicationRole
	replica.Epoch = report.Authority.Epoch
	replica.EndpointVersion = report.Authority.EndpointVersion
	for _, durable := range report.Durable {
		if durable.ReplicaID == report.Volume.ReplicaID || durable.ReplicaID == replica.ReplicaID {
			replica.DurableLatched = durable.Latched
			replica.DurableOperational = durable.Operational
			if durable.FrontierKnown {
				replica.CandidateFrontierKnown = true
				replica.CandidateFrontierLSN = durable.DurableLSN
			}
			break
		}
	}
	if len(report.Volume.Protocols) > 0 {
		replica.Protocol = strings.Join(report.Volume.Protocols, ",")
	}
	if (replica.FrontendAddress == "" || replica.FrontendAddress == Unavailable) &&
		len(report.Volume.Frontends) > 0 && report.Volume.Frontends[0].Addr != "" {
		replica.FrontendAddress = report.Volume.Frontends[0].Addr
	}
}

func opsStatusInventoryIssue(code int, report VolumeStatusReport) string {
	label := "ops_status=" + inventoryExitLabel(code)
	if code == VolumeStatusExitUnhealthy && !report.Authority.Assigned {
		return fmt.Sprintf("%s reason=authority_not_assigned assigned=false epoch=%d endpoint_version=%d", label, report.Authority.Epoch, report.Authority.EndpointVersion)
	}
	if code != VolumeStatusExitOK {
		issues := VolumeStatusReportIssues(report)
		if len(issues) > 0 {
			return label + " reason=" + inventoryReasonToken(issues[0])
		}
	}
	return label
}

func inventoryReasonToken(issue string) string {
	var b strings.Builder
	lastUnderscore := false
	for _, r := range strings.ToLower(issue) {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "unspecified"
	}
	return out
}

func statusAddressForKubernetesInventory(ctx context.Context, cfg KubernetesInventoryConfig, bundleDir string, replica VolumeInventoryReplicaInput) (string, func(), error) {
	remotePort, ok := loopbackStatusPort(replica.StatusAddress)
	if !ok {
		return replica.StatusAddress, nil, nil
	}
	if replica.GeneratedDeployment == "" || replica.GeneratedDeployment == Unavailable {
		return "", nil, fmt.Errorf("status endpoint %s is loopback and generated deployment is unavailable", replica.StatusAddress)
	}
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		return "", nil, fmt.Errorf("create replica status bundle dir: %w", err)
	}
	localPort, err := chooseInventoryLocalPort()
	if err != nil {
		return "", nil, err
	}
	logPath := filepath.Join(bundleDir, "status-port-forward.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return "", nil, fmt.Errorf("create status port-forward log: %w", err)
	}
	cmd := exec.CommandContext(ctx, "kubectl", "-n", cfg.Namespace, "port-forward", "deploy/"+replica.GeneratedDeployment, fmt.Sprintf("%d:%s", localPort, remotePort))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return "", nil, fmt.Errorf("start status port-forward deploy/%s: %w", replica.GeneratedDeployment, err)
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		_ = logFile.Close()
	}()
	cleanup := func() {
		if cmd.Process != nil && cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
	if err := waitInventoryTCPReady(ctx, "127.0.0.1", localPort, done); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("status port-forward deploy/%s %d:%s not ready: %w", replica.GeneratedDeployment, localPort, remotePort, err)
	}
	return fmt.Sprintf("127.0.0.1:%d", localPort), cleanup, nil
}

func loopbackStatusPort(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	if strings.Contains(raw, "://") {
		return "", false
	}
	raw = "http://" + raw
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || port == "" {
		return "", false
	}
	if host == "127.0.0.1" || host == "localhost" || host == "::1" {
		return port, true
	}
	return "", false
}

func chooseInventoryLocalPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("choose local status port: %w", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func waitInventoryTCPReady(ctx context.Context, host string, port int, done <-chan error) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			if err == nil {
				return fmt.Errorf("port-forward exited before readiness")
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("timed out")
}

func prefixErrorMessages(prefix string, err error) []string {
	var out []string
	for _, msg := range splitErrorMessages(err) {
		out = append(out, prefix+": "+msg)
	}
	return out
}

func isSeaweedBlockPVC(pvc k8sPVC, pv k8sPV, hasPV bool) bool {
	if strings.HasPrefix(pvc.Spec.StorageClassName, "sw-block") {
		return true
	}
	return hasPV && pv.Spec.CSI.Driver == seaweedBlockCSIDriver
}

func emptyKubernetesInventory(cfg KubernetesInventoryConfig) VolumeInventory {
	return BuildVolumeInventory(VolumeInventoryInput{
		Source:          ReportSource{Component: "sw-block ops inventory", Scenario: "namespace=" + cfg.Namespace},
		ProductRevision: cfg.ProductRevision,
		RunnerRevision:  cfg.RunnerRevision,
	})
}

func decodeK8sList[T any](raw []byte) ([]T, error) {
	var list struct {
		Items []T `json:"items"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	if list.Items == nil {
		return []T{}, nil
	}
	return list.Items, nil
}

func replicaFromDeployment(deploy k8sDeployment) VolumeInventoryReplicaInput {
	args := deploymentArgs(deploy)
	protocol := "iscsi"
	frontend := argValue(args, "--iscsi-listen")
	if nvme := argValue(args, "--nvme-listen"); nvme != "" {
		protocol = "nvme"
		frontend = nvme
	}
	ready := deploy.Status.ReadyReplicas > 0
	replicaID := deploy.Metadata.Labels["sw-block.seaweedfs.com/replica"]
	if replicaID == "" {
		replicaID = argValue(args, "--replica-id")
	}
	lifecycleOwner, ownerRef := deploymentLifecycleOwnership(deploy)
	return VolumeInventoryReplicaInput{
		ReplicaID:            replicaID,
		ServerID:             firstNonEmpty(argValue(args, "--server-id"), deploy.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]),
		NodeName:             firstNonEmpty(deploy.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"], argValue(args, "--server-id")),
		GeneratedDeployment:  deploy.Metadata.Name,
		LifecycleOwner:       lifecycleOwner,
		OwnerReference:       ownerRef,
		Protocol:             protocol,
		FrontendAddress:      frontend,
		StatusAddress:        argValue(args, "--status-addr"),
		AckProfile:           defaultString(argValue(args, "--replication-ack"), PromotionAckProfileBestEffort),
		DataAddr:             argValue(args, "--data-addr"),
		CtrlAddr:             argValue(args, "--ctrl-addr"),
		Observed:             true,
		AuthorityRole:        hostRoleFromReadiness(ready),
		Healthy:              ready,
		FrontendPrimaryReady: ready,
		ReplicationRole:      hostReplicationFromReadiness(ready),
	}
}

func deploymentLifecycleOwnership(deploy k8sDeployment) (string, string) {
	for _, owner := range deploy.Metadata.OwnerReferences {
		if owner.Kind == "PersistentVolumeClaim" && owner.Name != "" {
			return "pvc-owner-ref", "PersistentVolumeClaim/" + firstNonEmpty(deploy.Metadata.Namespace, "default") + "/" + owner.Name
		}
	}
	if deploy.Metadata.Labels["app"] == "sw-blockvolume" && deploy.Metadata.Labels["sw-block.seaweedfs.com/volume"] != "" {
		return "launcher-managed", Unavailable
	}
	return Unavailable, Unavailable
}

func deploymentArgs(deploy k8sDeployment) []string {
	for _, c := range deploy.Spec.Template.Spec.Containers {
		if c.Name == "blockvolume" {
			return c.Args
		}
	}
	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		return nil
	}
	return deploy.Spec.Template.Spec.Containers[0].Args
}

func argValue(args []string, flag string) string {
	prefix := flag + "="
	for i, arg := range args {
		if strings.HasPrefix(arg, prefix) {
			return strings.TrimPrefix(arg, prefix)
		}
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func hostRoleFromReadiness(ready bool) string {
	if ready {
		return "primary"
	}
	return Unavailable
}

func hostReplicationFromReadiness(ready bool) string {
	if ready {
		return "none"
	}
	return Unavailable
}

func claimKey(namespace, name string) string {
	return namespace + "/" + name
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func replicationFactorFromVolumeAttributes(attrs map[string]string, fallback int) (int, string) {
	raw := strings.TrimSpace(attrs["replicationFactor"])
	if raw == "" {
		return max(1, fallback), ""
	}
	rf, err := strconv.Atoi(raw)
	if err != nil || rf <= 0 {
		return max(1, fallback), "invalid: replication_factor_attribute=" + raw
	}
	return rf, ""
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func safePathSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unavailable"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "..", "_")
	return replacer.Replace(value)
}

type k8sObjectMeta struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	UID             string            `json:"uid"`
	Labels          map[string]string `json:"labels"`
	OwnerReferences []struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"ownerReferences"`
}

type k8sPVC struct {
	Metadata k8sObjectMeta `json:"metadata"`
	Spec     struct {
		VolumeName       string `json:"volumeName"`
		StorageClassName string `json:"storageClassName"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

type k8sPV struct {
	Metadata k8sObjectMeta `json:"metadata"`
	Spec     struct {
		ClaimRef struct {
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
			UID       string `json:"uid"`
		} `json:"claimRef"`
		CSI struct {
			Driver           string            `json:"driver"`
			VolumeHandle     string            `json:"volumeHandle"`
			VolumeAttributes map[string]string `json:"volumeAttributes"`
		} `json:"csi"`
	} `json:"spec"`
}

type k8sDeployment struct {
	Metadata k8sObjectMeta `json:"metadata"`
	Spec     struct {
		Replicas *int `json:"replicas"`
		Template struct {
			Spec struct {
				NodeSelector map[string]string `json:"nodeSelector"`
				Containers   []struct {
					Name string   `json:"name"`
					Args []string `json:"args"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		Replicas      int `json:"replicas"`
		ReadyReplicas int `json:"readyReplicas"`
	} `json:"status"`
}
