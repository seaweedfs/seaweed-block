// Command blockmaster is the block master daemon — the product
// host for authority components + master-volume control RPC.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/launcher"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/internal/buildinfo"
)

type flags struct {
	authorityStore          string
	lifecycleStore          string
	lifecyclePlacementSeed  string
	clusterSpec             string
	listen                  string
	topology                string
	expectedSlotsPerVol     int
	freshnessWindow         time.Duration
	pendingGrace            time.Duration
	lifecycleLoop           time.Duration
	launcherLoop            time.Duration
	launcherManifestDir     string
	launcherNamespace       string
	launcherImage           string
	launcherMasterAddr      string
	launcherDurableRoot     string
	launcherStateHostPath   string
	launcherISCSIPortBase   int
	launcherNVMePortBase    int
	launcherPVCOwnerRef     bool
	launcherStatus          bool
	launcherKubernetesApply bool
	launcherCHAPSecretName  string
	launcherCHAPUserKey     string
	launcherCHAPSecretKey   string
	version                 bool
	// printReadyLine: test-only flag that emits a single
	// structured JSON line to stdout after the gRPC listener is
	// bound, so L2 subprocess tests can parse the ready event.
	// Not operator-facing; documented in the T0 smoke scenario.
	printReadyLine bool
}

func parseFlags(args []string) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("blockmaster", flag.ContinueOnError)
	fs.StringVar(&f.authorityStore, "authority-store", "", "durable authority store directory (required)")
	fs.StringVar(&f.lifecycleStore, "lifecycle-store", "", "optional G9D product lifecycle registration store directory (desired volumes, node inventory, placement intents); read-only with respect to assignment publication")
	fs.StringVar(&f.lifecyclePlacementSeed, "lifecycle-placement-seed", "", "optional G9G seed JSON file containing placement intents to import into --lifecycle-store before the product loop starts")
	fs.StringVar(&f.clusterSpec, "cluster-spec", "", "optional G9G-3 cluster spec YAML facade; imports accepted topology and lifecycle placement intent")
	fs.StringVar(&f.listen, "listen", "127.0.0.1:0", "gRPC listen address (e.g. 127.0.0.1:9180)")
	fs.StringVar(&f.topology, "topology", "", "path to accepted-topology YAML (required for assignment to mint)")
	fs.IntVar(&f.expectedSlotsPerVol, "expected-slots-per-volume", 3, "RF/expected slot count per volume; the controller rejects observation snapshots whose slot count differs (default 3, set to 2 for 2-node smoke clusters)")
	fs.DurationVar(&f.freshnessWindow, "freshness-window", 30*time.Second, "observation freshness window before a server's heartbeat expires")
	fs.DurationVar(&f.pendingGrace, "pending-grace", 1*time.Second, "bootstrap/missing-observation grace before supportability reports unsupported")
	fs.DurationVar(&f.lifecycleLoop, "lifecycle-product-loop-interval", 0, "optional G9G product-loop interval; disabled when 0")
	fs.DurationVar(&f.launcherLoop, "launcher-loop-interval", 0, "optional G15d launcher planning/render loop interval; disabled when 0")
	fs.StringVar(&f.launcherManifestDir, "launcher-manifest-dir", "", "optional G15d output directory for rendered blockvolume workload manifests")
	fs.StringVar(&f.launcherNamespace, "launcher-namespace", "kube-system", "G15d rendered blockvolume manifest namespace")
	fs.StringVar(&f.launcherImage, "launcher-image", "sw-block:local", "G15d rendered blockvolume container image")
	fs.StringVar(&f.launcherMasterAddr, "launcher-master-addr", "", "G15d master address used in rendered blockvolume args; defaults to listener address after bind")
	fs.StringVar(&f.launcherDurableRoot, "launcher-durable-root", "/var/lib/sw-block", "G15d rendered blockvolume durable root base")
	fs.StringVar(&f.launcherStateHostPath, "launcher-state-hostpath", "", "optional hostPath base mounted at the blockvolume durable root; empty keeps generated blockvolume state on throwaway emptyDir")
	fs.IntVar(&f.launcherISCSIPortBase, "launcher-iscsi-port-base", 3260, "G15d iSCSI port base for generated blockvolume workloads")
	fs.IntVar(&f.launcherNVMePortBase, "launcher-nvme-port-base", 4420, "G15d NVMe/TCP port base for generated blockvolume workloads")
	fs.BoolVar(&f.launcherPVCOwnerRef, "launcher-pvc-owner-ref", false, "render generated blockvolume Deployments in the source PVC namespace with a PVC ownerReference; disabled by default for alpha harness compatibility")
	fs.BoolVar(&f.launcherStatus, "launcher-status", false, "render loopback-only blockvolume status endpoints in generated workload manifests; intended for TestOps/diagnostics")
	fs.BoolVar(&f.launcherKubernetesApply, "launcher-kubernetes-apply", false, "apply/delete generated blockvolume Deployments through the in-cluster Kubernetes API; scoped to app=sw-blockvolume and generated volume/replica labels")
	fs.StringVar(&f.launcherCHAPSecretName, "launcher-iscsi-chap-secret-name", "", "optional Kubernetes Secret name used by generated blockvolume Deployments for target-side iSCSI CHAP")
	fs.StringVar(&f.launcherCHAPUserKey, "launcher-iscsi-chap-username-key", "chapUsername", "Kubernetes Secret key for generated blockvolume iSCSI CHAP username")
	fs.StringVar(&f.launcherCHAPSecretKey, "launcher-iscsi-chap-secret-key", "chapSecret", "Kubernetes Secret key for generated blockvolume iSCSI CHAP secret")
	fs.BoolVar(&f.version, "version", false, "print build provenance and exit")
	fs.BoolVar(&f.printReadyLine, "t0-print-ready", false, "internal test-only: emit one structured JSON line on stdout after listener bound")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	if f.version {
		return f, nil
	}
	if f.authorityStore == "" {
		return flags{}, fmt.Errorf("--authority-store is required")
	}
	return f, nil
}

func main() {
	f, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster:", err)
		os.Exit(2)
	}
	if f.version {
		fmt.Println(buildinfo.Version("blockmaster"))
		return
	}
	os.Exit(run(f))
}

type readyLine struct {
	Component string `json:"component"`
	Phase     string `json:"phase"`
	Addr      string `json:"addr"`
}

func run(f flags) int {
	topo, clusterImport, err := loadProductInputs(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster:", err)
		return 2
	}
	if len(topo.Volumes) == 0 {
		// Without a topology the observation host classifies every
		// volume UnknownReplicaClaim; controller can never mint.
		// That is a valid "empty cluster" startup state and not an
		// error, but operators should know — log a prominent line.
		fmt.Fprintln(os.Stderr, "blockmaster: WARNING: --topology not supplied or empty; controller will not mint assignments until topology is provided")
	}
	cfg := master.Config{
		AuthorityStoreDir: f.authorityStore,
		LifecycleStoreDir: f.lifecycleStore,
		Listen:            f.listen,
		Topology:          topo,
		Freshness:         authority.FreshnessConfig{FreshnessWindow: f.freshnessWindow, PendingGrace: f.pendingGrace},
		ControllerConfig:  authority.TopologyControllerConfig{ExpectedSlotsPerVolume: f.expectedSlotsPerVol},
	}
	h, err := master.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster:", err)
		return 1
	}
	if len(clusterImport.Nodes) > 0 {
		if err := importLifecycleNodeInventory(h, clusterImport.Nodes); err != nil {
			fmt.Fprintln(os.Stderr, "blockmaster:", err)
			_ = h.Close(context.Background())
			return 1
		}
	}
	if len(clusterImport.Placements) > 0 {
		if err := importLifecyclePlacementIntents(h, clusterImport.Placements); err != nil {
			fmt.Fprintln(os.Stderr, "blockmaster:", err)
			_ = h.Close(context.Background())
			return 1
		}
	}
	if f.lifecyclePlacementSeed != "" {
		if err := importLifecyclePlacementSeed(h, f.lifecyclePlacementSeed); err != nil {
			fmt.Fprintln(os.Stderr, "blockmaster:", err)
			_ = h.Close(context.Background())
			return 1
		}
	}

	if f.printReadyLine {
		_ = json.NewEncoder(os.Stdout).Encode(readyLine{
			Component: "blockmaster",
			Phase:     "listening",
			Addr:      h.Addr(),
		})
	}

	h.Start()
	if f.lifecycleLoop > 0 {
		go runLifecycleProductLoop(h, f.lifecycleLoop)
	}
	if f.launcherLoop > 0 {
		go runLifecycleLauncherLoop(h, f, f.launcherLoop)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster: close:", err)
		return 1
	}
	return 0
}

func loadProductInputs(f flags) (authority.AcceptedTopology, clusterSpecImport, error) {
	if f.clusterSpec == "" {
		topo, err := loadTopology(f.topology)
		return topo, clusterSpecImport{}, err
	}
	if f.topology != "" {
		return authority.AcceptedTopology{}, clusterSpecImport{}, fmt.Errorf("--cluster-spec and --topology are mutually exclusive in this slice")
	}
	imports, err := loadClusterSpec(f.clusterSpec)
	if err != nil {
		return authority.AcceptedTopology{}, clusterSpecImport{}, err
	}
	topo, err := toAcceptedTopology(&imports.Topology)
	if err != nil {
		return authority.AcceptedTopology{}, clusterSpecImport{}, err
	}
	return topo, imports, nil
}

func importLifecycleNodeInventory(h *master.Host, nodes []lifecycle.NodeRegistration) error {
	stores := h.Lifecycle()
	if stores == nil {
		return fmt.Errorf("lifecycle node import requires --lifecycle-store")
	}
	for _, node := range nodes {
		if _, err := stores.Nodes.RegisterNode(node); err != nil {
			return fmt.Errorf("register lifecycle node %s: %w", node.ServerID, err)
		}
	}
	return nil
}

func importLifecyclePlacementSeed(h *master.Host, path string) error {
	stores := h.Lifecycle()
	if stores == nil {
		return fmt.Errorf("--lifecycle-placement-seed requires --lifecycle-store")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read lifecycle placement seed %q: %w", path, err)
	}
	var intents []lifecycle.PlacementIntent
	if err := json.Unmarshal(raw, &intents); err != nil {
		return fmt.Errorf("parse lifecycle placement seed %q: %w", path, err)
	}
	return importLifecyclePlacementIntents(h, intents)
}

func importLifecyclePlacementIntents(h *master.Host, intents []lifecycle.PlacementIntent) error {
	stores := h.Lifecycle()
	if stores == nil {
		return fmt.Errorf("lifecycle placement import requires --lifecycle-store")
	}
	for _, intent := range intents {
		plan := lifecycle.PlacementPlan{
			VolumeID:   intent.VolumeID,
			DesiredRF:  intent.DesiredRF,
			Candidates: make([]lifecycle.PlacementCandidate, 0, len(intent.Slots)),
		}
		for _, slot := range intent.Slots {
			plan.Candidates = append(plan.Candidates, lifecycle.PlacementCandidate{
				VolumeID:  intent.VolumeID,
				ServerID:  slot.ServerID,
				PoolID:    slot.PoolID,
				ReplicaID: slot.ReplicaID,
				Source:    slot.Source,
			})
		}
		if _, err := stores.Placements.ApplyPlan(plan); err != nil {
			return fmt.Errorf("apply lifecycle placement seed %s: %w", intent.VolumeID, err)
		}
	}
	return nil
}

func runLifecycleProductLoop(h *master.Host, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := h.RunLifecycleProductTick(); err != nil {
			fmt.Fprintln(os.Stderr, "blockmaster: lifecycle product loop:", err)
		}
		<-ticker.C
	}
}

func runLifecycleLauncherLoop(h *master.Host, f flags, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := runLifecycleLauncherTick(h, f); err != nil {
			fmt.Fprintln(os.Stderr, "blockmaster: lifecycle launcher loop:", err)
		}
		<-ticker.C
	}
}

func runLifecycleLauncherTick(h *master.Host, f flags) error {
	result, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{
		ISCSIPortBase: f.launcherISCSIPortBase,
		NVMePortBase:  f.launcherNVMePortBase,
	})
	if err != nil {
		return err
	}
	if f.launcherManifestDir == "" && !f.launcherKubernetesApply {
		return nil
	}
	masterAddr := f.launcherMasterAddr
	if masterAddr == "" {
		masterAddr = h.Addr()
	}
	var rendered []launcher.RenderedManifest
	for _, plan := range result.Plans {
		manifests, err := launcher.RenderBlockVolumeDeployments(plan, launcher.K8sRenderConfig{
			Namespace:           f.launcherNamespace,
			Image:               f.launcherImage,
			MasterAddr:          masterAddr,
			DurableRootBase:     f.launcherDurableRoot,
			StateHostPathBase:   f.launcherStateHostPath,
			OwnerReferenceToPVC: f.launcherPVCOwnerRef,
			EnableStatus:        f.launcherStatus,
			ISCSICHAP: launcher.CHAPSecretRef{
				Name:        f.launcherCHAPSecretName,
				UsernameKey: f.launcherCHAPUserKey,
				SecretKey:   f.launcherCHAPSecretKey,
			},
		})
		if err != nil {
			return err
		}
		rendered = append(rendered, manifests...)
	}
	if f.launcherManifestDir != "" {
		if err := launcher.SyncRenderedManifests(f.launcherManifestDir, rendered); err != nil {
			return err
		}
	}
	if f.launcherKubernetesApply {
		client, err := launcher.NewInClusterDeploymentClient()
		if err != nil {
			return err
		}
		for _, namespace := range renderedNamespaces(rendered, f.launcherNamespace) {
			desired := renderedForNamespace(rendered, namespace)
			existing, err := client.ListBlockVolumeDeployments(context.Background(), namespace)
			if err != nil {
				return err
			}
			reconcileResult, err := launcher.ReconcileBlockVolumeDeployments(context.Background(), launcher.ReconcileDeploymentsInput{
				Namespace: namespace,
				Desired:   desired,
				Existing:  existing,
				Client:    client,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "blockmaster: launcher kubernetes reconcile namespace=%s applied=%d deleted=%d skipped=%d\n", namespace, reconcileResult.Applied, reconcileResult.Deleted, reconcileResult.Skipped)
		}
	}
	return nil
}

func renderedNamespaces(rendered []launcher.RenderedManifest, fallback string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(namespace string) {
		if namespace == "" {
			namespace = fallback
		}
		if namespace == "" || seen[namespace] {
			return
		}
		seen[namespace] = true
		out = append(out, namespace)
	}
	add(fallback)
	for _, manifest := range rendered {
		identity, err := launcher.DecodeRenderedDeploymentIdentity(manifest)
		if err != nil {
			continue
		}
		add(identity.Namespace)
	}
	return out
}

func renderedForNamespace(rendered []launcher.RenderedManifest, namespace string) []launcher.RenderedManifest {
	var out []launcher.RenderedManifest
	for _, manifest := range rendered {
		identity, err := launcher.DecodeRenderedDeploymentIdentity(manifest)
		if err != nil {
			continue
		}
		if identity.Namespace == namespace {
			out = append(out, manifest)
		}
	}
	return out
}
