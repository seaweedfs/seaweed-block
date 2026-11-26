// Command blockmaster is the block master daemon â€” the product
// host for authority components + master-volume control RPC.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/launcher"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"github.com/seaweedfs/seaweed-block/internal/buildinfo"
)

type flags struct {
	insecureExternalBind                  bool
	authorityStore                        string
	lifecycleStore                        string
	lifecyclePlacementSeed                string
	clusterSpec                           string
	listen                                string
	topology                              string
	expectedSlotsPerVol                   int
	freshnessWindow                       time.Duration
	pendingGrace                          time.Duration
	lifecycleLoop                         time.Duration
	launcherLoop                          time.Duration
	launcherManifestDir                   string
	launcherNamespace                     string
	launcherImage                         string
	launcherMasterAddr                    string
	launcherDurableRoot                   string
	launcherDurableImpl                   string
	launcherWALMultiBlockRecords          bool
	launcherWALRecoveryTestDisableFlusher bool
	launcherStateHostPath                 string
	launcherReplicationAck                string
	launcherISCSIPortBase                 int
	launcherNVMePortBase                  int
	launcherNVMeMaxH2CDataLength          uint
	launcherPVCOwnerRef                   bool
	launcherStatus                        bool
	launcherKubernetesApply               bool
	launcherExternalISCSI                 bool
	launcherExternalNVMe                  bool
	launcherExternalStatus                bool
	launcherCHAPSecretName                string
	launcherMasterTLSSecretPrefix         string
	launcherCHAPUserKey                   string
	launcherCHAPSecretKey                 string
	failbackRuntimeRPC                    bool
	workspaceMaxClockSkew                 time.Duration
	workspaceRPC                          bool
	chunkFiler                            string
	chunkPath                             string
	chunkCollection                       string
	chunkReplication                      string
	frontendPublicationRuntimeHTTP        bool
	frontendPublicationRuntimeListen      string
	strictJournalIsolation                bool
	statusListen                          string
	version                               bool
	// printReadyLine: test-only flag that emits a single
	// structured JSON line to stdout after the gRPC listener is
	// bound, so L2 subprocess tests can parse the ready event.
	// Not operator-facing; documented in the T0 smoke scenario.
	printReadyLine bool
	auth           authFlags
}

// errRemovedInV1 refuses a flag that v1 removed, at start: a unit or script
// still passing it fails with the reason and what replaces it, rather than
// starting with the setting silently ignored.
var errRemovedInV1 = errors.New("blockmaster: flag removed in v1")

func parseFlags(args []string) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("blockmaster", flag.ContinueOnError)
	removedHeadBase := false
	fs.Func("workspace-head-base-url", "removed in v1, refused at start: blockmaster reads HEAD through the layout catalog (--chunk-filer, --chunk-path)", func(string) error {
		removedHeadBase = true
		return nil
	})
	fs.StringVar(&f.authorityStore, "authority-store", "", "durable authority store directory (required)")
	fs.StringVar(&f.lifecycleStore, "lifecycle-store", "", "optional G9D product lifecycle registration store directory (desired volumes, node inventory, placement intents); read-only with respect to assignment publication")
	fs.StringVar(&f.lifecyclePlacementSeed, "lifecycle-placement-seed", "", "placement-only seed JSON; lifecycle volumes must already be registered (CreateVolume)")
	fs.StringVar(&f.clusterSpec, "cluster-spec", "", "cluster spec YAML; positive size_bytes imports desired volumes before topology/placement publication")
	fs.StringVar(&f.listen, "listen", "127.0.0.1:0", "gRPC listen address (e.g. 127.0.0.1:9180); non-loopback requires mutual TLS (--tls-cert, --tls-key, --tls-client-ca, --role-file)")
	fs.StringVar(&f.auth.tlsCert, "tls-cert", "", "blockmaster's TLS certificate (PEM): with --tls-key, --tls-client-ca and --role-file, every gRPC call needs mutual TLS")
	fs.StringVar(&f.auth.tlsKey, "tls-key", "", "the key of --tls-cert (PEM)")
	fs.StringVar(&f.auth.tlsClientCA, "tls-client-ca", "", "CA (PEM) every client certificate must verify against")
	fs.StringVar(&f.auth.auditLog, "audit-log", "", "append-only audit log (JSON lines): every operator and admin call, refused node calls, role-file reloads")
	fs.StringVar(&f.auth.roleFile, "role-file", "", "role file: lines 'sha256:<fingerprint> <viewer|operator|admin|node> <id>'; SIGHUP reloads it")
	fs.BoolVar(&f.insecureExternalBind, "insecure-external-bind", false, "removed: refused at start (a non-loopback --listen needs mutual TLS; the runtime HTTP listener stays on loopback)")
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
	fs.StringVar(&f.launcherDurableImpl, "launcher-durable-impl", "walstore", "G15d rendered blockvolume durable implementation: walstore (default), smartwal, or opt-in parallel-walstore")
	fs.BoolVar(&f.launcherWALMultiBlockRecords, "launcher-durable-wal-multiblock-records", false, "G15d render opt-in for experimental walstore multi-block WAL records; default false")
	fs.BoolVar(&f.launcherWALRecoveryTestDisableFlusher, "launcher-durable-wal-recovery-test-disable-flusher", false, "test-only G15d render opt-in for blockvolume --durable-wal-recovery-test-disable-flusher; default false")
	fs.StringVar(&f.launcherStateHostPath, "launcher-state-hostpath", "", "optional hostPath base mounted at the blockvolume durable root; empty keeps generated blockvolume state on throwaway emptyDir")
	fs.StringVar(&f.launcherReplicationAck, "launcher-replication-ack", "best-effort", "G15d rendered blockvolume replication ACK profile: best-effort, sync-quorum, or sync-all")
	fs.IntVar(&f.launcherISCSIPortBase, "launcher-iscsi-port-base", 3260, "G15d iSCSI port base for generated blockvolume workloads")
	fs.IntVar(&f.launcherNVMePortBase, "launcher-nvme-port-base", 4420, "G15d NVMe/TCP port base for generated blockvolume workloads")
	fs.UintVar(&f.launcherNVMeMaxH2CDataLength, "launcher-nvme-max-h2c-data-length", 0, "G15d NVMe/TCP MaxH2CDataLength passed to generated blockvolume workloads. 0 preserves target default")
	fs.BoolVar(&f.launcherPVCOwnerRef, "launcher-pvc-owner-ref", false, "render generated blockvolume Deployments in the source PVC namespace with a PVC ownerReference; disabled by default for alpha harness compatibility")
	fs.BoolVar(&f.launcherStatus, "launcher-status", false, "render loopback-only blockvolume status endpoints in generated workload manifests; intended for TestOps/diagnostics")
	fs.BoolVar(&f.launcherKubernetesApply, "launcher-kubernetes-apply", false, "apply/delete generated blockvolume Deployments through the in-cluster Kubernetes API; scoped to app=sw-blockvolume and generated volume/replica labels")
	fs.BoolVar(&f.launcherExternalISCSI, "launcher-external-iscsi", false, "render generated iSCSI frontends on node addresses instead of loopback; requires --launcher-iscsi-chap-secret-name")
	fs.BoolVar(&f.launcherExternalNVMe, "launcher-external-nvme", false, "render generated NVMe/TCP frontends on node addresses instead of loopback; unauthenticated and intended only for explicit Kubernetes NVMe/TCP gates")
	fs.BoolVar(&f.launcherExternalStatus, "launcher-external-status", false, "render blockvolume status endpoints on node addresses instead of loopback; intended for explicit Kubernetes node-loss gates")
	fs.StringVar(&f.launcherMasterTLSSecretPrefix, "launcher-master-tls-secret-prefix", "", "generated blockvolume Deployments dial blockmaster with mutual TLS from the Secret <prefix>-<server id> (keys tls.crt, tls.key, ca.crt); needs blockmaster's own mutual TLS")
	fs.StringVar(&f.launcherCHAPSecretName, "launcher-iscsi-chap-secret-name", "", "optional Kubernetes Secret name used by generated blockvolume Deployments for target-side iSCSI CHAP")
	fs.StringVar(&f.launcherCHAPUserKey, "launcher-iscsi-chap-username-key", "chapUsername", "Kubernetes Secret key for generated blockvolume iSCSI CHAP username")
	fs.StringVar(&f.launcherCHAPSecretKey, "launcher-iscsi-chap-secret-key", "chapSecret", "Kubernetes Secret key for generated blockvolume iSCSI CHAP secret")
	fs.BoolVar(&f.failbackRuntimeRPC, "failback-runtime-rpc", false, "enable disabled-by-default authority failback RPC; requires expected-current and terminal evidence")
	fs.DurationVar(&f.workspaceMaxClockSkew, "workspace-max-clock-skew", 20*time.Second, "maximum workspace clock skew; host lease shortens by half, forced moves wait the full bound")
	fs.BoolVar(&f.workspaceRPC, "workspace-rpc", false, "enable RF1 workspace lease/revocation/move RPCs")
	chunkstore.RegisterSWFSFlags(fs, &f.chunkFiler, &f.chunkPath, &f.chunkCollection, &f.chunkReplication)
	fs.BoolVar(&f.frontendPublicationRuntimeHTTP, "frontend-publication-runtime-http", false, "enable disabled-by-default frontend publication HTTP runtime; confirms post-failback authority publication only")
	fs.StringVar(&f.frontendPublicationRuntimeListen, "frontend-publication-runtime-listen", "127.0.0.1:9334", "HTTP listen address for --frontend-publication-runtime-http")
	fs.BoolVar(&f.strictJournalIsolation, "strict-journal-isolation", false, "refuse new work on nodes whose durable root shares the authority store's file system (one journal): their heartbeats are refused until they serve a volume, and no placement or move targets them; off = alarm only")
	fs.StringVar(&f.statusListen, "status-listen", "", "loopback HTTP address for the read-only GET /status/workspace-lease endpoint; empty = off")
	fs.BoolVar(&f.version, "version", false, "print build provenance and exit")
	fs.DurationVar(&f.auth.connectionAge, "tls-connection-age-test-only", link.MasterConnectionAge, "internal test-only: how long a client connection lives before a reconnect (a fresh handshake)")
	fs.BoolVar(&f.printReadyLine, "t0-print-ready", false, "internal test-only: emit one structured JSON line on stdout after listener bound")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	if removedHeadBase {
		return flags{}, fmt.Errorf("%w: --workspace-head-base-url; blockmaster reads HEAD through the layout catalog: set --chunk-filer (and --chunk-path) instead", errRemovedInV1)
	}
	if f.version {
		return f, nil
	}
	if f.launcherNVMeMaxH2CDataLength > uint(^uint32(0)) {
		return flags{}, fmt.Errorf("--launcher-nvme-max-h2c-data-length=%d exceeds uint32", f.launcherNVMeMaxH2CDataLength)
	}
	if err := nvme.ValidateMaxH2CDataLength(uint32(f.launcherNVMeMaxH2CDataLength)); err != nil {
		return flags{}, fmt.Errorf("--launcher-nvme-max-h2c-data-length=%d invalid: %w", f.launcherNVMeMaxH2CDataLength, err)
	}
	if f.workspaceMaxClockSkew <= 0 || f.workspaceMaxClockSkew >= 60*time.Second {
		return flags{}, fmt.Errorf("--workspace-max-clock-skew must be positive and less than 60s")
	}
	fenceExitBudget := durable.ChunkFenceWatchInterval + volume.WorkspaceDrainTimeout
	if f.workspaceMaxClockSkew/2 <= fenceExitBudget {
		return flags{}, fmt.Errorf("--workspace-max-clock-skew=%s gives half-skew=%s, which must exceed configured fence-exit budget=%s (watch cadence=%s + drain timeout=%s), assuming normal scheduling; increase --workspace-max-clock-skew above %s", f.workspaceMaxClockSkew, f.workspaceMaxClockSkew/2, fenceExitBudget, durable.ChunkFenceWatchInterval, volume.WorkspaceDrainTimeout, 2*fenceExitBudget)
	}
	if f.authorityStore == "" {
		return flags{}, fmt.Errorf("--authority-store is required")
	}
	if err := master.ValidateStatusListen(f.statusListen); err != nil {
		return flags{}, err
	}
	if f.insecureExternalBind {
		return flags{}, fmt.Errorf("--insecure-external-bind is no longer accepted: a plaintext control listener stays on loopback; to listen on the node network start blockmaster with mutual TLS (--tls-cert, --tls-key, --tls-client-ca, --role-file) and drop the flag")
	}
	if err := f.auth.validate(); err != nil {
		return flags{}, err
	}
	if !f.auth.enabled() {
		if err := master.ValidateControlListen("listen", f.listen); err != nil {
			return flags{}, fmt.Errorf("%w; start blockmaster with mutual TLS (--tls-cert, --tls-key, --tls-client-ca, --role-file) to listen on the node network", err)
		}
	}
	if f.frontendPublicationRuntimeHTTP {
		if err := master.ValidateControlListen("frontend-publication-runtime-listen", f.frontendPublicationRuntimeListen); err != nil {
			return flags{}, fmt.Errorf("%w; the runtime HTTP API is unauthenticated and always stays on loopback", err)
		}
	}
	if f.launcherMasterTLSSecretPrefix != "" && !f.auth.enabled() {
		return flags{}, fmt.Errorf("--launcher-master-tls-secret-prefix needs blockmaster's own mutual TLS (--tls-cert, --tls-key, --tls-client-ca, --role-file)")
	}
	if f.launcherMasterTLSSecretPrefix != "" && f.launcherExternalStatus {
		return flags{}, fmt.Errorf("--launcher-external-status cannot be combined with --launcher-master-tls-secret-prefix: an authenticated volume's status listener stays on loopback")
	}
	if f.launcherExternalISCSI && f.launcherCHAPSecretName == "" {
		return flags{}, fmt.Errorf("cannot enable --launcher-external-iscsi without --launcher-iscsi-chap-secret-name")
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
	if err := requireProductLoop(f.lifecycleLoop, f.expectedSlotsPerVol, clusterImport); err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster:", err)
		return 2
	}
	if len(topo.Volumes) == 0 && len(clusterImport.Nodes) == 0 {
		// Without a topology the observation host classifies every
		// volume UnknownReplicaClaim; controller can never mint.
		// That is a valid "empty cluster" startup state and not an
		// error, but operators should know â€” log a prominent line.
		fmt.Fprintln(os.Stderr, "blockmaster: WARNING: --topology not supplied or empty; controller will not mint assignments until topology is provided")
	} else if len(topo.Volumes) == 0 {
		fmt.Fprintln(os.Stderr, "blockmaster: dynamic lifecycle topology enabled by cluster-spec node inventory; assignments will be derived from verified placement intents")
	}
	cfg := master.Config{
		AuthorityStoreDir:                f.authorityStore,
		LifecycleStoreDir:                f.lifecycleStore,
		Listen:                           f.listen,
		Topology:                         topo,
		Freshness:                        authority.FreshnessConfig{FreshnessWindow: f.freshnessWindow, PendingGrace: f.pendingGrace},
		ControllerConfig:                 authority.TopologyControllerConfig{ExpectedSlotsPerVolume: f.expectedSlotsPerVol},
		WorkspaceMaxClockSkew:            f.workspaceMaxClockSkew,
		FailbackRuntimeRPC:               f.failbackRuntimeRPC,
		LifecycleProductLoop:             f.lifecycleLoop > 0,
		FrontendPublicationRuntimeHTTP:   f.frontendPublicationRuntimeHTTP,
		FrontendPublicationRuntimeListen: f.frontendPublicationRuntimeListen,
		StrictJournalIsolation:           f.strictJournalIsolation,
		StatusListen:                     f.statusListen,
	}
	authn, serverOpts, err := f.auth.build()
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster:", err)
		return 1
	}
	cfg.Auth, cfg.ServerOptions = authn, serverOpts
	// The layout catalog is the master's one HEAD source (create, restore and
	// the workspace reader all read through it).
	var layoutCat *chunkstore.BCatalog
	if f.workspaceRPC {
		cat, client, err := chunkstore.DialBCatalog(context.Background(), chunkstore.SWFSConfig{
			FilerGRPC: f.chunkFiler, Collection: f.chunkCollection,
			Replication: f.chunkReplication, AssignPath: f.chunkPath,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockmaster: layout filer:", err)
			return 1
		}
		defer client.Close()
		cfg.LayoutCreateVolume = layoutVolumeCreator(cat)
		cfg.LayoutReadVolume = layoutVolumeReader(cat)
		cfg.LayoutReadHead = layoutHeadReader(cat)
		cfg.LayoutCreateTarget = layoutTargetCreator(cat)
		layoutCat = cat
	}
	if authn != nil {
		if days, until, err := certDaysLeft(f.auth.tlsCert, f.auth.tlsKey, time.Now()); err == nil {
			fmt.Fprintln(os.Stderr, fmt.Sprintf("blockmaster: mutual TLS on; role file %s; own certificate expires %s (%d days)", f.auth.roleFile, until.UTC().Format(time.RFC3339), days))
		}
	} else {
		warnUnauthenticated(f.listen)
	}
	// New starts no loops: bind h before Start permits any HEAD request.
	var h *master.Host
	if f.workspaceRPC {
		reader, err := master.NewLayoutWorkspaceHEADReader(layoutCat, func(ctx context.Context, name string) (chunkstore.VolumeRef, error) {
			return h.WorkspaceLayoutVolume(ctx, name)
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockmaster:", err)
			return 1
		}
		cfg.WorkspaceRPC = true
		cfg.WorkspaceHeadReader = reader
	}
	h, err = master.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster:", err)
		return 1
	}
	// Every promotion decides on the probe round's answers: each candidate
	// answers a fresh round over its authenticated node stream.
	h.SetPromotionEvidenceProvider(h.NewRoundPromotionEvidenceProvider(f.launcherReplicationAck, nil))
	if len(clusterImport.Volumes) > 0 {
		if err := importLifecycleVolumes(h, clusterImport.Volumes); err != nil {
			fmt.Fprintln(os.Stderr, "blockmaster:", err)
			_ = h.Close(context.Background())
			return 1
		}
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
	if authn != nil {
		reloadRolesOnSIGHUP(authn, f.auth.roleFile)
	}
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

// Import only the geometry/RF that the cluster specification owns. Preserve
// existing protocol and Kubernetes metadata; the store rejects geometry drift.
func importLifecycleVolumes(h *master.Host, volumes []lifecycle.VolumeSpec) error {
	stores := h.Lifecycle()
	if stores == nil {
		return fmt.Errorf("cluster volume import requires --lifecycle-store")
	}
	for _, spec := range volumes {
		if old, ok := stores.Volumes.GetVolume(spec.VolumeID); ok {
			merged := old.Spec
			merged.SizeBytes = spec.SizeBytes
			merged.ReplicationFactor = spec.ReplicationFactor
			spec = merged
		}
		imported, err := h.ImportLifecycleVolume(spec)
		if err != nil {
			return fmt.Errorf("register lifecycle volume %s: %w", spec.VolumeID, err)
		}
		if !imported {
			// A chunk volume delete reserved this id: it is never created
			// again, also when the cluster spec still lists it.
			fmt.Fprintf(os.Stderr, "blockmaster: cluster spec volume %s not imported: its id is reserved by a volume delete\n", spec.VolumeID)
		}
	}
	return nil
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
		if stores.Deletions.Reserved(intent.VolumeID) {
			continue // never placed again (see importLifecycleVolumes)
		}
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
			Namespace:                     f.launcherNamespace,
			Image:                         f.launcherImage,
			MasterAddr:                    masterAddr,
			DurableRootBase:               f.launcherDurableRoot,
			DurableImpl:                   f.launcherDurableImpl,
			WALMultiBlockRecords:          f.launcherWALMultiBlockRecords,
			WALRecoveryTestDisableFlusher: f.launcherWALRecoveryTestDisableFlusher,
			StateHostPathBase:             f.launcherStateHostPath,
			ReplicationAck:                f.launcherReplicationAck,
			OwnerReferenceToPVC:           f.launcherPVCOwnerRef,
			EnableStatus:                  f.launcherStatus,
			ExternalISCSI:                 f.launcherExternalISCSI,
			ExternalNVMe:                  f.launcherExternalNVMe,
			ExternalStatus:                f.launcherExternalStatus,
			MasterTLSSecretPrefix:         f.launcherMasterTLSSecretPrefix,
			NVMeMaxH2CDataLength:          uint32(f.launcherNVMeMaxH2CDataLength),
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
		namespaces, renderedByNamespace := renderedManifestsByNamespace(rendered, f.launcherNamespace)
		for _, namespace := range namespaces {
			desired := renderedByNamespace[namespace]
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

func renderedManifestsByNamespace(rendered []launcher.RenderedManifest, fallback string) ([]string, map[string][]launcher.RenderedManifest) {
	seen := map[string]bool{}
	var out []string
	byNamespace := map[string][]launcher.RenderedManifest{}
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
		byNamespace[identity.Namespace] = append(byNamespace[identity.Namespace], manifest)
	}
	return out, byNamespace
}
