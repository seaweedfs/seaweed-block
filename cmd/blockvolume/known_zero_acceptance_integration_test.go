//go:build subprocess && chunkstore_integration

package main_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// TestKnownZeroAcceptance is the KNOWN-ZERO acceptance test of a fresh volume,
// gated by KZ_RUN=1. Source role (this file, steps 1-2):
//
//	(1) a fresh volume of KZ_SIZE (default 1T) and an empty first snapshot S0:
//	    it finishes in seconds, stages and uploads nothing, and HEAD names no
//	    segment; the fresh attach certified chunks known-zero.
//	(2) mkfs (KZ_FS: ext4 and xfs, one volume each) on the kernel NVMe/TCP
//	    device, small files with known sha256, sync, snapshot S1: the chunks
//	    mkfs dirtied, staged chunks, uploaded bytes and HEAD size, bounded by
//	    the touched chunks.
//
// Device steps run only with KZ_DEVICE=1 (root through sudo -n, nvme-cli,
// mkfs). Every device path comes from kzResolve on this run's own NQN; a path
// given by name is never formatted. Without KZ_DEVICE the test writes known
// blocks through the user-space NVMe client instead (a smoke of the counters,
// not the acceptance run). Each step prints one "KZ STEP n PASS|FAIL <why>"
// line and its counters into KZ_REPORT.
func TestKnownZeroAcceptance(t *testing.T) {
	if os.Getenv("KZ_RUN") != "1" {
		t.Skip("KZ_RUN=1 not set")
	}
	size := os.Getenv("KZ_SIZE")
	if size == "" {
		size = "1T"
	}
	bytes := headSzParse(t, size)
	fss := strings.Split(os.Getenv("KZ_FS"), ",")
	if os.Getenv("KZ_FS") == "" {
		fss = []string{"ext4", "xfs"}
	}
	device := os.Getenv("KZ_DEVICE") == "1"
	run := os.Getenv("KZ_RUN_ID")
	if run == "" {
		run = fmt.Sprintf("local-%d", time.Now().Unix())
	}
	// KZ_ROLE: "" = every step in this process (the smoke); "source" = steps
	// 1, 2 and 5 here, the restore target on KZ_TARGET_HOST (the
	// second host), handed over through KZ_HANDOFF; "target" = steps
	// 3 and 4 on this host against the source's cluster named in KZ_HANDOFF.
	role := os.Getenv("KZ_ROLE")
	rep := &kzReport{Run: run, Size: size, Bytes: bytes, Device: device, Role: role, StartedAt: time.Now().UTC()}
	defer rep.write(t)
	if role == "target" {
		kzTargetRole(t, rep, bytes, device, run)
		return
	}
	targetHost := ""
	if role == "source" {
		if targetHost = os.Getenv("KZ_TARGET_HOST"); targetHost == "" || os.Getenv("KZ_HANDOFF") == "" {
			t.Fatal("KZ_ROLE=source needs KZ_TARGET_HOST and KZ_HANDOFF")
		}
	} else if role != "" {
		t.Fatalf("KZ_ROLE %q: want source, target or unset", role)
	}
	// per file system: the source and a restore target of its S1 on its own
	// server node (on KZ_TARGET_HOST in the source role); fork is not in v1
	var vols []headSzVol
	for _, fs := range fss {
		vols = append(vols, headSzVol{id: "kz" + fs, bytes: bytes},
			headSzVol{id: "kz" + fs + "r", bytes: bytes, server: "t-" + fs + "r", host: targetHost, restoreTarget: true})
	}
	// step 5: up to kzCrashTries fresh volumes (a try whose stop lands after the
	// attach commit has committed that store, so a retry needs a new one)
	for i := 1; i <= kzCrashTries; i++ {
		vols = append(vols, headSzVol{id: fmt.Sprintf("kzc%d", i), bytes: bytes})
	}
	env, _ := headSzStartCluster(t, "kz", vols)
	rep.ChunkPath = env.chunkPath
	ho := kzHandoff{Run: run, Bytes: bytes, FilerGRPC: env.cluster.filerGRPC, FilerHTTP: env.cluster.filerHTTP,
		Master: env.h.masterAddr, ChunkPath: env.chunkPath}
	for _, fs := range fss {
		v := kzSource(t, env, rep, env.hosts["kz"+fs], fs, bytes, device, run)
		rep.Volumes = append(rep.Volumes, v)
		if !v.S1.Completed {
			continue
		}
		for _, kind := range []string{"restore"} {
			tb := env.hosts["kz"+fs+kind[:1]]
			if role == "source" {
				ho.Targets = append(ho.Targets, kzHandoffTarget{Kind: kind, Volume: tb.volume, Server: tb.server, Data: tb.data, Ctrl: tb.ctrl,
					Source: v.Volume, S1: v.S1Name, Dirty1: v.Dirty1, Files: v.Files, Blocks: kzHexBlocks(v.Blocks)})
				continue
			}
			rep.Targets = append(rep.Targets, kzTarget(t, env, rep, v, kind, tb, bytes, device, run))
		}
	}
	if role == "source" {
		kzWriteHandoff(t, ho)
		kzWaitTargetDone(t)
	}
	rep.Crash = kzCrashFirstStart(t, env, rep, bytes, run)
}

// kzHandoff is what the source role hands to the target role (KZ_HANDOFF):
// the cluster to use and, per target, the S1 to restore and what the
// source wrote into it.
type kzHandoff struct {
	Run       string            `json:"run"`
	Bytes     uint64            `json:"bytes"`
	FilerGRPC string            `json:"filer_grpc"`
	FilerHTTP string            `json:"filer_http"`
	Master    string            `json:"master"`
	ChunkPath string            `json:"chunk_path"`
	Targets   []kzHandoffTarget `json:"targets"`
}

type kzHandoffTarget struct {
	Kind   string            `json:"kind"`
	Volume string            `json:"volume"`
	Server string            `json:"server"`
	Data   string            `json:"data_addr"`
	Ctrl   string            `json:"ctrl_addr"`
	Source string            `json:"source"`
	S1     string            `json:"s1"`
	Dirty1 int               `json:"dirty1"`
	Files  map[string]string `json:"files_sha256"`
	Blocks map[string]string `json:"blocks_hex"`
}

func kzHexBlocks(b map[uint64][]byte) map[string]string {
	out := map[string]string{}
	for lba, d := range b {
		out[fmt.Sprint(lba)] = hex.EncodeToString(d)
	}
	return out
}

func kzWriteHandoff(t *testing.T, ho kzHandoff) {
	t.Helper()
	b, _ := json.MarshalIndent(ho, "", " ")
	p := os.Getenv("KZ_HANDOFF")
	if err := os.WriteFile(p+".tmp", append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(p+".tmp", p); err != nil {
		t.Fatal(err)
	}
	t.Logf("KZ HANDOFF written: %s (%d targets)", p, len(ho.Targets))
}

// kzWaitTargetDone waits for KZ_TARGET_DONE (the window script creates it
// after the target role ended), at most KZ_TARGET_WAIT (default 90m). The
// source's cluster must stay up while the targets run.
func kzWaitTargetDone(t *testing.T) {
	t.Helper()
	done := os.Getenv("KZ_TARGET_DONE")
	if done == "" {
		t.Fatal("KZ_ROLE=source needs KZ_TARGET_DONE")
	}
	wait := 90 * time.Minute
	if v := os.Getenv("KZ_TARGET_WAIT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("KZ_TARGET_WAIT=%q: %v", v, err)
		}
		wait = d
	}
	poll(t, wait, "the target role never finished ("+done+" absent)", func() bool {
		_, err := os.Stat(done)
		return err == nil
	})
}

// kzTargetEnv reserves the data and ctrl ports the source's cluster spec names
// for the targets BEFORE the harness picks any port of its own (its
// constructor allocates five), then builds the env on the source's filer and
// blockmaster.
func kzTargetEnv(t *testing.T, ho kzHandoff, newHarness func(*testing.T) *plannedHandoffHarness) *handoffEnv {
	t.Helper()
	handoffPorts.Lock()
	if handoffPorts.used == nil {
		handoffPorts.used = map[int]bool{}
	}
	for _, ht := range ho.Targets {
		if err := kzReserve(handoffPorts.used, ht.Data, ht.Ctrl); err != nil {
			handoffPorts.Unlock()
			t.Fatalf("KZ_HANDOFF target %s: %v", ht.Volume, err)
		}
	}
	handoffPorts.Unlock()
	h := newHarness(t)
	h.masterAddr = ho.Master
	return &handoffEnv{h: h, cluster: &w4WeedCluster{filerGRPC: ho.FilerGRPC, filerHTTP: ho.FilerHTTP}, chunkPath: ho.ChunkPath, hosts: map[string]bvHost{}}
}

// TestKnownZeroTargetReservesBeforeHarness: when the harness constructor runs,
// every handed-off port is already reserved, so none of its own five ports can
// be one of them (and then be refused as a collision).
func TestKnownZeroTargetReservesBeforeHarness(t *testing.T) {
	ho := kzHandoff{Master: "10.9.9.1:1", Targets: []kzHandoffTarget{{Volume: "v", Data: "10.9.9.2:19991", Ctrl: "10.9.9.2:19992"}}}
	called := false
	kzTargetEnv(t, ho, func(t *testing.T) *plannedHandoffHarness {
		called = true
		handoffPorts.Lock()
		defer handoffPorts.Unlock()
		if !handoffPorts.used[19991] || !handoffPorts.used[19992] {
			t.Fatal("the harness was built before the handed-off ports were reserved")
		}
		return &plannedHandoffHarness{dir: t.TempDir()}
	})
	if !called {
		t.Fatal("the harness constructor was never called")
	}
}

// kzTargetRole runs steps 3 and 4 on this host: no weed cluster or
// blockmaster of its own; every blockvolume uses the source's filer and
// blockmaster from KZ_HANDOFF, with the data and ctrl addresses the source's
// cluster spec names for it.
func kzTargetRole(t *testing.T, rep *kzReport, bytes uint64, device bool, run string) {
	t.Helper()
	b, err := os.ReadFile(os.Getenv("KZ_HANDOFF"))
	if err != nil {
		t.Fatalf("KZ_HANDOFF: %v", err)
	}
	var ho kzHandoff
	if err := json.Unmarshal(b, &ho); err != nil {
		t.Fatalf("KZ_HANDOFF: %v", err)
	}
	if ho.Run != run || ho.Bytes != bytes || len(ho.Targets) == 0 {
		t.Fatalf("KZ_HANDOFF run %q bytes %d targets %d; this run %q bytes %d", ho.Run, ho.Bytes, len(ho.Targets), run, bytes)
	}
	env := kzTargetEnv(t, ho, newPlannedHandoffHarness)
	h := env.h
	rep.ChunkPath = ho.ChunkPath
	for _, ht := range ho.Targets {
		tb := bvHost{id: ht.Volume, server: ht.Server, replica: "r1", volume: ht.Volume, root: filepath.Join(h.dir, "vol-"+ht.Volume),
			data: ht.Data, ctrl: ht.Ctrl, status: headSzLoopbackAddr(t), nvme: headSzLoopbackAddr(t)}
		env.hosts[ht.Volume] = tb
		src := kzVolume{Volume: ht.Source, S1Name: ht.S1, Dirty1: ht.Dirty1, Files: ht.Files, Blocks: map[uint64][]byte{}}
		for k, v := range ht.Blocks {
			var lba uint64
			if _, err := fmt.Sscan(k, &lba); err != nil {
				t.Fatalf("KZ_HANDOFF block %q: %v", k, err)
			}
			d, err := hex.DecodeString(v)
			if err != nil {
				t.Fatalf("KZ_HANDOFF block %q: %v", k, err)
			}
			src.Blocks[lba] = d
		}
		rep.Targets = append(rep.Targets, kzTarget(t, env, rep, src, ht.Kind, tb, bytes, device, run))
	}
}

type kzTargetRecord struct {
	Volume     string      `json:"volume"`
	Kind       string      `json:"kind"`
	Source     string      `json:"source"`
	Snapshot   string      `json:"snapshot"`
	NQN        string      `json:"nqn"`
	Identity   string      `json:"device_identity,omitempty"`
	Before     kzStats     `json:"stats_before_reads"`
	After      kzStats     `json:"stats_after_reads"`
	ReadsEqual int         `json:"reads_equal"`
	ReadsZero  int         `json:"reads_zero"`
	Reads      int         `json:"reads"`
	FilesEqual int         `json:"files_equal"`
	FirstCut   kzCutRecord `json:"first_snapshot"`
}

// kzTarget restores the source's S1 into tb, then runs step 4 (read
// the S1-named blocks BEFORE hydration: data, never zeros; the read itself
// waits for its chunk) and step 3 (all contents equal the source; the target's
// first snapshot completes without a full scan).
func kzTarget(t *testing.T, env *handoffEnv, rep *kzReport, src kzVolume, kind string, tb bvHost, bytes uint64, device bool, run string) kzTargetRecord {
	t.Helper()
	r := kzTargetRecord{Volume: tb.volume, Kind: kind, Source: src.Volume, Snapshot: src.S1Name,
		NQN: fmt.Sprintf("nqn.2026-11.io.seaweedfs:kz-%s-%s", run, tb.volume)}
	// The target is placement only: before its restore the master has no
	// record of it, so the restore creates it.
	if out, errOut, code := env.h.run(t, "ops", "chunk", "show", "--master", env.h.masterAddr, "--volume", tb.volume,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath); code == 0 || !strings.Contains(out+errOut, "resolve --volume") || !strings.Contains(out+errOut, "not found") {
		t.Fatalf("restore target %s has a volume record before its restore: exit %d %s %s", tb.volume, code, out, errOut)
	}
	env.h.command(t, "ops", "chunk", "restore", "--source", src.Volume, "--snapshot", src.S1Name, "--target", tb.volume,
		"--master", env.h.masterAddr, "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath, "--request-id", "kz-"+kind+"-"+tb.volume)
	d, refusal := headSzStartVolume(t, env, tb, bytes, "--nvme-subsysnqn", r.NQN)
	if d == nil {
		rep.step(t, 3, tb.volume, false, kind+" target refused: "+refusal)
		return r
	}
	defer d.term(t, 60*time.Second)

	// (4) wrong-zero guard, before any hydration (a restore is driven only after)
	r.Before = kzReadStats(t, tb)
	c := dialNVMe(t, tb.nvme, r.NQN)
	zero := make([]byte, 4096)
	for lba, want := range src.Blocks {
		_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
		got := c.read(t, lba, 4096)
		r.Reads++
		switch {
		case string(got) == string(want):
			r.ReadsEqual++
		case string(got) == string(zero):
			r.ReadsZero++
		}
	}
	c.close()
	r.After = kzReadStats(t, tb)
	waited := r.After.Hydration.WaitCalls - r.Before.Hydration.WaitCalls
	ok4 := r.Reads > 0 && r.ReadsEqual == r.Reads && r.ReadsZero == 0 && r.Before.AbsentChunks > 0 && waited > 0
	rep.step(t, 4, tb.volume, ok4, fmt.Sprintf("%s reads=%d equal=%d zero=%d absent_before=%d absent_after=%d read_waits=%d fetches=%d",
		kind, r.Reads, r.ReadsEqual, r.ReadsZero, r.Before.AbsentChunks, r.After.AbsentChunks, waited,
		r.After.Hydration.Fetches-r.Before.Hydration.Fetches))

	// (3) contents equal + first snapshot without a full scan (v1 fills a
	// restore target on access: the reads below fetch any chunk still absent)
	if device {
		r.Identity, r.FilesEqual = kzDeviceVerify(t, tb, r.NQN, src, run)
	} else {
		c := dialNVMe(t, tb.nvme, r.NQN)
		for lba, want := range src.Blocks {
			_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
			if string(c.read(t, lba, 4096)) == string(want) {
				r.FilesEqual++
			}
		}
		c.close()
	}
	total := int((bytes + kzChunkBytes - 1) / kzChunkBytes)
	r.FirstCut = kzCut(t, env, headSzConfig{snapshotTimeout: "120s", attempts: 3, name: "kz-" + tb.volume + "-s"}, tb, 0, total)
	want := len(src.Files)
	cand := kzLC(r.FirstCut.Stats).CandidateChunks
	ok3 := r.FilesEqual == want && want > 0 && r.FirstCut.Completed && cand <= src.Dirty1
	rep.step(t, 3, tb.volume, ok3, fmt.Sprintf("%s files_equal=%d/%d first_snapshot completed=%v candidates=%d (source dirtied %d) of %d chunks wall_ms=%.0f",
		kind, r.FilesEqual, want, r.FirstCut.Completed, cand, src.Dirty1, total, r.FirstCut.WallMs))
	return r
}

// kzWaitDevice resolves this run's NQN from sysfs for up to 30 s; on timeout
// the last refusal is in the failure, so a lab log names the check that held.
func kzWaitDevice(t *testing.T, nqn string) kzDevice {
	t.Helper()
	sys := os.DirFS("/sys/class")
	var last error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		d, err := kzResolve(sys, nqn, "")
		if err == nil {
			return d
		}
		last = err
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("kernel device for %s never appeared: %v", nqn, last)
	return kzDevice{}
}

// kzDeviceVerify connects the target through the kernel, resolves it by
// NQN, mounts it read-only under the run's scratch, compares every source
// file's sha256, unmounts and disconnects.
func kzDeviceVerify(t *testing.T, tb bvHost, nqn string, src kzVolume, run string) (string, int) {
	t.Helper()
	host, port := kzSplit(t, tb.nvme)
	kzSudo(t, "nvme", "connect", "-t", "tcp", "-n", nqn, "-a", host, "-s", port)
	defer kzSudo(t, "nvme", "disconnect", "-n", nqn)
	var dev kzDevice
	dev = kzWaitDevice(t, nqn)
	t.Log(dev.Identity())
	mnt := filepath.Join(os.TempDir(), "kz-"+run, tb.volume)
	if err := os.MkdirAll(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	kzSudo(t, "mount", "-o", "ro", dev.Path, mnt)
	defer kzSudo(t, "umount", mnt)
	equal := 0
	for name, want := range src.Files {
		out := kzSudo(t, "sha256sum", filepath.Join(mnt, name))
		if f := strings.Fields(out); len(f) > 0 && f[0] == want {
			equal++
		}
	}
	return dev.Identity(), equal
}

// kzReport is the JSON evidence (KZ_REPORT); Lines are the step verdicts.
type kzReport struct {
	Run       string           `json:"run"`
	Size      string           `json:"size"`
	Bytes     uint64           `json:"bytes"`
	Device    bool             `json:"device"`
	Role      string           `json:"role,omitempty"`
	ChunkPath string           `json:"chunk_path"`
	StartedAt time.Time        `json:"started_at"`
	Volumes   []kzVolume       `json:"volumes"`
	Targets   []kzTargetRecord `json:"targets"`
	Crash     kzCrashRecord    `json:"crash"`
	HoldBin   string           `json:"hold_bin,omitempty"`
	Lines     []string         `json:"lines"`
}

const kzCrashTries = 3

type kzCrashTry struct {
	Volume  string   `json:"volume"`
	Markers []string `json:"markers_at_stop"`
	Before  bool     `json:"stopped_before_commit"`
}

type kzCrashRecord struct {
	Tries     []kzCrashTry `json:"tries"`
	Volume    string       `json:"volume,omitempty"`
	FreshLine string       `json:"fresh_line_after_restart,omitempty"`
	S0        kzCutRecord  `json:"s0_after_restart"`
	Dirty1    int          `json:"dirty_chunks_before_s1"`
	S1        kzCutRecord  `json:"s1_after_restart"`
	Equal     int          `json:"blocks_equal"`
	Blocks    int          `json:"blocks"`
}

// kzHoldBin is KZ_HOLD_BIN: the kzhold-tagged blockvolume (go build -tags kzhold
// ./cmd/blockvolume), test-only and never released; its sha256 goes into the
// report so the evidence names exactly which binary held.
func kzHoldBin(t *testing.T, rep *kzReport) string {
	t.Helper()
	p := os.Getenv("KZ_HOLD_BIN")
	if p == "" {
		t.Fatal("step 5 needs KZ_HOLD_BIN (a blockvolume built with -tags kzhold)")
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("KZ_HOLD_BIN: %v", err)
	}
	sum := sha256.Sum256(b)
	rep.HoldBin = p + " sha256:" + hex.EncodeToString(sum[:])
	return p
}

// kzFreshAtStop reads, after the held first start was killed, whether b's
// store is still fresh: the fresh state is in the store's descriptor
// (WALStore.LayoutIdentity), set before the first claim and cleared only by
// the attach commit; the .chunkfresh sidecar the step once looked for no
// longer exists. The store is the one under b's layout directory
// (<root>/<volume>-<id8>/, --layout-id), or under b's root without one. The
// returned record (markers_at_stop) names the store read and what it held.
func kzFreshAtStop(b bvHost) (bool, []string) {
	stores, _ := filepath.Glob(filepath.Join(b.root, b.volume+"-*", b.volume+".bin"))
	if len(stores) == 0 {
		stores, _ = filepath.Glob(filepath.Join(b.root, b.volume+".bin"))
	}
	if len(stores) != 1 {
		return false, []string{fmt.Sprintf("%d stores for %s under %s", len(stores), b.volume, b.root)}
	}
	ws, err := storage.OpenWALStore(stores[0])
	if err != nil {
		return false, []string{fmt.Sprintf("%s: open: %v", stores[0], err)}
	}
	defer ws.Close()
	claim, fresh, ok := ws.LayoutIdentity()
	return ok && fresh, []string{fmt.Sprintf("%s layout_identity set=%t fresh=%t claim=%s", stores[0], ok, fresh, claim)}
}

// kzCrashFirstStart is step 5:
// the first start of a fresh volume runs the kzhold test binary, which holds
// at the boundary before the fresh marker is retired; the test SIGKILLs it
// there and requires the store still fresh (kzFreshAtStop). More than one try fails
// the step. Then restart that volume and
// repeat steps 1-2: nothing certified known-zero (KnownZeroChunks 0, no
// fresh-attach line), the first cut scans every chunk once, the next cut only
// the touched ones, and the written blocks read back exactly.
func kzCrashFirstStart(t *testing.T, env *handoffEnv, rep *kzReport, bytes uint64, run string) kzCrashRecord {
	t.Helper()
	var r kzCrashRecord
	total := int((bytes + kzChunkBytes - 1) / kzChunkBytes)
	var b bvHost
	for i := 1; i <= kzCrashTries; i++ {
		b = env.hosts[fmt.Sprintf("kzc%d", i)]
		nqn := fmt.Sprintf("nqn.2026-11.io.seaweedfs:kz-%s-%s", run, b.volume)
		env.volExtra = []string{"--durable-blocks", fmt.Sprint(bytes / 4096), "--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off", "--nvme-subsysnqn", nqn}
		// Deterministic, with no hook in a release binary: the
		// first start runs KZ_HOLD_BIN, a blockvolume built with the kzhold tag
		// (test-only, never released), which holds at the provider's existing
		// boundary after presence and before the fresh marker is retired (the
		// attach commit) and writes <dir>/reached there. The release stays
		// absent: the process is killed while held.
		holdDir := filepath.Join(env.h.dir, "kzhold-"+b.id)
		if err := os.MkdirAll(holdDir, 0o755); err != nil {
			t.Fatal(err)
		}
		reached := filepath.Join(holdDir, "reached")
		os.Setenv("SWBLOCK_KZHOLD_DIR", holdDir)
		d := env.h.start(t, "blockvolume-"+b.id+"-first", kzHoldBin(t, rep), env.volArgsFor(b)...)
		os.Unsetenv("SWBLOCK_KZHOLD_DIR")
		end := time.Now().Add(120 * time.Second)
		for {
			if _, err := os.Stat(reached); err == nil {
				break
			}
			select {
			case <-d.done:
				t.Fatalf("%s exited before the hold was reached: %v", b.id, d.err)
			case <-time.After(20 * time.Millisecond):
			}
			if time.Now().After(end) {
				t.Fatalf("%s never reached the hold (%s)", b.id, reached)
			}
		}
		if !d.has("TEST-ONLY build (kzhold)") {
			t.Fatalf("%s: the first start is not the kzhold build", b.id)
		}
		if err := d.proc.Signal(syscall.SIGSTOP); err != nil {
			t.Fatalf("SIGSTOP %s: %v", b.id, err)
		}
		d.kill(t)
		fresh, markers := kzFreshAtStop(b)
		try := kzCrashTry{Volume: b.volume, Markers: markers, Before: fresh}
		r.Tries = append(r.Tries, try)
		if try.Before {
			break
		}
	}
	last := r.Tries[len(r.Tries)-1]
	if !last.Before {
		rep.step(t, 5, "kzc", false, fmt.Sprintf("no try stopped before the attach commit in %d tries (marker always gone)", len(r.Tries)))
		return r
	}
	r.Volume = b.volume
	nqn := fmt.Sprintf("nqn.2026-11.io.seaweedfs:kz-%s-%s", run, b.volume)
	d, refusal := headSzStartVolume(t, env, b, bytes, "--nvme-subsysnqn", nqn)
	if d == nil {
		rep.step(t, 5, b.volume, false, "restart refused: "+refusal)
		return r
	}
	defer d.term(t, 60*time.Second)
	r.FreshLine = strings.TrimSpace(d.line("known-zero"))
	cfg := headSzConfig{snapshotTimeout: "120s", attempts: 3, name: "kz-" + b.volume + "-s"}
	r.S0 = kzCut(t, env, cfg, b, 0, total)
	_, blocks := kzUserSpaceFiles(t, b, nqn, bytes)
	r.Dirty1 = kzReadStats(t, b).DirtyChunks
	r.S1 = kzCut(t, env, cfg, b, 1, total)
	c := dialNVMe(t, b.nvme, nqn)
	for lba, want := range blocks {
		_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
		r.Blocks++
		if string(c.read(t, lba, 4096)) == string(want) {
			r.Equal++
		}
	}
	c.close()
	s0, s1 := kzLC(r.S0.Stats), kzLC(r.S1.Stats)
	// the stop is deterministic (the kzhold hold), so an acceptance run must catch the marker on the FIRST try
	ok := len(r.Tries) == 1 && r.S0.Completed && r.S1.Completed && r.S0.Stats.KnownZeroChunks == 0 && r.FreshLine == "" &&
		s0.CandidateChunks == total && s1.CandidateChunks <= r.Dirty1 && r.Equal == r.Blocks && r.Blocks > 0
	rep.step(t, 5, b.volume, ok, fmt.Sprintf("tries=%d marker_at_stop=%v known_zero=%d fresh_line=%t s0_candidates=%d/%d s0_uploaded=%d s1_candidates=%d (dirtied %d) blocks_equal=%d/%d",
		len(r.Tries), last.Markers, r.S0.Stats.KnownZeroChunks, r.FreshLine != "", s0.CandidateChunks, total, s0.UploadedChunks, s1.CandidateChunks, r.Dirty1, r.Equal, r.Blocks))
	return r
}

func (r *kzReport) step(t *testing.T, n int, vol string, pass bool, why string) {
	t.Helper()
	v := "FAIL"
	if pass {
		v = "PASS"
	}
	l := fmt.Sprintf("KZ STEP %d %s %s %s", n, v, vol, why)
	r.Lines = append(r.Lines, l)
	t.Log(l)
	if !pass {
		t.Error(l)
	}
}

func (r *kzReport) write(t *testing.T) {
	if p := os.Getenv("KZ_REPORT"); p != "" {
		b, _ := json.MarshalIndent(r, "", " ")
		if err := os.WriteFile(p, append(b, '\n'), 0o644); err != nil {
			t.Errorf("KZ_REPORT: %v", err)
		}
	}
}

type kzVolume struct {
	Volume   string            `json:"volume"`
	FS       string            `json:"fs"`
	NQN      string            `json:"nqn"`
	Identity string            `json:"device_identity,omitempty"`
	Fresh    string            `json:"fresh_attach_line"`
	S0       kzCutRecord       `json:"s0"`
	Dirty1   int               `json:"dirty_chunks_before_s1"`
	Discard  string            `json:"discard,omitempty"`
	S1       kzCutRecord       `json:"s1"`
	Files    map[string]string `json:"files_sha256,omitempty"`
	S1Name   string            `json:"s1_name"`
	Blocks   map[uint64][]byte `json:"-"`
}

type kzCutRecord struct {
	Cut       headSzCut `json:"cut"`
	Stats     kzStats   `json:"stats_after"`
	Total     int       `json:"total_chunks"`
	WallMs    float64   `json:"wall_ms"`
	Completed bool      `json:"completed"`
}

func kzReadStats(t *testing.T, b bvHost) kzStats {
	t.Helper()
	var c struct{ Stats kzStats }
	raw := headSzChunkStatus(t, b)
	if raw == nil {
		t.Fatalf("%s: durable status has no chunk section", b.volume)
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("%s: chunk status: %v", b.volume, err)
	}
	return c.Stats
}

const kzChunkBytes = 4 << 20

func kzSource(t *testing.T, env *handoffEnv, rep *kzReport, b bvHost, fs string, bytes uint64, device bool, run string) kzVolume {
	t.Helper()
	total := int((bytes + kzChunkBytes - 1) / kzChunkBytes)
	v := kzVolume{Volume: b.volume, FS: fs, NQN: fmt.Sprintf("nqn.2026-11.io.seaweedfs:kz-%s-%s", run, b.volume)}
	d, refusal := headSzStartVolume(t, env, b, bytes, "--nvme-subsysnqn", v.NQN)
	if d == nil {
		rep.step(t, 1, b.volume, false, "volume refused: "+refusal)
		return v
	}
	defer d.term(t, 60*time.Second)
	v.Fresh = strings.TrimSpace(d.line("known-zero"))

	// (1) empty S0
	cfg := headSzConfig{snapshotTimeout: "120s", attempts: 3, name: "kz-" + b.volume + "-s"}
	v.S0 = kzCut(t, env, cfg, b, 0, total)
	st := v.S0.Stats
	ok1, why1 := kzStep1Pass(st, v.S0.Completed, v.S0.Cut.HeadSegments, total, v.Fresh != "")
	rep.step(t, 1, b.volume, ok1, fmt.Sprintf("%s s0 completed=%v wall_ms=%.0f candidates=%d staged=%d uploaded_chunks=%d head_segments=%d head_bytes=%d known_zero=%d/%d fresh_line=%t",
		why1, v.S0.Completed, v.S0.WallMs, kzLC(st).CandidateChunks, kzLC(st).StagedChunks, kzLC(st).UploadedChunks, v.S0.Cut.HeadSegments, v.S0.Cut.HeadBytes, st.KnownZeroChunks, total, v.Fresh != ""))

	// (2) mkfs + files + S1
	if device {
		var first map[string]uint64
		v.Identity, v.Discard, v.Files, first = kzDeviceFiles(t, b, v.NQN, fs, run)
		v.Blocks = kzDeviceWitnesses(t, b, v.NQN, first)
	} else {
		v.Files, v.Blocks = kzUserSpaceFiles(t, b, v.NQN, bytes)
	}
	v.Dirty1 = kzReadStats(t, b).DirtyChunks
	v.S1 = kzCut(t, env, cfg, b, 1, total)
	v.S1Name = cfg.name + "-1"
	st = v.S1.Stats
	touched := v.Dirty1
	ok2 := v.S1.Completed && st.LastCut != nil && touched > 0 && st.LastCut.CandidateChunks <= touched &&
		st.LastCut.StagedChunks <= touched && st.LastCut.UploadedBytes <= int64(touched)*kzChunkBytes && len(v.Files) > 0 &&
		len(v.Blocks) == len(v.Files) // one block witness per written file or block, for step 4
	rep.step(t, 2, b.volume, ok2, fmt.Sprintf("s1 completed=%v wall_ms=%.0f dirtied=%d candidates=%d staged=%d uploaded_bytes=%d head_bytes=%d files=%d discard=%q",
		v.S1.Completed, v.S1.WallMs, touched, kzLC(st).CandidateChunks, kzLC(st).StagedChunks, kzLC(st).UploadedBytes, v.S1.Cut.HeadBytes, len(v.Files), v.Discard))
	return v
}

// kzLC returns the last cut or a zero value, for printing only.
func kzLC(s kzStats) struct {
	CandidateChunks, StagedChunks, UploadedChunks, ZeroedChunks, UnchangedChunks int
	UploadedBytes                                                                int64
} {
	if s.LastCut == nil {
		return struct {
			CandidateChunks, StagedChunks, UploadedChunks, ZeroedChunks, UnchangedChunks int
			UploadedBytes                                                                int64
		}{}
	}
	return *s.LastCut
}

func kzCut(t *testing.T, env *handoffEnv, cfg headSzConfig, b bvHost, n, total int) kzCutRecord {
	t.Helper()
	start := time.Now()
	c := headSzCutOnce(t, env, cfg, b, n, 0)
	return kzCutRecord{Cut: c, Stats: kzReadStats(t, b), Total: total, WallMs: ms(time.Since(start)), Completed: c.Completed}
}

// kzUserSpaceFiles writes three known 4 KiB blocks (the first, middle and last
// block of the volume, so three different chunks) through the user-space
// client and flushes (smoke only: no file system).
func kzUserSpaceFiles(t *testing.T, b bvHost, nqn string, bytes uint64) (map[string]string, map[uint64][]byte) {
	t.Helper()
	c := dialNVMe(t, b.nvme, nqn)
	out, blocks := map[string]string{}, map[uint64][]byte{}
	last := bytes/4096 - 1
	for i, lba := range []uint64{0, last / 2, last} {
		blk := f1Block(b.id, 0, "kz", lba)
		_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
		c.write(t, lba, blk)
		blocks[lba] = blk
		sum := sha256.Sum256(blk)
		out[fmt.Sprintf("block-%d", i)] = hex.EncodeToString(sum[:])
	}
	_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
	c.flush(t)
	return out, blocks
}

// kzDeviceFiles connects this run's NQN through the kernel, resolves the
// device by NQN, formats it, writes files with known sha256, syncs, unmounts
// and disconnects. It returns the identity line, whether mkfs sent a discard,
// the files' sha256 and each file's first device block (FIEMAP, 4 KiB blocks).
func kzDeviceFiles(t *testing.T, b bvHost, nqn, fs, run string) (string, string, map[string]string, map[string]uint64) {
	t.Helper()
	host, port := kzSplit(t, b.nvme)
	kzSudo(t, "nvme", "connect", "-t", "tcp", "-n", nqn, "-a", host, "-s", port)
	defer kzSudo(t, "nvme", "disconnect", "-n", nqn)
	var dev kzDevice
	dev = kzWaitDevice(t, nqn)
	t.Log(dev.Identity())
	mkfs := map[string][]string{"ext4": {"mkfs.ext4", "-F", "-v"}, "xfs": {"mkfs.xfs", "-f"}}[fs]
	if mkfs == nil {
		t.Fatalf("KZ_FS %q: want ext4 or xfs", fs)
	}
	out := kzSudo(t, append(mkfs, dev.Path)...)
	discard := "no"
	if strings.Contains(strings.ToLower(out), "discard") {
		discard = "yes: " + kzFirstLine(out, "iscard")
	}
	mnt := filepath.Join(os.TempDir(), "kz-"+run, b.volume)
	if err := os.MkdirAll(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	kzSudo(t, "mount", dev.Path, mnt)
	files, first := map[string]string{}, map[string]uint64{}
	func() {
		defer kzSudo(t, "umount", mnt)
		if bs := strings.TrimSpace(kzSudo(t, "stat", "-f", "-c", "%S", mnt)); bs != "4096" {
			t.Fatalf("%s: file-system block size %s, the witness mapping needs 4096", mnt, bs)
		}
		for i := 0; i < 4; i++ {
			name := fmt.Sprintf("kz-%d.bin", i)
			data := make([]byte, (i+1)*64*1024)
			for j := range data {
				data[j] = byte(i*31 + j%251)
			}
			kzSudoWrite(t, filepath.Join(mnt, name), data)
			sum := sha256.Sum256(data)
			files[name] = hex.EncodeToString(sum[:])
		}
		kzSudo(t, "sync")
		for name := range files {
			pb, err := kzFirstPhysical(kzSudo(t, "filefrag", "-e", filepath.Join(mnt, name)))
			if err != nil {
				t.Fatalf("filefrag %s: %v", name, err)
			}
			first[name] = pb
		}
	}()
	return dev.Identity(), discard, files, first
}

// kzDeviceWitnesses reads, through the user-space client after the kernel
// disconnected, the first block of every file and requires it to be that
// file's first 4 KiB as written: the block witnesses step 4 reads on the
// targets before hydration.
func kzDeviceWitnesses(t *testing.T, b bvHost, nqn string, first map[string]uint64) map[uint64][]byte {
	t.Helper()
	c := dialNVMe(t, b.nvme, nqn)
	defer c.close()
	out := map[uint64][]byte{}
	for name, lba := range first {
		var i int
		if _, err := fmt.Sscanf(name, "kz-%d.bin", &i); err != nil {
			t.Fatalf("file %q: %v", name, err)
		}
		want := make([]byte, 4096)
		for j := range want {
			want[j] = byte(i*31 + j%251)
		}
		_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
		got := c.read(t, lba, 4096)
		if string(got) != string(want) {
			t.Fatalf("%s: device block %d is not the file's first 4 KiB (the witness mapping is wrong)", name, lba)
		}
		out[lba] = got
	}
	return out
}

func kzSudo(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("sudo", append([]string{"-n"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("sudo %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out)
}

func kzSudoWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	c := exec.Command("sudo", "-n", "tee", path)
	c.Stdin = strings.NewReader(string(data))
	out, err := c.CombinedOutput()
	sum := sha256.Sum256(data)
	back := ""
	if err == nil {
		if f := strings.Fields(kzSudo(t, "sha256sum", path)); len(f) > 0 {
			back = f[0]
		}
	}
	if err := kzCheckWrite(out, err, hex.EncodeToString(sum[:]), back); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func kzSplit(t *testing.T, addr string) (string, string) {
	t.Helper()
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		t.Fatalf("address %q has no port", addr)
	}
	return addr[:i], addr[i+1:]
}

func kzFirstLine(s, sub string) string {
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			return strings.TrimSpace(l)
		}
	}
	return ""
}
