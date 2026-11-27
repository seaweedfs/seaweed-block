//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// HEAD size against volume size, informative: it measures,
// it does not judge. Runs only with HEADSZ_RUN=1, on a host booked for it.
//
// For each size in HEADSZ_SIZES (default 10G,100G,1T): one volume of that
// size; one 4 KiB block written in every 1 GiB segment; a snapshot (cut 1);
// then HEADSZ_CHANGE_CUTS snapshots (default 3), each after changing one
// block. Per cut it records the HEAD entry's byte size as the filer returns
// it, the number of segments HEAD lists, the cut's total and barrier time,
// and the chunks and bytes the cut uploaded. A size the durable root has no
// room for is recorded as refused by the blockvolume's headroom check, never
// shrunk or bypassed. The JSON report goes to HEADSZ_REPORT.
//
// HEADSZ_SNAPSHOT_TIMEOUT is the snapshot command's --timeout (default 60s;
// the server caps it at its own operation deadline). HEADSZ_ATTEMPTS (default
// 1) retries a failed cut with the same request id; every attempt is
// recorded with the volume's chunk-key count in the filer after it, so a
// retry shows whether an earlier attempt's uploads were kept. A cut that never
// completes is recorded as cut_not_completed, not failed.
//
// The filer runs on its default store (leveldb2) under the test's directory;
// a filer.toml anywhere weed would read one fails the test before any start,
// so the store measured is the one named in the report.

const headSegmentBytes = 1 << 30

// headSzAttempt is one snapshot command for one cut: a failed attempt is
// resent with the same request id, as the command's refusal says.
type headSzAttempt struct {
	Attempt        int     `json:"attempt"`
	ExitCode       int     `json:"exit_code"`
	Code           string  `json:"code,omitempty"`
	Message        string  `json:"message,omitempty"`
	WallMs         float64 `json:"wall_ms"`
	KeysAfter      int     `json:"chunk_keys_after"`
	LastCutTotalMs float64 `json:"last_cut_total_ms"`
	LastCutChunks  int     `json:"last_cut_uploaded_chunks"`
	// DirtyAfter is the volume's DirtyChunks (capture candidates) after the
	// attempt: on a failed attempt it shows whether the work done was kept.
	DirtyAfter int `json:"dirty_chunks_after"`
	// ChunkStatusAfter is the volume's whole chunk section of its durable
	// status after the attempt, as served (every counter and the full
	// LastCut), so a term no named field covers can still be read.
	ChunkStatusAfter json.RawMessage `json:"chunk_status_after,omitempty"`
	// the volume's last SUCCESSFUL cut's candidates, unchanged chunks and
	// payload (streaming, copy, upload) wall time, as its durable status
	// reports them (a failed attempt does not change them)
	LastCutCandidates int     `json:"last_cut_candidate_chunks"`
	LastCutUnchanged  int     `json:"last_cut_unchanged_chunks"`
	LastCutPayloadMs  float64 `json:"last_cut_payload_ms"`
}

type headSzCut struct {
	Cut            int             `json:"cut"`
	Completed      bool            `json:"completed"`
	Attempts       []headSzAttempt `json:"attempts"`
	ChangedBlocks  int             `json:"changed_blocks"`
	HeadBytes      int             `json:"head_bytes"`
	HeadSegments   int             `json:"head_segments"`
	CutTotalMs     float64         `json:"cut_total_ms"`
	BarrierPauseMs float64         `json:"cut_barrier_pause_ms"`
	UploadedChunks int             `json:"uploaded_chunks"`
	UploadedBytes  int64           `json:"uploaded_bytes"`
	SnapshotMs     float64         `json:"snapshot_command_ms"`
}

type headSzSize struct {
	Size     string  `json:"size"`
	Bytes    uint64  `json:"bytes"`
	Segments int     `json:"segments"`
	Result   string  `json:"result"` // measured | refused_headroom | cut_not_completed
	Refusal  string  `json:"refusal,omitempty"`
	WriteMs  float64 `json:"write_ms,omitempty"`
	// DirtyBeforeCut1 is the volume's DirtyChunks (its capture candidates)
	// from the durable status right before the first cut.
	DirtyBeforeCut1 int         `json:"dirty_chunks_before_cut1"`
	Cuts            []headSzCut `json:"cuts,omitempty"`
	HeadPerGiB      float64     `json:"head_bytes_per_gib,omitempty"`
}

type headSzReport struct {
	StartedAt  time.Time    `json:"started_at"`
	FilerStore string       `json:"filer_store"`
	ChunkPath  string       `json:"chunk_path"`
	Sizes      []headSzSize `json:"sizes"`
}

func headSzParse(t *testing.T, s string) uint64 {
	t.Helper()
	s = strings.ToUpper(strings.TrimSpace(s))
	mul := map[byte]uint64{'G': 1 << 30, 'T': 1 << 40}[s[len(s)-1]]
	n, err := strconv.ParseUint(s[:len(s)-1], 10, 64)
	if mul == 0 || err != nil || n == 0 {
		t.Fatalf("HEADSZ_SIZES: %q is not <n>G or <n>T", s)
	}
	return n * mul
}

// headSzAmbientFilerToml fails the test when weed could read a filer.toml
// other than none: its working directory (the cluster dir) and the standard
// configuration directories.
func headSzAmbientFilerToml(t *testing.T, clusterDir string) {
	t.Helper()
	home, _ := os.UserHomeDir()
	for _, d := range []string{clusterDir, filepath.Join(home, ".seaweedfs"), "/usr/local/etc/seaweedfs", "/etc/seaweedfs"} {
		if _, err := os.Stat(filepath.Join(d, "filer.toml")); err == nil {
			t.Fatalf("a filer.toml in %s would replace the default filer store: remove it or run elsewhere", d)
		}
	}
}

func TestHeadSizeAgainstVolumeSize(t *testing.T) {
	if os.Getenv("HEADSZ_RUN") != "1" {
		t.Skip("HEADSZ_RUN=1 not set")
	}
	sizes := strings.Split(os.Getenv("HEADSZ_SIZES"), ",")
	if os.Getenv("HEADSZ_SIZES") == "" {
		sizes = []string{"10G", "100G", "1T"}
	}
	changeCuts := 3
	if v := os.Getenv("HEADSZ_CHANGE_CUTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			t.Fatalf("HEADSZ_CHANGE_CUTS=%q", v)
		}
		changeCuts = n
	}
	cfg := headSzConfig{snapshotTimeout: "60s", attempts: 1}
	if v := os.Getenv("HEADSZ_SNAPSHOT_TIMEOUT"); v != "" {
		if _, err := time.ParseDuration(v); err != nil {
			t.Fatalf("HEADSZ_SNAPSHOT_TIMEOUT=%q: %v", v, err)
		}
		cfg.snapshotTimeout = v
	}
	if v := os.Getenv("HEADSZ_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("HEADSZ_ATTEMPTS=%q", v)
		}
		cfg.attempts = n
	}
	var vols []headSzVol
	for _, s := range sizes {
		vols = append(vols, headSzVol{id: "h" + strings.ToLower(strings.TrimSpace(s)), bytes: headSzParse(t, s)})
	}
	env, clusterDir := headSzStartCluster(t, "headsz", vols)
	rep := &headSzReport{StartedAt: time.Now().UTC(), ChunkPath: env.chunkPath,
		FilerStore: "leveldb2 (weed filer default store), -defaultStoreDir=" + filepath.Join(clusterDir, "f") + ", no filer.toml"}
	defer func() {
		if p := os.Getenv("HEADSZ_REPORT"); p != "" {
			b, _ := json.MarshalIndent(rep, "", " ")
			if err := os.WriteFile(p, append(b, '\n'), 0o644); err != nil {
				t.Errorf("HEADSZ_REPORT: %v", err)
			}
		}
	}()

	for _, s := range sizes {
		rep.Sizes = append(rep.Sizes, headSzOne(t, env, cfg, s, changeCuts))
	}
}

// headSzVol is one volume of the test cluster spec. server "" places it on
// s1 (replica rN); any other server gets its own node in the spec and the
// volume's one replica r1 on it (a restore or fork target, as
// startHandoffEnvWith places its targets).
type headSzVol struct {
	id     string
	bytes  uint64
	server string
	// host, when set, is another machine that runs this volume's blockvolume:
	// its spec data and ctrl addresses are host:<port> (ports picked here, not
	// probed there), and the test running on that host uses the same ones.
	host string
	// restoreTarget lists the volume with its placement only, no size_bytes:
	// it has no volume record until a restore creates it.
	restoreTarget bool
}

// headSzStartCluster starts one weed cluster (the filer's default store, no
// filer.toml) and a blockmaster whose spec holds vols. It returns the env,
// with one bvHost per volume, and the cluster dir. The chunk path is
// /<prefix>-<unix ns>.
func headSzStartCluster(t *testing.T, prefix string, vols []headSzVol) (*handoffEnv, string) {
	t.Helper()
	h := newPlannedHandoffHarness(t)
	clusterDir := filepath.Join(h.dir, "weed")
	if err := os.MkdirAll(clusterDir, 0o700); err != nil {
		t.Fatal(err)
	}
	headSzAmbientFilerToml(t, clusterDir)
	env := &handoffEnv{h: h, cluster: startWorkspaceWeedWith(t, clusterDir, w4WeedOpts{VolumeSizeLimitMB: 1024, VolumeMax: 64}),
		chunkPath: fmt.Sprintf("/%s-%d", prefix, time.Now().UnixNano()), hosts: map[string]bvHost{}}
	var spec strings.Builder
	nodes := fmt.Sprintf("  - server_id: s1\n    data_addr: %s\n    ctrl_addr: %s\n", h.dataAddr, h.ctrlAddr)
	for i, v := range vols {
		server, replica := "s1", fmt.Sprintf("r%d", i+1)
		if v.server != "" {
			server, replica = v.server, "r1"
		}
		b := bvHost{id: v.id, server: server, replica: replica, volume: v.id, root: filepath.Join(h.dir, "vol-"+v.id),
			data: handoffAddress(t), ctrl: handoffAddress(t), status: headSzLoopbackAddr(t), nvme: headSzLoopbackAddr(t)}
		if v.host != "" {
			b.data, b.ctrl = headSzRemoteAddr(t, v.host), headSzRemoteAddr(t, v.host)
		}
		env.hosts[v.id] = b
		if v.server != "" {
			nodes += fmt.Sprintf("  - server_id: %s\n    data_addr: %s\n    ctrl_addr: %s\n", server, b.data, b.ctrl)
		}
		size := fmt.Sprintf("    size_bytes: %d\n", v.bytes)
		if v.restoreTarget {
			size = ""
		}
		fmt.Fprintf(&spec, "  - id: %s\n%s    replication_factor: 1\n    placements:\n      - server_id: %s\n        replica_id: %s\n", v.id, size, server, replica)
	}
	specPath := filepath.Join(h.dir, "cluster-spec.yaml")
	if err := os.WriteFile(specPath, []byte("nodes:\n"+nodes+"volumes:\n"+spec.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	mstate := filepath.Join(h.dir, "master")
	margs := []string{"--authority-store", filepath.Join(mstate, "authority"), "--lifecycle-store", filepath.Join(mstate, "lifecycle"),
		"--cluster-spec", specPath, "--expected-slots-per-volume", "1", "--listen", h.masterAddr,
		"--workspace-rpc", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath}
	if testListenHost() != "127.0.0.1" {
		// a lab run spanning two hosts: blockmaster refuses a non-loopback listen without this
		margs = append(margs, "--insecure-external-bind")
	}
	master := h.start(t, "blockmaster", h.master, margs...)
	master.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	return env, clusterDir
}

// headSzLoopbackAddr picks a free 127.0.0.1 port from the harness's range:
// blockvolume's NVMe/TCP target and status endpoint refuse any non-loopback
// bind, and the test (user-space client, kernel nvme connect, sw-block ops
// --status-addr) always reaches them from the same host as that blockvolume,
// also when SWBLOCK_TEST_LISTEN_HOST names a host address.
func headSzLoopbackAddr(t *testing.T) string {
	t.Helper()
	handoffPorts.Lock()
	defer handoffPorts.Unlock()
	if handoffPorts.used == nil {
		handoffPorts.used = map[int]bool{}
	}
	for i := 0; i < 500; i++ {
		lo, hi := testPortSpan(10000, 20000)
		p := lo + rand.Intn(hi-lo)
		if handoffPorts.used[p] {
			continue
		}
		addr := fmt.Sprintf("127.0.0.1:%d", p)
		if ln, err := net.Listen("tcp", addr); err == nil {
			ln.Close()
			handoffPorts.used[p] = true
			return addr
		}
	}
	t.Fatal("no free loopback port")
	return ""
}

// headSzRemoteAddr picks host:<port> for a process on another machine from
// the same port range handoffAddress uses, never handing out a port twice.
func headSzRemoteAddr(t *testing.T, host string) string {
	t.Helper()
	handoffPorts.Lock()
	defer handoffPorts.Unlock()
	if handoffPorts.used == nil {
		handoffPorts.used = map[int]bool{}
	}
	for i := 0; i < 500; i++ {
		lo, hi := testPortSpan(10000, 20000)
		p := lo + rand.Intn(hi-lo)
		if !handoffPorts.used[p] {
			handoffPorts.used[p] = true
			return fmt.Sprintf("%s:%d", host, p)
		}
	}
	t.Fatal("no port left for " + host)
	return ""
}

// headSzConfig: HEADSZ_SNAPSHOT_TIMEOUT is the snapshot command's --timeout
// (the server caps it at its own operation deadline); HEADSZ_ATTEMPTS is how
// many times one cut is tried before it is recorded as not completed. name
// is the snapshot name prefix ("headsz" when empty): cut N is "<name>-N".
type headSzConfig struct {
	snapshotTimeout string
	attempts        int
	name            string
}

// headSzStartVolume starts b's blockvolume for a volume of bytes (no capture
// interval, no snapshot schedule: cuts happen only when the test asks), waits
// until it adopted the volume as PRIMARY and its frontend is ready, and
// returns it. When the volume refuses for headroom it returns nil and the
// refusal line; any other early exit or timeout fails the test. The caller
// stops the daemon.
func headSzStartVolume(t *testing.T, env *handoffEnv, b bvHost, bytes uint64, extra ...string) (*daemon, string) {
	t.Helper()
	env.volExtra = append([]string{"--durable-blocks", strconv.FormatUint(bytes/4096, 10), "--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}, extra...)
	d := env.h.start(t, "blockvolume-"+b.id, env.h.volume, env.volArgsFor(b)...)
	primary := fmt.Sprintf("blockvolume: volume %s adopted as PRIMARY at %s@", b.volume, b.replica)
	end := time.Now().Add(120 * time.Second)
	for !d.has(primary) {
		select {
		case <-d.done:
			if l := d.line("bytes free"); l != "" {
				return nil, strings.TrimSpace(l)
			}
			t.Fatalf("blockvolume %s exited before it adopted its volume: %v", b.id, d.err)
		case <-time.After(200 * time.Millisecond):
		}
		if time.Now().After(end) {
			t.Fatalf("blockvolume %s did not adopt its volume in 120 s", b.id)
		}
	}
	poll(t, 60*time.Second, b.id+" never reported its frontend ready", func() bool { return volumeFrontendReady(b.status, b.volume) })
	return d, ""
}

// headSzOne measures one size; its blockvolume is stopped at the end.
func headSzOne(t *testing.T, env *handoffEnv, cfg headSzConfig, size string, changeCuts int) headSzSize {
	t.Helper()
	bytes := headSzParse(t, size)
	id := "h" + strings.ToLower(strings.TrimSpace(size))
	b := env.hosts[id]
	row := headSzSize{Size: strings.TrimSpace(size), Bytes: bytes, Segments: int((bytes + headSegmentBytes - 1) / headSegmentBytes)}
	d, refusal := headSzStartVolume(t, env, b, bytes)
	if d == nil {
		row.Result, row.Refusal = "refused_headroom", refusal
		return row
	}
	defer d.term(t, 60*time.Second)

	c := dialNVMe(t, b.nvme, plannedHandoffNQN)
	write := func(lba uint64, tag string) {
		_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
		c.write(t, lba, f1Block(id, 0, tag, lba))
	}
	start := time.Now()
	for seg := 0; seg < row.Segments; seg++ {
		write(uint64(seg)*headSegmentBytes/4096, "seg")
	}
	_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
	c.flush(t)
	row.WriteMs = ms(time.Since(start))
	row.DirtyBeforeCut1 = headSzDirty(t, b)
	for cut := 1; cut <= 1+changeCuts; cut++ {
		changed := 0
		if cut > 1 {
			write(uint64(cut), fmt.Sprintf("change%d", cut))
			_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
			c.flush(t)
			changed = 1
		}
		r := headSzCutOnce(t, env, cfg, b, cut, changed)
		row.Cuts = append(row.Cuts, r)
		if !r.Completed {
			row.Result = "cut_not_completed"
			return row
		}
	}
	row.Result = "measured"
	if row.Segments > 0 && len(row.Cuts) > 0 {
		row.HeadPerGiB = float64(row.Cuts[0].HeadBytes) / float64(row.Segments)
	}
	return row
}

// headSzCutOnce takes one snapshot, retried with the same request id up to
// cfg.attempts times, and reads what it wrote: HEAD from the filer, the cut
// from the volume's durable status. After every attempt it counts the
// volume's chunk keys in the filer, so a retry shows whether the chunks an
// earlier attempt uploaded are still there.
func headSzCutOnce(t *testing.T, env *handoffEnv, cfg headSzConfig, b bvHost, cut, changed int) headSzCut {
	t.Helper()
	r := headSzCut{Cut: cut, ChangedBlocks: changed}
	name := cfg.name
	if name == "" {
		name = "headsz"
	}
	start := time.Now()
	for a := 1; a <= cfg.attempts; a++ {
		as := time.Now()
		out, code := headSzRun(t, env, cfg.snapshotTimeout, "ops", "chunk", "snapshot", "--volume", b.volume, "--status-addr", b.status, "--timeout", cfg.snapshotTimeout,
			"--name", fmt.Sprintf("%s-%d", name, cut), "--request-id", fmt.Sprintf("%s-%s-%d", name, b.volume, cut))
		at := headSzAttempt{Attempt: a, ExitCode: code, WallMs: ms(time.Since(as)), KeysAfter: env.poolObjects(t, b.volume)}
		if code != 0 {
			var e struct{ Code, Message string }
			if json.Unmarshal([]byte(strings.TrimSpace(out)), &e) == nil {
				at.Code, at.Message = e.Code, e.Message
			} else {
				at.Message = strings.TrimSpace(out)
			}
		}
		at.DirtyAfter = headSzDirty(t, b)
		at.ChunkStatusAfter = headSzChunkStatus(t, b)
		lc := headSzLastCut(t, b)
		at.LastCutTotalMs, at.LastCutChunks = float64(lc.TotalNs)/1e6, lc.UploadedChunks
		at.LastCutCandidates, at.LastCutUnchanged, at.LastCutPayloadMs = lc.CandidateChunks, lc.UnchangedChunks, float64(lc.PayloadNs)/1e6
		r.Attempts = append(r.Attempts, at)
		if code == 0 {
			r.Completed = true
			break
		}
	}
	r.SnapshotMs = ms(time.Since(start))
	if !r.Completed {
		return r
	}
	var st struct {
		Volumes []struct {
			VolumeID string
			Chunk    *struct {
				Stats struct {
					LastCut *struct {
						TotalNs, BarrierPauseNs int64
						UploadedChunks          int
						UploadedBytes           int64
					}
				}
			}
		}
	}
	if err := json.Unmarshal(headSzGet(t, "http://"+b.status+"/status/durable?volume="+b.volume, 4<<20), &st); err != nil {
		t.Fatalf("durable status of %s: %v", b.volume, err)
	}
	for _, s := range st.Volumes {
		if s.VolumeID == b.volume && s.Chunk != nil && s.Chunk.Stats.LastCut != nil {
			l := s.Chunk.Stats.LastCut
			r.CutTotalMs, r.BarrierPauseMs = float64(l.TotalNs)/1e6, float64(l.BarrierPauseNs)/1e6
			r.UploadedChunks, r.UploadedBytes = l.UploadedChunks, l.UploadedBytes
		}
	}
	// The volume's store directory is <name>-<id8>, its id from blockmaster's
	// record. HEAD names the current root; the root names the segments.
	id, err := layout.ParseVolumeID(env.layoutID(b.volume))
	if err != nil {
		t.Fatal(err)
	}
	dir := "http://" + env.cluster.filerHTTP + path.Join(env.chunkPath, layout.VolumeDir(b.volume, id))
	raw := headSzGet(t, dir+"/"+layout.HeadName, 256<<20)
	r.HeadBytes = len(raw)
	head, err := layout.DecodeHead(raw, 0, id)
	if err != nil {
		t.Fatalf("HEAD of %s (%d bytes): %v", b.volume, len(raw), err)
	}
	if head.Current != nil {
		root, err := layout.DecodeRoot(headSzGet(t, dir+"/"+head.Current.Path(), 256<<20))
		if err != nil {
			t.Fatalf("current root of %s: %v", b.volume, err)
		}
		r.HeadSegments = len(root.Segments)
	}
	return r
}

// headSzLastCutStats is the part of the volume's last cut an attempt records.
type headSzLastCutStats struct {
	TotalNs, PayloadNs                               int64
	UploadedChunks, CandidateChunks, UnchangedChunks int
}

// headSzLastCut reads the volume's last cut from its durable status; zeros
// when there is no last cut yet.
func headSzLastCut(t *testing.T, b bvHost) headSzLastCutStats {
	t.Helper()
	var st struct {
		Volumes []struct {
			VolumeID string
			Chunk    *struct {
				Stats struct {
					LastCut *headSzLastCutStats
				}
			}
		}
	}
	if err := json.Unmarshal(headSzGet(t, "http://"+b.status+"/status/durable?volume="+b.volume, 4<<20), &st); err != nil {
		t.Fatalf("durable status of %s: %v", b.volume, err)
	}
	for _, s := range st.Volumes {
		if s.VolumeID == b.volume && s.Chunk != nil && s.Chunk.Stats.LastCut != nil {
			return *s.Chunk.Stats.LastCut
		}
	}
	return headSzLastCutStats{}
}

// headSzCountDir counts the entries of one filer directory by its JSON
// listing; a directory that does not exist yet counts 0.
func headSzCountDir(t *testing.T, env *handoffEnv, dir string) int {
	t.Helper()
	c := &http.Client{Timeout: 30 * time.Second}
	n, last := 0, ""
	for {
		req, _ := http.NewRequest(http.MethodGet, "http://"+env.cluster.filerHTTP+dir+"/?limit=1000&lastFileName="+url.QueryEscape(last), nil)
		req.Header.Set("Accept", "application/json")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("filer list %s: %v", dir, err)
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return n
		}
		var page struct {
			Entries               []struct{ FullPath string }
			ShouldDisplayLoadMore bool
			LastFileName          string
		}
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("filer list %s: %v", dir, err)
		}
		n += len(page.Entries)
		if !page.ShouldDisplayLoadMore || page.LastFileName == "" {
			return n
		}
		last = page.LastFileName
	}
}

// headSzRun runs sw-block with its own bound, the snapshot command's
// --timeout plus a minute, so the server's operation deadline answers first
// (the shared runner's fixed 150 s would kill a longer cut). A command that
// still has to be killed returns exit code -1 and its output, a recorded
// attempt, never a test failure.
func headSzRun(t *testing.T, env *handoffEnv, timeout string, args ...string) (string, int) {
	t.Helper()
	d, err := time.ParseDuration(timeout)
	if err != nil {
		t.Fatalf("timeout %q: %v", timeout, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), d+time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, env.h.cli, args...)
	cmd.Dir = env.h.dir
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	env.h.logCommand(args, stdout.String(), stderr.String(), err)
	var exit *exec.ExitError
	switch {
	case err == nil:
		return stdout.String(), 0
	case errors.As(err, &exit) && ctx.Err() == nil:
		return stdout.String(), exit.ExitCode()
	case ctx.Err() != nil:
		return "killed after " + (d + time.Minute).String() + ": " + strings.TrimSpace(stdout.String()+" "+stderr.String()), -1
	}
	t.Fatalf("sw-block %q: %v\nstdout: %s\nstderr: %s", args, err, stdout.String(), stderr.String())
	return "", -1
}

// headSzChunkStatus returns the volume's chunk section of its durable status
// as served, unparsed; nil when there is none.
func headSzChunkStatus(t *testing.T, b bvHost) json.RawMessage {
	t.Helper()
	var st struct {
		Volumes []struct {
			VolumeID string
			Chunk    json.RawMessage
		}
	}
	if err := json.Unmarshal(headSzGet(t, "http://"+b.status+"/status/durable?volume="+b.volume, 4<<20), &st); err != nil {
		t.Fatalf("durable status of %s: %v", b.volume, err)
	}
	for _, s := range st.Volumes {
		if s.VolumeID == b.volume && len(s.Chunk) > 0 && string(s.Chunk) != "null" {
			return s.Chunk
		}
	}
	return nil
}

// headSzDirty reads the volume's DirtyChunks (its capture candidates) from
// its durable status; -1 when the status has no chunk section.
func headSzDirty(t *testing.T, b bvHost) int {
	t.Helper()
	var st struct {
		Volumes []struct {
			VolumeID string
			Chunk    *struct {
				Stats struct{ DirtyChunks int }
			}
		}
	}
	if err := json.Unmarshal(headSzGet(t, "http://"+b.status+"/status/durable?volume="+b.volume, 4<<20), &st); err != nil {
		t.Fatalf("durable status of %s: %v", b.volume, err)
	}
	for _, s := range st.Volumes {
		if s.VolumeID == b.volume && s.Chunk != nil {
			return s.Chunk.Stats.DirtyChunks
		}
	}
	return -1
}

func headSzGet(t *testing.T, u string, limit int64) []byte {
	t.Helper()
	c := &http.Client{Timeout: 30 * time.Second}
	resp, err := c.Get(u)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d err %v", u, resp.StatusCode, err)
	}
	return body
}
