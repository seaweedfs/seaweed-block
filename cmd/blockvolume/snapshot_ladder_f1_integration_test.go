//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
)

// Snapshot ladder rung F1: N volumes on one host,
// every volume snapshotted at the same instant, round after round, under
// write load. It must hold: every snapshot restores exactly; no write fails;
// the host limiter paces the cuts and none starves (every snapshot commits
// within ceil(N/slots)+1 times the slowest single cut). It measures, per
// volume and round: the snapshot's cut (total and guest barrier pause), the
// client write latency inside and outside snapshot windows, request to
// commit time, host-slot waits, restore time, chunks and bytes uploaded,
// filer entries per snapshot, and blockvolume RSS. The JSON report goes to
// F1_REPORT.
//
// A large test: it runs only with F1_RUN=1, on a host booked for it, never in
// CI. Knobs: F1_VOLUMES (10), F1_ROUNDS (3), F1_WRITE_SECONDS (120),
// F1_CONTROL_PAUSE_SECONDS (a negative control: stop the last volume's
// process that long after the rounds; the rung must then fail),
// F1_LOAD_SPAN (8192 blocks per volume per round, written evenly across the
// write period, so writes run before, during and after the snapshots),
// F1_HOST_LIMIT_DIR (a local, non-temporary directory; required for the
// limiter checks, a smoke may leave it unset), F1_HOST_LIMIT_SLOTS
// (the limiter's normal slots; default hostlimit's), F1_REPORT.
//
// Each volume has two regions. The ledger region is written and flushed
// before a round's snapshot, so the snapshot must hold it exactly. The load
// region is written in LBA order during the snapshot, so the snapshot must
// hold a prefix of it (A3's one-point-in-time check).

type f1Config struct {
	Volumes, Rounds    int
	WriteSeconds       int
	HostLimitDir       string
	SlotRetryBound     time.Duration
	Slots              int
	Report             string
	LedgerBlocksPerRnd int
	LoadSpan           int
	ControlPauseSecs   int
}

func f1ConfigFromEnv(t *testing.T) f1Config {
	t.Helper()
	if os.Getenv("F1_RUN") != "1" {
		t.Skip("rung F1 is a large test: set F1_RUN=1 on a booked host")
	}
	num := func(k string, def int) int {
		if v := os.Getenv(k); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				t.Fatalf("%s=%q: want a positive integer", k, v)
			}
			return n
		}
		return def
	}
	retry := 60 // the snapshot command's default --timeout; 0 = no retry (the negative control)
	if v := os.Getenv("F1_SLOT_RETRY_SECONDS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			t.Fatalf("F1_SLOT_RETRY_SECONDS=%q: want a whole number of seconds, 0 or more", v)
		}
		retry = n
	}
	c := f1Config{Volumes: num("F1_VOLUMES", 10), Rounds: num("F1_ROUNDS", 3), WriteSeconds: num("F1_WRITE_SECONDS", 120),
		HostLimitDir: os.Getenv("F1_HOST_LIMIT_DIR"), Slots: num("F1_HOST_LIMIT_SLOTS", hostlimit.DefaultConfig("").NormalSlots),
		Report: os.Getenv("F1_REPORT"), LedgerBlocksPerRnd: 64, LoadSpan: num("F1_LOAD_SPAN", 8192), SlotRetryBound: time.Duration(retry) * time.Second}
	if v := os.Getenv("F1_CONTROL_PAUSE_SECONDS"); v != "" {
		c.ControlPauseSecs = num("F1_CONTROL_PAUSE_SECONDS", 0)
	}
	if c.Rounds*(c.LedgerBlocksPerRnd+c.LoadSpan) > ladderUntouched {
		t.Fatalf("F1_ROUNDS=%d does not fit below the untouched chunk", c.Rounds)
	}
	return c
}

// f1Volume is one source volume: its id, host record and daemon.
type f1Volume struct {
	id, node string
	d        *daemon
}

// f1Sample is one client write.
type f1Sample struct {
	start time.Time
	d     time.Duration
}

// f1Round is one volume's measurements in one round.
type f1Round struct {
	Volume         string  `json:"volume"`
	Round          int     `json:"round"`
	SnapshotMs     float64 `json:"snapshot_request_to_commit_ms"`
	CutTotalMs     float64 `json:"cut_total_ms"`
	BarrierPauseMs float64 `json:"cut_barrier_pause_ms"`
	UploadedChunks int     `json:"uploaded_chunks"`
	UploadedBytes  int64   `json:"uploaded_bytes"`
	HostSlotWaits  uint64  `json:"host_slot_waits"`
	SlotRetries    int     `json:"snapshot_slot_refusals_retried"`
	SlotWaitMs     float64 `json:"snapshot_slot_wait_ms"`
	RestoreMs      float64 `json:"restore_ms"`
	// RestoreExact is true or false for a snapshot that ran, "not_run" for
	// one that was never admitted.
	RestoreExact    any     `json:"restore_exact"`
	LoadPrefix      int     `json:"load_prefix_blocks"`
	WritesIn        int     `json:"writes_in_window"`
	WritesOut       int     `json:"writes_outside_window"`
	WriteP50InMs    float64 `json:"write_p50_in_ms"`
	WriteP99InMs    float64 `json:"write_p99_in_ms"`
	WriteMaxInMs    float64 `json:"write_max_in_ms"`
	WriteP50OutMs   float64 `json:"write_p50_out_ms"`
	WriteP99OutMs   float64 `json:"write_p99_out_ms"`
	WriteMaxOutMs   float64 `json:"write_max_out_ms"`
	RSSKiB          int64   `json:"blockvolume_rss_kib"`
	FilerEntriesNew int     `json:"filer_entries_added_by_round"`
	FilerPerSnap    float64 `json:"filer_entries_per_snapshot"`
}

type f1Report struct {
	Config      f1Config   `json:"config"`
	Limiter     string     `json:"limiter"`
	StartedAt   time.Time  `json:"started_at"`
	Rounds      []f1Round  `json:"rounds"`
	Health      []f1Health `json:"heartbeat_health"`
	CommitBound float64    `json:"commit_bound_ms"`
	WriteErrors int        `json:"write_errors"`
	Verdict     string     `json:"verdict"`
	Failures    []string   `json:"failures,omitempty"`
}

func TestSnapshotLadderF1VolumesOneHost(t *testing.T) {
	cfg := f1ConfigFromEnv(t)
	rep := &f1Report{Config: cfg, StartedAt: time.Now().UTC(), Limiter: "off"}
	env, master := f1StartEnv(t, cfg)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	if cfg.HostLimitDir != "" {
		env.volExtra = append(env.volExtra, "--host-limit-dir", cfg.HostLimitDir, "--host-limit-normal-slots", strconv.Itoa(cfg.Slots))
		rep.Limiter = fmt.Sprintf("%s, %d normal slots", cfg.HostLimitDir, cfg.Slots)
	}
	vols := f1StartSources(t, env, cfg.Volumes)
	defer f1WriteReport(t, cfg, rep)
	watch := f1StartWatch(env, vols)

	for round := 1; round <= cfg.Rounds; round++ {
		before := f1FilerEntries(t, env)
		rounds := f1RunRound(t, env, cfg, vols, round, rep)
		added := f1FilerEntries(t, env) - before
		for i := range rounds {
			rounds[i].FilerEntriesNew = added
			rounds[i].FilerPerSnap = float64(added) / float64(len(rounds))
		}
		rep.Rounds = append(rep.Rounds, rounds...)
	}
	if cfg.ControlPauseSecs > 0 {
		// Negative control: the last volume's blockvolume is stopped
		// (SIGSTOP) past the master's freshness window, so its heartbeats
		// stop; the watch must record a slot expiry and the rung must fail.
		last := vols[len(vols)-1]
		if err := last.d.proc.Signal(syscall.SIGSTOP); err != nil {
			t.Fatalf("control: SIGSTOP %s: %v", last.id, err)
		}
		time.Sleep(time.Duration(cfg.ControlPauseSecs) * time.Second)
		if err := last.d.proc.Signal(syscall.SIGCONT); err != nil {
			t.Fatalf("control: SIGCONT %s: %v", last.id, err)
		}
		time.Sleep(3 * time.Second)
	}
	rep.Health = watch.finish(master.log)
	f1Judge(t, cfg, rep)
}

func f1RestoreTarget(vol, round int) string { return fmt.Sprintf("v%dr%d", vol, round) }

// f1StartEnv is the procedure harness laid out as one host: ONE server id, s1, and one blockvolume process per
// volume, each with its own replica id and its own data/ctrl/status/NVMe
// ports. Source v<i> is replica r<i>; the restore target of v<i> in round k
// is replica r<i+N*k> on the same server. The node record carries the first
// source's pair, as the deployment page writes one node.
func f1StartEnv(t *testing.T, cfg f1Config) (*handoffEnv, *daemon) {
	t.Helper()
	h := newPlannedHandoffHarness(t)
	env := &handoffEnv{h: h, cluster: startWorkspaceWeed(t), chunkPath: fmt.Sprintf("/f1-%d", time.Now().UnixNano()), hosts: map[string]bvHost{}}
	must, _ := fatalOn(t)
	add := func(key, vol string, replica int) {
		b := bvHost{id: key, server: "s1", replica: fmt.Sprintf("r%d", replica), volume: vol, root: filepath.Join(h.dir, "vol-"+key),
			data: handoffAddress(t), ctrl: handoffAddress(t), status: handoffAddress(t), nvme: handoffAddress(t)}
		if key == "v1" {
			b.data, b.ctrl, b.status, b.nvme = h.dataAddr, h.ctrlAddr, h.statusAddr, h.nvmeAddr
		}
		env.hosts[key] = b
	}
	for i := 1; i <= cfg.Volumes; i++ {
		add(fmt.Sprintf("v%d", i), fmt.Sprintf("v%d", i), i)
	}
	targets := map[string]bool{}
	for r := 1; r <= cfg.Rounds; r++ {
		for i := 1; i <= cfg.Volumes; i++ {
			v := f1RestoreTarget(i, r)
			add(v, v, i+cfg.Volumes*r)
			targets[v] = true
		}
	}
	var vols strings.Builder
	keys := make([]string, 0, len(env.hosts))
	for k := range env.hosts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b := env.hosts[k]
		size := "    size_bytes: 268435456\n"
		if targets[k] {
			size = "" // a restore target: placement only, its restore creates it
		}
		fmt.Fprintf(&vols, "  - id: %s\n%s    replication_factor: 1\n    placements:\n      - server_id: s1\n        replica_id: %s\n", b.volume, size, b.replica)
	}
	spec := filepath.Join(h.dir, "cluster-spec.yaml")
	must(os.WriteFile(spec, []byte(fmt.Sprintf("nodes:\n  - server_id: s1\n    data_addr: %s\n    ctrl_addr: %s\nvolumes:\n%s", h.dataAddr, h.ctrlAddr, vols.String())), 0o644))
	mstate := filepath.Join(h.dir, "master")
	env.masterArgs = []string{"--authority-store", filepath.Join(mstate, "authority"), "--lifecycle-store", filepath.Join(mstate, "lifecycle"),
		"--cluster-spec", spec, "--expected-slots-per-volume", "1", "--listen", h.masterAddr,
		"--workspace-rpc", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath}
	master := h.start(t, "blockmaster", h.master, env.masterArgs...)
	master.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	env.masterProcess = master
	return env, master
}

// f1Up starts the blockvolume of key and waits for its volume to be primary
// and its frontend ready.
func f1Up(t *testing.T, env *handoffEnv, key string) *daemon {
	t.Helper()
	b := env.hosts[key]
	d := env.h.start(t, "blockvolume-"+key, env.h.volume, env.volArgsFor(b)...)
	d.waitLog(t, 60*time.Second, fmt.Sprintf("blockvolume: volume %s adopted as PRIMARY at %s@", b.volume, b.replica))
	poll(t, 60*time.Second, key+" never reported its frontend ready", func() bool { return volumeFrontendReady(b.status, b.volume) })
	return d
}

// f1StartSources starts the N source volumes.
func f1StartSources(t *testing.T, env *handoffEnv, n int) []f1Volume {
	t.Helper()
	var vols []f1Volume
	for i := 1; i <= n; i++ {
		v := fmt.Sprintf("v%d", i)
		vols = append(vols, f1Volume{id: v, node: v, d: f1Up(t, env, v)})
	}
	return vols
}

// f1Restore restores snapshot snap of src into target (a new volume on the
// same server), which fetches its chunks on access; the target's blockvolume
// is stopped once verified (f1RunRound), so restores do not pile up processes.
func f1Restore(t *testing.T, env *handoffEnv, src, snap, target string) *daemon {
	t.Helper()
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", src, "--snapshot", snap, "--target", target,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	d := f1Up(t, env, target)
	return d
}

// f1CommandDeadline bounds one load write (and its flush): a write held
// longer than this by a cut fails the rung.
const f1CommandDeadline = 30 * time.Second

// A snapshot refused because every host slot is held is resent with its
// request id every f1SlotRetryPause, for at most cfg.SlotRetryBound from its
// first request (the snapshot command's operation bound, its default
// --timeout of 60 s; F1_SLOT_RETRY_SECONDS=0 turns the retry off, the
// negative control). Its request-to-commit time includes the waits; the
// write window starts at the admitted attempt.
const f1SlotRetryPause = 250 * time.Millisecond

// f1SlotRefusal reports whether a failed snapshot's error body is a
// retryable refusal for a busy host slot.
func f1SlotRefusal(stdout string) bool {
	var body struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(stdout)), &body) != nil || !body.Retryable {
		return false
	}
	return body.Code == "host_slot_busy" || body.Code == "capture_refused" && strings.Contains(body.Message, "hostlimit: no background slot")
}

// f1Block is one deterministic 4096-byte block (pattern needs a length that
// divides 4096; these labels do not, so the block is filled explicitly).
func f1Block(vol string, round int, kind string, lba uint64) []byte {
	s := fmt.Sprintf("%s r%d %s %08d|", vol, round, kind, lba)
	b := make([]byte, 4096)
	for i := range b {
		b[i] = s[i%len(s)]
	}
	return b
}

func f1LedgerBase(cfg f1Config, round int) uint64 {
	return uint64((round - 1) * (cfg.LedgerBlocksPerRnd + cfg.LoadSpan))
}

// f1RunRound: write and flush each volume's ledger region, start the load
// writers, snapshot every volume at the same instant, stop the load, then
// restore and verify every snapshot.
func f1RunRound(t *testing.T, env *handoffEnv, cfg f1Config, vols []f1Volume, round int, rep *f1Report) []f1Round {
	t.Helper()
	for _, v := range vols {
		c := dialNVMe(t, env.hosts[v.node].nvme, plannedHandoffNQN)
		base := f1LedgerBase(cfg, round)
		for i := 0; i < cfg.LedgerBlocksPerRnd; i++ {
			c.write(t, base+uint64(i), f1Block(v.id, round, "ledger", base+uint64(i)))
		}
		c.flush(t)
		c.close()
	}
	type writer struct {
		mu      sync.Mutex
		samples []f1Sample
		written int
		err     error
	}
	stop := make(chan struct{})
	ws := make([]*writer, len(vols))
	var wg sync.WaitGroup
	for i, v := range vols {
		ws[i] = &writer{}
		c := dialNVMe(t, env.hosts[v.node].nvme, plannedHandoffNQN)
		wg.Add(1)
		go func(w *writer, c *nvmeInitiator, vol string) {
			defer wg.Done()
			defer c.close()
			first := f1LedgerBase(cfg, round) + uint64(cfg.LedgerBlocksPerRnd)
			// Paced so the span covers the whole write period.
			every := time.Duration(cfg.WriteSeconds) * time.Second / time.Duration(cfg.LoadSpan)
			begin := time.Now()
			for n := 0; n < cfg.LoadSpan; n++ {
				if wait := time.Until(begin.Add(time.Duration(n) * every)); wait > 0 {
					select {
					case <-stop:
						return
					case <-time.After(wait):
					}
				}
				select {
				case <-stop:
					return
				default:
				}
				lba := first + uint64(n)
				s := time.Now()
				// Per command: dialNVMe's deadline is 60 s from the dial,
				// shorter than the write period.
				_ = c.io.SetDeadline(s.Add(f1CommandDeadline))
				err := c.tryWrite(lba, f1Block(vol, round, "load", lba))
				if err == nil && n%16 == 15 {
					err = c.tryFlush()
				}
				w.mu.Lock()
				w.samples = append(w.samples, f1Sample{start: s, d: time.Since(s)})
				if err != nil {
					w.err = err
				} else {
					w.written = n + 1
				}
				w.mu.Unlock()
				if err != nil {
					return
				}
			}
		}(ws[i], c, v.id)
	}
	time.Sleep(time.Duration(cfg.WriteSeconds) * time.Second / 2) // load before the snapshots

	type snapRes struct {
		start, end time.Time
		admitted   time.Time // the start of the attempt that was not refused
		out, err   string
		code       int
		retries    int
	}
	res := make([]snapRes, len(vols))
	var sg sync.WaitGroup
	gate := make(chan struct{})
	for i, v := range vols {
		sg.Add(1)
		go func(i int, v f1Volume) {
			defer sg.Done()
			<-gate
			res[i].start = time.Now()
			// A refusal for a busy host slot is retryable by contract: resend
			// it with the same request id, as an operator would, within a bound.
			request := fmt.Sprintf("f1-r%d-%s", round, v.id)
			for {
				res[i].admitted = time.Now()
				res[i].out, res[i].err, res[i].code = env.h.run(t, "ops", "chunk", "snapshot", "--volume", v.id, "--status-addr", env.hosts[v.node].status,
					"--name", fmt.Sprintf("f1-r%d", round), "--request-id", request)
				if res[i].code == 0 || !f1SlotRefusal(res[i].out) || time.Since(res[i].start) > cfg.SlotRetryBound {
					break
				}
				res[i].retries++
				time.Sleep(f1SlotRetryPause)
			}
			res[i].end = time.Now()
		}(i, v)
	}
	close(gate)
	sg.Wait()
	time.Sleep(time.Duration(cfg.WriteSeconds) * time.Second / 2) // load after the snapshots
	close(stop)
	wg.Wait()

	out := make([]f1Round, len(vols))
	for i, v := range vols {
		r := f1Round{Volume: v.id, Round: round, RestoreExact: "not_run", SlotRetries: res[i].retries}
		w := ws[i]
		if w.err != nil {
			rep.WriteErrors++
			rep.Failures = append(rep.Failures, fmt.Sprintf("%s round %d: write error: %v", v.id, round, w.err))
		}
		if res[i].code != 0 && f1SlotRefusal(res[i].out) {
			rep.Failures = append(rep.Failures, fmt.Sprintf("%s round %d: snapshot not admitted within %v (%d slot refusals retried): %s", v.id, round, cfg.SlotRetryBound, res[i].retries, strings.Join(strings.Fields(res[i].out), " ")))
			out[i] = r
			continue
		}
		if res[i].code != 0 {
			rep.Failures = append(rep.Failures, fmt.Sprintf("%s round %d: snapshot exit %d: %s %s", v.id, round, res[i].code, res[i].out, res[i].err))
			out[i] = r
			continue
		}
		r.SnapshotMs = ms(res[i].end.Sub(res[i].start))
		r.SlotWaitMs = ms(res[i].admitted.Sub(res[i].start))
		f1Latencies(&r, w.samples, res[i].admitted, res[i].end)
		if r.WritesIn == 0 {
			rep.Failures = append(rep.Failures, fmt.Sprintf("%s round %d: no load write was issued inside the snapshot window", v.id, round))
		}
		f1CutStats(t, env, v, &r)
		r.RSSKiB = f1RSS(v.d)
		target := f1RestoreTarget(i+1, round)
		rs := time.Now()
		rd := f1Restore(t, env, v.id, fmt.Sprintf("f1-r%d", round), target)
		r.RestoreMs = ms(time.Since(rs))
		exact, prefix := f1Verify(t, env, cfg, v.id, target, round, w.written)
		r.RestoreExact, r.LoadPrefix = exact, prefix
		rd.term(t, 60*time.Second)
		if !exact {
			rep.Failures = append(rep.Failures, fmt.Sprintf("%s round %d: the restored snapshot is not the volume at its point", v.id, round))
		}
		out[i] = r
	}
	return out
}

// f1Verify reads the restored snapshot: every ledger block of rounds 1..round
// exact; each round's load region a prefix, at most what was written.
func f1Verify(t *testing.T, env *handoffEnv, cfg f1Config, vol, target string, round, written int) (bool, int) {
	t.Helper()
	c := dialNVMe(t, env.hosts[target].nvme, plannedHandoffNQN)
	defer c.close()
	zero := make([]byte, 4096)
	prefix := 0
	for r := 1; r <= round; r++ {
		base := f1LedgerBase(cfg, r)
		for i := 0; i < cfg.LedgerBlocksPerRnd; i++ {
			if !bytes.Equal(c.read(t, base+uint64(i), 4096), f1Block(vol, r, "ledger", base+uint64(i))) {
				return false, 0
			}
		}
		first := base + uint64(cfg.LedgerBlocksPerRnd)
		ended, k := false, 0
		for n := 0; n < cfg.LoadSpan; n++ {
			got := c.read(t, first+uint64(n), 4096)
			switch {
			case !ended && bytes.Equal(got, f1Block(vol, r, "load", first+uint64(n))):
				k++
			case bytes.Equal(got, zero) && r == round:
				ended = true
			case r < round && n >= k && bytes.Equal(got, zero):
				ended = true
			default:
				return false, 0
			}
		}
		if r == round {
			prefix = k
			if k > written {
				return false, k
			}
		}
	}
	return true, prefix
}

// f1Latencies splits the client write latencies at the snapshot window.
func f1Latencies(r *f1Round, samples []f1Sample, from, to time.Time) {
	var in, out []time.Duration
	for _, s := range samples {
		if !s.start.Before(from) && !s.start.After(to) {
			in = append(in, s.d)
		} else {
			out = append(out, s.d)
		}
	}
	r.WritesIn, r.WritesOut = len(in), len(out)
	r.WriteP50InMs, r.WriteP99InMs, r.WriteMaxInMs = pct(in, 50), pct(in, 99), pct(in, 100)
	r.WriteP50OutMs, r.WriteP99OutMs, r.WriteMaxOutMs = pct(out, 50), pct(out, 99), pct(out, 100)
}

func pct(d []time.Duration, p float64) float64 {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(math.Ceil(p/100*float64(len(s)))) - 1
	if i < 0 {
		i = 0
	}
	return ms(s[i])
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// f1CutStats reads the volume's last cut (the snapshot's) from its durable
// status.
func f1CutStats(t *testing.T, env *handoffEnv, v f1Volume, r *f1Round) {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get("http://" + env.hosts[v.node].status + "/status/durable?volume=" + v.id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st struct {
		Volumes []struct {
			VolumeID string
			Chunk    *struct {
				HostSlotWaits uint64
				Stats         struct {
					LastCut *struct {
						TotalNs, BarrierPauseNs int64
						UploadedChunks          int
						UploadedBytes           int64
					}
				}
			}
		}
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatalf("durable status of %s: %v", v.id, err)
	}
	for _, s := range st.Volumes {
		if s.VolumeID != v.id || s.Chunk == nil || s.Chunk.Stats.LastCut == nil {
			continue
		}
		l := s.Chunk.Stats.LastCut
		r.CutTotalMs, r.BarrierPauseMs = float64(l.TotalNs)/1e6, float64(l.BarrierPauseNs)/1e6
		r.UploadedChunks, r.UploadedBytes, r.HostSlotWaits = l.UploadedChunks, l.UploadedBytes, s.Chunk.HostSlotWaits
		return
	}
	t.Fatalf("durable status of %s has no last cut: %s", v.id, body)
}

// f1RSS is the blockvolume's resident set (KiB), 0 where /proc is absent.
func f1RSS(d *daemon) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", d.proc.Pid))
	if err != nil {
		return 0
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			f := strings.Fields(l)
			if len(f) >= 2 {
				n, _ := strconv.ParseInt(f[1], 10, 64)
				return n
			}
		}
	}
	return 0
}

// f1FilerEntries counts the filer entries (files and directories) under the
// chunk path, by the filer's JSON listing.
func f1FilerEntries(t *testing.T, env *handoffEnv) int {
	t.Helper()
	c := &http.Client{Timeout: 30 * time.Second}
	var walk func(dir string) int
	walk = func(dir string) int {
		n, last := 0, ""
		for {
			u := "http://" + env.cluster.filerHTTP + dir + "/?limit=1000&lastFileName=" + url.QueryEscape(last)
			req, _ := http.NewRequest(http.MethodGet, u, nil)
			req.Header.Set("Accept", "application/json")
			resp, err := c.Do(req)
			if err != nil {
				t.Fatalf("filer list %s: %v", dir, err)
			}
			var page struct {
				Entries []struct {
					FullPath string
					Mode     uint32
				}
				ShouldDisplayLoadMore bool
				LastFileName          string
			}
			err = json.NewDecoder(resp.Body).Decode(&page)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("filer list %s: %v", dir, err)
			}
			for _, e := range page.Entries {
				n++
				if e.Mode&uint32(os.ModeDir) != 0 {
					n += walk(e.FullPath)
				}
			}
			if !page.ShouldDisplayLoadMore || page.LastFileName == "" {
				return n
			}
			last = page.LastFileName
		}
	}
	return walk(env.chunkPath)
}

// f1Judge applies the pass rule: no failure recorded, and every snapshot
// committed within ceil(N/slots)+1 times the slowest single cut (N+1 when
// the limiter is off).
func f1Judge(t *testing.T, cfg f1Config, rep *f1Report) {
	t.Helper()
	worst, longest := 0.0, 0.0
	for _, r := range rep.Rounds {
		worst = math.Max(worst, r.CutTotalMs)
		longest = math.Max(longest, r.SnapshotMs)
	}
	waves := float64(cfg.Volumes + 1)
	if cfg.HostLimitDir != "" {
		waves = math.Ceil(float64(cfg.Volumes)/float64(cfg.Slots)) + 1
	}
	rep.CommitBound = waves * worst
	for _, h := range rep.Health {
		if h.SlotExpiries > 0 {
			rep.Failures = append(rep.Failures, fmt.Sprintf("%s: the master lost its slot %d time(s) for %.0f ms in total (during: %v)", h.Volume, h.SlotExpiries, h.SlotUnobservedMs, h.DuringExpiry))
		}
	}
	if longest > rep.CommitBound {
		rep.Failures = append(rep.Failures, fmt.Sprintf("a snapshot took %.0f ms, above the bound %.0f ms (%.0f waves x the slowest cut %.0f ms): a volume starved", longest, rep.CommitBound, waves, worst))
	}
	rep.Verdict = "PASS"
	if len(rep.Failures) > 0 {
		rep.Verdict = "FAIL"
		t.Errorf("rung F1: %s", strings.Join(rep.Failures, "; "))
	}
}

func f1WriteReport(t *testing.T, cfg f1Config, rep *f1Report) {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		t.Errorf("report: %v", err)
		return
	}
	t.Logf("rung F1 report: %s", b)
	if cfg.Report != "" {
		if err := os.WriteFile(cfg.Report, b, 0o644); err != nil {
			t.Errorf("write %s: %v", cfg.Report, err)
		}
	}
}

// tryWrite and tryFlush are write and flush returning the error (the load
// writers run off the test goroutine, where t.Fatal is not allowed).
func (c *nvmeInitiator) tryWrite(lba uint64, data []byte) error {
	cmd := c.ioCmd(0x01, lba, len(data)/4096)
	if err := c.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		return fmt.Errorf("nvme Write: %w", err)
	}
	ch, err := c.r.Dequeue()
	if err != nil || ch.Type != 0x9 {
		if err != nil {
			return fmt.Errorf("nvme Write: want R2T: %w", err)
		}
		return fmt.Errorf("nvme Write: want R2T, got type %v", ch)
	}
	var r2t nvme.R2THeader
	if err := c.r.Receive(&r2t); err != nil {
		return fmt.Errorf("nvme R2T: %w", err)
	}
	h2c := nvme.H2CDataHeader{CCCID: cmd.CID, TAG: r2t.TAG, DATAL: uint32(len(data))}
	if err := c.w.SendWithData(0x6, 0, &h2c, 16, data); err != nil {
		return fmt.Errorf("nvme H2CData: %w", err)
	}
	return c.tryStatus("Write")
}

func (c *nvmeInitiator) tryFlush() error {
	cmd := c.ioCmd(0x00, 0, 0)
	if err := c.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		return fmt.Errorf("nvme Flush: %w", err)
	}
	return c.tryStatus("Flush")
}

func (c *nvmeInitiator) tryStatus(op string) error {
	ch, err := c.r.Dequeue()
	if err != nil || ch.Type != 0x5 {
		if err != nil {
			// %w: a deadline on the queue stays a net timeout for errors.As.
			return fmt.Errorf("nvme %s response: %w", op, err)
		}
		return fmt.Errorf("nvme %s response: type %v", op, ch)
	}
	var resp nvme.CapsuleResponse
	if err := c.r.Receive(&resp); err != nil {
		return fmt.Errorf("nvme %s response body: %w", op, err)
	}
	if resp.Status != 0 {
		return fmt.Errorf("nvme %s status 0x%04x", op, resp.Status)
	}
	return nil
}
