//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// HEAD size against volume size (V1-SPEC r2.94), informative: it measures,
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
// The filer runs on its default store (leveldb2) under the test's directory;
// a filer.toml anywhere weed would read one fails the test before any start,
// so the store measured is the one named in the report.

const headSegmentBytes = 1 << 30

type headSzCut struct {
	Cut            int     `json:"cut"`
	ChangedBlocks  int     `json:"changed_blocks"`
	HeadBytes      int     `json:"head_bytes"`
	HeadSegments   int     `json:"head_segments"`
	CutTotalMs     float64 `json:"cut_total_ms"`
	BarrierPauseMs float64 `json:"cut_barrier_pause_ms"`
	UploadedChunks int     `json:"uploaded_chunks"`
	UploadedBytes  int64   `json:"uploaded_bytes"`
	SnapshotMs     float64 `json:"snapshot_command_ms"`
}

type headSzSize struct {
	Size       string      `json:"size"`
	Bytes      uint64      `json:"bytes"`
	Segments   int         `json:"segments"`
	Result     string      `json:"result"` // measured | refused_headroom
	Refusal    string      `json:"refusal,omitempty"`
	WriteMs    float64     `json:"write_ms,omitempty"`
	Cuts       []headSzCut `json:"cuts,omitempty"`
	HeadPerGiB float64     `json:"head_bytes_per_gib,omitempty"`
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
	h := newPlannedHandoffHarness(t)
	clusterDir := filepath.Join(h.dir, "weed")
	if err := os.MkdirAll(clusterDir, 0o700); err != nil {
		t.Fatal(err)
	}
	headSzAmbientFilerToml(t, clusterDir)
	env := &handoffEnv{h: h, cluster: startWorkspaceWeedWith(t, clusterDir, w4WeedOpts{VolumeSizeLimitMB: 1024, VolumeMax: 64}),
		chunkPath: fmt.Sprintf("/headsz-%d", time.Now().UnixNano()), hosts: map[string]bvHost{}}
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

	// one cluster spec with every size's volume; one replica each on s1
	var vols strings.Builder
	for i, s := range sizes {
		b := headSzParse(t, s)
		id := "h" + strings.ToLower(strings.TrimSpace(s))
		env.hosts[id] = bvHost{id: id, server: "s1", replica: fmt.Sprintf("r%d", i+1), volume: id, root: filepath.Join(h.dir, "vol-"+id),
			data: handoffAddress(t), ctrl: handoffAddress(t), status: handoffAddress(t), nvme: handoffAddress(t)}
		fmt.Fprintf(&vols, "  - id: %s\n    size_bytes: %d\n    replication_factor: 1\n    placements:\n      - server_id: s1\n        replica_id: r%d\n", id, b, i+1)
	}
	spec := filepath.Join(h.dir, "cluster-spec.yaml")
	if err := os.WriteFile(spec, []byte(fmt.Sprintf("nodes:\n  - server_id: s1\n    data_addr: %s\n    ctrl_addr: %s\nvolumes:\n%s", h.dataAddr, h.ctrlAddr, vols.String())), 0o644); err != nil {
		t.Fatal(err)
	}
	mstate := filepath.Join(h.dir, "master")
	master := h.start(t, "blockmaster", h.master, "--authority-store", filepath.Join(mstate, "authority"), "--lifecycle-store", filepath.Join(mstate, "lifecycle"),
		"--cluster-spec", spec, "--expected-slots-per-volume", "1", "--listen", h.masterAddr,
		"--workspace-rpc", "--workspace-head-base-url", "http://"+env.cluster.filerHTTP+env.chunkPath)
	master.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")

	for _, s := range sizes {
		rep.Sizes = append(rep.Sizes, headSzOne(t, env, s, changeCuts))
	}
}

// headSzOne measures one size; its blockvolume is stopped at the end.
func headSzOne(t *testing.T, env *handoffEnv, size string, changeCuts int) headSzSize {
	t.Helper()
	bytes := headSzParse(t, size)
	id := "h" + strings.ToLower(strings.TrimSpace(size))
	b := env.hosts[id]
	row := headSzSize{Size: strings.TrimSpace(size), Bytes: bytes, Segments: int((bytes + headSegmentBytes - 1) / headSegmentBytes)}
	env.volExtra = []string{"--durable-blocks", strconv.FormatUint(bytes/4096, 10), "--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	d := env.h.start(t, "blockvolume-"+id, env.h.volume, env.volArgsFor(b)...)
	primary := fmt.Sprintf("blockvolume: volume %s adopted as PRIMARY at %s@", b.volume, b.replica)
	end := time.Now().Add(120 * time.Second)
	for !d.has(primary) {
		select {
		case <-d.done:
			if l := d.line("bytes free"); l != "" {
				row.Result, row.Refusal = "refused_headroom", strings.TrimSpace(l)
				return row
			}
			t.Fatalf("blockvolume %s exited before it adopted its volume: %v", id, d.err)
		case <-time.After(200 * time.Millisecond):
		}
		if time.Now().After(end) {
			t.Fatalf("blockvolume %s did not adopt its volume in 120 s", id)
		}
	}
	defer d.term(t, 60*time.Second)
	poll(t, 60*time.Second, id+" never reported its frontend ready", func() bool { return volumeFrontendReady(b.status, b.volume) })

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
	for cut := 1; cut <= 1+changeCuts; cut++ {
		changed := 0
		if cut > 1 {
			write(uint64(cut), fmt.Sprintf("change%d", cut))
			_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
			c.flush(t)
			changed = 1
		}
		row.Cuts = append(row.Cuts, headSzCutOnce(t, env, b, cut, changed))
	}
	row.Result = "measured"
	if row.Segments > 0 && len(row.Cuts) > 0 {
		row.HeadPerGiB = float64(row.Cuts[0].HeadBytes) / float64(row.Segments)
	}
	return row
}

// headSzCutOnce takes one snapshot and reads what it wrote: HEAD from the
// filer, the cut from the volume's durable status.
func headSzCutOnce(t *testing.T, env *handoffEnv, b bvHost, cut, changed int) headSzCut {
	t.Helper()
	r := headSzCut{Cut: cut, ChangedBlocks: changed}
	start := time.Now()
	out, errOut := env.h.command(t, "ops", "chunk", "snapshot", "--volume", b.volume, "--status-addr", b.status,
		"--name", fmt.Sprintf("headsz-%d", cut), "--request-id", fmt.Sprintf("headsz-%s-%d", b.volume, cut))
	r.SnapshotMs = ms(time.Since(start))
	_, _ = out, errOut
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
	head := headSzGet(t, "http://"+env.cluster.filerHTTP+env.chunkPath+"/"+b.volume+"/HEAD", 256<<20)
	r.HeadBytes = len(head)
	var doc struct {
		Segments map[string]json.RawMessage `json:"segments"`
	}
	if err := json.Unmarshal(head, &doc); err != nil {
		t.Fatalf("HEAD of %s is not JSON (%d bytes): %v", b.volume, len(head), err)
	}
	r.HeadSegments = len(doc.Segments)
	return r
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
