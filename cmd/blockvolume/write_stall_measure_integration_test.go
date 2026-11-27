//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// Write-stall measurement, not a gate test: one client writes continuously
// (one 4 KiB write+flush in flight, a new LBA per op) through a failover of an
// RF3 volume, journals every acknowledged write, and redials the current
// primary after any error. Each run writes one JSON record; every journaled
// write must read back on the last primary.
//
// SWBLOCK_STALL_OUT    directory for the records (the tests skip without it)
// SWBLOCK_STALL_ACK    sync-quorum (default) or sync-all
// SWBLOCK_STALL_CONTROL ""; "pause" (the writer sleeps 3 s once); "badsum"
//                      (one ACK journaled with a wrong sha256)

const (
	stallBlocks = 65536 // the volume's 268435456 bytes in 4 KiB blocks
	stallPace   = 10 * time.Millisecond
	// stallOpWithin bounds one dial or write+flush, as the Linux NVMe host's
	// default io_timeout (30 s) bounds a guest's command before it retries.
	stallOpWithin = 30 * time.Second
)

// stallStatus reads a host's /status: its epoch and whether it serves as a
// healthy primary (false for an unreachable or stopped host).
func stallStatus(addr string) (uint64, bool) {
	resp, err := (&http.Client{Timeout: 500 * time.Millisecond}).Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	var st struct {
		Epoch         uint64
		Healthy       bool
		AuthorityRole string
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&st) != nil {
		return 0, false
	}
	return st.Epoch, st.Healthy && st.AuthorityRole == "primary"
}

type stallHost struct {
	id, rid, data, ctrl, status, nvme, root string
	d                                       *daemon
	args                                    []string
}

type stallEvent struct {
	Name string
	At   time.Time
}

type stallWriter struct {
	hosts   []*stallHost
	control string
	seed    uint64
	stop    chan struct{}
	done    chan struct{}

	mu      sync.Mutex
	acks    []time.Time
	sums    map[uint64][32]byte
	fails   []time.Time
	errs    map[string]int
	redials int
}

func stallBlock(seed, lba uint64) []byte {
	var key [16]byte
	binary.LittleEndian.PutUint64(key[:8], seed)
	binary.LittleEndian.PutUint64(key[8:], lba)
	s := sha256.Sum256(key[:])
	return bytes.Repeat(s[:], 4096/len(s))
}

// primary is the NVMe address of the highest-epoch healthy primary, or "".
func (w *stallWriter) primary() string {
	best, addr := uint64(0), ""
	for _, h := range w.hosts {
		if ep, ok := stallStatus(h.status); ok && ep > best {
			best, addr = ep, h.nvme
		}
	}
	return addr
}

func (w *stallWriter) fail(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fails = append(w.fails, time.Now())
	msg := err.Error()
	if len(msg) > 120 {
		msg = msg[:120]
	}
	w.errs[msg]++
}

func (w *stallWriter) run() {
	defer close(w.done)
	var c *nvmeInitiator
	defer func() {
		if c != nil {
			c.close()
		}
	}()
	start, paused, bad := time.Now(), false, false
	for n := uint64(0); ; {
		select {
		case <-w.stop:
			return
		case <-time.After(stallPace):
		}
		if w.control == "pause" && !paused && time.Since(start) > 10*time.Second {
			paused = true
			time.Sleep(3 * time.Second)
		}
		if c == nil {
			addr := w.primary()
			if addr == "" {
				continue
			}
			var err error
			if c, err = stallDial(addr); err != nil {
				w.fail(err)
				c = nil
				continue
			}
			w.mu.Lock()
			w.redials++
			w.mu.Unlock()
		}
		lba := n % stallBlocks
		n++
		data := stallBlock(w.seed, lba)
		if err := stallWriteFlush(c, lba, data); err != nil {
			w.fail(err)
			c.close()
			c = nil
			w.mu.Lock()
			delete(w.sums, lba) // a failed write leaves the block unknown
			w.mu.Unlock()
			continue
		}
		sum := sha256.Sum256(data)
		if w.control == "badsum" && !bad && n > 100 {
			bad = true
			sum[0] ^= 0xff
		}
		w.mu.Lock()
		w.acks = append(w.acks, time.Now())
		w.sums[lba] = sum
		w.mu.Unlock()
	}
}

// acksAfter waits until n writes are acknowledged after t0; false on timeout.
func (w *stallWriter) acksAfter(t0 time.Time, n int, within time.Duration) bool {
	end := time.Now().Add(within)
	for time.Now().Before(end) {
		w.mu.Lock()
		i := sort.Search(len(w.acks), func(i int) bool { return w.acks[i].After(t0) })
		got := len(w.acks) - i
		w.mu.Unlock()
		if got >= n {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// stallDial is dialNVMe returning errors (it runs off the test goroutine).
func stallDial(addr string) (c *nvmeInitiator, err error) {
	c = &nvmeInitiator{}
	defer func() {
		if err != nil {
			c.close()
		}
	}()
	cd := nvme.ConnectData{HostID: [16]byte{0x5e, 0xa1}, SubNQN: plannedHandoffNQN, HostNQN: "nqn.2026-11.io.seaweedfs:stall-measure"}
	var cntlID uint16
	for qid := uint32(0); qid <= 1; qid++ {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			return c, fmt.Errorf("dial: %w", err)
		}
		if qid == 0 {
			c.admin = conn
		} else {
			c.io = conn
		}
		_ = conn.SetDeadline(time.Now().Add(stallOpWithin))
		r, w := nvme.NewReader(conn), nvme.NewWriter(conn)
		if err := w.SendHeaderOnly(0x0, &nvme.ICRequest{PDUMaxR2T: 1}, 120); err != nil {
			return c, fmt.Errorf("ICReq: %w", err)
		}
		if ch, err := r.Dequeue(); err != nil || ch.Type != 0x1 {
			return c, fmt.Errorf("ICResp: type %v err %v", ch, err)
		}
		if err := r.Receive(&nvme.ICResponse{}); err != nil {
			return c, fmt.Errorf("ICResp body: %w", err)
		}
		cd.CNTLID = 0xFFFF
		if qid == 1 {
			cd.CNTLID = cntlID
		}
		buf := make([]byte, 1024)
		cd.Marshal(buf)
		c.cid++
		cmd := nvme.CapsuleCommand{OpCode: 0x7F, FCType: 0x01, CID: c.cid, D10: qid << 16}
		if err := w.SendWithData(0x4, 0, &cmd, 64, buf); err != nil {
			return c, fmt.Errorf("connect qid %d: %w", qid, err)
		}
		resp, err := stallResponse(r)
		if err != nil || resp.Status != 0 {
			return c, fmt.Errorf("connect qid %d: status 0x%04x err %v", qid, resp.Status, err)
		}
		if qid == 0 {
			cntlID = uint16(resp.DW0 & 0xFFFF)
		} else {
			c.r, c.w = r, w
		}
	}
	return c, nil
}

func stallResponse(r *nvme.Reader) (nvme.CapsuleResponse, error) {
	var resp nvme.CapsuleResponse
	ch, err := r.Dequeue()
	if err != nil || ch.Type != 0x5 {
		return resp, fmt.Errorf("response: type %v err %v", ch, err)
	}
	return resp, r.Receive(&resp)
}

// stallWriteFlush writes one block and flushes it; nil is the ACK.
func stallWriteFlush(c *nvmeInitiator, lba uint64, data []byte) error {
	_ = c.io.SetDeadline(time.Now().Add(stallOpWithin))
	cmd := c.ioCmd(0x01, lba, 1)
	if err := c.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	ch, err := c.r.Dequeue()
	if err != nil || ch.Type != 0x9 {
		return fmt.Errorf("write: want R2T, got type %v err %v", ch, err)
	}
	var r2t nvme.R2THeader
	if err := c.r.Receive(&r2t); err != nil {
		return fmt.Errorf("R2T: %w", err)
	}
	h2c := nvme.H2CDataHeader{CCCID: cmd.CID, TAG: r2t.TAG, DATAL: uint32(len(data))}
	if err := c.w.SendWithData(0x6, 0, &h2c, 16, data); err != nil {
		return fmt.Errorf("H2CData: %w", err)
	}
	if resp, err := stallResponse(c.r); err != nil || resp.Status != 0 {
		return fmt.Errorf("write status 0x%04x err %v", resp.Status, err)
	}
	fl := c.ioCmd(0x00, 0, 0)
	if err := c.w.SendHeaderOnly(0x4, &fl, 64); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	if resp, err := stallResponse(c.r); err != nil || resp.Status != 0 {
		return fmt.Errorf("flush status 0x%04x err %v", resp.Status, err)
	}
	return nil
}

type stallEnv struct {
	t      *testing.T
	ack    string
	h      *plannedHandoffHarness
	bins   l2bins
	master *daemon
	hosts  []*stallHost
	w      *stallWriter
	events []stallEvent
	out    string
}

func (e *stallEnv) mark(name string) { e.events = append(e.events, stallEvent{name, time.Now()}) }

// startStallEnv starts an RF3 volume v1 at the chosen acknowledgement
// profile (the double-failover test's cluster, parameterised) and waits for
// r1 to serve at epoch 1 with both replicas healthy.
func startStallEnv(t *testing.T) *stallEnv {
	out := os.Getenv("SWBLOCK_STALL_OUT")
	if out == "" {
		t.Skip("measurement only: SWBLOCK_STALL_OUT is not set")
	}
	if isWindows() {
		t.Skip("SIGSTOP: Linux only")
	}
	ack := os.Getenv("SWBLOCK_STALL_ACK")
	if ack == "" {
		ack = "sync-quorum"
	}
	if ack != "sync-quorum" && ack != "sync-all" {
		t.Fatalf("SWBLOCK_STALL_ACK=%q; want sync-quorum or sync-all", ack)
	}
	e := &stallEnv{t: t, ack: ack, out: out, bins: buildG54Binaries(t)}
	e.h = &plannedHandoffHarness{dir: t.TempDir(), t: t}
	e.h.masterAddr = handoffAddress(t)
	var nodes, placements strings.Builder
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("s%d", i+1)
		b := &stallHost{id: id, rid: fmt.Sprintf("r%d", i+1), data: handoffAddress(t), ctrl: handoffAddress(t), status: handoffAddress(t), nvme: handoffAddress(t), root: filepath.Join(e.h.dir, "vol-"+id)}
		e.hosts = append(e.hosts, b)
		fmt.Fprintf(&nodes, "  - server_id: %s\n    data_addr: %s\n    ctrl_addr: %s\n", id, b.data, b.ctrl)
		fmt.Fprintf(&placements, "      - server_id: %s\n        replica_id: %s\n", id, b.rid)
	}
	spec := filepath.Join(e.h.dir, "cluster-spec.yaml")
	if err := os.WriteFile(spec, []byte("nodes:\n"+nodes.String()+"volumes:\n  - id: v1\n    size_bytes: 268435456\n    replication_factor: 3\n    placements:\n"+placements.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	e.master = e.h.start(t, "blockmaster", e.bins.master,
		"--authority-store", filepath.Join(e.h.dir, "master", "authority"), "--lifecycle-store", filepath.Join(e.h.dir, "master", "lifecycle"),
		"--cluster-spec", spec, "--expected-slots-per-volume", "3", "--lifecycle-product-loop-interval", "200ms",
		"--freshness-window", "2s", "--launcher-replication-ack", ack, "--listen", e.h.masterAddr)
	e.master.waitLog(t, 30*time.Second, "blockmaster: lock acquired")
	for _, b := range e.hosts {
		lockDir := b.root + ".locks"
		if err := os.MkdirAll(lockDir, 0o700); err != nil {
			t.Fatal(err)
		}
		b.args = []string{"--volume-lock-dir-test-only", lockDir, "--master", e.h.masterAddr, "--server-id", b.id, "--volume-id", "v1",
			"--replica-id", b.rid, "--data-addr", b.data, "--ctrl-addr", b.ctrl, "--status-addr", b.status,
			"--durable-root", b.root, "--durable-impl", "walstore", "--durable-blocks", "65536", "--durable-blocksize", "4096",
			"--heartbeat-interval", "200ms", "--t1-readiness", "--replication-ack", ack,
			"--nvme-listen", b.nvme, "--nvme-subsysnqn", plannedHandoffNQN}
		e.startHost(b, "")
	}
	r1 := e.hosts[0]
	e.h.waitPrimaryAt(t, r1.status, 1)
	waitPeersHealthyAt(t, r1.status, 1, "r2", "r3")
	e.gateSawReady(r1)
	return e
}

func (e *stallEnv) startHost(b *stallHost, suffix string) {
	b.d = e.h.start(e.t, "blockvolume-"+b.id+suffix, e.bins.volume, b.args...)
	// Registered after the daemon's own cleanup, so it runs first: a stopped
	// child is resumed before it is cancelled.
	d := b.d
	e.t.Cleanup(func() { _ = d.proc.Signal(syscall.SIGCONT) })
}

func (e *stallEnv) others(p *stallHost) []*stallHost {
	var out []*stallHost
	for _, b := range e.hosts {
		if b != p {
			out = append(out, b)
		}
	}
	return out
}

// gateSawReady: the gate's probe of a ready primary p saw both other replicas
// ready on the stores p attested.
func (e *stallEnv) gateSawReady(p *stallHost) {
	o := e.others(p)
	poll(e.t, 60*time.Second, fmt.Sprintf("the gate never saw %s and %s ready behind %s", o[0].rid, o[1].rid, p.rid), func() bool {
		data, _ := os.ReadFile(e.master.log)
		for _, l := range strings.Split(string(data), "\n") {
			if strings.Contains(l, "promotion gate volume=v1 current="+p.rid) && strings.Contains(l, "{"+o[0].rid+" true") && strings.Contains(l, "{"+o[1].rid+" true") {
				return true
			}
		}
		return false
	})
}

// primaryFrom looks at least once, and up to within, for one of cands
// serving as primary at an epoch of at least min; nil if none does.
func (e *stallEnv) primaryFrom(min uint64, within time.Duration, cands ...*stallHost) *stallHost {
	end := time.Now().Add(within)
	for {
		for _, b := range cands {
			if ep, ok := stallStatus(b.status); ok && ep >= min {
				return b
			}
		}
		if !time.Now().Before(end) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (e *stallEnv) startWriter() {
	e.w = &stallWriter{hosts: e.hosts, control: os.Getenv("SWBLOCK_STALL_CONTROL"), seed: uint64(time.Now().UnixNano()),
		stop: make(chan struct{}), done: make(chan struct{}), sums: map[uint64][32]byte{}, errs: map[string]int{}}
	go e.w.run()
	e.t.Cleanup(func() { e.stopWriter() })
	if !e.w.acksAfter(time.Time{}, 200, 60*time.Second) {
		e.t.Fatal("the writer did not reach 200 acknowledged writes before the fault")
	}
}

func (e *stallEnv) stopWriter() {
	select {
	case <-e.w.stop:
		return
	default:
	}
	close(e.w.stop)
	select {
	case <-e.w.done:
	case <-time.After(2*stallOpWithin + 15*time.Second):
		e.t.Errorf("the writer did not stop")
	}
}

// settle waits for n ACKs after the last event; false (recorded) on timeout.
func (e *stallEnv) settle(n int, within time.Duration) {
	last := e.events[len(e.events)-1]
	if !e.w.acksAfter(last.At, n, within) {
		e.mark("no-" + fmt.Sprint(n) + "-acks-within-" + within.String() + "-after-" + last.Name)
	}
}

type stallEventRecord struct {
	Event      string
	AtS        float64 // from the writer's first ACK
	GapS       float64 // the ACK gap spanning the event (-1: no ACK after it)
	FirstAckS  float64 // event -> first ACK after it (-1: none)
	RefusedOps int     // failed ops inside that gap
	GapStartS  float64
}

type stallRecord struct {
	Test, Ack, Control string
	Acked, Unknown     int
	Mismatched         int
	ReadBack           int
	Redials            int
	BaselineP50S       float64
	MaxGapS            float64
	Events             []stallEventRecord
	Errors             map[string]int
	Started            time.Time
}

// finish stops the writer, reads back every journaled block on the last
// primary, writes the record and fails the test on any lost ACKed write.
func (e *stallEnv) finish() {
	t := e.t
	e.stopWriter()
	w := e.w
	rec := stallRecord{Test: t.Name(), Ack: e.ack, Control: w.control, Acked: len(w.acks), Unknown: len(w.fails), Redials: w.redials, Errors: w.errs}
	if len(w.acks) == 0 {
		t.Fatal("no write was acknowledged")
	}
	rec.Started = w.acks[0]
	since := func(x time.Time) float64 { return x.Sub(w.acks[0]).Seconds() }
	gaps := make([]float64, 0, len(w.acks))
	for i := 1; i < len(w.acks); i++ {
		g := w.acks[i].Sub(w.acks[i-1]).Seconds()
		gaps = append(gaps, g)
		if g > rec.MaxGapS {
			rec.MaxGapS = g
		}
	}
	if len(e.events) > 0 {
		var before []float64
		for i := 1; i < len(w.acks) && w.acks[i].Before(e.events[0].At); i++ {
			before = append(before, w.acks[i].Sub(w.acks[i-1]).Seconds())
		}
		sort.Float64s(before)
		if len(before) > 0 {
			rec.BaselineP50S = before[len(before)/2]
		}
	} else if len(gaps) > 0 {
		s := append([]float64(nil), gaps...)
		sort.Float64s(s)
		rec.BaselineP50S = s[len(s)/2]
	}
	for _, ev := range e.events {
		r := stallEventRecord{Event: ev.Name, AtS: since(ev.At), GapS: -1, FirstAckS: -1}
		i := sort.Search(len(w.acks), func(i int) bool { return w.acks[i].After(ev.At) })
		if i < len(w.acks) {
			r.FirstAckS = w.acks[i].Sub(ev.At).Seconds()
			if i > 0 {
				r.GapS = w.acks[i].Sub(w.acks[i-1]).Seconds()
				r.GapStartS = since(w.acks[i-1])
				for _, f := range w.fails {
					if f.After(w.acks[i-1]) && f.Before(w.acks[i]) {
						r.RefusedOps++
					}
				}
			}
		}
		rec.Events = append(rec.Events, r)
		t.Logf("STALL %s ack=%s event=%s gap=%.3fs first_ack=%.3fs refused=%d", t.Name(), e.ack, r.Event, r.GapS, r.FirstAckS, r.RefusedOps)
	}

	// Read back every journaled block on the last primary, 64 blocks a read,
	// redialing every 30 s (the helpers' connection deadline is 60 s).
	addr := w.primary()
	if addr == "" {
		if p := e.primaryFrom(1, 60*time.Second, e.hosts...); p != nil {
			addr = p.nvme
		}
	}
	if addr == "" {
		t.Fatal("no primary to read back on")
	}
	lbas := make([]uint64, 0, len(w.sums))
	for l := range w.sums {
		lbas = append(lbas, l)
	}
	sort.Slice(lbas, func(i, j int) bool { return lbas[i] < lbas[j] })
	c, dialed := dialNVMe(t, addr, plannedHandoffNQN), time.Now()
	for i := 0; i < len(lbas); {
		if time.Since(dialed) > 30*time.Second {
			c.close()
			c, dialed = dialNVMe(t, addr, plannedHandoffNQN), time.Now()
		}
		first := lbas[i] - lbas[i]%64
		data := c.read(t, first, 64*4096)
		for ; i < len(lbas) && lbas[i] < first+64; i++ {
			off := (lbas[i] - first) * 4096
			if sha256.Sum256(data[off:off+4096]) != w.sums[lbas[i]] {
				rec.Mismatched++
			}
			rec.ReadBack++
		}
	}
	c.close()
	t.Logf("STALL %s ack=%s acked=%d unknown=%d read_back=%d mismatched=%d baseline_p50=%.3fs max_gap=%.3fs redials=%d",
		t.Name(), e.ack, rec.Acked, rec.Unknown, rec.ReadBack, rec.Mismatched, rec.BaselineP50S, rec.MaxGapS, rec.Redials)
	body, _ := json.MarshalIndent(rec, "", "  ")
	name := fmt.Sprintf("%s-%s-%s.json", strings.ReplaceAll(t.Name(), "/", "_"), e.ack, time.Now().UTC().Format("20060102T150405.000Z"))
	if err := os.WriteFile(filepath.Join(e.out, name), body, 0o644); err != nil {
		t.Errorf("record: %v", err)
	}
	if rec.Mismatched > 0 {
		t.Errorf("%d acknowledged writes did not read back", rec.Mismatched)
	}
}

// TestStallMeasureNoFault: the writer for 30 s with no fault (controls C1, C2).
func TestStallMeasureNoFault(t *testing.T) {
	e := startStallEnv(t)
	e.startWriter()
	time.Sleep(30 * time.Second)
	e.finish()
}

// TestStallMeasureFenceHeal: fence-heal's faults with writes flowing. r3's
// floor is broken and r1 stopped until the master has acted (the heal path
// gives up on r1, or a survivor is promoted); r1 resumes; r3's floor heals.
func TestStallMeasureFenceHeal(t *testing.T) {
	e := startStallEnv(t)
	r1, r3 := e.hosts[0], e.hosts[2]
	aside := r3.root + ".aside"
	t.Cleanup(func() {
		if st, err := os.Stat(aside); err == nil && st.IsDir() {
			_ = os.Remove(r3.root)
			_ = os.Rename(aside, r3.root)
		}
	})
	e.startWriter()
	if err := os.Rename(r3.root, aside); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r3.root, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	e.mark("floor-broken")
	if err := r1.d.proc.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	e.mark("r1-stopped")
	acted := ""
	poll(t, 90*time.Second, "the master neither gave up on r1 nor promoted a survivor", func() bool {
		switch {
		case e.master.has("heal: primary r1 did not answer ready"):
			acted = "heal-gave-up"
		case e.primaryFrom(2, 0, e.hosts[1], e.hosts[2]) != nil:
			acted = "promoted"
		}
		return acted != ""
	})
	e.mark("master-" + acted)
	if err := r1.d.proc.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	e.mark("r1-resumed")
	e.settle(100, 120*time.Second)
	if err := os.Remove(r3.root); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(aside, r3.root); err != nil {
		t.Fatal(err)
	}
	e.mark("floor-healed")
	e.settle(100, 120*time.Second)
	e.finish()
}

// TestStallMeasureDoubleFailover: double failover's faults with writes
// flowing: kill r1; restart it (it rejoins); kill the second primary.
func TestStallMeasureDoubleFailover(t *testing.T) {
	e := startStallEnv(t)
	p1 := e.hosts[0]
	e.startWriter()
	p1.d.kill(t)
	e.mark("kill-" + p1.rid)
	p2 := e.primaryFrom(2, 120*time.Second, e.others(p1)...)
	if p2 == nil {
		t.Fatal("no survivor became primary after the first kill")
	}
	e.mark("promoted-" + p2.rid)
	e.settle(100, 60*time.Second)
	e.startHost(p1, "-restart")
	e.mark("restart-" + p1.rid)
	waitPeersHealthyAt(t, p2.status, 2, p1.rid)
	e.mark("rejoined-" + p1.rid)
	e.gateSawReady(p2)
	e.settle(100, 60*time.Second)
	p2.d.kill(t)
	e.mark("kill-" + p2.rid)
	p3 := e.primaryFrom(3, 120*time.Second, e.others(p2)...)
	if p3 == nil {
		t.Fatal("no survivor became primary after the second kill")
	}
	e.mark("promoted-" + p3.rid)
	e.settle(100, 60*time.Second)
	e.finish()
}
