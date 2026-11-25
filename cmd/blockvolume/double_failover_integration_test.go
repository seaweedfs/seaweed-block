//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDoubleFailoverReadsBackEveryAckedWrite runs two failovers of an RF3
// sync-quorum volume through the production binaries and reads back every
// acknowledged write on the last primary:
//
//  1. r1 serves at epoch 1; block A is written and flushed (acknowledged).
//  2. r1 is killed. The gate fences the two survivors at 2 and promotes one
//     of them (P2); block B is written and flushed on P2.
//  3. r1 restarts and rejoins P2 at epoch 2 as a healthy replica. At RF3
//     sync_quorum a fenced decision needs two survivors, so without the
//     rejoin the second failover could not be decided.
//  4. P2 is killed. The gate fences the two survivors at 3 and promotes one
//     of them (P3); block C is written and flushed on P3.
//  5. A, B and C all read back on P3.
func TestDoubleFailoverReadsBackEveryAckedWrite(t *testing.T) {
	bins := buildG54Binaries(t)
	h := &plannedHandoffHarness{dir: t.TempDir(), t: t}
	h.masterAddr = handoffAddress(t)
	type host struct {
		id, rid, data, ctrl, status, nvme, root string
		d                                       *daemon
		args                                    []string
	}
	hosts := make([]*host, 3)
	var nodes, placements strings.Builder
	for i := range hosts {
		id := fmt.Sprintf("s%d", i+1)
		hosts[i] = &host{id: id, rid: fmt.Sprintf("r%d", i+1), data: handoffAddress(t), ctrl: handoffAddress(t), status: handoffAddress(t), nvme: handoffAddress(t), root: filepath.Join(h.dir, "vol-"+id)}
		fmt.Fprintf(&nodes, "  - server_id: %s\n    data_addr: %s\n    ctrl_addr: %s\n", id, hosts[i].data, hosts[i].ctrl)
		fmt.Fprintf(&placements, "      - server_id: %s\n        replica_id: %s\n", id, hosts[i].rid)
	}
	spec := filepath.Join(h.dir, "cluster-spec.yaml")
	if err := os.WriteFile(spec, []byte("nodes:\n"+nodes.String()+"volumes:\n  - id: v1\n    size_bytes: 268435456\n    replication_factor: 3\n    placements:\n"+placements.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	master := h.start(t, "blockmaster", bins.master,
		"--authority-store", filepath.Join(h.dir, "master", "authority"), "--lifecycle-store", filepath.Join(h.dir, "master", "lifecycle"),
		"--cluster-spec", spec, "--expected-slots-per-volume", "3", "--lifecycle-product-loop-interval", "200ms",
		"--freshness-window", "2s", "--launcher-replication-ack", "sync-quorum", "--listen", h.masterAddr)
	master.waitLog(t, 30*time.Second, "blockmaster: lock acquired")
	for _, b := range hosts {
		lockDir := b.root + ".locks"
		if err := os.MkdirAll(lockDir, 0o700); err != nil {
			t.Fatal(err)
		}
		b.args = []string{"--volume-lock-dir-test-only", lockDir, "--master", h.masterAddr, "--server-id", b.id, "--volume-id", "v1",
			"--replica-id", b.rid, "--data-addr", b.data, "--ctrl-addr", b.ctrl, "--status-addr", b.status,
			"--durable-root", b.root, "--durable-impl", "walstore", "--durable-blocks", "65536", "--durable-blocksize", "4096",
			"--heartbeat-interval", "200ms", "--t1-readiness", "--replication-ack", "sync-quorum",
			"--nvme-listen", b.nvme, "--nvme-subsysnqn", plannedHandoffNQN}
		b.d = h.start(t, "blockvolume-"+b.id, bins.volume, b.args...)
	}
	rids := func(hs []*host) []string {
		var out []string
		for _, b := range hs {
			out = append(out, b.rid)
		}
		return out
	}
	others := func(p *host) []*host {
		var out []*host
		for _, b := range hosts {
			if b != p {
				out = append(out, b)
			}
		}
		return out
	}
	// gateSawReady waits until the gate's probe of a ready primary p has
	// seen both other replicas ready on the stores p attested.
	gateSawReady := func(p *host) {
		o := others(p)
		poll(t, 60*time.Second, fmt.Sprintf("the gate never saw %s and %s ready behind %s", o[0].rid, o[1].rid, p.rid), func() bool {
			data, _ := os.ReadFile(master.log)
			for _, l := range strings.Split(string(data), "\n") {
				if strings.Contains(l, "promotion gate volume=v1 current="+p.rid) && strings.Contains(l, "{"+o[0].rid+" true") && strings.Contains(l, "{"+o[1].rid+" true") {
					return true
				}
			}
			return false
		})
	}
	// promotedAmong waits for one of cands to serve as primary at epoch.
	promotedAmong := func(epoch uint64, cands []*host) *host {
		var got *host
		poll(t, 120*time.Second, fmt.Sprintf("no survivor became primary at epoch %d", epoch), func() bool {
			for _, b := range cands {
				if statusIsPrimaryAt(b.status, epoch) {
					got = b
					return true
				}
			}
			return false
		})
		return got
	}
	writeAcked := func(p *host, lba uint64, fill byte) []byte {
		data := bytes.Repeat([]byte{fill}, 4096)
		c := dialNVMe(t, p.nvme, plannedHandoffNQN)
		defer c.close()
		c.write(t, lba, data)
		c.flush(t) // the acknowledgement
		return data
	}
	readBack := func(p *host, want []byte, what string) {
		c := dialNVMe(t, p.nvme, plannedHandoffNQN)
		defer c.close()
		if got := c.read(t, 0, len(want)); !bytes.Equal(got, want) {
			t.Fatalf("%s did not read back on %s", what, p.rid)
		}
	}

	// 1. r1 serves at epoch 1 with both replicas healthy; A is acknowledged.
	p1 := hosts[0]
	h.waitPrimaryAt(t, p1.status, 1)
	waitPeersHealthyAt(t, p1.status, 1, rids(others(p1))...)
	acked := writeAcked(p1, 0, 0xa1)
	readBack(p1, acked, "block A")
	gateSawReady(p1)

	// 2. r1 is killed: a survivor is promoted at 2 on a fenced round; B.
	p1.d.kill(t)
	p2 := promotedAmong(2, others(p1))
	// sync_quorum: B is acknowledged only with the other survivor counted.
	for _, b := range others(p1) {
		if b != p2 {
			waitPeersHealthyAt(t, p2.status, 2, b.rid)
		}
	}
	acked = append(acked, writeAcked(p2, 1, 0xb2)...)
	readBack(p2, acked, "blocks A and B")

	// 3. r1 restarts and rejoins P2 at epoch 2 (two survivors are needed
	// for the next fenced decision).
	p1.d = h.start(t, "blockvolume-"+p1.id+"-restart", bins.volume, p1.args...)
	waitPeersHealthyAt(t, p2.status, 2, rids(others(p2))...)
	gateSawReady(p2)

	// 4. P2 is killed: a survivor is promoted at 3 on a fenced round; C.
	p2.d.kill(t)
	p3 := promotedAmong(3, others(p2))
	for _, b := range others(p2) {
		if b != p3 {
			waitPeersHealthyAt(t, p3.status, 3, b.rid)
		}
	}
	acked = append(acked, writeAcked(p3, 2, 0xc3)...)

	// 5. Every acknowledged write reads back on the last primary.
	readBack(p3, acked, "blocks A, B and C")
}

// statusIsPrimaryAt reports whether the host at statusAddr serves as a
// healthy primary at epoch (an unreachable host is not).
func statusIsPrimaryAt(statusAddr string, epoch uint64) bool {
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get("http://" + statusAddr + "/status?volume=v1")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return statusPrimary(b, epoch)
}

// waitPeersHealthyAt waits until the primary's /status/peers lists every
// replica in rids healthy at epoch: the primary ships to them and counts them.
func waitPeersHealthyAt(t *testing.T, statusAddr string, epoch uint64, rids ...string) {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	var last string
	end := time.Now().Add(120 * time.Second)
	for time.Now().Before(end) {
		if resp, err := c.Get("http://" + statusAddr + "/status/peers?volume=v1"); err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			last = string(body)
			var st struct {
				Peers []struct {
					ReplicaID string
					State     string
					Epoch     uint64
				}
			}
			if json.Unmarshal(body, &st) == nil {
				healthy := map[string]bool{}
				for _, p := range st.Peers {
					if p.State == "healthy" && p.Epoch == epoch {
						healthy[p.ReplicaID] = true
					}
				}
				all := true
				for _, r := range rids {
					all = all && healthy[r]
				}
				if all {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("the primary at %s never had %v healthy at epoch %d; last: %s", statusAddr, rids, epoch, last)
}
