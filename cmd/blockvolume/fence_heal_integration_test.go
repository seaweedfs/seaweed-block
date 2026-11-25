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
	"syscall"
	"testing"
	"time"
)

// TestFencingRoundHealContinuesThePrimary runs the heal of a failed fencing
// round through the production binaries: an RF3 sync-quorum volume whose
// primary r1 stops long enough to be judged lost. The fencing round reaches
// r2, but r3 cannot make its floor durable (its durable root is read-only),
// so it does not answer: one fenced survivor, two needed, nothing is
// published, and r1 does not answer the heal. When r1 resumes, the master
// continues it at epoch 2 on its own evidence; a write after that reads back
// with the one before it, and once r3 can persist again both replicas serve
// r1 at epoch 2 (healthy peers of the primary).
func TestFencingRoundHealContinuesThePrimary(t *testing.T) {
	if isWindows() {
		t.Skip("SIGSTOP and directory permissions: Linux only")
	}
	if os.Geteuid() == 0 {
		t.Skip("a read-only directory does not stop root from writing the floor record")
	}
	bins := buildG54Binaries(t)
	h := &plannedHandoffHarness{dir: t.TempDir(), t: t}
	h.masterAddr = handoffAddress(t)
	type host struct {
		id, data, ctrl, status, nvme, root string
		d                                  *daemon
		args                               []string
	}
	hosts := make([]*host, 3)
	var nodes, placements strings.Builder
	for i := range hosts {
		id := fmt.Sprintf("s%d", i+1)
		hosts[i] = &host{id: id, data: handoffAddress(t), ctrl: handoffAddress(t), status: handoffAddress(t), nvme: handoffAddress(t), root: filepath.Join(h.dir, "vol-"+id)}
		fmt.Fprintf(&nodes, "  - server_id: %s\n    data_addr: %s\n    ctrl_addr: %s\n", id, hosts[i].data, hosts[i].ctrl)
		fmt.Fprintf(&placements, "      - server_id: %s\n        replica_id: r%d\n", id, i+1)
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
	for i, b := range hosts {
		lockDir := b.root + ".locks"
		if err := os.MkdirAll(lockDir, 0o700); err != nil {
			t.Fatal(err)
		}
		b.args = []string{"--volume-lock-dir-test-only", lockDir, "--master", h.masterAddr, "--server-id", b.id, "--volume-id", "v1",
			"--replica-id", fmt.Sprintf("r%d", i+1), "--data-addr", b.data, "--ctrl-addr", b.ctrl, "--status-addr", b.status,
			"--durable-root", b.root, "--durable-impl", "walstore", "--durable-blocks", "65536", "--durable-blocksize", "4096",
			"--heartbeat-interval", "200ms", "--t1-readiness", "--replication-ack", "sync-quorum",
			"--nvme-listen", b.nvme, "--nvme-subsysnqn", plannedHandoffNQN}
		b.d = h.start(t, "blockvolume-"+b.id, bins.volume, b.args...)
		// Registered after the daemon's own cleanup, so it runs first: a
		// stopped child is resumed before it is cancelled.
		d := b.d
		t.Cleanup(func() { _ = d.proc.Signal(syscall.SIGCONT) })
	}
	r1, r2, r3 := hosts[0], hosts[1], hosts[2]
	t.Cleanup(func() { _ = os.Chmod(r3.root, 0o755) })

	// 1. r1 serves at epoch 1 with both replicas healthy behind it.
	h.waitPrimaryAt(t, r1.status, 1)
	waitPeersHealthy(t, r1.status, 1)
	a := bytes.Repeat([]byte{0xa1}, 4096)
	c := dialNVMe(t, r1.nvme, plannedHandoffNQN)
	c.write(t, 0, a)
	c.flush(t)
	if got := c.read(t, 0, 4096); !bytes.Equal(got, a) {
		t.Fatal("block A did not read back at epoch 1")
	}
	c.close()
	// The gate has seen both replicas ready on the stores r1 attested (its
	// probe of a ready primary logs each candidate's readiness).
	poll(t, 30*time.Second, "the gate never saw r2 and r3 ready on attested stores", func() bool {
		data, _ := os.ReadFile(master.log)
		for _, l := range strings.Split(string(data), string(rune(10))) {
			if strings.Contains(l, "promotion gate volume=v1 current=r1") && strings.Contains(l, "{r2 true") && strings.Contains(l, "{r3 true") {
				return true
			}
		}
		return false
	})

	// 2. r3 can no longer persist a floor; r1 stops: a fencing round at 2
	// gets only r2, publishes nothing, and r1 cannot answer the heal.
	if err := os.Chmod(r3.root, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := r1.d.proc.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	master.waitLog(t, 60*time.Second, "fencing round at 2: 1 fenced survivors, need 2")
	master.waitLog(t, 30*time.Second, "heal: primary r1 did not answer ready")
	r3.d.waitLog(t, 30*time.Second, "fencing round=")
	if master.has("heal: continuing") || master.has("direct_reassign") {
		t.Fatal("a line was published while the fencing round had too few survivors")
	}
	for _, b := range []*host{r2, r3} {
		if e := statusEpoch(t, b.status); e != 1 {
			t.Fatalf("%s is at epoch %d while nothing may be published; want 1", b.id, e)
		}
	}

	// 3. r1 resumes: the remembered round heals it at epoch 2.
	if err := r1.d.proc.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	master.waitLog(t, 60*time.Second, "heal: continuing r1 at epoch 2")
	h.waitPrimaryAt(t, r1.status, 2)
	if master.has("direct_reassign=r2") || master.has("direct_reassign=r3") {
		t.Fatal("a survivor was promoted instead of the heal")
	}

	// 4. A write at epoch 2 reads back with the one before it.
	b := bytes.Repeat([]byte{0xb2}, 4096)
	c = dialNVMe(t, r1.nvme, plannedHandoffNQN)
	c.write(t, 1, b)
	c.flush(t)
	if got := c.read(t, 0, 8192); !bytes.Equal(got, append(append([]byte(nil), a...), b...)) {
		t.Fatal("blocks A and B did not read back at epoch 2")
	}
	c.close()

	// 5. r3 can persist again: both replicas rejoin r1 at epoch 2.
	if err := os.Chmod(r3.root, 0o755); err != nil {
		t.Fatal(err)
	}
	// EXPERIMENT: restart r3 so its role call runs again.
	r3.d.kill(t)
	r3.d = h.start(t, "blockvolume-s3-restarted", bins.volume, r3.args...)
	waitPeersHealthy(t, r1.status, 2)
}

// statusEpoch is the epoch a host's /status reports.
func statusEpoch(t *testing.T, statusAddr string) uint64 {
	t.Helper()
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + statusAddr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("status %s: %v", statusAddr, err)
	}
	defer resp.Body.Close()
	var st struct{ Epoch uint64 }
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("status %s: %v", statusAddr, err)
	}
	return st.Epoch
}

// waitPeersHealthy waits until the primary's /status/peers lists r2 and r3
// healthy at epoch: the primary ships to both and counts them again.
func waitPeersHealthy(t *testing.T, statusAddr string, epoch uint64) {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	var last string
	end := time.Now().Add(90 * time.Second)
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
				if healthy["r2"] && healthy["r3"] {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("the primary's peers never were r2 and r3 healthy at epoch %d; last: %s", epoch, last)
}
