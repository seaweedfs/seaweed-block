//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestAVolumeThatServedBeforeItsFirstGateTickFailsOver is the creation
// phase's gap, through the production binaries: an RF3 sync-quorum volume
// whose primary serves and acknowledges a write right after its stores are
// attested, and dies before the master's next product tick could read a
// ready answer from it. The volume served, so its creation ended (the
// survivors hold the write): the gate fails it over, and the write reads
// back on the new primary. A master that ended creation only at the
// primary's ready answer would hold this volume as creating for good.
//
// The product loop ticks every 30 s, so the tick that binds the line and the
// next one leave a window the test uses (and checks it did).
func TestAVolumeThatServedBeforeItsFirstGateTickFailsOver(t *testing.T) {
	bins := buildG54Binaries(t)
	h := &plannedHandoffHarness{dir: t.TempDir(), t: t}
	h.masterAddr = handoffAddress(t)
	type host struct {
		id, rid, data, ctrl, status, nvme, root string
		d                                       *daemon
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
		"--cluster-spec", spec, "--expected-slots-per-volume", "3", "--lifecycle-product-loop-interval", "30s",
		"--freshness-window", "2s", "--launcher-replication-ack", "sync-quorum", "--listen", h.masterAddr)
	master.waitLog(t, 30*time.Second, "blockmaster: lock acquired")
	for _, b := range hosts {
		lockDir := b.root + ".locks"
		if err := os.MkdirAll(lockDir, 0o700); err != nil {
			t.Fatal(err)
		}
		b.d = h.start(t, "blockvolume-"+b.id, bins.volume, "--volume-lock-dir-test-only", lockDir, "--master", h.masterAddr,
			"--server-id", b.id, "--volume-id", "v1", "--replica-id", b.rid, "--data-addr", b.data, "--ctrl-addr", b.ctrl,
			"--status-addr", b.status, "--durable-root", b.root, "--durable-impl", "walstore", "--durable-blocks", "65536",
			"--durable-blocksize", "4096", "--heartbeat-interval", "200ms", "--t1-readiness", "--replication-ack", "sync-quorum",
			"--nvme-listen", b.nvme, "--nvme-subsysnqn", plannedHandoffNQN)
	}
	ended := func() bool { return master.has("blockmaster: volume=v1 creation complete") }

	// 1. r1 serves at epoch 1 with both replicas counted (its stores are
	// attested); block A is written and flushed (acknowledged by the quorum).
	p1, survivors := hosts[0], hosts[1:]
	h.waitPrimaryAt(t, p1.status, 1)
	waitPeersHealthyAt(t, p1.status, 1, survivors[0].rid, survivors[1].rid)
	acked := bytes.Repeat([]byte{0xa1}, 4096)
	c := dialNVMe(t, p1.nvme, plannedHandoffNQN)
	c.write(t, 0, acked)
	c.flush(t) // the acknowledgement
	c.close()

	// 2. r1 dies before any tick read a ready answer from it.
	if ended() {
		t.Fatal("fixture: a tick ended creation before the primary died (the window this test needs did not occur)")
	}
	p1.d.kill(t)

	// 3. The next tick reads the survivors' write: creation ended, and the
	// gate fails the volume over.
	var p2 *host
	poll(t, 120*time.Second, "no survivor became primary at epoch 2 (the volume held as creating?)", func() bool {
		for _, b := range survivors {
			if statusIsPrimaryAt(b.status, 2) {
				p2 = b
				return true
			}
		}
		return false
	})
	if !master.has("creation complete: survivor") {
		t.Fatal("creation did not end on the survivors' write")
	}

	// 4. The acknowledged write reads back on the new primary.
	r := dialNVMe(t, p2.nvme, plannedHandoffNQN)
	defer r.close()
	if got := r.read(t, 0, len(acked)); !bytes.Equal(got, acked) {
		t.Fatalf("the acknowledged write did not read back on %s", p2.rid)
	}
}
