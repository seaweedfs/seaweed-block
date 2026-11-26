//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Must-pass table rows A1 and A7, written for the 1/2/3-volume cells: run
// them with SWBLOCK_TEST_VOLUMES=1, 2 and 3 (multi_volume_harness_test.go).
// The sibling volumes check themselves in the harness cleanup.

// TestCellCreateMountWriteRead (row A1): v1 starts, serves as primary, takes
// writes of one and of several blocks at scattered LBAs, and every
// acknowledged write reads back with the same bytes, on the writing
// connection and again on a new one.
func TestCellCreateMountWriteRead(t *testing.T) {
	env := startHandoffEnv(t, 1)
	holder := env.start(t, "blockvolume-s1", "s1")
	env.adopt(t, holder, "s1")

	type write struct {
		lba    uint64
		blocks int
	}
	writes := []write{{0, 1}, {1, 1}, {7, 3}, {1023, 1}, {1024, 8}, {4095, 2}, {32768, 16}, {65535, 1}}
	data := func(w write) []byte {
		var b []byte
		for i := 0; i < w.blocks; i++ {
			b = append(b, siblingBlock("cell-a1", w.lba+uint64(i))...)
		}
		return b
	}
	c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
	for _, w := range writes {
		c.write(t, w.lba, data(w))
		c.flush(t)
	}
	check := func(c *nvmeInitiator, where string) {
		for _, w := range writes {
			if got := c.read(t, w.lba, w.blocks*4096); string(got) != string(data(w)) {
				t.Fatalf("%s: LBA %d (+%d blocks) read back other bytes", where, w.lba, w.blocks)
			}
		}
	}
	check(c, "the writing connection")
	c.close()
	c = dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
	defer c.close()
	check(c, "a new connection")
}

// createVolumeV1 asks the master to create v1 again.
func (env *handoffEnv) createVolumeV1(t *testing.T) error {
	t.Helper()
	conn, err := grpc.NewClient(env.h.masterAddr, env.masterDialOption(t, "admin"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "v1", SizeBytes: 268435456, ReplicationFactor: 1})
	return err
}

// requireNameRefused checks CreateVolume's documented v1 limit: a deleted
// volume's id is never reused (FailedPrecondition).
func requireNameRefused(t *testing.T, err error, when string) {
	t.Helper()
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "its id is never reused") {
		t.Fatalf("%s: CreateVolume v1 answered %v; want FailedPrecondition, id never reused", when, err)
	}
}

// TestCellDeletedNameIsNotReused (row A7): v1 is written, sealed, revoked
// and deleted; CreateVolume with the name v1 is then refused, and refused
// again after a master restart (the reservation is durable). The HEAD stays
// a deleted tombstone with an empty pool throughout.
func TestCellDeletedNameIsNotReused(t *testing.T) {
	env := startHandoffEnv(t, 1)
	holder := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, pattern("name-not-reused!"))
	g := env.seal(t, "s1", e)
	env.revoke(t, e, g)
	if err := holder.wait(30 * time.Second); err != nil {
		t.Fatalf("s1 after the revocation: %v; want a clean exit by itself", err)
	}
	env.waitAck(t, e, g)
	out, errOut, code := env.volumeDelete(t, "del-v1-reuse")
	requireDeleteCompleted(t, out, errOut, code)

	requireNameRefused(t, env.createVolumeV1(t), "after the delete")
	if state, n := env.headShown(t); state != "deleted" || n != 0 {
		t.Fatalf("after the refused create: HEAD state %q, pool objects %d; want deleted, 0", state, n)
	}

	env.masterProcess.term(t, 30*time.Second)
	env.masterProcess = env.h.start(t, "blockmaster-restarted", env.h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	requireNameRefused(t, env.createVolumeV1(t), "after a master restart")
	if state, n := env.headShown(t); state != "deleted" || n != 0 {
		t.Fatalf("after the restart: HEAD state %q, pool objects %d; want deleted, 0", state, n)
	}
}
