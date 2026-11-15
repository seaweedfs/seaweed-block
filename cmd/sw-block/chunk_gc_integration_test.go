//go:build chunkstore_integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// No mocked filer, needle service or authority RPC. Only the GC clock is
// advanced so the test exercises two grace-separated marks without a long soak.
func TestIntegration_OpsChunkGC_RealMasterAndStore(t *testing.T) {
	weed := os.Getenv("CHUNKSTORE_WEED")
	if weed == "" {
		t.Fatal("CHUNKSTORE_WEED required: real-service acceptance must not silently skip")
	}
	filer := startGCWeed(t, weed)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const base = "/gc-cli-test"
	s, err := chunkstore.DialSWFS(chunkstore.SWFSConfig{FilerGRPC: filer, Collection: "swblock", AssignPath: base})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Wait for the real volume registration/assign path, bounded by ctx.
	for {
		_, err = s.Put(ctx, []byte("readiness"))
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	cat, err := chunkstore.NewCatalog(s, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cat.EnsureVolume(ctx, "vol-a", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	remote, err := chunkstore.NewSWFSChunkRemote(s, base+"/vol-a")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("exact reachable bytes")
	for _, key := range []string{"live", "held", "copy", "orphan"} {
		if err = remote.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}
	h, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.Epoch = 1
	h.Holder = "holder-a"
	h.RemoteLSN = 1
	h.Chunks = map[uint64]storage.ChunkRef{0: {Key: "live", SHA256: sha256.Sum256(data)}}
	h.Gen, err = remote.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: 0}, h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cat.CreateSnapshot(ctx, "vol-a", chunkstore.SnapshotRequest{Name: "saved", RequestID: "gc-snapshot"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err = remote.PutIntent(ctx, storage.ChunkIntent{Gen: h.Gen, Epoch: 1, Holder: "holder-a", Keys: []string{"held"}}); err != nil {
		t.Fatal(err)
	}
	// Old-format copy intents have no deadline: they must remain roots.
	if _, err = remote.PutIntent(ctx, storage.ChunkIntent{Gen: h.Gen, Epoch: 1, Holder: "snapshot:vol-a/pending/attempt", Keys: []string{"copy"}}); err != nil {
		t.Fatal(err)
	}
	m, err := master.New(master.Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := m.Close(c); err != nil {
			t.Error(err)
		}
	})
	if _, err = m.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "vol-a", DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{VolumeID: "vol-a", ServerID: "node-a", ReplicaID: "r2", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
		t.Fatal(err)
	}
	if err = m.ObservationHost().Ingest(authority.Observation{ServerID: "node-a", ObservedAt: time.Now().UTC(), Slots: []authority.SlotFact{{VolumeID: "vol-a", ReplicaID: "r2", DataAddr: "127.0.0.1:9202", CtrlAddr: "127.0.0.1:9102"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = m.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	status, err := control.NewEvidenceServiceClient(conn).QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: "vol-a"})
	if err != nil || !status.GetAssigned() || status.GetEpoch() != 1 {
		t.Fatalf("real authority: %v %v", status, err)
	}
	oldClock := chunkGCNow
	defer func() { chunkGCNow = oldClock }()
	now := time.Now().Add(2 * time.Hour)
	chunkGCNow = func() time.Time { return now }
	call := func(apply bool) chunkstore.GCExecution {
		t.Helper()
		args := []string{"gc", "--master", m.Addr(), "--chunk-filer", filer, "--chunk-path", base}
		if apply {
			args = append(args, "--apply")
		}
		rc, out, stderr := runChunk(args...)
		if rc != 0 {
			t.Fatalf("gc rc=%d %s %s", rc, out, stderr)
		}
		var result chunkstore.GCExecution
		if err := json.Unmarshal([]byte(out), &result); err != nil {
			t.Fatal(err)
		}
		t.Logf("apply=%v %s", apply, out)
		return result
	}
	preview := call(false)
	for _, kind := range []string{"HEAD", "snapshot", "capture_intent", "retained_copy_intent"} {
		found := false
		for _, r := range preview.Roots {
			found = found || r.Kind == kind
		}
		if !found {
			t.Fatalf("missing root %s: %+v", kind, preview)
		}
	}
	if preview.MarkSaved || preview.Reclaimed != 0 || len(preview.WouldDelete) != 0 {
		t.Fatalf("first preview: %+v", preview)
	}
	first := call(true)
	if !first.MarkSaved || first.Reclaimed != 0 {
		t.Fatalf("first mark: %+v", first)
	}
	now = now.Add(time.Hour + time.Second)
	preview = call(false)
	if preview.Reclaimed != 0 || len(preview.WouldDelete) != 1 || preview.WouldDelete[0].Key != "orphan" {
		t.Fatalf("mature preview: %+v", preview)
	}
	if got, err := remote.Get(ctx, "orphan"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("preview deleted blob: %q %v", got, err)
	}
	applied := call(true)
	if applied.Reclaimed != 1 || applied.ExpiredIntents != 0 || !applied.MarkSaved {
		t.Fatalf("apply: %+v", applied)
	}
	if _, err = remote.Get(ctx, "orphan"); err == nil {
		t.Fatal("orphan still readable")
	}
	for _, key := range []string{"live", "held", "copy"} {
		if got, err := remote.Get(ctx, key); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("lost root %s: %q %v", key, got, err)
		}
	}
	intents, err := remote.ListIntents(ctx)
	if err != nil || len(intents) != 2 {
		t.Fatalf("lost intent: %v %v", intents, err)
	}
	snaps, _, err := cat.ListSnapshots(ctx, "vol-a", chunkstore.ListOptions{})
	if err != nil || len(snaps) != 1 {
		t.Fatalf("lost snapshot: %v %v", snaps, err)
	}
	if err = remote.Put(ctx, "orphan", data); err == nil {
		t.Fatal("GC removed permanent binding tombstone")
	}
}

func startGCWeed(t *testing.T, weed string) string {
	t.Helper()
	dir := t.TempDir()
	used := map[int]bool{}
	port := func() int {
		t.Helper()
		for p := 21000; p < 40000; p++ {
			if used[p] || used[p+10000] {
				continue
			}
			a, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
			if e != nil {
				continue
			}
			b, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+10000))
			a.Close()
			if e != nil {
				continue
			}
			b.Close()
			used[p] = true
			used[p+10000] = true
			return p
		}
		t.Fatal("no free ports")
		return 0
	}
	mp, vp, fp := port(), port(), port()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(weed, args...)
		cmd.Dir = dir
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("weed child did not settle")
			}
		})
	}
	for _, name := range []string{"m", "v", "f"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	masterAddr := fmt.Sprintf("127.0.0.1:%d", mp)
	run("master", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", mp), "-mdir="+filepath.Join(dir, "m"), "-volumeSizeLimitMB=256", "-defaultReplication=000")
	wait := func(p int) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			c, e := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", p), 100*time.Millisecond)
			if e == nil {
				c.Close()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("weed port %d not ready", p)
	}
	wait(mp)
	run("volume", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", vp), "-dir="+filepath.Join(dir, "v"), "-master="+masterAddr, "-max=32")
	run("filer", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", fp), "-master="+masterAddr, "-defaultStoreDir="+filepath.Join(dir, "f"))
	wait(fp + 10000)
	return fmt.Sprintf("127.0.0.1:%d", fp+10000)
}
