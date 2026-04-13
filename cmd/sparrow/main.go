// Command sparrow runs a minimal V3 block slice demonstrating
// healthy, catch-up, and rebuild paths through the semantic engine.
//
// Usage: go run ./cmd/sparrow
//
// The sparrow starts a primary + replica in-process, writes blocks,
// and exercises all three recovery paths through the V3 adapter.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

func main() {
	log.SetFlags(log.Lmicroseconds)
	fmt.Println("=== V3 Block Sparrow ===")
	fmt.Println()

	// --- Setup ---
	primaryStore := storage.NewBlockStore(256, 4096)
	replicaStore := storage.NewBlockStore(256, 4096)

	// Start replica listener.
	replicaListener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		log.Fatalf("replica listener: %v", err)
	}
	replicaListener.Serve()
	defer replicaListener.Stop()
	replicaAddr := replicaListener.Addr()
	fmt.Printf("Replica listening on %s\n", replicaAddr)

	// Create executor and adapter.
	exec := transport.NewBlockExecutor(primaryStore, replicaAddr, 1)
	adpt := adapter.NewVolumeReplicaAdapter(exec)

	// ==========================================
	// Demo 1: Healthy (R >= H, no recovery needed)
	// ==========================================
	fmt.Println("\n--- Demo 1: Healthy (no recovery needed) ---")

	// Write some blocks on primary.
	for i := uint32(0); i < 10; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		primaryStore.Write(i, data)
	}
	primaryStore.Sync()

	// Ship blocks to replica directly (simulating normal replication).
	blocks := primaryStore.AllBlocks()
	_, _, pH := primaryStore.Boundaries()
	for lba, data := range blocks {
		replicaStore.ApplyEntry(lba, data, pH) // use primary's head LSN
	}
	replicaStore.Sync()

	// Now assign and probe — replica is caught up.
	adpt.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: replicaAddr, CtrlAddr: replicaAddr,
	})

	// Wait for async probe.
	time.Sleep(200 * time.Millisecond)
	p := adpt.Projection()
	fmt.Printf("Mode: %s  Decision: %s  R=%d S=%d H=%d\n",
		p.Mode, p.RecoveryDecision, p.R, p.S, p.H)
	printResult("Demo 1", p.Mode == engine.ModeHealthy)

	// ==========================================
	// Demo 2: Catch-up (short gap, R < H but R >= S)
	// ==========================================
	fmt.Println("\n--- Demo 2: Catch-up (short gap) ---")

	// Write more on primary but DON'T ship to replica.
	for i := uint32(10); i < 20; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		primaryStore.Write(i, data)
	}
	primaryStore.Sync()

	// Create fresh adapter (simulates replica reconnect).
	exec2 := transport.NewBlockExecutor(primaryStore, replicaAddr, 2)
	adpt2 := adapter.NewVolumeReplicaAdapter(exec2)

	adpt2.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: replicaAddr, CtrlAddr: replicaAddr,
	})

	// Wait for probe + catch-up + session close callback.
	p2 := waitForMode(adpt2, engine.ModeHealthy, 5*time.Second)
	fmt.Printf("Mode: %s  Decision: %s  R=%d S=%d H=%d\n",
		p2.Mode, p2.RecoveryDecision, p2.R, p2.S, p2.H)
	printResult("Demo 2", p2.Mode == engine.ModeHealthy)

	// ==========================================
	// Demo 3: Rebuild (long gap, R < S)
	// ==========================================
	fmt.Println("\n--- Demo 3: Rebuild (long gap) ---")

	// Create a fresh replica store (simulates total data loss).
	replicaStore2 := storage.NewBlockStore(256, 4096)
	replicaListener.Stop()
	replicaListener2, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore2)
	if err != nil {
		log.Fatalf("replica listener 2: %v", err)
	}
	replicaListener2.Serve()
	defer replicaListener2.Stop()
	replicaAddr2 := replicaListener2.Addr()

	// Advance primary WAL tail past replica's position (simulates WAL recycling).
	primaryStore.AdvanceWALTail(primaryStore.NextLSN())

	// Create fresh adapter for rebuild scenario.
	exec3 := transport.NewBlockExecutor(primaryStore, replicaAddr2, 3)
	adpt3 := adapter.NewVolumeReplicaAdapter(exec3)

	adpt3.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 3, EndpointVersion: 3,
		DataAddr: replicaAddr2, CtrlAddr: replicaAddr2,
	})

	// Wait for probe + rebuild + session close callback.
	p3 := waitForMode(adpt3, engine.ModeHealthy, 5*time.Second)
	fmt.Printf("Mode: %s  Decision: %s  R=%d S=%d H=%d\n",
		p3.Mode, p3.RecoveryDecision, p3.R, p3.S, p3.H)
	printResult("Demo 3", p3.Mode == engine.ModeHealthy)

	// ==========================================
	// Summary
	// ==========================================
	fmt.Println("\n=== Sparrow Complete ===")
	fmt.Println("Three paths demonstrated:")
	fmt.Println("  1. Healthy (R >= H)")
	fmt.Println("  2. Catch-up (R >= S, R < H) → session close → healthy")
	fmt.Println("  3. Rebuild (R < S) → full base copy → session close → healthy")
}

func waitForMode(adpt *adapter.VolumeReplicaAdapter, want engine.Mode, timeout time.Duration) engine.ReplicaProjection {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p := adpt.Projection()
		if p.Mode == want {
			return p
		}
		time.Sleep(50 * time.Millisecond)
	}
	return adpt.Projection()
}

func printResult(demo string, passed bool) {
	if passed {
		fmt.Printf("  %s: PASS\n", demo)
	} else {
		fmt.Printf("  %s: FAIL\n", demo)
	}
}
