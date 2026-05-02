package recovery

// C3 — BASE ∥ WAL wall-clock overlap in Sender.Run (§6.8 #6 / P6 /
// G3). Base lane and WAL pump run as concurrent goroutines. Both
// share writeMu (via C1) so frame integrity holds; mutex-bounded
// interleaving on the wire is correct.
//
// Test: receiver observes interleaved frameBaseBlock and frameWALEntry
// frames — proves the two goroutines made progress concurrently
// (not strictly base-then-WAL).
//
// We use the bridging path (NewSenderWithBacklogRelay) for this test
// because it ships frameWALEntry on the same conn — easy to observe
// interleave from a single capture buffer.

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// TestC3_BaseWalParallel_FramesInterleave — receiver-side capture
// shows base + WAL frames arriving interleaved (not strictly
// sequential). Pins §6.8 #6 BASE ∥ WAL overlap.
//
// Setup: enough base blocks (numBlocks * blockSize ≈ 64KB) and
// substrate WAL entries that both goroutines have meaningful work
// to do. The frames are decoded and we assert the WAL frame index
// in the sequence is < (last base frame index) — i.e., a WAL frame
// appeared BEFORE the last base frame.
func TestC3_BaseWalParallel_FramesInterleave(t *testing.T) {
	const numBlocks = 64
	const blockSize = 1024
	const seedN = 30 // backlog WAL entries

	primary := memorywal.NewStore(numBlocks, blockSize)
	for lba := uint32(0); lba < seedN; lba++ {
		_, _ = primary.Write(lba, bytes.Repeat([]byte{byte(lba)}, blockSize))
	}
	_, _ = primary.Sync()

	// Custom listener: capture all bytes the sender writes; parse
	// frames; record (type, lsn) sequence.
	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	type frameObs struct {
		ftype frameType
		// lsn captured for frameWALEntry only (else 0).
		lsn uint64
	}
	var (
		framesMu sync.Mutex
		frames   []frameObs
		readDone = make(chan struct{})
	)

	go func() {
		defer close(readDone)
		for {
			ft, payload, err := readFrame(replicaConn)
			if err != nil {
				return
			}
			f := frameObs{ftype: ft}
			if ft == frameWALEntry {
				if _, _, lsn, _, decErr := decodeWALEntry(payload); decErr == nil {
					f.lsn = lsn
				}
			}
			framesMu.Lock()
			frames = append(frames, f)
			framesMu.Unlock()
			if ft == frameBarrierReq {
				// Reply with BarrierResp so sender unblocks and Run returns.
				_ = writeFrame(replicaConn, frameBarrierResp, encodeBarrierResp(uint64(seedN)))
				return
			}
		}
	}()

	coord := NewPeerShipCoordinator()
	if err := coord.StartSessionLegacyBand("r1", 7, 0, uint64(seedN)); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	sender := NewSenderWithBacklogRelay(primary, coord, primaryConn, "r1")

	runDone := make(chan struct{})
	var runErr error
	go func() {
		defer close(runDone)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, runErr = sender.Run(ctx, 7, 0, uint64(seedN))
	}()

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Sender.Run did not finish in 5s")
	}
	<-readDone

	if runErr != nil {
		t.Logf("sender.Run returned: %v (acceptable for this test if frames were observed)", runErr)
	}

	framesMu.Lock()
	defer framesMu.Unlock()

	// Must have observed: SessionStart, several BaseBlock, BaseDone,
	// several frameWALEntry, BarrierReq.
	var (
		firstWALIdx = -1
		lastBaseIdx = -1
	)
	for i, f := range frames {
		if f.ftype == frameWALEntry && firstWALIdx == -1 {
			firstWALIdx = i
		}
		if f.ftype == frameBaseBlock {
			lastBaseIdx = i
		}
	}

	if firstWALIdx == -1 {
		t.Fatalf("no frameWALEntry observed (frames=%d)", len(frames))
	}
	if lastBaseIdx == -1 {
		t.Fatalf("no frameBaseBlock observed (frames=%d)", len(frames))
	}

	// PROOF OF OVERLAP: at least one WAL frame appears BEFORE the
	// last base frame. If base ran fully before WAL (sequential),
	// firstWALIdx > lastBaseIdx. Interleave means firstWALIdx <
	// lastBaseIdx for at least the first WAL emission.
	if firstWALIdx > lastBaseIdx {
		t.Errorf("BASE ∥ WAL overlap NOT observed: firstWAL=%d > lastBase=%d (sequential, §6.8 #6 violated)",
			firstWALIdx, lastBaseIdx)
	} else {
		t.Logf("BASE ∥ WAL overlap confirmed: firstWAL=%d < lastBase=%d (frames=%d)",
			firstWALIdx, lastBaseIdx, len(frames))
	}
}
