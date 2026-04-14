package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// runPersistDemo is the single-node persistence demonstration:
// write a known pattern, Sync, Close, reopen, Recover, read it back.
//
// What this exercises:
//   - the LogicalStorage interface admits a real on-disk backend
//   - acked data (covered by a successful Sync) survives Close +
//     Reopen + Recover round trip
//
// Crash consistency for acked writes is verified separately by
// TestWALStore_AckedWritesSurviveSimulatedCrash in the storage
// package, which bypasses Close to simulate a process kill.
//
// What this DOES NOT prove (intentional scope boundary):
//   - distributed durability across nodes
//
// Exit codes follow the sparrow contract:
//
//	0  data round-tripped intact
//	1  data did not survive restart (the actual failure mode for this demo)
//	2  usage / flag error
//	3  runtime error (file create/open/read/write/sync failure)
func runPersistDemo(opts options) int {
	dir := opts.persistDir
	if dir == "" {
		fmt.Fprintln(os.Stderr, "sparrow: --persist-demo requires --persist-dir DIR")
		return 2
	}
	path := filepath.Join(dir, "sparrow-persist.dat")

	// Demo is meant to be reproducible: every invocation creates a
	// fresh file. If a previous demo left bytes behind, remove them
	// before Round 1 so CreateWALStore's O_EXCL doesn't collide.
	// We only remove the demo file we know we own — never the dir.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "sparrow: persist demo: cannot remove prior file %s: %v\n", path, err)
		return 3
	}

	const (
		numBlocks = 16
		blockSize = 4096
		nWrites   = 4
	)

	// Round 1: create, write, sync, close.
	r1, err := persistRound1(path, numBlocks, blockSize, nWrites)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sparrow: persist round1: %v\n", err)
		return 3
	}

	// Round 2: open, recover, read back, compare.
	r2, err := persistRound2(path, nWrites)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sparrow: persist round2: %v\n", err)
		return 3
	}

	report := persistReport{
		Path:           path,
		NumBlocks:      numBlocks,
		BlockSize:      blockSize,
		NWrites:        nWrites,
		SyncedBefore:   r1.synced,
		RecoveredAfter: r2.recovered,
		AllMatched:     r2.allMatched,
		FailedLBAs:     r2.failedLBAs,
	}

	if opts.json {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
	} else {
		fmt.Println("=== Single-Node Persistence Demo (WAL+extent backend) ===")
		fmt.Println()
		fmt.Printf("  path:            %s\n", report.Path)
		fmt.Printf("  blocks written:  %d (block size %d)\n", report.NWrites, report.BlockSize)
		fmt.Printf("  synced LSN:      %d\n", report.SyncedBefore)
		fmt.Printf("  recovered LSN:   %d\n", report.RecoveredAfter)
		fmt.Println()
		if report.AllMatched {
			fmt.Println("  result:          PASS — all blocks survived restart")
		} else {
			fmt.Printf("  result:          FAIL — LBAs %v did not survive\n", report.FailedLBAs)
		}
		fmt.Println()
		fmt.Println("Scope:")
		fmt.Println("  - acked writes survive crash (verified by simulated-kill test in storage package)")
		fmt.Println("  - single-node only; distributed durability NOT in scope")
	}

	if !report.AllMatched {
		return 1
	}
	return 0
}

type persistReport struct {
	Path           string   `json:"path"`
	NumBlocks      uint32   `json:"num_blocks"`
	BlockSize      int      `json:"block_size"`
	NWrites        int      `json:"n_writes"`
	SyncedBefore   uint64   `json:"synced_before_close"`
	RecoveredAfter uint64   `json:"recovered_after_open"`
	AllMatched     bool     `json:"all_matched"`
	FailedLBAs     []uint32 `json:"failed_lbas,omitempty"`
}

type round1Result struct {
	synced uint64
}

func persistRound1(path string, numBlocks uint32, blockSize, nWrites int) (round1Result, error) {
	s, err := storage.CreateWALStore(path, numBlocks, blockSize)
	if err != nil {
		return round1Result{}, fmt.Errorf("create: %w", err)
	}
	for i := 0; i < nWrites; i++ {
		data := makePattern(blockSize, byte(0xA0+i))
		if _, err := s.Write(uint32(i), data); err != nil {
			_ = s.Close()
			return round1Result{}, fmt.Errorf("write LBA %d: %w", i, err)
		}
	}
	synced, err := s.Sync()
	if err != nil {
		_ = s.Close()
		return round1Result{}, fmt.Errorf("sync: %w", err)
	}
	if err := s.Close(); err != nil {
		return round1Result{}, fmt.Errorf("close: %w", err)
	}
	return round1Result{synced: synced}, nil
}

type round2Result struct {
	recovered  uint64
	allMatched bool
	failedLBAs []uint32
}

func persistRound2(path string, nWrites int) (round2Result, error) {
	s, err := storage.OpenWALStore(path)
	if err != nil {
		return round2Result{}, fmt.Errorf("open: %w", err)
	}
	defer s.Close()
	recovered, err := s.Recover()
	if err != nil {
		return round2Result{}, fmt.Errorf("recover: %w", err)
	}

	out := round2Result{recovered: recovered, allMatched: true}
	for i := 0; i < nWrites; i++ {
		got, err := s.Read(uint32(i))
		if err != nil {
			return round2Result{}, fmt.Errorf("read LBA %d: %w", i, err)
		}
		want := makePattern(s.BlockSize(), byte(0xA0+i))
		if !bytes.Equal(got, want) {
			out.allMatched = false
			out.failedLBAs = append(out.failedLBAs, uint32(i))
		}
	}
	return out, nil
}

func makePattern(blockSize int, fill byte) []byte {
	b := make([]byte, blockSize)
	for i := range b {
		b[i] = fill
	}
	return b
}
