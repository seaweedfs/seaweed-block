package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWALStore_MultiBlockProfile_RecordCountReduction(t *testing.T) {
	const (
		numBlocks      = 4096
		blockSize      = 4096
		batches        = 128
		blocksPerBatch = 16
	)

	type profile struct {
		instr WriteInstrumentationStatus
	}

	run := func(t *testing.T, multiblock bool) profile {
		t.Helper()
		path := filepath.Join(t.TempDir(), "store.bin")
		s, err := CreateWALStore(path, numBlocks, blockSize)
		if err != nil {
			t.Fatalf("CreateWALStore: %v", err)
		}
		defer s.Close()
		s.flusher.Stop()
		s.enableMultiBlockRecordsForTest(multiblock)

		var lastBlocks [][]byte
		var lastStart uint32
		for b := 0; b < batches; b++ {
			start := uint32(b * blocksPerBatch)
			blocks := make([][]byte, blocksPerBatch)
			for i := range blocks {
				blocks[i] = makeBlock(blockSize, byte((b+i)%251+1))
			}
			if _, err := s.WriteBatch(start, blocks); err != nil {
				t.Fatalf("WriteBatch multiblock=%v batch=%d: %v", multiblock, b, err)
			}
			lastBlocks = blocks
			lastStart = start
		}
		for i, want := range lastBlocks {
			got, err := s.Read(lastStart + uint32(i))
			if err != nil {
				t.Fatalf("Read multiblock=%v block=%d: %v", multiblock, i, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("Read multiblock=%v block=%d mismatch", multiblock, i)
			}
		}
		return profile{instr: s.WriteInstrumentation()}
	}

	single := run(t, false)
	multi := run(t, true)

	t.Logf("phase149_single_block_wal_encode_ops=%d", single.instr.WALEncodeOps)
	t.Logf("phase149_multiblock_wal_encode_ops=%d", multi.instr.WALEncodeOps)
	t.Logf("phase149_single_block_wal_append_ops=%d", single.instr.WALAppendOps)
	t.Logf("phase149_multiblock_wal_append_ops=%d", multi.instr.WALAppendOps)
	t.Logf("phase149_single_block_wal_writeat_calls=%d", single.instr.WALAppendWriteAtCalls)
	t.Logf("phase149_multiblock_wal_writeat_calls=%d", multi.instr.WALAppendWriteAtCalls)

	if single.instr.WALEncodeOps == 0 || multi.instr.WALEncodeOps == 0 {
		t.Fatalf("missing encode ops: single=%d multi=%d", single.instr.WALEncodeOps, multi.instr.WALEncodeOps)
	}
	if multi.instr.WALEncodeOps >= single.instr.WALEncodeOps {
		t.Fatalf("multi-block encode ops=%d, want < single=%d", multi.instr.WALEncodeOps, single.instr.WALEncodeOps)
	}
	expectedMulti := uint64(batches)
	if multi.instr.WALEncodeOps != expectedMulti {
		t.Fatalf("multi-block encode ops=%d want %d", multi.instr.WALEncodeOps, expectedMulti)
	}
	expectedSingle := uint64(batches * blocksPerBatch)
	if single.instr.WALEncodeOps != expectedSingle {
		t.Fatalf("single-block encode ops=%d want %d", single.instr.WALEncodeOps, expectedSingle)
	}
}
