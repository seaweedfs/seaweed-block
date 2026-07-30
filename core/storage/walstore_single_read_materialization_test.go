package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"path/filepath"
	"strings"
	"testing"
)

func TestWALStoreSingleReadMaterializationDisabledByDefault(t *testing.T) {
	s := createSingleReadTestStore(t)
	if _, err := s.Write(2, makeBlock(4096, 0x21)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}

	got := s.FlusherInstrumentation()
	if got.WALHeaderReadOps != 1 || got.WALRecordReadOps != 1 ||
		got.MaterializationReadOps != 2 {
		t.Fatalf("default header/record/materialization reads=%d/%d/%d want 1/1/2",
			got.WALHeaderReadOps, got.WALRecordReadOps, got.MaterializationReadOps)
	}
}

func TestWALStoreSingleReadMaterializesOrdinaryAndMultiBlockRecords(t *testing.T) {
	t.Run("ordinary", func(t *testing.T) {
		s := createSingleReadTestStore(t)
		s.enableSingleReadMaterializationForTest(true)
		want := makeBlock(4096, 0x31)
		if _, err := s.Write(3, want); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}

		assertSingleReadCounts(t, s.FlusherInstrumentation(), 1, 0)
		got, err := s.readFromExtent(3)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("ordinary extent data mismatch")
		}
	})

	t.Run("multi block without reuse", func(t *testing.T) {
		s := createSingleReadTestStore(t)
		s.enableSingleReadMaterializationForTest(true)
		s.enableMultiBlockRecordsForTest(true)
		blocks := [][]byte{
			makeBlock(4096, 0x41),
			makeBlock(4096, 0x42),
			makeBlock(4096, 0x43),
		}
		if _, err := s.WriteBatch(5, blocks); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}

		got := s.FlusherInstrumentation()
		assertSingleReadCounts(t, got, 3, 0)
		if got.SnapshotUniqueWALRecords != 1 ||
			got.SnapshotRecordReuseCandidates != 2 {
			t.Fatalf("unique/reuse candidates=%d/%d want 1/2",
				got.SnapshotUniqueWALRecords, got.SnapshotRecordReuseCandidates)
		}
		for index, want := range blocks {
			data, err := s.readFromExtent(uint32(5 + index))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, want) {
				t.Fatalf("multi-block extent %d mismatch", index)
			}
		}
	})
}

func TestWALStoreSingleReadMaterializesLegacyRangeTrim(t *testing.T) {
	const blockSize = 4096
	s := createSingleReadTestStore(t)
	s.enableSingleReadMaterializationForTest(true)
	for lba := uint32(4); lba < 7; lba++ {
		if err := s.WriteExtentDirect(lba, makeBlock(blockSize, 0x55)); err != nil {
			t.Fatal(err)
		}
	}

	trim := &walEntry{LSN: 1, Type: walEntryTrim, LBA: 4, Length: 3 * blockSize}
	walOffset, err := s.wal.append(trim)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint32(0); index < 3; index++ {
		s.dm.putAt(
			4+uint64(index), walOffset, index*blockSize,
			1, blockSize, walEntryHeaderSize,
		)
	}
	s.mu.Lock()
	s.nextLSN = 2
	s.walHead = 1
	s.walTail = 1
	s.mu.Unlock()
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}

	assertSingleReadCounts(t, s.FlusherInstrumentation(), 3, 0)
	for lba := uint32(4); lba < 7; lba++ {
		got, err := s.readFromExtent(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, make([]byte, blockSize)) {
			t.Fatalf("trim extent LBA %d is not zero", lba)
		}
	}
}

func TestWALStoreSingleReadFailsClosedOnInvalidRecord(t *testing.T) {
	tests := []struct {
		name            string
		mutate          func(*testing.T, *WALStore, snapshotEntry, int64)
		wantReadFailure uint64
		wantErr         string
	}{
		{
			name: "zero geometry",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 2, func(entry *dirtyEntry) {
					entry.recordSize = 0
				})
			},
			wantErr: "record size",
		},
		{
			name: "short geometry",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 2, func(dirty *dirtyEntry) {
					dirty.recordSize = entry.RecordSize - 1
				})
			},
			wantErr: "record size",
		},
		{
			name: "long geometry",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 2, func(dirty *dirtyEntry) {
					dirty.recordSize = entry.RecordSize + 1
				})
			},
			wantErr: "record size",
		},
		{
			name: "out of bounds geometry",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 2, func(dirty *dirtyEntry) {
					dirty.recordSize = s.sb.WALSize + 1
				})
			},
			wantErr: "record size",
		},
		{
			name: "short read",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, absoluteOffset int64) {
				if err := s.fd.Truncate(absoluteOffset + int64(entry.RecordSize) - 1); err != nil {
					t.Fatal(err)
				}
			},
			wantReadFailure: 1,
			wantErr:         "read WAL record",
		},
		{
			name: "stale LSN",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 2, func(entry *dirtyEntry) {
					entry.lsn++
				})
			},
			wantErr: "WAL slot mismatch",
		},
		{
			name: "dirty length",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 2, func(entry *dirtyEntry) {
					entry.length = 2048
				})
			},
			wantErr: "WAL slot mismatch",
		},
		{
			name: "dirty data offset",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 2, func(entry *dirtyEntry) {
					entry.dataOffset = 4096
				})
			},
			wantErr: "WAL slot mismatch",
		},
		{
			name: "record LBA",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, absoluteOffset int64) {
				rewriteRecordForTest(t, s, entry, absoluteOffset, func(record []byte) {
					binary.LittleEndian.PutUint64(record[18:26], 3)
				})
			},
			wantErr: "WAL slot mismatch",
		},
		{
			name: "corrupt length",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, absoluteOffset int64) {
				var length [4]byte
				binary.LittleEndian.PutUint32(length[:], 8192)
				if _, err := s.fd.WriteAt(length[:], absoluteOffset+26); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: "record size",
		},
		{
			name: "corrupt payload",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, absoluteOffset int64) {
				var value [1]byte
				if _, err := s.fd.ReadAt(value[:], absoluteOffset+walEntryPrefixSize); err != nil {
					t.Fatal(err)
				}
				value[0] ^= 0xff
				if _, err := s.fd.WriteAt(value[:], absoluteOffset+walEntryPrefixSize); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: "CRC mismatch",
		},
		{
			name: "flags",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, absoluteOffset int64) {
				rewriteRecordForTest(t, s, entry, absoluteOffset, func(record []byte) {
					record[17] = 1
				})
			},
			wantErr: "flags",
		},
		{
			name: "unsupported type",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, absoluteOffset int64) {
				if _, err := s.fd.WriteAt([]byte{walEntryBarrier}, absoluteOffset+16); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: "invalid dirty WAL record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createSingleReadTestStore(t)
			s.enableSingleReadMaterializationForTest(true)
			if _, err := s.Write(2, makeBlock(4096, 0x62)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			entry := snapshotEntriesByLBA(s.dm.snapshot())[2]
			absoluteOffset := int64(s.sb.WALOffset + entry.WALOffset)
			tt.mutate(t, s, entry, absoluteOffset)

			err := s.flusher.flushOnce()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("flush error=%v want substring %q", err, tt.wantErr)
			}
			assertSingleReadFailureState(t, s, 1, tt.wantReadFailure)
		})
	}
}

func TestWALStoreSingleReadRejectsInvalidMultiBlockSemantics(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*testing.T, *WALStore, snapshotEntry, int64)
		wantErr string
	}{
		{
			name: "reserved count",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, absoluteOffset int64) {
				rewriteRecordForTest(t, s, entry, absoluteOffset, func(record []byte) {
					binary.LittleEndian.PutUint64(record[8:16], 4)
				})
			},
			wantErr: "dirty WAL batch",
		},
		{
			name: "record base LBA",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, absoluteOffset int64) {
				rewriteRecordForTest(t, s, entry, absoluteOffset, func(record []byte) {
					binary.LittleEndian.PutUint64(record[18:26], 6)
				})
			},
			wantErr: "WAL slot mismatch",
		},
		{
			name: "dirty data offset",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 6, func(entry *dirtyEntry) {
					entry.dataOffset = 2 * 4096
				})
			},
			wantErr: "WAL slot mismatch",
		},
		{
			name: "dirty length",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 6, func(entry *dirtyEntry) {
					entry.length = 2048
				})
			},
			wantErr: "dirty WAL batch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createSingleReadTestStore(t)
			s.enableSingleReadMaterializationForTest(true)
			s.enableMultiBlockRecordsForTest(true)
			if _, err := s.WriteBatch(5, [][]byte{
				makeBlock(4096, 0x51),
				makeBlock(4096, 0x52),
				makeBlock(4096, 0x53),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			entry := snapshotEntriesByLBA(s.dm.snapshot())[6]
			absoluteOffset := int64(s.sb.WALOffset + entry.WALOffset)
			tt.mutate(t, s, entry, absoluteOffset)

			err := s.flusher.flushOnce()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("flush error=%v want substring %q", err, tt.wantErr)
			}
			assertSingleReadFailureState(t, s, 3, 0)
		})
	}
}

func TestWALStoreSingleReadRejectsInvalidTrimSemantics(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*testing.T, *WALStore, snapshotEntry, int64)
		wantErr string
	}{
		{
			name: "record length",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, absoluteOffset int64) {
				rewriteRecordForTest(t, s, entry, absoluteOffset, func(record []byte) {
					binary.LittleEndian.PutUint32(record[26:30], 3*4096-1)
				})
			},
			wantErr: "dirty WAL trim",
		},
		{
			name: "dirty offset unaligned",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 5, func(entry *dirtyEntry) {
					entry.dataOffset = 1
				})
			},
			wantErr: "dirty WAL trim",
		},
		{
			name: "dirty offset outside range",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 5, func(entry *dirtyEntry) {
					entry.dataOffset = 3 * 4096
				})
			},
			wantErr: "slot mismatch trim",
		},
		{
			name: "dirty length",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 5, func(entry *dirtyEntry) {
					entry.length = 2048
				})
			},
			wantErr: "dirty WAL trim",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createSingleReadTestStore(t)
			s.enableSingleReadMaterializationForTest(true)
			trim := &walEntry{
				LSN: 1, Type: walEntryTrim, LBA: 4, Length: 3 * 4096,
			}
			walOffset, err := s.wal.append(trim)
			if err != nil {
				t.Fatal(err)
			}
			for index := uint32(0); index < 3; index++ {
				s.dm.putAt(
					4+uint64(index), walOffset, index*4096,
					1, 4096, walEntryHeaderSize,
				)
			}
			s.mu.Lock()
			s.nextLSN = 2
			s.walHead = 1
			s.walTail = 1
			s.mu.Unlock()
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			entry := snapshotEntriesByLBA(s.dm.snapshot())[5]
			absoluteOffset := int64(s.sb.WALOffset + entry.WALOffset)
			tt.mutate(t, s, entry, absoluteOffset)

			err = s.flusher.flushOnce()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("flush error=%v want substring %q", err, tt.wantErr)
			}
			assertSingleReadFailureState(t, s, 3, 0)
		})
	}
}

func TestWALStoreSingleReadFailureAtEachPhysicalRecordKeepsWholeSnapshot(t *testing.T) {
	for target := uint32(0); target < 3; target++ {
		t.Run(string(rune('a'+target)), func(t *testing.T) {
			s := createSingleReadTestStore(t)
			s.enableSingleReadMaterializationForTest(true)
			for lba := uint32(0); lba < 3; lba++ {
				if _, err := s.Write(lba, makeBlock(4096, byte(0x70+lba))); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			entry := snapshotEntriesByLBA(s.dm.snapshot())[uint64(target)]
			absoluteOffset := int64(s.sb.WALOffset + entry.WALOffset + walEntryPrefixSize)
			var value [1]byte
			if _, err := s.fd.ReadAt(value[:], absoluteOffset); err != nil {
				t.Fatal(err)
			}
			value[0] ^= 0xff
			if _, err := s.fd.WriteAt(value[:], absoluteOffset); err != nil {
				t.Fatal(err)
			}

			if err := s.flusher.flushOnce(); err == nil {
				t.Fatal("flush succeeded after record corruption")
			}
			assertSingleReadFailureState(t, s, 3, 0)
		})
	}
}

func TestWALStoreSingleReadHandlesReverseGappedAndWrappedRecords(t *testing.T) {
	t.Run("reverse and gapped", func(t *testing.T) {
		s := createSingleReadTestStore(t)
		s.enableSingleReadMaterializationForTest(true)
		for _, lba := range []uint32{13, 8, 2} {
			if _, err := s.Write(lba, makeBlock(4096, byte(0x80+lba))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
		assertSingleReadCounts(t, s.FlusherInstrumentation(), 3, 0)
	})

	t.Run("ring wrap", func(t *testing.T) {
		const (
			blockSize = 4096
			walSize   = 16 * 1024
		)
		s := createWALStoreWithWALSizeForTest(
			t, filepath.Join(t.TempDir(), "store.bin"), 16, blockSize, walSize,
		)
		s.DisableAutoFlushForRecoveryTest()
		t.Cleanup(func() { _ = s.Close() })
		for lba := uint32(0); lba < 3; lba++ {
			if _, err := s.Write(lba, makeBlock(blockSize, byte(0x90+lba))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
		s.enableSingleReadMaterializationForTest(true)
		if _, err := s.Write(7, makeBlock(blockSize, 0x99)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		before := s.FlusherInstrumentation()
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
		after := s.FlusherInstrumentation()
		if after.WALHeaderReadOps != before.WALHeaderReadOps ||
			after.WALRecordReadOps != before.WALRecordReadOps+1 {
			t.Fatalf("wrapped read deltas header=%d record=%d want 0/1",
				after.WALHeaderReadOps-before.WALHeaderReadOps,
				after.WALRecordReadOps-before.WALRecordReadOps)
		}
	})
}

func createSingleReadTestStore(t *testing.T) *WALStore {
	t.Helper()
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func assertSingleReadCounts(
	t *testing.T,
	got FlusherInstrumentationStatus,
	records uint64,
	reuseHits uint64,
) {
	t.Helper()
	if got.ValidatedRecords != records ||
		got.WALHeaderReadOps != 0 ||
		got.WALRecordReadOps != records ||
		got.MaterializationReadOps != records ||
		got.MaterializationRecordReuseHits != reuseHits {
		t.Fatalf("validated/header/record/materialization/reuse=%d/%d/%d/%d/%d want %d/0/%d/%d/%d",
			got.ValidatedRecords, got.WALHeaderReadOps, got.WALRecordReadOps,
			got.MaterializationReadOps, got.MaterializationRecordReuseHits,
			records, records, records, reuseHits)
	}
}

func assertSingleReadFailureState(
	t *testing.T,
	s *WALStore,
	dirtyEntries int,
	readFailures uint64,
) {
	t.Helper()
	if got := s.CheckpointLSN(); got != 0 {
		t.Fatalf("checkpoint after failure=%d want 0", got)
	}
	if got := s.dm.len(); got != dirtyEntries {
		t.Fatalf("dirty entries after failure=%d want %d", got, dirtyEntries)
	}
	s.mu.RLock()
	wantHead := s.nextLSN - 1
	s.mu.RUnlock()
	_, tail, head := s.Boundaries()
	if tail != 1 || head != wantHead {
		t.Fatalf("boundaries after failure tail/head=%d/%d want 1/%d",
			tail, head, wantHead)
	}
	if physicalTail := s.wal.logicalTailValue(); physicalTail != 0 {
		t.Fatalf("physical WAL tail after failure=%d want 0", physicalTail)
	}
	status := s.FlusherInstrumentation()
	if status.CyclesFailed != 1 || status.CyclesSucceeded != 0 ||
		status.WALRecordReadFailures != readFailures {
		t.Fatalf("cycles failed/succeeded and read failures=%d/%d/%d want 1/0/%d",
			status.CyclesFailed, status.CyclesSucceeded,
			status.WALRecordReadFailures, readFailures)
	}
	if readFailures == 0 && status.ValidationFailures != 1 {
		t.Fatalf("validation failures=%d want 1", status.ValidationFailures)
	}
}

func mutateDirtyEntryForTest(
	t *testing.T,
	s *WALStore,
	lba uint64,
	mutate func(*dirtyEntry),
) {
	t.Helper()
	shard := s.dm.shard(lba)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry, ok := shard.m[lba]
	if !ok {
		t.Fatalf("dirty entry LBA %d missing", lba)
	}
	mutate(&entry)
	shard.m[lba] = entry
}

func readRecordForTest(
	t *testing.T,
	s *WALStore,
	entry snapshotEntry,
	absoluteOffset int64,
) []byte {
	t.Helper()
	record := make([]byte, entry.RecordSize)
	if _, err := s.fd.ReadAt(record, absoluteOffset); err != nil {
		t.Fatal(err)
	}
	return record
}

func rewriteRecordForTest(
	t *testing.T,
	s *WALStore,
	entry snapshotEntry,
	absoluteOffset int64,
	mutate func([]byte),
) {
	t.Helper()
	record := readRecordForTest(t, s, entry, absoluteOffset)
	mutate(record)
	binary.LittleEndian.PutUint32(
		record[len(record)-8:],
		crc32.ChecksumIEEE(record[:len(record)-8]),
	)
	if _, err := s.fd.WriteAt(record, absoluteOffset); err != nil {
		t.Fatal(err)
	}
}
