package storage

import (
	"bytes"
	"encoding/binary"

	"hash/crc32"

	"testing"
)

func crashRecoveryHardeningStore(t *testing.T, s *WALStore) {
	t.Helper()
	s.committer.Stop()
	if s.v3 != nil && s.v3.ownedMap != nil {
		if err := s.v3.ownedMap.Close(); err != nil {
			t.Fatal(err)
		}
		s.v3.ownedMap = nil
	}
	if err := s.fd.Close(); err != nil {
		t.Fatal(err)
	}
}

func recoverySnapshotEntriesByLBA(entries []snapshotEntry) map[uint64]snapshotEntry {
	result := make(map[uint64]snapshotEntry, len(entries))
	for _, entry := range entries {
		result[entry.LBA] = entry
	}
	return result
}

func rewriteRecoveryRecordForTest(
	t *testing.T,
	s *WALStore,
	recordSize uint64,
	absoluteOffset int64,
	mutate func([]byte),
) {
	t.Helper()
	record := make([]byte, recordSize)
	if _, err := s.fd.ReadAt(record, absoluteOffset); err != nil {
		t.Fatal(err)
	}
	mutate(record)
	binary.LittleEndian.PutUint32(
		record[len(record)-8:],
		crc32.ChecksumIEEE(record[:len(record)-8]),
	)
	if _, err := s.fd.WriteAt(record, absoluteOffset); err != nil {
		t.Fatal(err)
	}
}

func assertRecoveryHardeningBlock(t *testing.T, s *WALStore, lba uint32, want []byte) {
	t.Helper()
	got, err := s.Read(lba)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("current LBA %d mismatch", lba)
	}
}
