package storage

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sort"
)

// recoveryResult summarizes what RecoverWAL did on one open.
type recoveryResult struct {
	EntriesReplayed int    // valid entries past checkpoint that were replayed
	HighestLSN      uint64 // highest LSN observed across the whole scan
	WALHead         uint64 // reconstructed logical byte head of retained WAL
	WALTail         uint64 // reconstructed logical byte tail of retained WAL
	TornEntries     int    // entries discarded due to CRC failure or truncation
	DefensiveScan   bool   // true when the superblock was empty and we scanned the whole region
}

// recoverWAL scans the WAL region and replays any entries that have
// not yet been checkpointed into the extent. Replayed entries are
// inserted into the dirty map so subsequent reads can find them and
// the flusher can apply them to the extent.
//
// Recovery is deliberately defensive: even when the superblock says
// WAL is empty, the scanner walks the whole region looking for valid
// entries past the recorded head. CRC validation is the stop signal
// — the first record that fails CRC marks where torn writes begin
// and scanning halts.
//
// On a clean shutdown the first byte past head is zero or padding,
// CRC fails immediately, and the defensive scan adds zero overhead.
//
// Side effects: if the extended scan finds entries past the
// superblock's recorded head, sb.WALHead is bumped so the writer
// resumes after them.
func recoverWAL(fd *os.File, sb *superblock, dm *dirtyMap) (recoveryResult, error) {
	result := recoveryResult{}
	type retainedRecord struct {
		firstLSN uint64
		start    uint64
		end      uint64
	}
	var retainedRecords []retainedRecord

	logicalHead := sb.WALHead
	logicalTail := sb.WALTail
	walOffset := sb.WALOffset
	walSize := sb.WALSize
	checkpointLSN := sb.WALCheckpointLSN

	type scanRange struct {
		start, end uint64 // physical positions within WAL
	}
	var ranges []scanRange

	if logicalHead == logicalTail {
		// A pre-boundary-persistence crash can leave a wrapped retained
		// window while the superblock still says empty. Find the high
		// physical run from record footers, then scan high and low as
		// disjoint ranges so neither side is hidden behind the gap.
		highStart, found := findDefensiveHighRunStart(fd, walOffset, walSize)
		if found && highStart > 0 {
			ranges = append(
				ranges,
				scanRange{highStart, walSize},
				scanRange{0, highStart},
			)
		} else {
			ranges = append(ranges, scanRange{0, walSize})
		}
		result.DefensiveScan = true
		log.Printf("storage: recovery defensive scan (head==tail=%d checkpoint=%d)",
			logicalHead, checkpointLSN)
	} else {
		physHead := logicalHead % walSize
		physTail := logicalTail % walSize
		if physHead > physTail {
			ranges = append(ranges, scanRange{physTail, physHead})
			ranges = append(ranges, scanRange{physHead, walSize})
			if physTail > 0 {
				ranges = append(ranges, scanRange{0, physTail})
			}
		} else {
			ranges = append(ranges, scanRange{physTail, walSize})
			if physHead > 0 {
				ranges = append(ranges, scanRange{0, physHead})
			}
			if physHead < physTail {
				ranges = append(ranges, scanRange{physHead, physTail})
			}
		}
	}

	for _, r := range ranges {
		pos := r.start
		for pos < r.end {
			remaining := r.end - pos
			if remaining < uint64(walEntryHeaderSize) {
				break
			}
			headerBuf := make([]byte, walEntryHeaderSize)
			absOff := int64(walOffset + pos)
			if _, err := fd.ReadAt(headerBuf, absOff); err != nil {
				return result, fmt.Errorf("storage: recovery read header at %d: %w", pos, err)
			}
			entryType := headerBuf[16]
			lengthField := parseLengthFromHeader(headerBuf)

			if entryType == walEntryPadding {
				pos += uint64(walEntryHeaderSize) + uint64(lengthField)
				continue
			}
			var payloadLen uint64
			if entryType == walEntryWrite || entryType == walEntryWriteBatch {
				payloadLen = uint64(lengthField)
			}
			entrySize := uint64(walEntryHeaderSize) + payloadLen
			if entrySize > remaining {
				result.TornEntries++
				break
			}
			fullBuf := make([]byte, entrySize)
			if _, err := fd.ReadAt(fullBuf, absOff); err != nil {
				return result, fmt.Errorf("storage: recovery read entry at %d: %w", pos, err)
			}
			entry, err := decodeWALEntry(fullBuf)
			if err != nil {
				// Torn write or trailing zeros — stop scanning this range.
				result.TornEntries++
				break
			}
			highestEntryLSN := entry.LSN
			if entry.Type == walEntryWriteBatch {
				maxBlocks := uint64(^uint32(0)) / uint64(sb.BlockSize)
				if entry.Reserved == 0 ||
					entry.Reserved > maxBlocks ||
					entry.Reserved-1 > ^uint64(0)-entry.LSN ||
					uint64(entry.Length) != entry.Reserved*uint64(sb.BlockSize) {
					return result, NewWALIntegrityFailure(
						nil,
						fmt.Sprintf(
							"invalid walstore batch at offset=%d LSN=%d reserved=%d length=%d block_size=%d",
							pos, entry.LSN, entry.Reserved, entry.Length, sb.BlockSize,
						),
					)
				}
				highestEntryLSN = entry.LSN + entry.Reserved - 1
			}
			if highestEntryLSN <= checkpointLSN {
				pos += entrySize
				continue
			}
			firstRetainedLSN := entry.LSN
			if firstRetainedLSN <= checkpointLSN {
				firstRetainedLSN = checkpointLSN + 1
			}
			retainedRecords = append(retainedRecords, retainedRecord{
				firstLSN: firstRetainedLSN,
				start:    pos,
				end:      pos + entrySize,
			})
			switch entry.Type {
			case walEntryWrite:
				blocks := entry.Length / sb.BlockSize
				for i := uint32(0); i < blocks; i++ {
					dm.putAt(
						entry.LBA+uint64(i), pos, i*sb.BlockSize,
						entry.LSN, sb.BlockSize,
					)
				}
				result.EntriesReplayed++
			case walEntryWriteBatch:
				blocks := uint32(entry.Reserved)
				for i := uint32(0); i < blocks; i++ {
					lsn := entry.LSN + uint64(i)
					if lsn <= checkpointLSN {
						continue
					}
					dm.putAt(
						entry.LBA+uint64(i), pos, i*sb.BlockSize,
						lsn, sb.BlockSize,
					)
				}
				result.EntriesReplayed++
			case walEntryTrim:
				blocks := entry.Length / sb.BlockSize
				if blocks == 0 {
					blocks = 1
				}
				for i := uint32(0); i < blocks; i++ {
					dm.putAt(
						entry.LBA+uint64(i), pos, i*sb.BlockSize,
						entry.LSN, sb.BlockSize,
					)
				}
				result.EntriesReplayed++
			case walEntryBarrier:
				// no data; skip
			}
			if highestEntryLSN > result.HighestLSN {
				result.HighestLSN = highestEntryLSN
			}
			pos += entrySize
		}
	}

	sort.Slice(retainedRecords, func(left, right int) bool {
		return retainedRecords[left].firstLSN < retainedRecords[right].firstLSN
	})
	if len(retainedRecords) > 0 {
		result.WALTail = retainedRecords[0].start
		result.WALHead = retainedRecords[0].end
		previousStart := retainedRecords[0].start
		var wrapBase uint64
		for _, record := range retainedRecords[1:] {
			if record.start < previousStart {
				wrapBase += walSize
			}
			logicalStart := wrapBase + record.start
			logicalEnd := wrapBase + record.end
			if logicalStart < result.WALHead || logicalEnd-logicalStart > walSize {
				return result, NewWALIntegrityFailure(
					nil,
					fmt.Sprintf(
						"invalid walstore recovery order at LSN=%d offset=%d",
						record.firstLSN, record.start,
					),
				)
			}
			result.WALHead = logicalEnd
			previousStart = record.start
		}
		if result.WALHead < result.WALTail ||
			result.WALHead-result.WALTail > walSize {
			return result, NewWALIntegrityFailure(
				nil,
				fmt.Sprintf(
					"invalid walstore recovery bounds tail=%d head=%d size=%d",
					result.WALTail, result.WALHead, walSize,
				),
			)
		}
	}
	if result.DefensiveScan && result.EntriesReplayed > 0 {
		log.Printf(
			"storage: recovery reconstructed WAL bytes tail=%d head=%d frontier=%d (%d replayed)",
			result.WALTail, result.WALHead, result.HighestLSN, result.EntriesReplayed,
		)
	}
	return result, nil
}

func findDefensiveHighRunStart(
	fd *os.File,
	walOffset uint64,
	walSize uint64,
) (uint64, bool) {
	maxGap := uint64(walEntryHeaderSize - 1)
	if maxGap >= walSize {
		maxGap = walSize - 1
	}
	for gap := uint64(0); gap <= maxGap; gap++ {
		cursor := walSize - gap
		start, entry, ok := previousValidWALRecord(fd, walOffset, cursor)
		if !ok {
			continue
		}
		earliest := start
		foundData := entry.Type != walEntryPadding
		cursor = start
		for cursor > 0 {
			previousStart, previous, ok := previousValidWALRecord(
				fd, walOffset, cursor,
			)
			if !ok {
				break
			}
			earliest = previousStart
			if previous.Type != walEntryPadding {
				foundData = true
			}
			cursor = previousStart
		}
		if foundData {
			return earliest, true
		}
	}
	return 0, false
}

func previousValidWALRecord(
	fd *os.File,
	walOffset uint64,
	end uint64,
) (uint64, walEntry, bool) {
	if end < 4 {
		return 0, walEntry{}, false
	}
	var sizeBytes [4]byte
	if _, err := fd.ReadAt(sizeBytes[:], int64(walOffset+end-4)); err != nil {
		return 0, walEntry{}, false
	}
	size := uint64(binary.LittleEndian.Uint32(sizeBytes[:]))
	if size < uint64(walEntryHeaderSize) || size > end {
		return 0, walEntry{}, false
	}
	start := end - size
	record := make([]byte, size)
	if _, err := fd.ReadAt(record, int64(walOffset+start)); err != nil {
		return 0, walEntry{}, false
	}
	entry, err := decodeWALEntry(record)
	if err != nil {
		return 0, walEntry{}, false
	}
	switch entry.Type {
	case walEntryWrite, walEntryWriteBatch, walEntryTrim,
		walEntryBarrier, walEntryPadding:
		return start, entry, true
	default:
		return 0, walEntry{}, false
	}
}
