package storage

import (
	"fmt"
	"log"
	"os"
)

// recoveryResult summarizes what RecoverWAL did on one open.
type recoveryResult struct {
	EntriesReplayed int    // valid entries past checkpoint that were replayed
	HighestLSN      uint64 // highest LSN observed across the whole scan
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
		// Superblock claims WAL is empty. Scan the whole region; on a
		// genuinely empty WAL the first byte is zero and CRC fails
		// immediately.
		ranges = append(ranges, scanRange{0, walSize})
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
			if entryType == walEntryWrite {
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
			if entry.LSN <= checkpointLSN {
				pos += entrySize
				continue
			}
			switch entry.Type {
			case walEntryWrite:
				blocks := entry.Length / sb.BlockSize
				for i := uint32(0); i < blocks; i++ {
					dm.put(entry.LBA+uint64(i), pos, entry.LSN, sb.BlockSize)
				}
				result.EntriesReplayed++
			case walEntryTrim:
				blocks := entry.Length / sb.BlockSize
				if blocks == 0 {
					blocks = 1
				}
				for i := uint32(0); i < blocks; i++ {
					dm.put(entry.LBA+uint64(i), pos, entry.LSN, sb.BlockSize)
				}
				result.EntriesReplayed++
			case walEntryBarrier:
				// no data; skip
			}
			if entry.LSN > result.HighestLSN {
				result.HighestLSN = entry.LSN
			}
			pos += entrySize
		}
	}

	if result.HighestLSN > sb.WALHead {
		log.Printf("storage: recovery extended scan found entries past WALHead (%d → %d, %d replayed)",
			sb.WALHead, result.HighestLSN, result.EntriesReplayed)
		sb.WALHead = result.HighestLSN
	}
	return result, nil
}
