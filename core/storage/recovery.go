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
	// HoleSuspected: a bad entry was followed by a valid later one (issue
	// #51); replay stopped before it. HoleDetail says where.
	HoleSuspected   bool
	HoleDetail      string
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

	type scanRange = recoveryScanRange
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

	// Pass 1 decodes every range and collects the entries; nothing is
	// replayed yet. Replay order is by LSN, not by scan order: a persisted
	// head/tail can predate a wrap, so a range read first may hold newer
	// entries than one read later (issue #51).
	type scanned struct {
		entry      walEntry
		start, end uint64
		lowLSN     uint64 // first LSN above the checkpoint
		highLSN    uint64
		padding    bool
	}
	var items []scanned
	byEnd := map[uint64]int{} // end offset -> index in items
	type badEntry struct {
		ri       int
		pos      uint64
		cause    error
		resyncTo uint64
	}
	var bad []badEntry
	for ri, r := range ranges {
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
				end := pos + uint64(walEntryHeaderSize) + uint64(lengthField)
				items = append(items, scanned{start: pos, end: end, padding: true})
				byEnd[end] = len(items) - 1
				pos = end
				continue
			}
			var payloadLen uint64
			if entryType == walEntryWrite || entryType == walEntryWriteBatch {
				payloadLen = uint64(lengthField)
			}
			entrySize := uint64(walEntryHeaderSize) + payloadLen
			var entry walEntry
			var derr error
			if entrySize > remaining {
				derr = errEntryTruncated
			} else {
				fullBuf := make([]byte, entrySize)
				if _, err := fd.ReadAt(fullBuf, absOff); err != nil {
					return result, fmt.Errorf("storage: recovery read entry at %d: %w", pos, err)
				}
				entry, derr = decodeWALEntry(fullBuf)
			}
			if derr != nil {
				// Torn write, corruption, or trailing zeros: this range
				// ends here. A non-zero bad entry is a possible hole,
				// decided after the scan.
				result.TornEntries++
				if !allZeroBytes(headerBuf) {
					bad = append(bad, badEntry{ri: ri, pos: pos, cause: derr})
				}
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
			entry.Data = nil // replay reads the WAL at its offset, never this copy
			lowLSN := entry.LSN
			if lowLSN <= checkpointLSN {
				lowLSN = checkpointLSN + 1
			}
			items = append(items, scanned{entry: entry, start: pos, end: pos + entrySize, lowLSN: lowLSN, highLSN: highestEntryLSN})
			byEnd[pos+entrySize] = len(items) - 1
			pos += entrySize
		}
	}

	// The earliest hole: a bad entry was appended after the entry that
	// ends where it starts (its circular predecessor, padding skipped), so
	// its LSN is above that entry's. Nothing at or above that bound is
	// replayed; any valid entry there makes it a mid-history hole.
	holeLSN := ^uint64(0)
	var holeAt uint64
	var holeCause error
	for _, b := range bad {
		pred := checkpointLSN
		cur := b.pos
		for guard := 0; guard <= len(items); guard++ {
			i, ok := byEnd[cur]
			if !ok && cur == 0 {
				i, ok = byEnd[walSize]
			}
			if !ok {
				break
			}
			if items[i].padding {
				cur = items[i].start
				continue
			}
			pred = max(pred, items[i].highLSN)
			break
		}
		if pred+1 < holeLSN {
			holeLSN, holeAt, holeCause = pred+1, b.pos, b.cause
		}
	}
	hole := false
	if len(bad) > 0 {
		for _, it := range items {
			if !it.padding && it.highLSN > checkpointLSN && it.highLSN >= holeLSN {
				hole = true
				break
			}
		}
		if !hole {
			// Valid entries the scan did not reach (after the bad one in
			// its range) count too.
			for _, b := range bad {
				found, _, err := laterValidEntry(fd, walOffset, ranges, b.ri, b.pos+1, holeLSN-1, sb.BlockSize)
				if err != nil {
					return result, err
				}
				if found {
					hole = true
					break
				}
			}
		}
	}
	if red := true; red { // RED: no hole bound, no warning
		hole, holeLSN = false, ^uint64(0)
	}
	if hole {
		result.HoleSuspected = true
		result.HoleDetail = fmt.Sprintf("WAL entry at offset=%d fails its check and valid entries from LSN %d on follow it; recovered the entries below LSN %d", holeAt, holeLSN, holeLSN)
		if holeCause != nil {
			result.HoleDetail += " (" + holeCause.Error() + ")"
		}
		log.Printf("storage: recovery %s: %s", WarningWALHoleSuspected, result.HoleDetail)
	}

	// Pass 2: replay every entry above the checkpoint and below the hole.
	for _, it := range items {
		if it.padding || it.highLSN <= checkpointLSN || it.highLSN >= holeLSN {
			continue
		}
		entry, pos := it.entry, it.start
		retainedRecords = append(retainedRecords, retainedRecord{
			firstLSN: it.lowLSN,
			start:    it.start,
			end:      it.end,
		})
		switch entry.Type {
		case walEntryWrite:
			blocks := entry.Length / sb.BlockSize
			for i := uint32(0); i < blocks; i++ {
				dm.putAt(entry.LBA+uint64(i), pos, i*sb.BlockSize, entry.LSN, sb.BlockSize)
			}
			result.EntriesReplayed++
		case walEntryWriteBatch:
			blocks := uint32(entry.Reserved)
			for i := uint32(0); i < blocks; i++ {
				lsn := entry.LSN + uint64(i)
				if lsn <= checkpointLSN {
					continue
				}
				dm.putAt(entry.LBA+uint64(i), pos, i*sb.BlockSize, lsn, sb.BlockSize)
			}
			result.EntriesReplayed++
		case walEntryTrim:
			blocks := entry.Length / sb.BlockSize
			if blocks == 0 {
				blocks = 1
			}
			for i := uint32(0); i < blocks; i++ {
				dm.putAt(entry.LBA+uint64(i), pos, i*sb.BlockSize, entry.LSN, sb.BlockSize)
			}
			result.EntriesReplayed++
		case walEntryBarrier:
			// no data; skip
		}
		if it.highLSN > result.HighestLSN {
			result.HighestLSN = it.highLSN
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

// recoveryScanRange is a physical [start, end) span of the WAL region.
type recoveryScanRange struct {
	start, end uint64
}

func allZeroBytes(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// laterValidEntry looks, from pos in ranges[ri] through the later ranges,
// for an offset holding a valid WAL entry (it decodes, CRC included) whose
// LSN is above minLSN. It runs only after a bad entry, so a linear resync
// over the remaining WAL is acceptable; a cheap header prefilter keeps the
// full decodes to plausible candidates.
func laterValidEntry(fd *os.File, walOffset uint64, ranges []recoveryScanRange, ri int, pos, minLSN uint64, blockSize uint32) (bool, uint64, error) {
	const window = 1 << 20
	hdr := uint64(walEntryHeaderSize)
	for i := ri; i < len(ranges); i++ {
		start, end := ranges[i].start, ranges[i].end
		if i == ri {
			start = pos
		}
		for p := start; p+hdr <= end; p += window {
			n := min(window+hdr, end-p)
			buf := make([]byte, n)
			if _, err := fd.ReadAt(buf, int64(walOffset+p)); err != nil {
				return false, 0, fmt.Errorf("storage: recovery resync read at %d: %w", p, err)
			}
			for q := uint64(0); q+hdr <= n && q < window; q++ {
				size, ok := plausibleEntryHeader(buf[q:q+hdr], minLSN, end-(p+q), blockSize)
				if !ok {
					continue
				}
				full := make([]byte, size)
				if _, err := fd.ReadAt(full, int64(walOffset+p+q)); err != nil {
					return false, 0, fmt.Errorf("storage: recovery resync read entry at %d: %w", p+q, err)
				}
				if _, err := decodeWALEntry(full); err == nil {
					return true, p + q, nil
				}
			}
		}
	}
	return false, 0, nil
}

// plausibleEntryHeader is the resync prefilter: a known record type, an
// LSN above minLSN, and a length that fits in what remains.
func plausibleEntryHeader(h []byte, minLSN, remaining uint64, blockSize uint32) (uint64, bool) {
	lsn := binary.LittleEndian.Uint64(h[0:8])
	if lsn <= minLSN {
		return 0, false
	}
	length := uint64(parseLengthFromHeader(h))
	size := uint64(walEntryHeaderSize)
	switch h[16] {
	case walEntryWrite, walEntryWriteBatch:
		if length == 0 || blockSize == 0 || length%uint64(blockSize) != 0 {
			return 0, false
		}
		size += length
	case walEntryTrim, walEntryBarrier:
	default:
		return 0, false
	}
	if size > remaining {
		return 0, false
	}
	return size, true
}
