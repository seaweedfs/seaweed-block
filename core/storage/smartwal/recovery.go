package smartwal

import (
	"fmt"
	"hash/crc32"
	"log"
	"os"
)

// runRecovery rebuilds the LSN frontier from a smartwal store's ring
// buffer and verifies extent contents. Returns the highest LSN whose
// extent block was verified to match its metadata record's CRC.
//
// Algorithm (deliberately last-writer-wins per LBA):
//
//  1. Scan the ring; collect every record whose magic + record-CRC
//     check out. Sort ascending by LSN.
//  2. Build a map LBA → highest-LSN record. Older records for the
//     same LBA are dominated and ignored.
//  3. For each surviving record:
//       - Trim record: counts as recovered (its CRC32 covers a zero
//         block; if the extent matches that, the trim is durable).
//       - Write record: read the extent block, compute its CRC32,
//         compare to record.DataCRC32. If they match, the write is
//         durable and counts toward the recovered frontier. If they
//         do NOT match, this is a torn pre-Sync write — skip the
//         record so the LBA reads as whatever the extent currently
//         holds (which is either the pre-write bytes, if the rollback
//         path ran, or torn bytes if it didn't; either way the
//         metadata no longer claims authority over them).
//  4. The recovered frontier is the highest LSN among VALIDATED
//     records, not just the highest LSN seen in the ring. A record
//     whose CRC didn't verify cannot advance the frontier.
//
// What this does NOT attempt:
//   - rollback to a known-good prior version (V2's "save old data
//     before write" runs at write time, not recovery time)
//   - per-block CRC stored alongside extent (extent stays raw blocks)
//   - cross-checking against any external snapshot
func runRecovery(r *ring, fd *os.File, extentBase int64, blockSize int) (uint64, error) {
	records, err := r.scanValid()
	if err != nil {
		return 0, fmt.Errorf("smartwal: recovery scan: %w", err)
	}
	if len(records) == 0 {
		log.Printf("smartwal: recovery: ring empty; nothing to verify")
		return 0, nil
	}

	// Last-writer-wins map. Records arrive sorted by LSN ascending,
	// so the final value in the map is naturally the highest LSN.
	lastWrite := make(map[uint32]record, len(records))
	for _, rec := range records {
		if cur, ok := lastWrite[rec.LBA]; !ok || rec.LSN > cur.LSN {
			lastWrite[rec.LBA] = rec
		}
	}

	var recovered, torn int
	var highestVerifiedLSN uint64
	for _, rec := range lastWrite {
		if rec.Flags&flagTrim != 0 {
			// Trim: extent should be all zeros at this LBA. Verify by
			// checking the CRC against zero bytes — same path as a
			// regular Write check below.
			data := make([]byte, blockSize)
			off := extentBase + int64(rec.LBA)*int64(blockSize)
			if _, err := fd.ReadAt(data, off); err != nil {
				return 0, fmt.Errorf("smartwal: recovery read trim LBA %d: %w", rec.LBA, err)
			}
			if crc32.ChecksumIEEE(data) != rec.DataCRC32 {
				log.Printf("smartwal: recovery trim CRC mismatch LSN=%d LBA=%d — skipping",
					rec.LSN, rec.LBA)
				torn++
				continue
			}
			recovered++
			if rec.LSN > highestVerifiedLSN {
				highestVerifiedLSN = rec.LSN
			}
			continue
		}

		data := make([]byte, blockSize)
		off := extentBase + int64(rec.LBA)*int64(blockSize)
		if _, err := fd.ReadAt(data, off); err != nil {
			return 0, fmt.Errorf("smartwal: recovery read LBA %d: %w", rec.LBA, err)
		}
		actual := crc32.ChecksumIEEE(data)
		if actual != rec.DataCRC32 {
			log.Printf("smartwal: recovery CRC mismatch LSN=%d LBA=%d expected=%08x actual=%08x — skipping",
				rec.LSN, rec.LBA, rec.DataCRC32, actual)
			torn++
			continue
		}
		recovered++
		if rec.LSN > highestVerifiedLSN {
			highestVerifiedLSN = rec.LSN
		}
	}

	log.Printf("smartwal: recovery: %d LBAs verified, %d torn, frontier=%d",
		recovered, torn, highestVerifiedLSN)
	return highestVerifiedLSN, nil
}
