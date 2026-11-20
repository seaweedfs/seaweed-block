package storage

import (
	"strings"
)

func walOffsetsByLBA(s *WALStore) map[uint64]uint64 {
	offs := map[uint64]uint64{}
	for _, e := range s.dm.snapshot() {
		offs[e.LBA] = e.WALOffset
	}
	return offs
}

func holeWarning(s *WALStore) bool {
	for _, w := range s.RecoveryWarnings() {
		if strings.HasPrefix(w, WarningWALHoleSuspected+":") {
			return true
		}
	}
	return false
}
