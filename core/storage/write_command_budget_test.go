package storage

import (
	"errors"
	"testing"
	"time"
)

func TestWALStoreV3WriteBudgetIncludesPostAdmissionWait(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "batch"}[batch], func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			s.admissionTimeout = 40 * time.Millisecond
			// Admission is healthy. A delayed admitted writer must not start a fresh
			// budget after waiting to reach its mutation boundary.
			s.testAfterAdmit = func() { time.Sleep(60 * time.Millisecond) }
			before := s.NextLSN()
			var err error
			if batch {
				_, err = s.WriteBatch(0, [][]byte{blk(1), blk(2)})
			} else {
				_, err = s.Write(0, blk(1))
			}
			if !errors.Is(err, ErrWALPressureTimeout) {
				t.Fatalf("expired command mutated: %v", err)
			}
			if s.NextLSN() != before || s.dm.len() != 0 {
				t.Fatal("expired write changed WAL/dirty state")
			}
			s.testAfterAdmit = nil
			if _, err := s.Write(0, blk(2)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
