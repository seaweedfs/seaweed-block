package storage

import "testing"

func TestFlusherV3ProfileCountsRealRecordStages(t *testing.T) {
	for _, mode := range []string{"ok", "header", "record", "decode"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			if _, err := s.WriteBatch(0, [][]byte{blk(1), blk(2)}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "header":
				_ = s.fd.Close()
			case "record":
				if err := s.fd.Truncate(int64(s.sb.WALOffset + walEntryPrefixSizeV3)); err != nil {
					t.Fatal(err)
				}
			case "decode":
				if _, err := s.fd.WriteAt([]byte{0xff}, int64(s.sb.WALOffset+walEntryPrefixSizeV3)); err != nil {
					t.Fatal(err)
				}
			}
			err := s.flusher.flushOnce()
			got := s.FlusherInstrumentation()
			if mode == "ok" {
				if err != nil {
					t.Fatal(err)
				}
				if got.ValidatedRecords != 2 || got.WALHeaderReadOps != 1 || got.WALRecordReadOps != 1 || got.WALRecordDecodeOps != 1 || got.ValidationFailures != 0 || got.CyclesSucceeded != 1 {
					t.Fatalf("successful batch stages: %+v", got)
				}
			} else {
				if err == nil {
					t.Fatal("faulted flush succeeded")
				}
				var h, r, d, v uint64
				switch mode {
				case "header":
					h = 1
				case "record":
					r = 1
				case "decode":
					d = 1
					v = 1
				}
				if got.WALHeaderReadFailures != h || got.WALRecordReadFailures != r || got.WALRecordDecodeFailures != d || got.ValidationFailures != v || got.ValidatedRecords != 0 || got.CyclesFailed != 1 {
					t.Fatalf("%s failure stages: %+v", mode, got)
				}
			}
		})
	}
}
