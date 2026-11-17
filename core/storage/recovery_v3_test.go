package storage

import (
	"errors"
	"reflect"
	"testing"
)

// Row 5 pure recovery decisions (DESIGN r15 6.5, 6.6).

func rec(first, last, end uint64) trustedRecordV3 {
	return trustedRecordV3{FirstLSN: first, LastLSN: last, End: end}
}

func TestRecoveryV3ScanStartAndFloor(t *testing.T) {
	d := descriptorV3{RetainedTail: 500, CheckpointLSN: 40}
	if scanStartV3(d) != 500 || replayFloorV3(d) != 40 {
		t.Fatalf("none: start %d floor %d, want 500 40", scanStartV3(d), replayFloorV3(d))
	}
	d.BaseState, d.BaseLSN, d.BaseCursor = BaseInstalling, 100, 600
	if scanStartV3(d) != 600 || replayFloorV3(d) != 100 {
		t.Fatalf("installing: start %d floor %d, want 600 100", scanStartV3(d), replayFloorV3(d))
	}
	// G2: after installed the tail moved past BaseCursor; start at the tail.
	d.BaseState, d.BaseCursor = BaseInstalled, 300
	if scanStartV3(d) != 500 {
		t.Fatalf("installed with tail past BaseCursor: start %d, want 500 (max)", scanStartV3(d))
	}
}

func TestRecoveryV3ContiguousPrefix(t *testing.T) {
	cases := []struct {
		name  string
		F     uint64
		recs  []trustedRecordV3
		R, E  uint64
		isErr bool
	}{
		{"empty", 10, nil, 10, 1000, false},
		{"contiguous", 10, []trustedRecordV3{rec(11, 11, 1100), rec(12, 12, 1200)}, 12, 1200, false},
		{"gap drops the rest", 10, []trustedRecordV3{rec(11, 11, 1100), rec(13, 13, 1300)}, 11, 1100, false},
		{"first record above the floor is a gap", 10, []trustedRecordV3{rec(12, 12, 1200)}, 10, 1000, false},
		{"records at or below F are kept", 10, []trustedRecordV3{rec(9, 9, 1050), rec(10, 10, 1080), rec(11, 11, 1100)}, 11, 1100, false},
		{"batch counts as its whole range", 10, []trustedRecordV3{rec(11, 14, 1400), rec(15, 15, 1500)}, 15, 1500, false},
		{"batch straddling the floor", 10, []trustedRecordV3{rec(9, 12, 1200), rec(13, 13, 1300)}, 13, 1300, false},
		{"padding inside the run moves E", 10, []trustedRecordV3{rec(11, 11, 1100), {Padding: true, End: 1150}, rec(12, 12, 1250)}, 12, 1250, false},
		{"padding at the end of the run", 10, []trustedRecordV3{rec(11, 11, 1100), {Padding: true, End: 1150}}, 11, 1150, false},
		{"repeated or decreasing LSN inside the run is refused", 10, []trustedRecordV3{rec(11, 11, 1100), rec(11, 11, 1200)}, 0, 0, true},
		{"malformed span is refused", 10, []trustedRecordV3{rec(12, 11, 1200)}, 0, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			R, E, err := contiguousPrefixV3(1000, c.F, c.recs)
			if c.isErr {
				if err == nil {
					t.Fatal("want an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if R != c.R || E != c.E {
				t.Fatalf("R %d E %d, want R %d E %d", R, E, c.R, c.E)
			}
		})
	}
}

func TestRecoveryV3Decision(t *testing.T) {
	base := descriptorV3{RetainedTail: 1000, RetainedHead: 1000, CheckpointLSN: 10, W: 12}
	recs := []trustedRecordV3{rec(11, 11, 1100), rec(12, 12, 1200), rec(13, 13, 1300)}

	o, err := decideRecoveryV3(base, recs, 12)
	if err != nil || !o.Serve || o.R != 13 || o.E != 1300 {
		t.Fatalf("serve: %+v %v, want serve R 13 E 1300", o, err)
	}
	// A synced record (12, W=12) corrupt: the chain stops at 11 < W*.
	_, err = decideRecoveryV3(base, recs[:1], 12)
	var rf *RecoveryFailure
	if !errors.As(err, &rf) || rf.Kind != StorageRecoveryFailureWALIntegrity {
		t.Fatalf("R < W: err = %v, want a WALIntegrity failure", err)
	}
	// W* takes the max DurableLSN of the trusted records.
	low := base
	low.W = 10
	if _, err := decideRecoveryV3(low, recs[:1], 12); err == nil {
		t.Fatal("max DurableLSN 12 above R 11 was not refused")
	}
	// Installing: intact and damaged, never a bare wal_corrupt.
	inst := base
	inst.BaseState, inst.BaseLSN, inst.BaseCursor = BaseInstalling, 10, 1000
	inst.BaseInstallID[0] = 1
	o, err = decideRecoveryV3(inst, recs, 12)
	if err != nil || !o.Installing || o.Damaged {
		t.Fatalf("installing intact: %+v %v", o, err)
	}
	if !errors.Is(installingErrorV3(o), ErrBaseIncomplete) || errors.Is(installingErrorV3(o), ErrInstallSuffixDamaged) {
		t.Fatal("intact installing error is not a plain ErrBaseIncomplete")
	}
	o, err = decideRecoveryV3(inst, recs[:1], 12)
	if err != nil || !o.Installing || !o.Damaged {
		t.Fatalf("installing damaged: %+v %v (the check runs while installing)", o, err)
	}
	ie := installingErrorV3(o)
	if !errors.Is(ie, ErrBaseIncomplete) || !errors.Is(ie, ErrInstallSuffixDamaged) || !errors.As(ie, &rf) ||
		rf.Kind != StorageRecoveryFailureWALIntegrity {
		t.Fatalf("damaged installing error %v must wrap ErrBaseIncomplete, ErrInstallSuffixDamaged and WALIntegrity", ie)
	}
}

func TestRecoveryV3FenceRangesAreExact(t *testing.T) {
	cases := []struct {
		name              string
		E, floor, walSize uint64
		sameNonce         bool
		want              []fenceRangeV3
	}{
		// codex03 r13 example: WALSize 256, fallback tail 0, selected 100,
		// E 120. The fence is exactly [120, 256): physical [0, 100), which
		// the fallback still references, is untouched.
		{"floor bound, not the selected tail", 120, 0, 256, true, []fenceRangeV3{{120, 136}}},
		// Unaligned E after a barrier (70) and a one-block write (4166):
		// never rounded to a page.
		{"unaligned E 70", 70, 0, 1 << 20, true, []fenceRangeV3{{70, 1<<20 - 70}}},
		{"unaligned E 4166", 4166, 0, 1 << 20, true, []fenceRangeV3{{4166, 1<<20 - 4166}}},
		// The interval wraps physically: two exact ranges.
		{"no wrap: logical [300,356) is physical [44,100)", 300, 100, 256, true, []fenceRangeV3{{44, 56}}},
		{"wrap: two exact ranges, [floor,E) untouched", 200, 100, 256, true, []fenceRangeV3{{200, 56}, {0, 100}}},
		{"unaligned end after a wrap", 5000, 4166, 4096, true, []fenceRangeV3{{904, 3192}, {0, 70}}},
		{"head at the end: nothing", 356, 100, 256, true, nil},
		{"mixed nonces: nothing", 120, 0, 256, false, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := fenceRangesV3(c.E, c.floor, c.walSize, c.sameNonce)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("ranges %v, want %v", got, c.want)
			}
			var total uint64
			for _, r := range got {
				total += r.Len
			}
			if c.sameNonce && c.E < c.floor+c.walSize && total != c.floor+c.walSize-c.E {
				t.Fatalf("fenced %d bytes, want exactly %d", total, c.floor+c.walSize-c.E)
			}
		})
	}
	if _, err := fenceRangesV3(50, 100, 256, true); err == nil {
		t.Fatal("a head below the floor was accepted")
	}
	if _, err := fenceRangesV3(counterMaxV3, counterMaxV3, 256, true); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("fence end overflow: err = %v, want ErrCounterExhausted", err)
	}
}
