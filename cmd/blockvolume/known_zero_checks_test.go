package main_test

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// kzStats is the part of the chunk layer's durable status the KNOWN-ZERO
// acceptance test reads.
type kzStats struct {
	DirtyChunks     int
	StagedChunks    uint64
	KnownZeroChunks uint64
	AbsentChunks    int
	Hydration       struct{ WaitCalls, Fetches, Completed, FetchErrors uint64 }
	LastCut         *struct {
		CandidateChunks, StagedChunks, UploadedChunks, ZeroedChunks, UnchangedChunks int
		UploadedBytes                                                                int64
	}
}

// kzStep1Pass is step 1's verdict: the empty first snapshot completed,
// considered no chunk, staged and uploaded nothing, HEAD names no segment, and
// the fresh attach certified EVERY chunk known-zero (a partial certification
// means the first cut scanned the rest).
func kzStep1Pass(s kzStats, completed bool, headSegments, total int, freshLine bool) (bool, string) {
	switch {
	case !completed:
		return false, "s0 did not complete"
	case s.LastCut == nil:
		return false, "no last cut"
	case s.LastCut.CandidateChunks != 0:
		return false, fmt.Sprintf("s0 considered %d chunks (want 0: no scan)", s.LastCut.CandidateChunks)
	case s.LastCut.StagedChunks != 0 || s.LastCut.UploadedChunks != 0:
		return false, "s0 staged or uploaded chunks"
	case headSegments != 0:
		return false, fmt.Sprintf("HEAD names %d segments", headSegments)
	case s.KnownZeroChunks != uint64(total):
		return false, fmt.Sprintf("known-zero %d of %d chunks (want all)", s.KnownZeroChunks, total)
	case !freshLine:
		return false, "no fresh-attach line"
	}
	return true, ""
}

// kzCheckWrite: a file write is good only when the writer exited 0 (any
// error fails, also one that came with output, as tee prints the data it was
// given) and the file read back has the sha256 of what was written.
func kzCheckWrite(out []byte, err error, want, readBack string) error {
	if err != nil {
		return fmt.Errorf("write failed: %v (%d bytes of output)", err, len(out))
	}
	if readBack != want {
		return fmt.Errorf("read back sha256 %s, wrote %s", readBack, want)
	}
	return nil
}

var kzExtentRE = regexp.MustCompile(`^\s*\d+:\s+\d+\.\.\s*\d+:\s+(\d+)\.\.\s*\d+:\s+\d+:`)

// kzFirstPhysical parses `filefrag -e FILE` and returns the physical start
// (in file-system blocks) of the file's first extent: where its first block
// lives on the device. ext4 and XFS print the same table.
func kzFirstPhysical(out string) (uint64, error) {
	for _, l := range strings.Split(out, "\n") {
		if m := kzExtentRE.FindStringSubmatch(l); m != nil {
			return strconv.ParseUint(m[1], 10, 64)
		}
	}
	return 0, errors.New("no extent in filefrag output")
}

// kzReserve marks the ports of addrs used, refusing one already taken: the
// target role reserves the data and ctrl ports the source's cluster spec
// names for it before it picks its own local ports.
func kzReserve(used map[int]bool, addrs ...string) error {
	for _, a := range addrs {
		_, p, err := net.SplitHostPort(a)
		if err != nil {
			return fmt.Errorf("address %q: %v", a, err)
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return fmt.Errorf("address %q: %v", a, err)
		}
		if used[n] {
			return fmt.Errorf("port %d of %q is already taken", n, a)
		}
		used[n] = true
	}
	return nil
}

func TestKnownZeroStep1Verdict(t *testing.T) {
	lc := func(cand, staged, up int) *struct {
		CandidateChunks, StagedChunks, UploadedChunks, ZeroedChunks, UnchangedChunks int
		UploadedBytes                                                                int64
	} {
		return &struct {
			CandidateChunks, StagedChunks, UploadedChunks, ZeroedChunks, UnchangedChunks int
			UploadedBytes                                                                int64
		}{CandidateChunks: cand, StagedChunks: staged, UploadedChunks: up}
	}
	cases := []struct {
		name  string
		s     kzStats
		segs  int
		fresh bool
		want  bool
	}{
		{"all-certified", kzStats{KnownZeroChunks: 256, LastCut: lc(0, 0, 0)}, 0, true, true},
		{"partial-certification", kzStats{KnownZeroChunks: 1, LastCut: lc(255, 0, 0)}, 0, true, false},
		{"partial-certification-no-candidates-shown", kzStats{KnownZeroChunks: 255, LastCut: lc(0, 0, 0)}, 0, true, false},
		{"full-zero-scan", kzStats{KnownZeroChunks: 0, LastCut: lc(256, 0, 0)}, 0, true, false},
		{"all-certified-but-scanned", kzStats{KnownZeroChunks: 256, LastCut: lc(3, 0, 0)}, 0, true, false},
		{"uploaded", kzStats{KnownZeroChunks: 256, LastCut: lc(0, 0, 1)}, 0, true, false},
		{"head-names-a-segment", kzStats{KnownZeroChunks: 256, LastCut: lc(0, 0, 0)}, 1, true, false},
		{"no-fresh-line", kzStats{KnownZeroChunks: 256, LastCut: lc(0, 0, 0)}, 0, false, false},
		{"no-last-cut", kzStats{KnownZeroChunks: 256}, 0, true, false},
	}
	for _, c := range cases {
		if got, why := kzStep1Pass(c.s, true, c.segs, 256, c.fresh); got != c.want {
			t.Errorf("%s: got %v (%s), want %v", c.name, got, why, c.want)
		}
	}
	if ok, _ := kzStep1Pass(kzStats{KnownZeroChunks: 256, LastCut: lc(0, 0, 0)}, false, 0, 256, true); ok {
		t.Error("an incomplete s0 passed")
	}
}

func TestKnownZeroWriteCheck(t *testing.T) {
	if err := kzCheckWrite([]byte("data"), nil, "aa", "aa"); err != nil {
		t.Errorf("good write refused: %v", err)
	}
	if err := kzCheckWrite([]byte("data echoed by tee"), errors.New("exit status 1"), "aa", "aa"); err == nil {
		t.Error("a failed write that produced output passed")
	}
	if err := kzCheckWrite(nil, nil, "aa", "bb"); err == nil {
		t.Error("a short or wrong file (read-back sha256 differs) passed")
	}
}

func TestKnownZeroFirstPhysical(t *testing.T) {
	ext4 := "Filesystem type is: ef53\nFile size of /m/kz-0.bin is 65536 (16 blocks of 4096 bytes)\n ext:     logical_offset:        physical_offset: length:   expected: flags:\n   0:        0..      15:      34816..     34831:     16:             last,eof\n/m/kz-0.bin: 1 extent found\n"
	xfs := "Filesystem type is: 58465342\nFile size of /m/kz-1.bin is 131072 (32 blocks of 4096 bytes)\n ext:     logical_offset:        physical_offset: length:   expected: flags:\n   0:        0..      31:         24..        55:     32:             last,eof\n/m/kz-1.bin: 1 extent found\n"
	for name, c := range map[string]struct {
		out  string
		want uint64
	}{"ext4": {ext4, 34816}, "xfs": {xfs, 24}} {
		got, err := kzFirstPhysical(c.out)
		if err != nil || got != c.want {
			t.Errorf("%s: got %d %v, want %d", name, got, err, c.want)
		}
	}
	if _, err := kzFirstPhysical("Filesystem type is: ef53\n/m/x: 0 extents found\n"); err == nil {
		t.Error("an empty file (no extent) gave a block")
	}
}

func TestKnownZeroImportedPortReservation(t *testing.T) {
	used := map[int]bool{}
	if err := kzReserve(used, "10.0.0.2:12001", "10.0.0.2:12002"); err != nil {
		t.Fatalf("fresh ports refused: %v", err)
	}
	if err := kzReserve(used, "10.0.0.2:12002"); err == nil {
		t.Error("a port collision with an imported address passed")
	}
	if err := kzReserve(used, "no-port"); err == nil {
		t.Error("an address without a port passed")
	}
}
