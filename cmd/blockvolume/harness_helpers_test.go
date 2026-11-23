package main_test

import (
	"bytes"
	"fmt"
	"testing"
)

// pattern repeats s to exactly one 4096-byte block. It panics unless len(s)
// divides 4096: a shorter block would be a short NVMe write, which hangs the
// in-test initiator until its deadline instead of failing at once.
func pattern(s string) []byte {
	if len(s) == 0 || 4096%len(s) != 0 {
		panic(fmt.Sprintf("pattern %q: %d bytes do not divide a 4096-byte block", s, len(s)))
	}
	return bytes.Repeat([]byte(s), 4096/len(s))
}

// TestPatternIsExactlyOneBlock: a 16-byte string fills one block; a 15-byte
// one (it would make 4095 bytes) panics with its name.
func TestPatternIsExactlyOneBlock(t *testing.T) {
	if b := pattern("sixteen-bytes!!!"); len(b) != 4096 {
		t.Fatalf("pattern of 16 bytes: %d bytes; want 4096", len(b))
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("pattern of 15 bytes returned a block; want a panic (4096 is not a multiple of 15)")
		}
	}()
	pattern("fifteen-bytes!!")
}

// TestHarnessParsersReadTheNamedVolume: the adoption, plain-detach and
// forced-move parsers read the volume they are given, and refuse a line or
// answer for another volume; the v1 forms are those parsers for v1.
func TestHarnessParsersReadTheNamedVolume(t *testing.T) {
	v4 := `2026/11/23 07:30:06 blockvolume: volume v4 adopted as PRIMARY at r1@3 (EV=1) process="n-v4" server="n-v4"`
	if e, err := adoptedEpochOf("v4", v4); err != nil || e != 3 {
		t.Fatalf("adoptedEpochOf v4: %d %v; want 3", e, err)
	}
	if _, err := adoptedEpochOf("v1", v4); err == nil {
		t.Fatal("adoptedEpochOf v1 read a v4 adoption line")
	}
	if _, err := adoptedEpoch(v4); err == nil {
		t.Fatal("adoptedEpoch (v1) read a v4 adoption line")
	}

	seal := `{"volumeID":"v6","headGen":4,"headEpoch":2,"cutLSN":9,"lastWrittenLSN":9}`
	if g, err := sealedGenOf(seal, "sealed HEAD generation: 4\n", "v6", 2); err != nil || g != 4 {
		t.Fatalf("sealedGenOf v6: %d %v; want 4", g, err)
	}
	if _, err := sealedGen(seal, "sealed HEAD generation: 4\n", 2); err == nil {
		t.Fatal("sealedGen (v1) accepted a v6 detach answer")
	}

	move := "forced move issued: volume v6 request force-v6: r1@1 on n-v6 -> r1@2 on n-v7 (kept HEAD gen 3, remote LSN 8)\n"
	if e2, err := forcedEpochOf(move, "v6", 1, "n-v6", "n-v7"); err != nil || e2 != 2 {
		t.Fatalf("forcedEpochOf v6: %d %v; want 2", e2, err)
	}
	if _, err := forcedEpoch(move, 1, "n-v6", "n-v7"); err == nil {
		t.Fatal("forcedEpoch (v1) read a v6 forced move")
	}
}
