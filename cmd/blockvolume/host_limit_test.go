package main

import (
	"path/filepath"
	"testing"
)

func TestHostCutLimitFlags(t *testing.T) {
	base := append(phase150RequiredBlockvolumeArgs(), "--durable-root="+t.TempDir(), "--durable-impl=walstore-chunk", "--chunk-filer=x:1")
	dir := t.TempDir()
	args := append(append([]string{}, base...), "--host-limit-dir="+dir, "--host-limit-normal-slots=2", "--host-limit-urgent-slots=1")
	f, e := parseFlags(args)
	if e != nil {
		t.Fatal(e)
	}
	if f.chunk.hostLimit.Directory != dir || f.chunk.hostLimit.NormalSlots != 2 || f.chunk.hostLimit.UrgentSlots != 1 {
		t.Fatal("lost host limits")
	}
	for _, extra := range [][]string{{"--host-limit-normal-slots=2"}, {"--host-limit-dir=" + dir, "--host-limit-normal-slots=0"}, {"--host-limit-dir=" + dir, "--host-limit-max-hold=-1s"}, {"--host-limit-dir=" + filepath.Join("relative", "pool")}} {
		a := append(append([]string{}, base...), extra...)
		if _, e = parseFlags(a); e == nil {
			t.Fatalf("accepted invalid limits: %v", extra)
		}
	}
}
