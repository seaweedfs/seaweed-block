package main

import "testing"

func TestC12RequireChecksumsFlags(t *testing.T) {
	for _, impl := range []string{"walstore", "walstore-chunk"} {
		args := append(phase150RequiredBlockvolumeArgs(), "--durable-root=/tmp/c12", "--durable-impl="+impl, "--require-data-checksums")
		if impl == "walstore-chunk" {
			args = append(args, "--chunk-filer=localhost:18888")
		}
		f, err := parseFlags(args)
		if err != nil || !f.requireDataChecksums {
			t.Fatalf("%s: flag lost: %+v %v", impl, f, err)
		}
	}
	for _, extra := range [][]string{nil, {"--durable-root=/tmp/c12"}, {"--durable-root=/tmp/c12", "--durable-impl=parallel-walstore"}} {
		args := append(phase150RequiredBlockvolumeArgs(), extra...)
		if _, err := parseFlags(append(args, "--require-data-checksums")); err == nil {
			t.Fatalf("unsupported checksum request admitted: %v", args)
		}
	}
}
