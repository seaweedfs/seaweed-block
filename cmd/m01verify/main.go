//go:build m01verify

// Hardware-test byte-equal verifier — built only under the
// `m01verify` build tag; never compiled into the production
// blockvolume binary.
//
// Build:
//
//	go build -tags m01verify -o m01verify ./cmd/blockvolume/
//
// Usage:
//
//	m01verify --walstore /tmp/g5sm/replica-store/v1.walstore \
//	  --lba-start 0 --lba-count 256 --block-size 4096 \
//	  --expected-pattern <hex-byte> | --expected-file payload.bin
//
// Opens the replica's walstore through `core/storage.OpenReadOnly`
// (NOT raw extent peek), invokes Read(lba) over the target LBA
// range, SHA-256-compares against the known primary write payload.
// Prints OK on equality, MISMATCH lba=N exp=... got=... + exit 1
// on diff.
//
// Caller (script) MUST ensure the daemon owning the walstore is
// stopped or quiesced before invocation; OpenReadOnly opens the
// file O_RDONLY but the underlying WAL replay assumes no
// concurrent writer.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

type verifyFlags struct {
	walstorePath    string
	lbaStart        uint
	lbaCount        uint
	blockSize       uint
	expectedPattern string // single hex byte (e.g. "ab")
	expectedFile    string // path to a binary file with the expected per-LBA bytes
}

func parseVerifyFlags() (verifyFlags, error) {
	var f verifyFlags
	fs := flag.NewFlagSet("m01verify", flag.ContinueOnError)
	fs.StringVar(&f.walstorePath, "walstore", "", "path to replica's walstore file (required)")
	fs.UintVar(&f.lbaStart, "lba-start", 0, "first LBA to verify")
	fs.UintVar(&f.lbaCount, "lba-count", 1, "number of consecutive LBAs to verify")
	fs.UintVar(&f.blockSize, "block-size", 4096, "block size in bytes")
	fs.StringVar(&f.expectedPattern, "expected-pattern", "", "expected per-byte pattern as a single hex byte (e.g. 'ab'); each LBA filled with this byte")
	fs.StringVar(&f.expectedFile, "expected-file", "", "expected payload file (lba_count * block_size bytes); read concatenated per-LBA")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return f, err
	}
	if f.walstorePath == "" {
		return f, fmt.Errorf("--walstore required")
	}
	if f.lbaCount == 0 {
		return f, fmt.Errorf("--lba-count must be > 0")
	}
	if f.expectedPattern == "" && f.expectedFile == "" {
		return f, fmt.Errorf("one of --expected-pattern or --expected-file required")
	}
	if f.expectedPattern != "" && f.expectedFile != "" {
		return f, fmt.Errorf("--expected-pattern and --expected-file are mutually exclusive")
	}
	return f, nil
}

func main() {
	f, err := parseVerifyFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "m01verify:", err)
		os.Exit(2)
	}

	expected, err := buildExpected(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, "m01verify: build expected:", err)
		os.Exit(2)
	}

	r, err := storage.OpenReadOnly(f.walstorePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "m01verify: OpenReadOnly:", err)
		os.Exit(1)
	}
	defer func() { _ = r.Close() }()

	mismatches := 0
	for i := uint(0); i < f.lbaCount; i++ {
		lba := uint32(f.lbaStart + i)
		got, rerr := r.Read(lba)
		if rerr != nil {
			fmt.Fprintf(os.Stderr, "m01verify: Read lba=%d: %v\n", lba, rerr)
			os.Exit(1)
		}
		want := expected[i]
		if !bytesEqual(got, want) {
			gotSum := sha256.Sum256(got)
			wantSum := sha256.Sum256(want)
			fmt.Printf("MISMATCH lba=%d exp_sha256=%s got_sha256=%s\n",
				lba, hex.EncodeToString(wantSum[:8]), hex.EncodeToString(gotSum[:8]))
			mismatches++
		}
	}
	if mismatches > 0 {
		fmt.Printf("FAIL %d mismatch(es) over %d LBA(s)\n", mismatches, f.lbaCount)
		os.Exit(1)
	}
	fmt.Printf("OK %d LBA(s) byte-equal\n", f.lbaCount)
}

// buildExpected returns the expected bytes for each verified LBA.
// Either every LBA is filled with --expected-pattern, or the bytes
// come from --expected-file split into block-size chunks.
func buildExpected(f verifyFlags) ([][]byte, error) {
	out := make([][]byte, f.lbaCount)
	if f.expectedPattern != "" {
		raw, err := hex.DecodeString(f.expectedPattern)
		if err != nil || len(raw) != 1 {
			return nil, fmt.Errorf("--expected-pattern must be a single hex byte (e.g. 'ab'); got %q", f.expectedPattern)
		}
		b := raw[0]
		for i := range out {
			block := make([]byte, f.blockSize)
			for j := range block {
				block[j] = b
			}
			out[i] = block
		}
		return out, nil
	}
	data, err := os.ReadFile(f.expectedFile)
	if err != nil {
		return nil, fmt.Errorf("read expected file: %w", err)
	}
	want := int(f.lbaCount * f.blockSize)
	if len(data) < want {
		return nil, fmt.Errorf("expected file too short: got %d bytes, want >= %d", len(data), want)
	}
	for i := uint(0); i < f.lbaCount; i++ {
		out[i] = data[i*f.blockSize : (i+1)*f.blockSize]
	}
	return out, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
