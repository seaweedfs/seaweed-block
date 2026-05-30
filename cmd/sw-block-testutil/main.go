package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "smartwal-corrupt-latest-record":
		return runSmartWALCorruptLatest(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "sw-block-testutil: unknown command %q\n", args[0])
		usage(stderr)
		return 2
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: sw-block-testutil smartwal-corrupt-latest-record --path <store.bin> --out <dir>")
}

func runSmartWALCorruptLatest(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("smartwal-corrupt-latest-record", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var path, outDir string
	fs.StringVar(&path, "path", "", "SmartWAL store file to mutate")
	fs.StringVar(&outDir, "out", "", "directory for smartwal-corruption-evidence.txt")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if path == "" || outDir == "" {
		fmt.Fprintln(stderr, "smartwal-corrupt-latest-record: --path and --out are required")
		return 2
	}

	layout, rec, err := smartwal.LatestRecord(path)
	if err != nil {
		fmt.Fprintf(stderr, "smartwal-corrupt-latest-record: %v\n", err)
		return 1
	}
	mutateOffset := rec.Offset + layout.RecordSize - 1
	if !layout.ContainsWALOffset(mutateOffset) || layout.ContainsExtentOffset(mutateOffset) {
		fmt.Fprintf(stderr, "smartwal-corrupt-latest-record: mutate offset %d is not safely inside WAL\n", mutateOffset)
		return 1
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintf(stderr, "smartwal-corrupt-latest-record: open %s: %v\n", path, err)
		return 1
	}
	defer f.Close()

	before := make([]byte, layout.RecordSize)
	if _, err := f.ReadAt(before, rec.Offset); err != nil {
		fmt.Fprintf(stderr, "smartwal-corrupt-latest-record: read before: %v\n", err)
		return 1
	}
	after := make([]byte, len(before))
	copy(after, before)
	after[len(after)-1] ^= 0xff
	if _, err := f.WriteAt(after[len(after)-1:], mutateOffset); err != nil {
		fmt.Fprintf(stderr, "smartwal-corrupt-latest-record: write corruption: %v\n", err)
		return 1
	}
	if err := f.Sync(); err != nil {
		fmt.Fprintf(stderr, "smartwal-corrupt-latest-record: fsync: %v\n", err)
		return 1
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(stderr, "smartwal-corrupt-latest-record: mkdir %s: %v\n", outDir, err)
		return 1
	}
	evidencePath := filepath.Join(outDir, "smartwal-corruption-evidence.txt")
	evidence := fmt.Sprintf(`smartwal_corruption_status=ok
smartwal_path=%s
wal_offset=%d
wal_length=%d
wal_end=%d
extent_start=%d
target_lsn=%d
target_lba=%d
target_slot=%d
target_record_offset=%d
mutated_offset=%d
target_offset_inside_wal=%t
target_offset_inside_extent=%t
before_bytes=%s
after_bytes=%s
mutation=flip_last_record_crc_byte
`,
		path,
		layout.WALOffset,
		layout.WALLength,
		layout.WALEnd,
		layout.ExtentStart,
		rec.LSN,
		rec.LBA,
		rec.Slot,
		rec.Offset,
		mutateOffset,
		layout.ContainsWALOffset(mutateOffset),
		layout.ContainsExtentOffset(mutateOffset),
		hex.EncodeToString(before),
		hex.EncodeToString(after),
	)
	if err := os.WriteFile(evidencePath, []byte(evidence), 0o644); err != nil {
		fmt.Fprintf(stderr, "smartwal-corrupt-latest-record: write evidence: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, evidence)
	return 0
}
