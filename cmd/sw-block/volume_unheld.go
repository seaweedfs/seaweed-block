package main

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const volumeUnheldUsage = `usage: sw-block ops volume-unheld --path <volume-file>

Exit 0 only when no process on THIS host holds the v3 volume file's lock
(the lock every v3 owner holds while its store is open; the kernel releases
it when the owner process exits). Exit 1: held. Exit 2: unknown (missing
file, not a v3 volume, lock error) or bad usage.

This proof is per host and for local filesystems only. It says nothing about
another host, and nothing on a network or shared mount. It does not prove the
owner process is dead: host cleanup must require BOTH this command's exit 0
AND the host tooling's own check that the recorded owner process is gone.

The probe holds the lock only for an instant and reserves nothing: it does not
keep a new owner out after it returns, and an owner opening the volume at that
instant is refused. Run it only when the owner is believed gone.
`

// runOpsVolumeUnheld is `sw-block ops volume-unheld`.
func runOpsVolumeUnheld(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops volume-unheld", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, volumeUnheldUsage) }
	path := fs.String("path", "", "the volume file")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if *path == "" || fs.NArg() != 0 {
		fmt.Fprint(stderr, volumeUnheldUsage)
		return ops.VolumeStatusExitInvalid
	}
	switch err := storage.ProbeVolumeUnheld(*path); {
	case err == nil:
		fmt.Fprintf(stdout, "unheld: no process on this host holds %s (local proof only; also check the owner process is gone)\n", *path)
		return ops.VolumeStatusExitOK
	case errors.Is(err, storage.ErrVolumeHeld):
		fmt.Fprintf(stdout, "held: a live process on this host holds %s\n", *path)
		return ops.VolumeStatusExitUnhealthy
	default:
		fmt.Fprintf(stderr, "unknown: %v\n", err)
		return ops.VolumeStatusExitInvalid
	}
}
