package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

// bCatalog dials the filer and opens the volume catalog; a command that
// writes first passes the single-filer start check. It returns exit -1 on
// success, else the exit code after reporting.
func bCatalog(name, filer, base string, writes bool, timeout time.Duration, stdout, stderr io.Writer) (*chunkstore.BCatalog, func() error, int) {
	if filer == "" {
		return nil, nil, chunkFail(name, stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--chunk-filer is required", false)
	}
	f, closeFn, err := dialChunkFiler(filer)
	if err != nil {
		return nil, nil, chunkFail(name, stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	fail := func(msg string) (*chunkstore.BCatalog, func() error, int) {
		_ = closeFn()
		return nil, nil, chunkFail(name, stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, msg, false)
	}
	if writes {
		if exit := refuseUnlessSingleFiler(name, f, timeout, stdout, stderr); exit >= 0 {
			_ = closeFn()
			return nil, nil, exit
		}
	}
	blobs, ok := f.(chunkstore.Blobs)
	if !ok {
		return fail("the filer client does not reach the volume servers")
	}
	cat, err := chunkstore.NewBCatalog(f, blobs, base)
	if err != nil {
		return fail(err.Error())
	}
	return cat, closeFn, -1
}

// volumeFlags are a volume's name and layout id, as its master record
// holds them (sw-block volume status prints both).
type volumeFlags struct{ name, id string }

func (v *volumeFlags) bind(fs *flag.FlagSet, prefix, what string) {
	fs.StringVar(&v.name, prefix, "", what+" name")
	fs.StringVar(&v.id, prefix+"-layout-id", "", what+" layout id (from the master's volume record)")
}

func (v volumeFlags) ref(flagName string) (chunkstore.VolumeRef, error) {
	if v.name == "" || v.id == "" {
		return chunkstore.VolumeRef{}, errors.New("--" + flagName + " and --" + flagName + "-layout-id are required")
	}
	id, err := layout.ParseVolumeID(v.id)
	if err != nil {
		return chunkstore.VolumeRef{}, err
	}
	return chunkstore.VolumeRef{Name: v.name, ID: id}, nil
}

// bCommand parses a catalog command's flags (with --chunk-filer,
// --chunk-path and --timeout) and opens the catalog.
type bCommand struct {
	fs             *flag.FlagSet
	filer, base    string
	timeout        time.Duration
	stdout, stderr io.Writer
}

func newBCommand(name string, timeout time.Duration, stdout, stderr io.Writer) *bCommand {
	c := &bCommand{fs: chunkFlagSet(name, stderr), stdout: stdout, stderr: stderr}
	c.fs.StringVar(&c.filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	c.fs.StringVar(&c.base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	c.fs.DurationVar(&c.timeout, "timeout", timeout, "filer timeout")
	return c
}

// run parses args, resolves the refs, opens the catalog and calls do.
func (c *bCommand) run(args []string, writes bool, refs func() error, do func(context.Context, *chunkstore.BCatalog) (any, error)) int {
	name := chunkName(c.fs)
	if err := c.fs.Parse(args); err != nil {
		return chunkFail(name, c.stdout, c.stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if err := refs(); err != nil {
		return chunkFail(name, c.stdout, c.stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	cat, closeFn, exit := bCatalog(name, c.filer, c.base, writes, c.timeout, c.stdout, c.stderr)
	if exit >= 0 {
		return exit
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	v, err := do(ctx, cat)
	return chunkResult(name, v, err, c.stdout, c.stderr)
}

// SnapshotEntry is one named snapshot of a volume's HEAD.
type SnapshotEntry struct {
	Name       string   `json:"name"`
	Root       string   `json:"root"`
	Dependents []string `json:"dependents,omitempty"`
}

// chunkSnapshots lists a volume's named snapshots, in name order.
func chunkSnapshots(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("snapshots", 30*time.Second, stdout, stderr)
	var vol volumeFlags
	var prefix string
	vol.bind(c.fs, "volume", "volume")
	c.fs.StringVar(&prefix, "prefix", "", "only names with this prefix")
	var ref chunkstore.VolumeRef
	return c.run(args, false, func() (err error) { ref, err = vol.ref("volume"); return err }, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		h, found, err := cat.ReadHead(ctx, ref)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, layout.ErrHeadAbsent
		}
		out := []SnapshotEntry{}
		for n, s := range h.Snapshots {
			if !strings.HasPrefix(n, prefix) {
				continue
			}
			e := SnapshotEntry{Name: n, Root: s.Root.String()}
			for _, d := range s.Dependents {
				e.Dependents = append(e.Dependents, layout.VolumeDir(d.Target, d.TargetID))
			}
			out = append(out, e)
		}
		sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
		return out, nil
	})
}

// chunkSnapshotDelete removes a named snapshot; refused while a restore
// target depends on it.
func chunkSnapshotDelete(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("snapshot-delete", 30*time.Second, stdout, stderr)
	var vol volumeFlags
	var name string
	vol.bind(c.fs, "volume", "volume")
	c.fs.StringVar(&name, "name", "", "snapshot name")
	var ref chunkstore.VolumeRef
	return c.run(args, true, func() (err error) {
		if name == "" {
			return errors.New("--name is required")
		}
		ref, err = vol.ref("volume")
		return err
	}, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		if _, err := cat.DeleteSnapshot(ctx, ref, name); err != nil {
			return nil, err
		}
		return map[string]string{"volume": ref.Dir(), "name": name, "state": "absent"}, nil
	})
}

// chunkShow prints a volume's VOLUME record and HEAD.
func chunkShow(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("show", 30*time.Second, stdout, stderr)
	var vol volumeFlags
	vol.bind(c.fs, "volume", "volume")
	var ref chunkstore.VolumeRef
	return c.run(args, false, func() (err error) { ref, err = vol.ref("volume"); return err }, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		s, err := cat.Store(ref)
		if err != nil {
			return nil, err
		}
		body := struct {
			Volume *layout.Volume `json:"volume"`
			Head   *layout.Head   `json:"head"`
		}{}
		if v, ok, err := s.ReadVolumeRecord(ctx); err != nil {
			return nil, err
		} else if ok {
			body.Volume = &v
		}
		if h, ok, err := s.ReadHead(ctx); err != nil {
			return nil, err
		} else if ok {
			body.Head = &h
		}
		return body, nil
	})
}

// chunkUsage prints a volume's snapshots, dependents and the chunks its
// current image names.
func chunkUsage(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("usage", 60*time.Second, stdout, stderr)
	var vol volumeFlags
	vol.bind(c.fs, "volume", "volume")
	var ref chunkstore.VolumeRef
	return c.run(args, false, func() (err error) { ref, err = vol.ref("volume"); return err }, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		return cat.Usage(ctx, ref)
	})
}

// chunkRestore creates a restore target from a source snapshot: the
// target reads the snapshot's chunks from the source's pool until its own
// writes replace them; the snapshot cannot be deleted while it depends on
// it. The target's layout id comes from its master record.
func chunkRestore(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("restore", 6*time.Minute, stdout, stderr)
	var src, tgt volumeFlags
	var snapshot string
	src.bind(c.fs, "source", "source volume")
	tgt.bind(c.fs, "target", "new volume")
	c.fs.StringVar(&snapshot, "snapshot", "", "source snapshot name")
	var sref, tref chunkstore.VolumeRef
	return c.run(args, true, func() (err error) {
		if snapshot == "" {
			return errors.New("--snapshot is required")
		}
		if sref, err = src.ref("source"); err != nil {
			return err
		}
		tref, err = tgt.ref("target")
		return err
	}, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		h, err := cat.Restore(ctx, sref, snapshot, tref)
		if err != nil {
			return nil, err
		}
		return map[string]string{"source": sref.Dir(), "snapshot": snapshot, "target": tref.Dir(), "root": h.Current.String()}, nil
	})
}

// chunkRelease removes a restore target from its source snapshot's
// dependents once the target is deleted or no longer names the source's
// chunks; a target without HEAD (a restore that never finished) is
// tombstoned first so it can never be created later.
func chunkRelease(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("release", 30*time.Second, stdout, stderr)
	var src, tgt volumeFlags
	src.bind(c.fs, "source", "source volume")
	tgt.bind(c.fs, "target", "restore target")
	var sref, tref chunkstore.VolumeRef
	return c.run(args, true, func() (err error) {
		if sref, err = src.ref("source"); err != nil {
			return err
		}
		tref, err = tgt.ref("target")
		return err
	}, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		if _, err := cat.ReleaseTarget(ctx, sref, tref); err != nil {
			return nil, err
		}
		return map[string]string{"source": sref.Dir(), "target": tref.Dir(), "state": "released"}, nil
	})
}

// layoutCode is the error code of a layout kernel refusal ("" for any
// other error); a lost conditional update is the one retryable code.
func layoutCode(err error) string {
	for _, c := range []struct {
		err  error
		code string
	}{
		{layout.ErrNoSnapshot, "snapshot_not_found"},
		{layout.ErrHasDependents, "snapshot_has_dependents"},
		{layout.ErrForeignSource, "restore_source_not_own"},
		{layout.ErrHeadAbsent, "volume_not_found"},
		{layout.ErrExists, "target_exists"},
		{layout.ErrVolumeDeleting, "volume_deleting"},
		{layout.ErrHolderActive, "not_admitted"},
		{chunkstore.ErrNotAdmitted, "not_admitted"},
		{layout.ErrTargetLive, "target_live"},
		{layout.ErrIdentity, "identity_mismatch"},
		{layout.ErrPrecondition, "conflict"},
	} {
		if errors.Is(err, c.err) {
			return c.code
		}
	}
	return ""
}
