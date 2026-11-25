package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/retain"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/lifecycleclient"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
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

// volumeFlags name a volume; its layout id is resolved through the
// master's volume record (never minted, never read from the filer).
type volumeFlags struct {
	name string
	cmd  *bCommand
}

// resolveVolumeID resolves a volume name to its layout id at the master.
// Tests replace it.
var resolveVolumeID = func(ctx context.Context, master, name string) (layout.VolumeID, error) {
	conn, err := link.DialMasterFromEnv(master)
	if err != nil {
		return layout.VolumeID{}, err
	}
	defer conn.Close()
	return lifecycleclient.ResolveVolume(ctx, control.NewLifecycleServiceClient(conn), name)
}

func (c *bCommand) bindVolume(v *volumeFlags, flagName, what string) {
	v.cmd = c
	c.fs.StringVar(&v.name, flagName, "", what+" name")
}

func (v volumeFlags) ref(flagName string) (chunkstore.VolumeRef, error) {
	if v.name == "" {
		return chunkstore.VolumeRef{}, errors.New("--" + flagName + " is required")
	}
	if v.cmd.master == "" {
		return chunkstore.VolumeRef{}, errors.New("--master is required (it resolves the volume's layout id)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), v.cmd.timeout)
	defer cancel()
	id, err := resolveVolumeID(ctx, v.cmd.master, v.name)
	if err != nil {
		return chunkstore.VolumeRef{}, fmt.Errorf("resolve --%s %q at the master: %w", flagName, v.name, err)
	}
	return chunkstore.VolumeRef{Name: v.name, ID: id}, nil
}

// bCommand parses a catalog command's flags (with --chunk-filer,
// --chunk-path and --timeout) and opens the catalog.
type bCommand struct {
	fs                  *flag.FlagSet
	filer, base, master string
	timeout             time.Duration
	stdout, stderr      io.Writer
}

func newBCommand(name string, timeout time.Duration, stdout, stderr io.Writer) *bCommand {
	c := &bCommand{fs: chunkFlagSet(name, stderr), stdout: stdout, stderr: stderr}
	c.fs.StringVar(&c.filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	c.fs.StringVar(&c.base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	c.fs.DurationVar(&c.timeout, "timeout", timeout, "filer timeout")
	c.fs.StringVar(&c.master, "master", "", "blockmaster gRPC address (resolves each volume's layout id)")
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
	c.bindVolume(&vol, "volume", "volume")
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
	c.bindVolume(&vol, "volume", "volume")
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
	c.bindVolume(&vol, "volume", "volume")
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
	c.bindVolume(&vol, "volume", "volume")
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
	c.bindVolume(&src, "source", "source volume")
	c.bindVolume(&tgt, "target", "new volume")
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
	c.bindVolume(&src, "source", "source volume")
	c.bindVolume(&tgt, "target", "restore target")
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
		{layout.ErrGeometryMismatch, "geometry_mismatch"},
		{layout.ErrPrecondition, "conflict"},
		{retain.ErrInvalidPolicy, "invalid_policy"},
	} {
		if errors.Is(err, c.err) {
			return c.code
		}
	}
	return ""
}

// chunkRetain: sw-block ops chunk retain --volume v --master m
// --chunk-filer host:port [--dry-run] applies the volume's retention policy:
// it deletes the expired snapshots by the automatic snapshot delete, which
// refuses a pinned snapshot or one a restore target depends on. With
// --set-policy it stores the policy given by the --keep-* flags in the
// volume's RETENTION entry instead; a policy change applies from the next
// pass.
func chunkRetain(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("retain", 5*time.Minute, stdout, stderr)
	var vol volumeFlags
	var keepYounger time.Duration
	var dryRun, setPolicy bool
	var keepNewest, keepHourly, keepDaily, keepWeekly int
	c.bindVolume(&vol, "volume", "volume")
	c.fs.BoolVar(&dryRun, "dry-run", false, "list what would be deleted; write nothing")
	c.fs.BoolVar(&setPolicy, "set-policy", false, "store the --keep-* policy on the volume instead of applying it")
	c.fs.IntVar(&keepNewest, "keep-newest", retain.DefaultKeepNewest, "with --set-policy: keep the newest N unpinned snapshots")
	c.fs.DurationVar(&keepYounger, "keep-younger", retain.DefaultKeepYounger, "with --set-policy: keep every snapshot younger than this")
	c.fs.IntVar(&keepHourly, "keep-hourly", 0, "with --set-policy: keep the newest snapshot of each of the last N hours")
	c.fs.IntVar(&keepDaily, "keep-daily", 0, "with --set-policy: keep the newest snapshot of each of the last N days")
	c.fs.IntVar(&keepWeekly, "keep-weekly", 0, "with --set-policy: keep the newest snapshot of each of the last N weeks")
	var ref chunkstore.VolumeRef
	// The start check runs only for a pass that writes.
	writes := true
	for _, a := range args {
		if a == "--dry-run" || a == "-dry-run" || a == "--dry-run=true" {
			writes = false
		}
	}
	return c.run(args, writes, func() (err error) {
		if setPolicy && dryRun {
			return errors.New("--set-policy and --dry-run do not combine")
		}
		ref, err = vol.ref("volume")
		return err
	}, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		if setPolicy {
			p := retain.Policy{KeepNewest: keepNewest, KeepYoungerNs: int64(keepYounger), KeepHourly: keepHourly, KeepDaily: keepDaily, KeepWeekly: keepWeekly}
			if err := cat.SetRetention(ctx, ref, p); err != nil {
				return nil, err
			}
			return map[string]any{"volume": ref.Dir(), "policy": p, "policySet": true}, nil
		}
		return cat.Retain(ctx, ref, chunkRetainNow(), dryRun)
	})
}

// chunkRetainNow is the wall clock retention decides by. Tests replace it.
var chunkRetainNow = time.Now
