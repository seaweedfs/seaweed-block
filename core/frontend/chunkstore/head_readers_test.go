package chunkstore

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/retain"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// headReaders is every function outside tests that reads a volume's HEAD
// (ReadHead, ReadHeadObserved, GetHead, GetHeadRoot, getHeadRoot, headExists, or a LookupEntry
// of HEAD), with what it does when HEAD is absent and the probe below that
// pins it. "GAP" marks a reader that takes an absent HEAD for an empty
// one; a change that treats an absent HEAD as unknown flips those probes.
var headReaders = map[string]string{
	"core/frontend/durable/provider_seal_replay.go readLayoutSeal -> ReadHead":                   "absent, unreadable and foreign layout proof refuse without cached success (TestLayoutPlainDetachReplayAfterRestartNeedsNoLegacyFactory)",
	"core/frontend/durable/provider_layout.go settleLayoutStop -> ReadHead":                      "absent refuses unknown stop outcome, never resumes (probe TestLayoutStopAbortMustConfirmItsOutcome)",
	"core/frontend/durable/provider_layout.go startLayoutRoot -> ReadHead":                       "the diagnostic re-read after Start refusal cannot turn an absent HEAD into a claim; Start already refused it (probe TestLayoutStoreMissingHEADRefusesStart)",
	"cmd/blockvolume/workspace_lease.go workspaceLayoutDetachedGeneration -> ReadHead":           "absent refuses ACK (probe TestWorkspaceLayoutDetachedProof)",
	"cmd/blockvolume/chunk_remote.go chunkAttachCheck -> ReadHead":                               "absent refuses attach without creating VOLUME or HEAD (TestChunkAttachCheckAppliesVolumeRules)",
	"core/frontend/chunkstore/layout_store.go ReadHead -> ReadHeadObserved":                      "found=false passes through; no missing HEAD is invented (TestLayoutStoreMissingHEADRefusesStart)",
	"core/frontend/chunkstore/layout_store.go ReadHeadObserved -> LookupEntry":                   "found=false; Start refuses without writing (probe TestLayoutStoreMissingHEADRefusesStart)",
	"core/frontend/chunkstore/layout_store.go ReadHeadObserved -> DecodeLayoutHeadEntry":         "shared canonical entry decoder for every HEAD reader (TestLayoutStoreMissingHEADRefusesStart)",
	"core/frontend/chunkstore/catalog_b.go ReadHeadObserved -> ReadHeadObserved":                 "found=false to the caller (probe BCatalog)",
	"core/host/master/workspace_layout_head.go NewLayoutWorkspaceHEADReader -> ReadHeadObserved": "absent refuses ErrHeadAbsent, any failed read refuses; identity comes from master intent (TestWorkspaceLayoutHEADRefusalsAndStates)",
	"cmd/blockmaster/layout_create.go layoutHeadReader -> ReadHead":                              "absent refuses restore before target persistence (TestMasterLayoutHeadReaderRefusesAbsentHead)",
	"core/host/volume/chunk_control.go ListSnapshots -> ReadHead":                                "refuses ErrHeadAbsent without creating HEAD (TestChunkControlUsesTheBoundLayoutIdentity)",
	"core/frontend/chunkstore/layout/delete.go OperatorReleaseDependent -> ReadHead":             "reads the source's HEAD (absent: refuses ErrHeadAbsent) and the target's: a target without HEAD (never created, or a restore that died between its VOLUME and its HEAD) gets a deleted HEAD written exclusively before the release (probe LayoutOperatorRelease)",
	"core/frontend/chunkstore/layout/start.go Start -> ReadHead":                                 "refuses ErrHeadAbsent and creates nothing (probe LayoutStart)",
	"core/frontend/chunkstore/layout/remove.go RemoveUnreachable -> ReadHead":                    "refuses ErrHeadAbsent and writes nothing to the objects (probe LayoutRemoval)",
	"core/frontend/chunkstore/layout/snapshots.go RunAddDependent -> ReadHead":                   "refuses ErrHeadAbsent before reading any root and writes nothing (probe LayoutAddDependent)",
	"core/frontend/chunkstore/catalog_b.go DeleteVolume -> ReadHead":                             "refuses ErrHeadAbsent before any write (probe BCatalog)",
	"core/frontend/chunkstore/catalog_b.go ReadHead -> ReadHead":                                 "found=false to the caller, which reports no HEAD (probe BCatalog)",
	"core/frontend/chunkstore/retain_b.go Retain -> ReadHead":                                    "refuses ErrHeadAbsent, deletes nothing (probe BCatalog)",
	"core/frontend/chunkstore/retain_b.go SetRetention -> ReadHead":                              "refuses ErrHeadAbsent, writes no policy (probe BCatalog)",
	"core/frontend/chunkstore/catalog_b.go Usage -> ReadHead":                                    "refuses ErrHeadAbsent (probe BCatalog)",
	"core/frontend/chunkstore/layout_remote.go AbortOpenSession -> ReadHead":                     "refuses ErrHeadAbsent, aborts nothing (probe LayoutRemote)",
	"core/frontend/chunkstore/layout_remote.go CommitPendingSnapshot -> ReadHead":                "refuses ErrHeadAbsent, commits nothing (probe LayoutRemote)",
	"core/frontend/durable/provider_layout_snapshot.go SnapshotVolume -> ReadHead":               "the layout remote's HEAD read: refuses ErrHeadAbsent, names nothing (probe LayoutRemote)",
	"core/frontend/chunkstore/layout_remote.go prepareCommitLocked -> ReadHead":                  "refuses ErrHeadAbsent, writes nothing (probe LayoutRemote: CASHead, CheckCommit)",
	"core/frontend/chunkstore/layout_remote.go CASHead -> ReadHead":                              "refuses ErrHeadAbsent, writes nothing (probe LayoutRemote)",
	"core/frontend/chunkstore/layout_remote.go ReadHead -> ReadHead":                             "refuses ErrHeadAbsent (probe LayoutRemote)",
	"core/frontend/chunkstore/layout_remote.go rollBackLocked -> ReadHead":                       "refuses ErrHeadAbsent before any Begin or upload (probe LayoutRemote)",
	"core/frontend/chunkstore/layout/create.go createVolume -> ReadHead":                         "reads HEAD only after its exclusive create found one; absent then refuses ErrIdentity, writes nothing more (probe LayoutCreate)",
	"core/frontend/chunkstore/layout/head.go runTransition -> ReadHead":                          "refuses ErrHeadAbsent and writes nothing; no layout transition creates a HEAD (probe LayoutTransition)",
	// above the chunk store: they read through the layout remote's GetHead, so an
	// absent HEAD reaches them as ErrHeadAbsent, never as a zero HEAD (probe LayoutRemote)
	"cmd/sw-block/chunk_b.go chunkShow -> ReadHead":                      "found=false: prints a null head (probe TestOpsChunk_AbsentVolume)",
	"cmd/sw-block/chunk_b.go chunkSnapshots -> ReadHead":                 "refuses volume_not_found (probe TestOpsChunk_AbsentVolume)",
	"core/frontend/durable/provider_chunk.go GetHead -> GetHead":         "ErrHeadAbsent from the layout remote (probe LayoutRemote)",
	"core/frontend/durable/provider_chunk.go GetHeadRoot -> GetHead":     "ErrHeadAbsent from the layout remote (probe LayoutRemote)",
	"core/frontend/durable/provider_chunk.go GetHeadRoot -> GetHeadRoot": "ErrHeadAbsent from the layout remote (probe LayoutRemote)",
	"core/storage/chunk_head_belt.go confirmChunkHead -> GetHead":        "ErrHeadAbsent from the layout remote: not confirmed (probe LayoutRemote)",
	"core/storage/chunk_head_belt.go confirmChunkHead -> GetHeadRoot":    "ErrHeadAbsent from the layout remote: not confirmed (probe LayoutRemote)",
	"core/storage/chunk_head_belt.go readChunkHead -> GetHead":           "ErrHeadAbsent from the layout remote (probe LayoutRemote)",
	"core/storage/chunk_layer.go attachChunkLayerImpl -> GetHead":        "ErrHeadAbsent from the layout remote (probe LayoutRemote)",
}

// TestHeadReaders_Enumeration lists every HEAD reader in core/ and cmd/ from
// the source and fails when one is added or removed without updating
// headReaders (and its probe).
func TestHeadReaders_Enumeration(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	found := map[string]bool{}
	for _, top := range []string{"core", "cmd"} {
		err := filepath.Walk(filepath.Join(root, top), func(p string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fi.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.Contains(p, "filerpb") || strings.Contains(p, "masterpb") {
				return nil
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, p, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, p)
			rel = filepath.ToSlash(rel)
			for _, d := range file.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					name := ""
					switch x := call.Fun.(type) {
					case *ast.SelectorExpr:
						name = x.Sel.Name
					case *ast.Ident:
						name = x.Name
					}
					hit := name == "ReadHead" || name == "ReadHeadObserved" || name == "GetHead" || name == "GetHeadRoot" || name == "getHeadRoot" || name == "headExists" || name == "DecodeLayoutHeadEntry"
					if name == "LookupEntry" {
						for _, a := range call.Args {
							if id, ok := a.(*ast.Ident); ok && id.Name == "headName" {
								hit = true
							}
							if sel, ok := a.(*ast.SelectorExpr); ok && sel.Sel.Name == "HeadName" {
								hit = true // layout.HeadName
							}
						}
					}
					if hit {
						found[rel+" "+fn.Name.Name+" -> "+name] = true
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var added, removed []string
	for k := range found {
		if _, ok := headReaders[k]; !ok {
			added = append(added, k)
		}
	}
	for k := range headReaders {
		if !found[k] {
			removed = append(removed, k)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	if len(added) != 0 || len(removed) != 0 {
		t.Fatalf("HEAD readers changed: classify each new reader's absent-HEAD behaviour in headReaders with a probe.\nnew: %v\ngone: %v", added, removed)
	}
}

// TestHeadReaders_AbsentHeadProbes pins, per reader, what today's code does
// when HEAD is absent (see headReaders).
func TestHeadReaders_AbsentHeadProbes(t *testing.T) {
	ctx := context.Background()
	t.Run("LayoutTransition", func(t *testing.T) {
		st := &absentLayoutHead{}
		if _, err := layout.RunBegin(ctx, st, layout.HolderID{Epoch: 1, Holder: "h"}, "s"); !errors.Is(err, layout.ErrHeadAbsent) {
			t.Fatalf("layout transition over an absent HEAD: %v; want ErrHeadAbsent", err)
		}
		if st.updates != 0 {
			t.Fatalf("layout transition over an absent HEAD wrote %d times", st.updates)
		}
	})
	t.Run("LayoutAddDependent", func(t *testing.T) {
		r, err := layout.NewReader("/swblock/chunks", "v1", func(context.Context, string) ([]byte, bool, error) { return nil, false, nil })
		if err != nil {
			t.Fatal(err)
		}
		st := &absentLayoutHead{}
		if _, err := layout.RunAddDependent(ctx, st, r, "daily", layout.Dependent{Target: "t", TargetID: layout.VolumeID{1}}); !errors.Is(err, layout.ErrHeadAbsent) || st.updates != 0 {
			t.Fatalf("add dependent over an absent HEAD: %v (%d writes); want ErrHeadAbsent and none", err, st.updates)
		}
	})
	t.Run("BCatalog", func(t *testing.T) {
		f := newFakeFiler()
		c, err := NewBCatalog(f, newFakeBlobs(), "/blocks")
		if err != nil {
			t.Fatal(err)
		}
		v := VolumeRef{Name: "v", ID: layout.VolumeID{1}}
		if _, found, err := c.ReadHead(ctx, v); found || err != nil {
			t.Fatalf("catalog read of an absent HEAD: found %t, %v", found, err)
		}
		if _, err := c.Usage(ctx, v); !errors.Is(err, layout.ErrHeadAbsent) {
			t.Fatalf("usage of an absent HEAD: %v; want ErrHeadAbsent", err)
		}
		if _, err := c.Retain(ctx, v, time.Now(), false); !errors.Is(err, layout.ErrHeadAbsent) {
			t.Fatalf("retain of an absent HEAD: %v; want ErrHeadAbsent", err)
		}
		if err := c.SetRetention(ctx, v, retain.DefaultPolicy()); !errors.Is(err, layout.ErrHeadAbsent) || len(f.written) != 0 {
			t.Fatalf("policy of an absent HEAD: %v (%d writes); want ErrHeadAbsent and none", err, len(f.written))
		}
		if _, err := c.DeleteVolume(ctx, v, okDeleteAuthority{}); !errors.Is(err, layout.ErrHeadAbsent) || len(f.written) != 0 {
			t.Fatalf("delete of an absent HEAD: %v (%d writes); want ErrHeadAbsent and none", err, len(f.written))
		}
	})
	t.Run("LayoutRemote", func(t *testing.T) {
		f := newFakeFiler()
		c, err := NewBCatalog(f, newFakeBlobs(), "/blocks")
		if err != nil {
			t.Fatal(err)
		}
		r, err := NewLayoutRemote(c, VolumeRef{Name: "v", ID: layout.VolumeID{1}}, layout.HolderID{Epoch: 1, Holder: "a", ClaimID: "k"})
		if err != nil {
			t.Fatal(err)
		}
		r.SetSnapshot("x", layout.SnapshotMeta{})
		if err := r.CommitPendingSnapshot(ctx); !errors.Is(err, layout.ErrHeadAbsent) || len(f.written) != 0 {
			t.Fatalf("naming over an absent HEAD: %v (%d writes); want ErrHeadAbsent and none", err, len(f.written))
		}
		if _, err := r.AbortOpenSession(ctx); !errors.Is(err, layout.ErrHeadAbsent) {
			t.Fatalf("settling a stop over an absent HEAD: %v; want ErrHeadAbsent", err)
		}
		if _, err := r.ReadHead(ctx); !errors.Is(err, layout.ErrHeadAbsent) {
			t.Fatalf("kernel HEAD read of an absent HEAD: %v; want ErrHeadAbsent", err)
		}
		if _, err := r.GetHead(ctx); !errors.Is(err, layout.ErrHeadAbsent) {
			t.Fatalf("engine HEAD read of an absent HEAD: %v; want ErrHeadAbsent", err)
		}
		if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: 0, Epoch: 1}, storage.ChunkHead{Epoch: 1}); !errors.Is(err, layout.ErrHeadAbsent) || len(f.written) != 0 {
			t.Fatalf("engine commit over an absent HEAD: %v (%d writes); want ErrHeadAbsent and none", err, len(f.written))
		}
		if err := r.CheckCommit(ctx, storage.ChunkHeadVersion{Gen: 0, Epoch: 1}, storage.ChunkHead{Epoch: 1}); !errors.Is(err, layout.ErrHeadAbsent) || len(f.written) != 0 {
			t.Fatalf("engine commit check over an absent HEAD: %v (%d writes); want ErrHeadAbsent and none", err, len(f.written))
		}
	})
	t.Run("LayoutCreate", func(t *testing.T) {
		st := &existsButAbsent{}
		if _, err := layout.CreateVolume(ctx, st, layout.Volume{ID: layout.VolumeID{1}, Name: "v", Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: layout.VolumeFormat}); !errors.Is(err, layout.ErrIdentity) {
			t.Fatalf("create whose HEAD create found one that then reads absent: %v; want ErrIdentity", err)
		}
	})
	t.Run("LayoutRemoval", func(t *testing.T) {
		r, err := layout.NewReader("/swblock/chunks", "v1", func(context.Context, string) ([]byte, bool, error) { return nil, false, nil })
		if err != nil {
			t.Fatal(err)
		}
		pool := &countingPool{}
		if _, err := layout.RemoveUnreachable(ctx, &absentLayoutHead{}, r, pool, layout.HolderID{Epoch: 1, Holder: "h"}, "c", 0, []layout.ChunkName{layout.NameChunk(1, []byte("x"))}); !errors.Is(err, layout.ErrHeadAbsent) {
			t.Fatalf("layout removal over an absent HEAD: %v; want ErrHeadAbsent", err)
		}
		if pool.writes != 0 {
			t.Fatalf("layout removal over an absent HEAD wrote %d times to the objects", pool.writes)
		}
	})
	t.Run("LayoutOperatorRelease", func(t *testing.T) {
		tid, err := layout.NewVolumeID()
		if err != nil {
			t.Fatal(err)
		}
		vb, err := layout.Volume{ID: tid, Name: "t", Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: layout.VolumeFormat}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		withVolume := func(context.Context, string) ([]byte, bool, error) { return vb, true, nil }
		r, err := layout.NewReader("/swblock/chunks", "t", withVolume)
		if err != nil {
			t.Fatal(err)
		}
		src := &presentLayoutHead{}
		withVol := &absentLayoutHead{}
		if _, err := layout.OperatorReleaseDependent(ctx, src, withVol, r, tid); err != nil || withVol.created == nil || withVol.created.State != layout.StateDeleted {
			t.Fatalf("operator release of a target with VOLUME and no HEAD: %v, tombstone %+v; want a deleted HEAD written, then the release", err, withVol.created)
		}
		none, _ := layout.NewReader("/swblock/chunks", "t", func(context.Context, string) ([]byte, bool, error) { return nil, false, nil })
		tomb := &absentLayoutHead{}
		if _, err := layout.OperatorReleaseDependent(ctx, src, tomb, none, tid); err != nil || tomb.created == nil || tomb.created.State != layout.StateDeleted || tomb.created.VolumeID != tid {
			t.Fatalf("operator release of a target never created: %v, tombstone %+v; want a deleted HEAD of the target's id written before the release", err, tomb.created)
		}
		absent := &absentLayoutHead{}
		if _, err := layout.OperatorReleaseDependent(ctx, absent, &absentLayoutHead{}, r, tid); !errors.Is(err, layout.ErrHeadAbsent) || absent.updates != 0 {
			t.Fatalf("operator release with the source HEAD absent: %v; want ErrHeadAbsent", err)
		}
	})
	t.Run("LayoutStart", func(t *testing.T) {
		st := &absentLayoutHead{}
		if _, err := layout.Start(ctx, st, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "h"}, layout.LocalRoot{Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k", Fresh: true, CanStartFrom: anyStart}); !errors.Is(err, layout.ErrHeadAbsent) || st.updates != 0 {
			t.Fatalf("start over an absent HEAD: %v, %d writes; want ErrHeadAbsent and none", err, st.updates)
		}
	})
}

// absentLayoutHead is a layout HEAD store whose HEAD is absent; it records
// a tombstone created in it.
type absentLayoutHead struct {
	updates int
	created *layout.Head
}

func (a *absentLayoutHead) CreateDeletedHead(_ context.Context, h layout.Head) error {
	a.created = &h
	return nil
}

func (a *absentLayoutHead) ReadHead(context.Context) (layout.Head, bool, error) {
	return layout.Head{}, false, nil
}

func (a *absentLayoutHead) UpdateHead(context.Context, uint64, layout.Head) error {
	a.updates++
	return nil
}

// countingPool is a layout object store that counts writes and holds
// nothing.
type countingPool struct{ writes int }

func (p *countingPool) Stat(context.Context, string) (layout.ObjectMeta, bool, error) {
	return layout.ObjectMeta{}, false, nil
}
func (p *countingPool) Create(context.Context, string, []byte, string) error { p.writes++; return nil }
func (p *countingPool) Reuse(context.Context, string, layout.ObjectMeta, string) error {
	p.writes++
	return nil
}
func (p *countingPool) Mark(context.Context, string, layout.ObjectMeta, string) error {
	p.writes++
	return nil
}
func (p *countingPool) DeleteMarked(context.Context, string, string) error { p.writes++; return nil }
func (p *countingPool) DeleteOfVolume(context.Context, string, layout.VolumeID) error {
	p.writes++
	return nil
}

// presentLayoutHead is a layout HEAD store whose HEAD is present (a live,
// idle volume); it counts updates.
type presentLayoutHead struct{ updates int }

func (p *presentLayoutHead) ReadHead(context.Context) (layout.Head, bool, error) {
	id, _ := layout.NewVolumeID()
	return layout.Head{Gen: 1, VolumeID: id, Epoch: 1, Holder: "h", ClaimID: "k", Activity: layout.Activity{Kind: layout.ActIdle}}, true, nil
}

func (p *presentLayoutHead) UpdateHead(context.Context, uint64, layout.Head) error {
	p.updates++
	return nil
}

type okClaimLease struct{}

func (okClaimLease) OldLeaseEnded(context.Context, uint64) error { return nil }

// existsButAbsent: VOLUME is created; the HEAD create reports one exists,
// yet HEAD then reads absent (removed in between).
type existsButAbsent struct{}

func (existsButAbsent) ReadHead(context.Context) (layout.Head, bool, error) {
	return layout.Head{}, false, nil
}
func (existsButAbsent) CreateHead(context.Context, layout.Head) error           { return layout.ErrExists }
func (existsButAbsent) CreateVolumeRecord(context.Context, layout.Volume) error { return nil }
func (existsButAbsent) ReadVolumeRecord(context.Context) (layout.Volume, bool, error) {
	return layout.Volume{}, false, nil
}

func (s *absentLayoutHead) ReadVolumeRecord(context.Context) (layout.Volume, bool, error) {
	return layout.Volume{}, false, nil
}
