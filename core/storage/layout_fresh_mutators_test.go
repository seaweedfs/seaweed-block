package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// walStoreMethods classes every exported WALStore method by what a fresh
// local root allows: guest writes are refused while fresh; base and
// replication mutators, control and reads are not (a restore's base goes
// through them, so a fresh root never deadlocks its own restore). A new
// method fails until it is classed here.
var walStoreMethods = map[string]string{
	"Write": "guest (delegates to WriteContext)", "WriteContext": "guest: refused while fresh",
	"WriteBatch": "guest (delegates to WriteBatchContext)", "WriteBatchContext": "guest: refused while fresh",

	"ApplyEntry": "replication", "WriteExtentDirect": "base (v3: install handle)", "AdvanceFrontier": "base/replication",
	"BeginBaseInstall": "base", "CommitChunkAttachV3": "base", "SeedChunkFrontier": "base", "SyncInstallLog": "base",
	"AdvanceWALTail": "replication", "FormatV3": "base", "Recover": "recovery",

	"SetLayoutIdentity": "control", "SetWMode": "control", "SetMultiBlockRecords": "control", "SetRecoveryRetentionLSNs": "control",
	"SetRecycleFloorSource": "control", "AllowBaseRetirementV3": "control", "DeferBaseRetirementV3": "control", "HoldExtentDrainUntilChunkLayer": "control (holds the extent drain until the chunk layer attaches)",
	"DisableAutoFlushForRecoveryTest": "test control", "ResetFlusherForMeasurement": "test control", "Sync": "control", "Close": "control",

	"AllBlocks": "read", "AppliedLSNs": "read", "CanSeedChunkFrontier": "read (the seed precondition, no write)", "BlockSize": "read", "StoreID": "read (the superblock UUID)", "ChunkBlocks": "read (immutable local map shape)", "Boundaries": "read", "CheckpointLSN": "read",
	"CheckpointLSNForPOC": "read", "CheckpointPersistence": "read", "ChunkMapStatsV3": "read", "DataSyncPolicy": "read",
	"DurabilityLatency": "read", "ExtentDrainHeld": "read", "FlushCount": "read", "FlusherInstrumentation": "read", "HolesSuspected": "read",
	"InstalledBaseLSN": "read", "LayoutIdentity": "read", "NextLSN": "read", "NumBlocks": "read",
	"OwnerChunkAttachDurableV3": "read", "Read": "read", "ReadContext": "read", "RecoveryMode": "read",
	"RecoveryRetentionLSNs": "read", "RecoveryWarnings": "read", "ScanLBAs": "read", "WALCapacityBytes": "read",
	"WALSyncCount": "read", "WriteInstrumentation": "read",
}

func TestWALStoreMutators_FreshGuardEnumeration(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]*ast.FuncDecl{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() {
				continue
			}
			if st, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
				if id, ok := st.X.(*ast.Ident); ok && id.Name == "WALStore" {
					found[fn.Name.Name] = fn
				}
			}
		}
	}
	var unclassed, gone []string
	for n := range found {
		if _, ok := walStoreMethods[n]; !ok {
			unclassed = append(unclassed, n)
		}
	}
	for n := range walStoreMethods {
		if found[n] == nil {
			gone = append(gone, n)
		}
	}
	sort.Strings(unclassed)
	sort.Strings(gone)
	if len(unclassed)+len(gone) != 0 {
		t.Fatalf("WALStore methods changed: class each (guest writes must check layoutFresh).\nnew: %v\ngone: %v", unclassed, gone)
	}
	for n, class := range walStoreMethods {
		if class != "guest: refused while fresh" {
			continue
		}
		guarded := false
		ast.Inspect(found[n].Body, func(x ast.Node) bool {
			if sel, ok := x.(*ast.SelectorExpr); ok && sel.Sel.Name == "layoutFresh" {
				guarded = true
			}
			return true
		})
		if !guarded {
			t.Errorf("%s is a guest write and does not check layoutFresh", n)
		}
	}
}

// A fresh root restores through the base path (never refused), refuses a
// guest write until fresh is cleared, then takes it; a crash after the base
// and before the clear reopens with the same claim id, still fresh, and
// the clear runs again (idempotent).
func TestLayoutFresh_RestoreThenFlipThenFirstGuestWrite(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s")
	s, err := CreateWALStoreWithOptions(p, 64, 4096, WALStoreOptions{ChunkBlocks: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetLayoutIdentity("k-r", true); err != nil {
		t.Fatal(err)
	}
	h, err := s.BeginBaseInstall(7, [16]byte{1})
	if err != nil {
		t.Fatalf("base install on a fresh root: %v", err)
	}
	base := blk(0x55)
	if err := h.WriteBase(5, base); err != nil {
		t.Fatalf("base write on a fresh root: %v", err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(6, blk(0x66)); err == nil {
		t.Fatal("a guest write before the clear was accepted")
	}
	// Crash after the base, before the clear.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWALStore(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	if id, fresh, ok := s.LayoutIdentity(); !ok || id != "k-r" || !fresh {
		t.Fatalf("after the crash: %q fresh %t ok %t; want the same claim id, still fresh", id, fresh, ok)
	}
	for i := 0; i < 2; i++ {
		if err := s.SetLayoutIdentity("k-r", false); err != nil {
			t.Fatalf("clear #%d: %v", i+1, err)
		}
	}
	if _, err := s.Write(6, blk(0x66)); err != nil {
		t.Fatalf("first guest write after the clear: %v", err)
	}
	if got, err := s.Read(5); err != nil || got[0] != 0x55 {
		t.Fatalf("base block after restore: %x %v", got[:1], err)
	}
}
