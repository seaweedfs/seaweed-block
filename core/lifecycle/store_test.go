package lifecycle

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileStore_CreateVolumePersistsAndIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	spec := VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
		PVCName:           "demo-pvc",
		PVCNamespace:      "demo-ns",
		PVName:            "pvc-a",
	}
	rec, err := s.CreateVolume(spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if rec.Spec != spec {
		t.Fatalf("record spec=%+v want %+v", rec.Spec, spec)
	}
	rec2, err := s.CreateVolume(spec)
	if err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	if rec2 != rec {
		t.Fatalf("idempotent create changed record: got %+v want %+v", rec2, rec)
	}

	reopened, err := OpenFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, ok := reopened.GetVolume("vol-a")
	if !ok {
		t.Fatal("reopened store missing volume")
	}
	if got.Spec != spec {
		t.Fatalf("reopened spec=%+v want %+v", got.Spec, spec)
	}
}

func TestFileStore_CreateVolumeRejectsConflictingSpec(t *testing.T) {
	s, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	spec := VolumeSpec{VolumeID: "vol-a", SizeBytes: 1 << 20, ReplicationFactor: 2}
	if _, err := s.CreateVolume(spec); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.CreateVolume(VolumeSpec{VolumeID: "vol-a", SizeBytes: 2 << 20, ReplicationFactor: 2}); err == nil {
		t.Fatal("conflicting create must fail")
	}
}

func TestFileStore_AttachDetachAreIdempotent(t *testing.T) {
	s, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	spec := VolumeSpec{VolumeID: "vol-a", SizeBytes: 1 << 20, ReplicationFactor: 2}
	if _, err := s.CreateVolume(spec); err != nil {
		t.Fatalf("create: %v", err)
	}
	rec, err := s.AttachVolume("vol-a", "node-1")
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if rec.AttachedTo != "node-1" {
		t.Fatalf("attached_to=%q want node-1", rec.AttachedTo)
	}
	if _, err := s.AttachVolume("vol-a", "node-1"); err != nil {
		t.Fatalf("idempotent attach: %v", err)
	}
	if _, err := s.AttachVolume("vol-a", "node-2"); err == nil {
		t.Fatal("attach to a different node must fail until detached")
	}
	if _, err := s.DetachVolume("vol-a", "node-1"); err != nil {
		t.Fatalf("detach: %v", err)
	}
	rec, err = s.DetachVolume("vol-a", "node-1")
	if err != nil {
		t.Fatalf("idempotent detach: %v", err)
	}
	if rec.AttachedTo != "" {
		t.Fatalf("detached volume attached_to=%q want empty", rec.AttachedTo)
	}
}

func TestFileStore_DeleteVolumeRemovesRecord(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.CreateVolume(VolumeSpec{VolumeID: "vol-a", SizeBytes: 1 << 20, ReplicationFactor: 2}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.DeleteVolume("vol-a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.DeleteVolume("vol-a"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
	reopened, err := OpenFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, ok := reopened.GetVolume("vol-a"); ok {
		t.Fatal("deleted volume still present after reopen")
	}
}

func TestLifecyclePackageDoesNotImportAdapterOrAssignmentInfo(t *testing.T) {
	root := "."
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(root, entry.Name())
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports %s: %v", path, err)
		}
		for _, imp := range file.Imports {
			if imp.Path.Value == `"github.com/seaweedfs/seaweed-block/core/adapter"` {
				t.Fatalf("lifecycle package must not import adapter: %s", path)
			}
		}
		full, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(full, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok && ident.Name == "AssignmentInfo" {
				t.Fatalf("lifecycle package must not mention AssignmentInfo: %s", path)
			}
			if ident, ok := n.(*ast.Ident); ok && ident.Name == "AssignmentFact" {
				t.Fatalf("lifecycle package must not mention AssignmentFact: %s", path)
			}
			return true
		})
	}
}
