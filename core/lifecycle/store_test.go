package lifecycle

import (
	"errors"
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
		Protocol:          "iscsi",
		PVCName:           "demo-pvc",
		PVCNamespace:      "demo-ns",
		PVCUID:            "uid-123",
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

func TestFileStore_CreateVolumePersistsProtocolSelection(t *testing.T) {
	s, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, tc := range []struct {
		name string
		spec VolumeSpec
		want string
	}{
		{
			name: "default-iscsi",
			spec: VolumeSpec{VolumeID: "vol-iscsi", SizeBytes: 1 << 20, ReplicationFactor: 1},
			want: "iscsi",
		},
		{
			name: "explicit-nvme",
			spec: VolumeSpec{VolumeID: "vol-nvme", SizeBytes: 1 << 20, ReplicationFactor: 1, Protocol: "nvme"},
			want: "nvme",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, err := s.CreateVolume(tc.spec)
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			if rec.Spec.Protocol != tc.want {
				t.Fatalf("protocol=%q want %q", rec.Spec.Protocol, tc.want)
			}
			reopened, err := OpenFileStore(s.dir)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			got, ok := reopened.GetVolume(tc.spec.VolumeID)
			if !ok {
				t.Fatalf("reopened store missing %s", tc.spec.VolumeID)
			}
			if got.Spec.Protocol != tc.want {
				t.Fatalf("persisted protocol=%q want %q", got.Spec.Protocol, tc.want)
			}
		})
	}
}

func TestPhase165_FileStorePersistsNVMeTransportAndDefaultsLegacyToTCP(t *testing.T) {
	s, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, tc := range []struct {
		id        string
		transport string
		want      string
	}{
		{id: "legacy", want: "tcp"},
		{id: "rdma", transport: "rdma", want: "rdma"},
	} {
		rec, err := s.CreateVolume(VolumeSpec{VolumeID: tc.id, SizeBytes: 1 << 20, ReplicationFactor: 1, Protocol: "nvme", FrontendTransport: tc.transport})
		if err != nil {
			t.Fatalf("create %s: %v", tc.id, err)
		}
		if rec.Spec.FrontendTransport != tc.want {
			t.Fatalf("%s transport=%q want %q", tc.id, rec.Spec.FrontendTransport, tc.want)
		}
	}
	for _, spec := range []VolumeSpec{
		{VolumeID: "bad-iscsi", SizeBytes: 1 << 20, ReplicationFactor: 1, Protocol: "iscsi", FrontendTransport: "rdma"},
		{VolumeID: "bad-nvme", SizeBytes: 1 << 20, ReplicationFactor: 1, Protocol: "nvme", FrontendTransport: "bogus"},
	} {
		if _, err := s.CreateVolume(spec); err == nil {
			t.Fatalf("CreateVolume(%+v) succeeded; want invalid transport rejected", spec)
		}
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
	if _, err := s.CreateVolume(VolumeSpec{VolumeID: "vol-a", SizeBytes: 1 << 20, ReplicationFactor: 2, Protocol: "nvme"}); err == nil {
		t.Fatal("conflicting protocol create must fail")
	}
}

func TestFileStore_CreateVolumeMergesMissingKubernetesMetadata(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	base := VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
		PVCName:           "demo-pvc",
		PVCNamespace:      "demo-ns",
		PVName:            "pvc-a",
	}
	if _, err := s.CreateVolume(base); err != nil {
		t.Fatalf("create base: %v", err)
	}
	withUID := base
	withUID.PVCUID = "uid-123"
	rec, err := s.CreateVolume(withUID)
	if err != nil {
		t.Fatalf("create with uid: %v", err)
	}
	if rec.Spec.PVCUID != "uid-123" {
		t.Fatalf("pvc uid=%q", rec.Spec.PVCUID)
	}
	reopened, err := OpenFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, ok := reopened.GetVolume("vol-a")
	if !ok {
		t.Fatal("reopened store missing volume")
	}
	if got.Spec.PVCUID != "uid-123" {
		t.Fatalf("persisted pvc uid=%q", got.Spec.PVCUID)
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

func TestPhase175VolumeSourceSnapshotIsDurableAndImmutable(t *testing.T) {
	root := t.TempDir()
	store, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{VolumeID: "restored-a", SizeBytes: 1 << 20, ReplicationFactor: 2, SourceSnapshotID: "snap-abc"}
	if _, err := store.CreateVolume(spec); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := reopened.GetVolume(spec.VolumeID)
	if !ok || rec.Spec.SourceSnapshotID != "snap-abc" || rec.RestoreState != VolumeRestorePending {
		t.Fatalf("record=%+v ok=%v", rec, ok)
	}
	rec, err = reopened.MarkRestoreComplete(spec.VolumeID, spec.SourceSnapshotID)
	if err != nil || rec.RestoreState != VolumeRestoreComplete {
		t.Fatalf("complete record=%+v error=%v", rec, err)
	}
	if _, err := reopened.MarkRestoreComplete(spec.VolumeID, spec.SourceSnapshotID); err != nil {
		t.Fatalf("idempotent completion: %v", err)
	}
	changed := spec
	changed.SourceSnapshotID = "snap-def"
	if _, err := reopened.CreateVolume(changed); err != ErrVolumeConflict {
		t.Fatalf("source snapshot mutation error=%v", err)
	}
	changed = spec
	changed.SourceSnapshotID = "../catalog"
	changed.VolumeID = "restored-b"
	if _, err := reopened.CreateVolume(changed); err == nil {
		t.Fatal("unsafe source snapshot id accepted")
	}
}

func TestPhase175PendingRestoreDeletionIsDurablyHeld(t *testing.T) {
	root := t.TempDir()
	store, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{VolumeID: "restored-a", SizeBytes: 1 << 20, ReplicationFactor: 1, SourceSnapshotID: "snap-abc"}
	if _, err := store.CreateVolume(spec); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteVolume(spec.VolumeID); !errors.Is(err, ErrRestorePending) {
		t.Fatalf("delete pending restore error=%v", err)
	}
	reopened, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.DeleteVolume(spec.VolumeID); !errors.Is(err, ErrRestorePending) {
		t.Fatalf("delete pending restore after restart error=%v", err)
	}
	if _, err := reopened.MarkRestoreComplete(spec.VolumeID, spec.SourceSnapshotID); err != nil {
		t.Fatal(err)
	}
	if err := reopened.DeleteVolume(spec.VolumeID); err != nil {
		t.Fatalf("delete completed restore: %v", err)
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
