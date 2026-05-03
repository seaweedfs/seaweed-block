package master

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestMasterLifecycleStore_OptionalNilByDefault(t *testing.T) {
	h := newTestMaster(t, "")
	defer closeTestMaster(t, h)
	if h.Lifecycle() != nil {
		t.Fatal("Lifecycle() = non-nil without LifecycleStoreDir")
	}
}

func TestMasterLifecycleStore_OpensRegistrationStoresWithoutAuthority(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if stores == nil {
		t.Fatal("Lifecycle() = nil with LifecycleStoreDir")
	}
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
	}); err != nil {
		t.Fatalf("create desired volume: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9101",
		Pools: []lifecycle.StoragePool{{
			PoolID:     "pool-a",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "node-b",
		Addr:     "127.0.0.1:9102",
		Pools: []lifecycle.StoragePool{{
			PoolID:     "pool-b",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register second node: %v", err)
	}
	results := lifecycle.ReconcilePlacement(stores.Volumes.ListVolumes(), stores.Nodes.ListNodes(), stores.Placements)
	if len(results) != 1 || !results[0].Applied {
		t.Fatalf("reconcile results=%+v want one applied", results)
	}
	if _, ok := stores.Placements.GetPlacement("vol-a"); !ok {
		t.Fatal("placement intent not written")
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("vol-a"); ok {
		t.Fatal("lifecycle registration must not mint authority")
	}
	snapshot, ok := h.LifecycleSnapshot()
	if !ok {
		t.Fatal("missing lifecycle snapshot")
	}
	if len(snapshot.Volumes) != 1 || len(snapshot.Nodes) != 2 || len(snapshot.Placements) != 1 {
		t.Fatalf("snapshot counts got volumes=%d nodes=%d placements=%d",
			len(snapshot.Volumes), len(snapshot.Nodes), len(snapshot.Placements))
	}
	if len(snapshot.VerifiedPlacements) != 1 || !snapshot.VerifiedPlacements[0].Verified {
		t.Fatalf("verified placements=%+v want one verified", snapshot.VerifiedPlacements)
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("vol-a"); ok {
		t.Fatal("verified lifecycle snapshot must not mint authority")
	}
}

func TestMasterLifecycleSnapshot_IsNotAuthorityShaped(t *testing.T) {
	typ := mustParseStruct(t, "lifecycle_snapshot.go", "LifecycleSnapshot")
	for _, forbidden := range []string{"Epoch", "EndpointVersion", "Assignment", "Ready", "Healthy", "Primary"} {
		if _, ok := typ.Fields[forbidden]; ok {
			t.Fatalf("LifecycleSnapshot must not carry %s", forbidden)
		}
	}
}

func TestMasterLifecycleSnapshot_PlacementWithoutNodeObservationIsNotVerified(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{
			VolumeID: "vol-a",
			ServerID: "node-a",
			PoolID:   "pool-a",
			Source:   lifecycle.PlacementSourceBlankPool,
		}},
	}); err != nil {
		t.Fatalf("apply placement: %v", err)
	}
	snapshot, ok := h.LifecycleSnapshot()
	if !ok {
		t.Fatal("missing lifecycle snapshot")
	}
	if len(snapshot.VerifiedPlacements) != 1 {
		t.Fatalf("verified count=%d want 1", len(snapshot.VerifiedPlacements))
	}
	got := snapshot.VerifiedPlacements[0]
	if got.Verified {
		t.Fatalf("placement without node observation verified: %+v", got)
	}
	if got.Reason != lifecycle.VerifyReasonMissingObservation {
		t.Fatalf("reason=%q want %q", got.Reason, lifecycle.VerifyReasonMissingObservation)
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("vol-a"); ok {
		t.Fatal("unverified placement must not mint authority")
	}
}

func newTestMaster(t *testing.T, lifecycleDir string) *Host {
	t.Helper()
	h, err := New(Config{
		AuthorityStoreDir: t.TempDir(),
		LifecycleStoreDir: lifecycleDir,
		Listen:            "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	return h
}

type parsedStruct struct {
	Fields map[string]struct{}
}

func mustParseStruct(t *testing.T, fileName, structName string) parsedStruct {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Clean(fileName), nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", fileName, err)
	}
	var out parsedStruct
	ast.Inspect(file, func(n ast.Node) bool {
		typeSpec, ok := n.(*ast.TypeSpec)
		if !ok || typeSpec.Name.Name != structName {
			return true
		}
		st, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			t.Fatalf("%s is not a struct", structName)
		}
		out.Fields = make(map[string]struct{})
		for _, field := range st.Fields.List {
			for _, name := range field.Names {
				out.Fields[name.Name] = struct{}{}
			}
		}
		return false
	})
	if out.Fields == nil {
		t.Fatalf("struct %s not found in %s", structName, fileName)
	}
	return out
}

func closeTestMaster(t *testing.T, h *Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}
