package lifecycle

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

func TestPlanPlacement_BlankCapacityProducesCandidatesOnly(t *testing.T) {
	volume := VolumeRecord{Spec: VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
	}}
	nodes := []NodeRegistration{
		{
			ServerID: "node-b",
			Addr:     "127.0.0.1:9102",
			Pools: []StoragePool{{
				PoolID:     "pool-b",
				TotalBytes: 1 << 30,
				FreeBytes:  1 << 29,
				BlockSize:  4096,
			}},
		},
		{
			ServerID: "node-a",
			Addr:     "127.0.0.1:9101",
			Pools: []StoragePool{{
				PoolID:     "pool-a",
				TotalBytes: 1 << 30,
				FreeBytes:  1 << 30,
				BlockSize:  4096,
			}},
		},
	}
	plan := PlanPlacement(volume, nodes)
	if !plan.EnoughCandidates() {
		t.Fatalf("plan should have enough candidates: %+v", plan)
	}
	if len(plan.Candidates) != 2 {
		t.Fatalf("candidate count=%d want 2", len(plan.Candidates))
	}
	if plan.Candidates[0].ServerID != "node-a" || plan.Candidates[1].ServerID != "node-b" {
		t.Fatalf("candidates should be deterministic by server id: %+v", plan.Candidates)
	}
	for _, c := range plan.Candidates {
		if c.Source != PlacementSourceBlankPool {
			t.Fatalf("source=%q want %q", c.Source, PlacementSourceBlankPool)
		}
		if c.ReplicaID != "" {
			t.Fatalf("blank-pool candidate must not assign replica id: %+v", c)
		}
	}
}

func TestPlanPlacement_ExistingReplicaIsCandidateButNotReady(t *testing.T) {
	volume := VolumeRecord{Spec: VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}}
	plan := PlanPlacement(volume, []NodeRegistration{{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9101",
		Replicas: []ReplicaInventory{{
			VolumeID:   "vol-a",
			ReplicaID:  "r2",
			StoreUUID:  "store-1",
			SizeBytes:  1 << 20,
			DurableLSN: 99,
			State:      "existing",
		}},
	}})
	if !plan.EnoughCandidates() {
		t.Fatalf("plan should have existing candidate: %+v", plan)
	}
	got := plan.Candidates[0]
	if got.Source != PlacementSourceExistingReplica || got.ReplicaID != "r2" {
		t.Fatalf("existing candidate mismatch: %+v", got)
	}
	typ := mustParseStruct(t, "planner.go", "PlacementCandidate")
	if _, ok := typ.Fields["Ready"]; ok {
		t.Fatal("placement candidate must not carry Ready")
	}
	if _, ok := typ.Fields["Primary"]; ok {
		t.Fatal("placement candidate must not carry Primary")
	}
}

func TestPlanPlacement_ExistingReplicaSizeMismatchConflicts(t *testing.T) {
	volume := VolumeRecord{Spec: VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}}
	plan := PlanPlacement(volume, []NodeRegistration{{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9101",
		Pools: []StoragePool{{
			PoolID:     "pool-a",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
		Replicas: []ReplicaInventory{{
			VolumeID:  "vol-a",
			ReplicaID: "r2",
			StoreUUID: "store-1",
			SizeBytes: 2 << 20,
			State:     "existing",
		}},
	}})
	if plan.EnoughCandidates() {
		t.Fatalf("conflicting existing replica must not be candidate: %+v", plan)
	}
	if len(plan.Conflicts) != 1 {
		t.Fatalf("conflict count=%d want 1: %+v", len(plan.Conflicts), plan)
	}
	if plan.Candidates != nil {
		t.Fatalf("candidates=%+v want none", plan.Candidates)
	}
}

func TestPlanPlacement_InsufficientCapacityDoesNotOverclaim(t *testing.T) {
	volume := VolumeRecord{Spec: VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
	}}
	plan := PlanPlacement(volume, []NodeRegistration{
		{
			ServerID: "node-a",
			Addr:     "127.0.0.1:9101",
			Pools: []StoragePool{{
				PoolID:     "pool-a",
				TotalBytes: 1 << 30,
				FreeBytes:  1 << 29,
				BlockSize:  4096,
			}},
		},
		{
			ServerID: "node-b",
			Addr:     "127.0.0.1:9102",
			Pools: []StoragePool{{
				PoolID:     "pool-b",
				TotalBytes: 1 << 30,
				FreeBytes:  512,
				BlockSize:  4096,
			}},
		},
	})
	if plan.EnoughCandidates() {
		t.Fatalf("plan must not claim enough candidates: %+v", plan)
	}
	if len(plan.Candidates) != 1 || len(plan.Conflicts) != 1 {
		t.Fatalf("candidate/conflict counts got %d/%d want 1/1: %+v",
			len(plan.Candidates), len(plan.Conflicts), plan)
	}
}

func TestPlannerPackageDoesNotMentionAuthorityTypes(t *testing.T) {
	for _, name := range []string{"PlacementCandidate", "PlacementPlan"} {
		typ := mustParseStruct(t, "planner.go", name)
		for _, forbidden := range []string{"Epoch", "EndpointVersion", "Assignment", "Ready", "Healthy"} {
			if _, ok := typ.Fields[forbidden]; ok {
				t.Fatalf("%s must not carry %s", name, forbidden)
			}
		}
	}
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
