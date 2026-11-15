package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

func TestMemFiler_Contract(t *testing.T) {
	ctx := context.Background()
	m := NewMemFiler()
	ent := func(name, v string) *filerpb.Entry {
		return &filerpb.Entry{Name: name, Extended: map[string][]byte{"k": []byte(v)}}
	}
	for _, n := range []string{"b", "a", "c"} {
		if err := m.CreateEntry(ctx, "/d", ent(n, "1"), true); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.CreateEntry(ctx, "/d", ent("a", "2"), true); !errors.Is(err, ErrExists) {
		t.Fatalf("exclusive create over an entry: %v", err)
	}
	if err := m.CreateEntry(ctx, "/d/sub", ent("x", "1"), true); err != nil {
		t.Fatal(err)
	}
	list, err := m.ListEntries(ctx, "/d", "", "a", 10)
	if err != nil || len(list) != 2 || list[0].Name != "b" || list[1].Name != "c" {
		t.Fatalf("list after a: %v %v (children only, in name order)", list, err)
	}
	if err := m.UpdateEntry(ctx, "/d", ent("a", "3"), map[string][]byte{"k": []byte("9")}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("update with a wrong expectation: %v", err)
	}
	if err := m.UpdateEntry(ctx, "/d", ent("a", "3"), map[string][]byte{"k": []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if e, err := m.LookupEntry(ctx, "/d", "a"); err != nil || string(e.Extended["k"]) != "3" {
		t.Fatalf("lookup after update: %v %v", e, err)
	}
	if err := m.DeleteEntry(ctx, "/d", "a", map[string][]byte{"k": []byte("1")}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("delete with a wrong expectation: %v", err)
	}
	if err := m.DeleteEntry(ctx, "/d", "a", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := m.LookupEntry(ctx, "/d", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lookup after delete: %v", err)
	}
}
