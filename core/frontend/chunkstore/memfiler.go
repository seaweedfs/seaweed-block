package chunkstore

// MemFiler is an in-memory Filer for development and for tests outside this
// package: exclusive create, conditional update and delete on extended
// attributes, and name-ordered listing of one directory's children. Like the
// filer, an update or delete hands the chunks it drops to DeleteChunk (the
// filer's deletion queue). It has no fault injection; this package's own
// tests use their own fake.

import (
	"context"
	"path"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

type MemFiler struct {
	mu sync.Mutex
	e  map[string]*filerpb.Entry
	// DeleteChunk, if set (before use), receives the file id of every chunk
	// an UpdateEntry or DeleteEntry drops, after the change is stored.
	DeleteChunk func(fileID string)
}

// NewMemFiler returns an empty MemFiler.
func NewMemFiler() *MemFiler { return &MemFiler{e: map[string]*filerpb.Entry{}} }

var _ Filer = (*MemFiler)(nil)

func (m *MemFiler) LookupEntry(_ context.Context, dir, name string) (*filerpb.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.e[path.Join(dir, name)]
	if !ok {
		return nil, ErrNotFound
	}
	return proto.Clone(e).(*filerpb.Entry), nil
}

func (m *MemFiler) CreateEntry(_ context.Context, dir string, e *filerpb.Entry, oExcl bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := path.Join(dir, e.GetName())
	if _, ok := m.e[p]; ok && oExcl {
		return ErrExists
	}
	m.e[p] = proto.Clone(e).(*filerpb.Entry)
	return nil
}

func (m *MemFiler) UpdateEntry(_ context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := path.Join(dir, e.GetName())
	cur, ok := m.e[p]
	if !ok {
		return ErrNotFound
	}
	if !extendedMatch(cur, expected) {
		return ErrPrecondition
	}
	m.e[p] = proto.Clone(e).(*filerpb.Entry)
	m.drop(droppedChunks(cur, e))
	return nil
}

func (m *MemFiler) ListEntries(_ context.Context, dir, prefix, startAfter string, limit uint32) ([]*filerpb.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var names []string
	for p := range m.e {
		if name := path.Base(p); path.Dir(p) == dir && strings.HasPrefix(name, prefix) && name > startAfter {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if limit > 0 && uint32(len(names)) > limit {
		names = names[:limit]
	}
	out := make([]*filerpb.Entry, 0, len(names))
	for _, n := range names {
		out = append(out, proto.Clone(m.e[path.Join(dir, n)]).(*filerpb.Entry))
	}
	return out, nil
}

func (m *MemFiler) DeleteEntry(_ context.Context, dir, name string, expected map[string][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := path.Join(dir, name)
	cur, ok := m.e[p]
	if !ok {
		return ErrNotFound
	}
	if !extendedMatch(cur, expected) {
		return ErrPrecondition
	}
	delete(m.e, p)
	m.drop(droppedChunks(cur, nil))
	return nil
}

func (m *MemFiler) drop(ids []string) {
	if m.DeleteChunk != nil {
		for _, id := range ids {
			m.DeleteChunk(id)
		}
	}
}

// droppedChunks is the filer's garbage of replacing old by new (nil: a
// delete): the file ids of old's chunks that new does not name.
func droppedChunks(old, new *filerpb.Entry) []string {
	keep := map[string]bool{}
	for _, c := range new.GetChunks() {
		keep[c.GetFileId()] = true
	}
	var out []string
	for _, c := range old.GetChunks() {
		if !keep[c.GetFileId()] {
			out = append(out, c.GetFileId())
		}
	}
	return out
}

func extendedMatch(e *filerpb.Entry, expected map[string][]byte) bool {
	for k, v := range expected {
		if string(e.GetExtended()[k]) != string(v) {
			return false
		}
	}
	return true
}
