package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// fakeFiler is an in-memory filer with the same precondition semantics as
// seaweed-mono's UpdateEntry (validateUpdateEntryPreconditions) and
// CreateEntry(o_excl). It records every entry ever written so tests can
// check that no HEAD or snapshot entry carries chunks.
type fakeFiler struct {
	mu      sync.Mutex
	entries map[string]*filerpb.Entry
	written []*filerpb.Entry

	// failUpdate, if set, is called before an UpdateEntry is applied; a
	// non-nil return fails the call and the update is NOT applied.
	failUpdate func(p string) error
	// failUpdateAfterApply, if set, is called after an UpdateEntry was
	// applied; a non-nil return is reported to the caller (ambiguous error).
	failUpdateAfterApply func(p string) error
	updates              int
	deletes              int
	// failDelete, if set, is called before a DeleteEntry is applied; a
	// non-nil return fails the call and the entry is NOT deleted.
	failDelete func(p string) error
	// failLookup, if set, is called before a LookupEntry reads p; a non-nil
	// return is the lookup's error (a read or transport failure).
	failLookup func(p string) error
	// beforeCreate, if set, runs under the lock just before an entry is
	// created (it may read f.entries but must not call f's methods).
	beforeCreate func(p string)
	// failCreate, if set, is called before a create is applied; a non-nil
	// return fails the call and the entry is NOT created (an ambiguous
	// transport error from the caller's view).
	failCreate func(p string) error
	// failCreateAfterApply, if set, is called after a create was applied; a
	// non-nil return is reported to the caller (applied, response lost).
	failCreateAfterApply func(p string) error
	// deferCreate, if set and true for a create, makes the server retain
	// it (not applied yet) and return an error; releaseDeferredCreates
	// applies retained creates later, each only if its name is still free
	// (exclusive create, evaluated when the server finally runs it). It
	// runs under the lock and may read or change f.entries (e is the
	// caller's entry: read it, do not keep it).
	deferCreate func(p string, e *filerpb.Entry) bool
	stallCreate func(p string) bool
	// afterLookup, if set, runs under the lock after a lookup of p has read
	// its result: a change it makes is seen by the next read only.
	afterLookup     func(p string)
	deferredCreates []deferredCreate
}

func newFakeFiler() *fakeFiler { return &fakeFiler{entries: map[string]*filerpb.Entry{}} }

func (f *fakeFiler) LookupEntry(_ context.Context, dir, name string) (*filerpb.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := path.Join(dir, name)
	if f.failLookup != nil {
		if err := f.failLookup(p); err != nil {
			return nil, err
		}
	}
	e, ok := f.entries[p]
	var out *filerpb.Entry
	if ok {
		out = proto.Clone(e).(*filerpb.Entry)
	}
	if hook := f.afterLookup; hook != nil {
		hook(p)
	}
	if !ok {
		return nil, ErrNotFound
	}
	return out, nil
}

func (f *fakeFiler) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, oExcl bool) error {
	// stallCreate, if set and true for a path, makes the create hang until
	// its context ends (a filer that never answers); set it before use.
	if stall := f.stallCreate; stall != nil && stall(path.Join(dir, e.Name)) {
		<-ctx.Done()
		return ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	p := path.Join(dir, e.Name)
	if _, ok := f.entries[p]; ok && oExcl {
		return fmt.Errorf("%w: %s", ErrExists, p)
	}
	if f.beforeCreate != nil {
		f.beforeCreate(p)
	}
	if f.failCreate != nil {
		if err := f.failCreate(p); err != nil {
			return err
		}
	}
	if f.deferCreate != nil && f.deferCreate(p, e) {
		f.deferredCreates = append(f.deferredCreates, deferredCreate{p: p, e: proto.Clone(e).(*filerpb.Entry)})
		return errors.New("fake: create retained by the server")
	}
	c := proto.Clone(e).(*filerpb.Entry)
	f.entries[p] = c
	f.written = append(f.written, proto.Clone(e).(*filerpb.Entry))
	if f.failCreateAfterApply != nil {
		if err := f.failCreateAfterApply(p); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeFiler) UpdateEntry(_ context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := path.Join(dir, e.Name)
	if f.failUpdate != nil {
		if err := f.failUpdate(p); err != nil {
			return err
		}
	}
	cur, ok := f.entries[p]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, p)
	}
	for k, want := range expected {
		got, has := cur.Extended[k]
		if has && !bytes.Equal(got, want) || !has && len(want) > 0 {
			return fmt.Errorf("%w: extended %q changed", ErrPrecondition, k)
		}
	}
	f.entries[p] = proto.Clone(e).(*filerpb.Entry)
	f.written = append(f.written, proto.Clone(e).(*filerpb.Entry))
	f.updates++
	if f.failUpdateAfterApply != nil {
		if err := f.failUpdateAfterApply(p); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeFiler) ListEntries(_ context.Context, dir, prefix, startAfter string, limit uint32) ([]*filerpb.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for p := range f.entries {
		if path.Dir(p) != path.Clean(dir) {
			continue
		}
		n := path.Base(p)
		if strings.HasPrefix(n, prefix) && n > startAfter {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if limit > 0 && len(names) > int(limit) {
		names = names[:limit]
	}
	out := make([]*filerpb.Entry, 0, len(names))
	for _, n := range names {
		out = append(out, proto.Clone(f.entries[path.Join(dir, n)]).(*filerpb.Entry))
	}
	return out, nil
}

func (f *fakeFiler) DeleteEntry(_ context.Context, dir, name string, expected map[string][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := path.Join(dir, name)
	if f.failDelete != nil {
		if err := f.failDelete(p); err != nil {
			return err
		}
	}
	cur, ok := f.entries[p]
	if len(expected) > 0 {
		if !ok {
			return fmt.Errorf("%w: %s not found", ErrPrecondition, p)
		}
		for k, want := range expected {
			got, has := cur.Extended[k]
			if has && !bytes.Equal(got, want) || !has && len(want) > 0 {
				return fmt.Errorf("%w: extended %q changed", ErrPrecondition, k)
			}
		}
	}
	delete(f.entries, p)
	f.deletes++
	return nil
}

type deferredCreate struct {
	p string
	e *filerpb.Entry
}

// releaseDeferredCreates lets the server finish retained creates; it returns
// how many applied (a name that is taken by then is not overwritten).
func (f *fakeFiler) releaseDeferredCreates() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	applied := 0
	for _, d := range f.deferredCreates {
		if _, taken := f.entries[d.p]; taken {
			continue
		}
		f.entries[d.p] = d.e
		f.written = append(f.written, proto.Clone(d.e).(*filerpb.Entry))
		applied++
	}
	f.deferredCreates = nil
	return applied
}

// bumpExtended mutates a stored entry's extended key, as another writer would.
func (f *fakeFiler) bumpExtended(p, key, val string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[p].Extended[key] = []byte(val)
}

func (f *fakeFiler) allWritten() []*filerpb.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*filerpb.Entry(nil), f.written...)
}

// fakeBlobs is an in-memory needle store.
type fakeBlobs struct {
	mu      sync.Mutex
	data    map[string][]byte
	next    int
	puts    int
	gets    int
	failPut func(n int) error
}

func newFakeBlobs() *fakeBlobs { return &fakeBlobs{data: map[string][]byte{}} }

func (b *fakeBlobs) Put(_ context.Context, d []byte) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.puts++
	if b.failPut != nil {
		if err := b.failPut(b.puts); err != nil {
			return "", err
		}
	}
	b.next++
	fid := fmt.Sprintf("3,%08x", b.next)
	b.data[fid] = append([]byte(nil), d...)
	return fid, nil
}

func (b *fakeBlobs) Get(_ context.Context, fid string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gets++
	d, ok := b.data[fid]
	if !ok {
		return nil, fmt.Errorf("fake: no such fid %s: %w", fid, ErrBlobNotFound)
	}
	return append([]byte(nil), d...), nil
}

func (b *fakeBlobs) counts() (puts, gets, stored int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.puts, b.gets, len(b.data)
}
