package storage

// MemChunkRemote is an in-memory chunkRemote for development, demos and
// provider tests: immutable keys (a key that ever existed is never
// rewritten), SHA-256-checked chunk refs, and the {gen, epoch} HEAD CAS.
// It has no fault injection; the contract tests use their own fake.

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
)

type MemChunkRemote struct {
	mu   sync.Mutex
	objs map[string][]byte
	ever map[string]bool
	head chunkHead
	puts int
}

func NewMemChunkRemote() *MemChunkRemote {
	return &MemChunkRemote{objs: map[string][]byte{}, ever: map[string]bool{}}
}

func (m *MemChunkRemote) Put(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ever[key] {
		return errChunkKeyExists
	}
	m.objs[key] = append([]byte(nil), data...)
	m.ever[key] = true
	m.puts++
	return nil
}

func (m *MemChunkRemote) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("storage: mem remote: no key %q", key)
	}
	return append([]byte(nil), b...), nil
}

func (m *MemChunkRemote) GetHead(ctx context.Context) (chunkHead, error) {
	if err := ctx.Err(); err != nil {
		return chunkHead{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return copyHead(m.head), nil
}

func (m *MemChunkRemote) CASHead(ctx context.Context, expected chunkHeadVersion, next chunkHead) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if expected.Gen != m.head.Gen || expected.Epoch != m.head.Epoch {
		return 0, errChunkHeadPrecondition
	}
	for idx, ref := range next.Chunks {
		b, ok := m.objs[ref.Key]
		if !ok || sha256.Sum256(b) != ref.SHA256 {
			return 0, fmt.Errorf("storage: mem remote: HEAD chunk %d ref %q is not stored with that hash", idx, ref.Key)
		}
	}
	installed := copyHead(next)
	installed.Gen = m.head.Gen + 1
	m.head = installed
	return installed.Gen, nil
}

func (m *MemChunkRemote) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	return nil
}

// Puts reports how many chunks were stored (for tests and demos).
func (m *MemChunkRemote) Puts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.puts
}

var _ chunkRemote = (*MemChunkRemote)(nil)
