package storage

// Impl-3 COW map. Pages and roots are immutable once written; their extents
// are reused only after no live root references them
// (chunk_map_extents_v3.go), so neither slot root nor a pending candidate can
// lose a dependency.
import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const mapPageDomainV3 = "swblock:chunk-map:page:v1\x00"
const mapRootDomainV3 = "swblock:chunk-map:root:v1\x00"

var ErrMapInvalid = errors.New("storage: v3 chunk map invalid")

type chunkLSNEntryV3 struct {
	LSN     uint64
	Unknown bool
}
type mapPageRefV3 struct {
	Offset uint64
	Hash   [32]byte
}
type mapShapeV3 struct{ chunks, pages, raw, aligned uint64 }

func mapShapeForV3(d descriptorV3, cb uint64) (mapShapeV3, error) {
	var s mapShapeV3
	if d.BlockSize == 0 || d.VolumeSize == 0 || d.VolumeSize%uint64(d.BlockSize) != 0 || cb == 0 {
		return s, ErrMapInvalid
	}
	nb := d.VolumeSize / uint64(d.BlockSize)
	s.chunks = nb / cb
	if nb%cb != 0 {
		s.chunks++
	}
	s.pages = s.chunks / 250
	if s.chunks%250 != 0 {
		s.pages++
	}
	if s.pages == 0 || s.pages > (counterMaxV3-120)/40 {
		return s, ErrCounterExhausted
	}
	s.raw = 120 + 40*s.pages
	if s.raw > uint64(math.MaxInt64)-4095 {
		return s, ErrCounterExhausted
	}
	s.aligned = (s.raw + 4095) &^ uint64(4095)
	if s.aligned > uint64(int(^uint(0)>>1)) {
		return s, ErrCounterExhausted
	}
	return s, nil
}
func mapZeroV3(b []byte) bool { return firstNonZero(b) < 0 }

func mapDigestV3(domain string, b []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(domain))
	h.Write(b)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
func encodeMapPageV3(d descriptorV3, g, index uint64, es []chunkLSNEntryV3) ([]byte, error) {
	if g > counterMaxV3 {
		return nil, ErrCounterExhausted
	}
	if g == 0 || len(es) == 0 || len(es) > 250 {
		return nil, ErrMapInvalid
	}
	b := make([]byte, 4096)
	copy(b, "CLM3")
	le := binary.LittleEndian
	le.PutUint16(b[4:], 1)
	copy(b[8:], d.UUID[:])
	copy(b[24:], d.MapLineage[:])
	le.PutUint64(b[40:], g)
	le.PutUint64(b[48:], index)
	le.PutUint32(b[56:], uint32(len(es)))
	for i, e := range es {
		if e.LSN > counterMaxV3 {
			return nil, ErrCounterExhausted
		}
		le.PutUint64(b[64+16*i:], e.LSN)
		if e.Unknown {
			b[72+16*i] = 1
		}
	}
	hash := mapDigestV3(mapPageDomainV3, b[:4064])
	copy(b[4064:], hash[:])
	return b, nil
}
func encodeMapRootV3(d descriptorV3, g, cb, C uint64, refs []mapPageRefV3) ([]byte, error) {
	s, e := mapShapeForV3(d, cb)
	if e != nil {
		return nil, e
	}
	if g > counterMaxV3 || C > counterMaxV3 {
		return nil, ErrCounterExhausted
	}
	if g == 0 || uint64(len(refs)) != s.pages {
		return nil, ErrMapInvalid
	}
	b := make([]byte, int(s.aligned))
	copy(b, "CLR3")
	le := binary.LittleEndian
	le.PutUint16(b[4:], 1)
	copy(b[8:], d.UUID[:])
	copy(b[24:], d.MapLineage[:])
	le.PutUint64(b[40:], g)
	le.PutUint64(b[48:], s.chunks)
	le.PutUint64(b[56:], cb)
	le.PutUint64(b[64:], C)
	le.PutUint64(b[72:], s.pages)
	le.PutUint64(b[80:], s.raw)
	for i, r := range refs {
		if r.Offset%4096 != 0 {
			return nil, ErrMapInvalid
		}
		off := 88 + 40*i
		le.PutUint64(b[off:], r.Offset)
		copy(b[off+8:], r.Hash[:])
	}
	h := mapDigestV3(mapRootDomainV3, b[:s.raw-32])
	copy(b[s.raw-32:], h[:])
	return b, nil
}
func mapReadAtV3(f *os.File, off, n, size uint64) ([]byte, error) {
	if off%4096 != 0 || off > size || n > size-off || off > math.MaxInt64 || n > uint64(int(^uint(0)>>1)) {
		return nil, ErrMapInvalid
	}
	b := make([]byte, int(n))
	if _, e := f.ReadAt(b, int64(off)); e != nil {
		return nil, e
	}
	return b, nil
}

// readChunkLSNMapV3 checks the descriptor tuple and every immutable dependency
// before returning any entries. No corrupt root is treated as an empty map.
func readChunkLSNMapV3(f *os.File, d descriptorV3, cb uint64) ([]chunkLSNEntryV3, []mapPageRefV3, error) {
	s, e := mapShapeForV3(d, cb)
	if e != nil {
		return nil, nil, e
	}
	if d.MapGeneration == 0 || d.MapGeneration > counterMaxV3 || d.MapCoveredCheckpoint > counterMaxV3 || d.UUID == [16]byte{} || d.MapLineage == [16]byte{} {
		return nil, nil, ErrMapInvalid
	}
	// Reject an impossible entry allocation before root allocation or file I/O.
	if s.chunks > uint64(int(^uint(0)>>1))/16 {
		return nil, nil, ErrMapInvalid
	}
	info, e := f.Stat()
	if e != nil {
		return nil, nil, e
	}
	if info.Size() < 0 {
		return nil, nil, ErrMapInvalid
	}
	size := uint64(info.Size())
	b, e := mapReadAtV3(f, d.MapRootLocator, s.aligned, size)
	if e != nil {
		return nil, nil, e
	}
	le := binary.LittleEndian
	if string(b[:4]) != "CLR3" || le.Uint16(b[4:]) != 1 || le.Uint16(b[6:]) != 0 || !bytes.Equal(b[8:24], d.UUID[:]) || !bytes.Equal(b[24:40], d.MapLineage[:]) || le.Uint64(b[40:]) != d.MapGeneration || d.MapGeneration == 0 || le.Uint64(b[48:]) != s.chunks || le.Uint64(b[56:]) != cb || le.Uint64(b[64:]) != d.MapCoveredCheckpoint || le.Uint64(b[72:]) != s.pages || le.Uint64(b[80:]) != s.raw || d.MapFormatID != 1 {
		return nil, nil, ErrMapInvalid
	}
	hash := mapDigestV3(mapRootDomainV3, b[:s.raw-32])
	if hash != d.MapRootHash || !bytes.Equal(hash[:], b[s.raw-32:s.raw]) || !mapZeroV3(b[s.raw:]) {
		return nil, nil, ErrMapInvalid
	}
	// Root is file-bounded before page and entry allocations; every page needs
	// 4096 distinct bytes. Reject impossible geometry before allocating entries.
	if s.pages > size/4096 {
		return nil, nil, ErrMapInvalid
	}
	entries := make([]chunkLSNEntryV3, 0, int(s.chunks))
	refs := make([]mapPageRefV3, 0, int(s.pages))
	seen := make(map[uint64]bool)
	for j := uint64(0); j < s.pages; j++ {
		pos := 88 + 40*j
		r := mapPageRefV3{Offset: le.Uint64(b[pos:])}
		copy(r.Hash[:], b[pos+8:pos+40])
		if seen[r.Offset] || (r.Offset < d.MapRootLocator+s.aligned && r.Offset+4096 > d.MapRootLocator) {
			return nil, nil, ErrMapInvalid
		}
		seen[r.Offset] = true
		p, e := mapReadAtV3(f, r.Offset, 4096, size)
		if e != nil {
			return nil, nil, e
		}
		count := min(uint64(250), s.chunks-j*250)
		pg := le.Uint64(p[40:])
		ph := mapDigestV3(mapPageDomainV3, p[:4064])
		if string(p[:4]) != "CLM3" || le.Uint16(p[4:]) != 1 || le.Uint16(p[6:]) != 0 || !bytes.Equal(p[8:24], d.UUID[:]) || !bytes.Equal(p[24:40], d.MapLineage[:]) || pg == 0 || pg > d.MapGeneration || le.Uint64(p[48:]) != j || uint64(le.Uint32(p[56:])) != count || !mapZeroV3(p[60:64]) || ph != r.Hash || !bytes.Equal(ph[:], p[4064:]) || !mapZeroV3(p[64+16*count:4064]) {
			return nil, nil, ErrMapInvalid
		}
		for i := uint64(0); i < count; i++ {
			q := p[64+16*i : 80+16*i]
			lsn := le.Uint64(q)
			if lsn > counterMaxV3 || q[8]&^byte(1) != 0 || !mapZeroV3(q[9:]) {
				return nil, nil, ErrMapInvalid
			}
			entries = append(entries, chunkLSNEntryV3{LSN: lsn, Unknown: q[8] == 1})
		}
		refs = append(refs, r)
	}
	return entries, refs, nil
}

type chunkMapCandidateV3 struct {
	owner                           *chunkLSNMapV3
	generation, covered, rootOffset uint64
	hash                            [32]byte
	refs                            []mapPageRefV3
	entries                         []chunkLSNEntryV3
}

func (c *chunkMapCandidateV3) descriptor(d descriptorV3) descriptorV3 {
	d.MapFormatID = 1
	d.MapGeneration = c.generation
	d.MapCoveredCheckpoint = c.covered
	d.MapRootLocator = c.rootOffset
	d.MapRootHash = c.hash
	return d
}

type chunkLSNMapV3 struct {
	mu               sync.Mutex
	fd               *os.File
	identity         descriptorV3
	chunkBlocks      uint64
	failed           error
	lastGeneration   uint64
	syncFile         func() error
	syncDirectory    func() error
	latchPersistence func(error)
	// liveMu guards live (chunk_map_extents_v3.go); never held across I/O.
	liveMu sync.Mutex
	live   chunkMapLiveV3
}

func (m *chunkLSNMapV3) initLiveV3() {
	m.live.roots = map[mapRootKeyV3]mapRootExtentsV3{}
	m.live.inflight = map[mapRootKeyV3]bool{}
}

func createChunkLSNMapV3(path string, d descriptorV3, cb uint64) (*chunkLSNMapV3, error) {
	if _, e := mapShapeForV3(d, cb); e != nil {
		return nil, e
	}
	if d.UUID == [16]byte{} || d.MapLineage == [16]byte{} {
		return nil, ErrMapInvalid
	}
	f, e := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, e
	}
	m := &chunkLSNMapV3{fd: f, identity: d, chunkBlocks: cb}
	m.initLiveV3()
	m.syncFile = f.Sync
	m.syncDirectory = func() error { return syncDir(filepath.Dir(path)) }
	if e = m.syncFile(); e == nil {
		e = m.syncDirectory()
	}
	if e != nil {
		f.Close()
		return nil, e
	}
	return m, nil
}

// Open validates the selected generation before permitting any append. The
// caller must also validate the fallback descriptor before serving/retiring.
func openChunkLSNMapV3(path string, d descriptorV3, cb uint64) (*chunkLSNMapV3, *chunkMapCandidateV3, error) {
	f, e := os.OpenFile(path, os.O_RDWR, 0)
	if e != nil {
		return nil, nil, e
	}
	es, refs, e := readChunkLSNMapV3(f, d, cb)
	if e != nil {
		f.Close()
		return nil, nil, e
	}
	m := &chunkLSNMapV3{fd: f, identity: d, chunkBlocks: cb, lastGeneration: d.MapGeneration}
	m.initLiveV3()
	if s, e := mapShapeForV3(d, cb); e == nil {
		m.registerRootV3(d.MapGeneration, d.MapRootLocator, s.aligned, refs)
	}
	if info, e := f.Stat(); e == nil && info.Size() > 0 {
		m.live.fileBytes.Store(uint64(info.Size()))
	}
	m.syncFile = f.Sync
	m.syncDirectory = func() error { return syncDir(filepath.Dir(path)) }
	c := &chunkMapCandidateV3{owner: m, generation: d.MapGeneration, covered: d.MapCoveredCheckpoint, rootOffset: d.MapRootLocator, hash: d.MapRootHash, refs: refs, entries: es}
	return m, c, nil
}

// Prepare persists pages -> page fsync -> root -> root fsync -> directory fsync.
// It returns a pinned tuple, never a descriptor publication or reuse authority.
// Preflight and Stat errors do not latch. Once the append transaction starts,
// an error latches and refuses subsequent persistence I/O until reopen.
// New pages and the root go into extents no live root references
// (chunk_map_extents_v3.go); the candidate stays live until a publication
// names it or supersedes it.
func (m *chunkLSNMapV3) Prepare(g, C uint64, entries []chunkLSNEntryV3, prior *chunkMapCandidateV3) (out *chunkMapCandidateV3, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.prepareLockedV3(g, C, entries, prior)
}

// Generation reservation and persistence share the existing sidecar mutex.
// Concurrent preparers cannot select the same generation; no new lock or
// scheduler is introduced. A candidate known never to reach a slot is
// released (abandonCandidateV3); an uncertain one stays pinned until reopen.
func (m *chunkLSNMapV3) prepareNextV3(C uint64, entries []chunkLSNEntryV3, prior *chunkMapCandidateV3) (*chunkMapCandidateV3, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failed != nil {
		return nil, m.failed
	}
	if m.lastGeneration >= counterMaxV3 {
		return nil, ErrCounterExhausted
	}
	return m.prepareLockedV3(m.lastGeneration+1, C, entries, prior)
}
func (m *chunkLSNMapV3) prepareLockedV3(g, C uint64, entries []chunkLSNEntryV3, prior *chunkMapCandidateV3) (out *chunkMapCandidateV3, err error) {
	if m.failed != nil {
		return nil, m.failed
	}
	s, e := mapShapeForV3(m.identity, m.chunkBlocks)
	if e != nil {
		return nil, e
	}
	if g > counterMaxV3 || C > counterMaxV3 {
		return nil, ErrCounterExhausted
	}
	if uint64(len(entries)) != s.chunks || g == 0 || g <= m.lastGeneration {
		return nil, ErrMapInvalid
	}
	if prior != nil && (prior.owner != m || prior.generation >= g) {
		return nil, ErrMapInvalid
	}
	// Validate every entry before any I/O.
	for _, v := range entries {
		if v.LSN > counterMaxV3 {
			return nil, ErrCounterExhausted
		}
	}
	info, e := m.fd.Stat()
	if e != nil {
		return nil, e
	}
	if info.Size() < 0 {
		return nil, ErrMapInvalid
	}
	fileBytes := uint64(info.Size())
	if fileBytes > math.MaxInt64-4095 {
		return nil, ErrCounterExhausted
	}
	// The live set: every page and root extent a live root references.
	// Roots are only added here (under m.mu); the publisher's report only
	// removes them, so this snapshot is a superset for the whole prepare.
	used, liveBytes := usedPagesV3(m.liveSnapshotV3())
	defer func() {
		if err != nil {
			m.failed = err
			if m.latchPersistence != nil {
				m.latchPersistence(err)
			}
		}
	}()
	// Bounded file: truncate the free tail once the file exceeds 4 x the
	// live bytes. Nothing at or below the highest live page is touched, so
	// a root the fallback slot names stays readable.
	if liveBytes > 0 && fileBytes > 4*liveBytes {
		keep := (used[len(used)-1] + 1) * 4096
		if keep < fileBytes {
			if e := m.fd.Truncate(int64(keep)); e != nil {
				return nil, e
			}
			fileBytes = keep
			m.live.fileBytes.Store(keep) // the file is shorter even if the fsync fails
			m.live.truncations.Add(1)
			if e := m.syncFile(); e != nil {
				return nil, e
			}
		}
	}
	alloc := newExtentAllocatorV3(used, fileBytes)
	write := func(b []byte) (uint64, error) {
		at := alloc.alloc(uint64(len(b)+4095) / 4096)
		if uint64(len(b)) > math.MaxInt64-at {
			return 0, ErrCounterExhausted
		}
		n, e := m.fd.WriteAt(b, int64(at))
		if end := at + uint64(n); end > m.live.fileBytes.Load() {
			m.live.fileBytes.Store(end) // also on a failed or short write
		}
		if e == nil && n != len(b) {
			e = io.ErrShortWrite
		}
		if e != nil {
			return 0, e
		}
		return at, nil
	}
	refs := make([]mapPageRefV3, s.pages)
	for j := uint64(0); j < s.pages; j++ {
		start := j * 250
		end := min(start+250, s.chunks)
		same := prior != nil && len(prior.entries) == len(entries)
		if same {
			for i := start; i < end; i++ {
				if prior.entries[i] != entries[i] {
					same = false
					break
				}
			}
		}
		if same {
			refs[j] = prior.refs[j]
			continue
		}
		p, e := encodeMapPageV3(m.identity, g, j, entries[start:end])
		if e != nil {
			return nil, e
		}
		at, e := write(p)
		if e != nil {
			return nil, e
		}
		refs[j] = mapPageRefV3{Offset: at, Hash: mapDigestV3(mapPageDomainV3, p[:4064])}
	}
	if e = m.syncFile(); e != nil {
		return nil, e
	}
	root, e := encodeMapRootV3(m.identity, g, m.chunkBlocks, C, refs)
	if e != nil {
		return nil, e
	}
	at, e := write(root)
	if e != nil {
		return nil, e
	}
	if e = m.syncFile(); e != nil {
		return nil, e
	}
	if e = m.syncDirectory(); e != nil {
		return nil, e
	}
	m.lastGeneration = g
	out = &chunkMapCandidateV3{owner: m, generation: g, covered: C, rootOffset: at, hash: mapDigestV3(mapRootDomainV3, root[:s.raw-32]), refs: refs, entries: append([]chunkLSNEntryV3(nil), entries...)}
	// The candidate is live (in flight) until a publication names it or
	// supersedes it.
	x := rootExtentsV3(at, s.aligned, refs)
	m.liveMu.Lock()
	k := mapRootKeyV3{at, g}
	m.live.roots[k] = x
	m.live.inflight[k] = true
	m.liveMu.Unlock()
	m.live.fileBytes.Store(max(alloc.end*4096, fileBytes))
	return out, nil
}
func (m *chunkLSNMapV3) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed = fmt.Errorf("%w: closed", ErrMapInvalid)
	return m.fd.Close()
}
