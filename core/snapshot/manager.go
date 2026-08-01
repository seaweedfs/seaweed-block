package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

type Manager struct {
	mu            sync.Mutex
	root          string
	archivesDir   string
	recordsDir    string
	byID          map[string]Record
	byName        map[string]string
	activeReaders map[string]int
	now           func() time.Time
}

func OpenManager(root string) (*Manager, error) {
	if root == "" {
		return nil, fmt.Errorf("snapshot: root is required")
	}
	m := &Manager{
		root:          root,
		archivesDir:   filepath.Join(root, "archives"),
		recordsDir:    filepath.Join(root, "records"),
		byID:          make(map[string]Record),
		byName:        make(map[string]string),
		activeReaders: make(map[string]int),
		now:           time.Now,
	}
	for _, dir := range []string{m.root, m.archivesDir, m.recordsDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("snapshot: mkdir %q: %w", dir, err)
		}
	}
	if err := m.recoverCatalog(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) Create(ctx context.Context, req CreateRequest, source storage.SnapshotSource) (Record, error) {
	if err := validateCreateRequest(req); err != nil {
		return Record{}, err
	}
	if source == nil {
		return Record{}, fmt.Errorf("snapshot: source is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.byName[req.Name]; ok {
		existing := m.byID[id]
		if existing.SourceVolumeID != req.SourceVolumeID {
			return Record{}, ErrNameConflict
		}
		return existing, nil
	}

	id := snapshotID(req.Name, req.SourceVolumeID)
	if existing, ok := m.byID[id]; ok {
		if existing.Name != req.Name || existing.SourceVolumeID != req.SourceVolumeID {
			return Record{}, ErrNameConflict
		}
		return existing, nil
	}
	tmp, err := os.CreateTemp(m.archivesDir, ".tmp-"+id+"-*")
	if err != nil {
		return Record{}, fmt.Errorf("snapshot: create archive temp: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return Record{}, fmt.Errorf("snapshot: close archive temp: %w", err)
	}
	if err := os.Remove(tmpPath); err != nil {
		return Record{}, fmt.Errorf("snapshot: prepare archive temp: %w", err)
	}
	defer os.Remove(tmpPath)
	cut, archiveBytes, digest, err := writeArchive(ctx, tmpPath, source)
	if err != nil {
		return Record{}, err
	}
	finalArchive := m.archivePath(id)
	if err := os.Rename(tmpPath, finalArchive); err != nil {
		return Record{}, fmt.Errorf("snapshot: publish archive: %w", err)
	}
	archivePublished := true
	defer func() {
		if archivePublished {
			_ = os.Remove(finalArchive)
		}
	}()
	if err := syncDir(m.archivesDir); err != nil {
		return Record{}, err
	}
	sizeBytes := uint64(cut.NumBlocks) * uint64(cut.BlockSize)
	rec := Record{
		SnapshotID:     id,
		Name:           req.Name,
		SourceVolumeID: req.SourceVolumeID,
		CreatedAt:      m.now().UTC(),
		State:          StateReady,
		Frontier:       cut.Frontier,
		SizeBytes:      sizeBytes,
		NumBlocks:      cut.NumBlocks,
		BlockSize:      cut.BlockSize,
		RecordCount:    cut.BlockCount,
		DataBytes:      cut.DataBytes,
		ArchiveBytes:   archiveBytes,
		ArchiveSHA256:  digest,
	}
	if err := m.writeRecord(rec); err != nil {
		_ = os.Remove(m.recordPath(id))
		_ = syncDir(m.recordsDir)
		return Record{}, err
	}
	archivePublished = false
	m.byID[id] = rec
	m.byName[req.Name] = id
	return rec, nil
}

func (m *Manager) Get(snapshotID string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.byID[snapshotID]
	return rec, ok
}

func (m *Manager) GetByName(name string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.byName[name]
	if !ok {
		return Record{}, false
	}
	return m.byID[id], true
}

func (m *Manager) List(sourceVolumeID string) []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, 0, len(m.byID))
	for _, rec := range m.byID {
		if sourceVolumeID == "" || rec.SourceVolumeID == sourceVolumeID {
			out = append(out, rec)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].SnapshotID < out[j].SnapshotID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (m *Manager) ReadBlocks(ctx context.Context, snapshotID string, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	rec, release, err := m.beginRead(snapshotID)
	if err != nil {
		return storage.SnapshotCut{}, err
	}
	defer release()
	return readArchive(ctx, m.archivePath(snapshotID), rec.ArchiveSHA256, sink)
}

// StreamArchive writes one immutable archive while holding its catalog read
// lease. The bytes and digest are revalidated on every transfer so a damaged
// archive is never reported as a successful restore source.
func (m *Manager) StreamArchive(ctx context.Context, snapshotID string, w io.Writer) (Record, error) {
	if w == nil {
		return Record{}, fmt.Errorf("%w: archive writer is required", ErrInvalidRequest)
	}
	rec, release, err := m.beginRead(snapshotID)
	if err != nil {
		return Record{}, err
	}
	defer release()

	f, err := os.Open(m.archivePath(snapshotID))
	if err != nil {
		return Record{}, fmt.Errorf("snapshot: open archive: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Record{}, fmt.Errorf("snapshot: stat archive: %w", err)
	}
	if info.Size() != rec.ArchiveBytes {
		return Record{}, fmt.Errorf("%w: archive size got %d want %d", ErrArchiveCorrupt, info.Size(), rec.ArchiveBytes)
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(w, h), contextReader{ctx: ctx, r: f}); err != nil {
		return Record{}, fmt.Errorf("snapshot: stream archive: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != rec.ArchiveSHA256 {
		return Record{}, fmt.Errorf("%w: digest got %s want %s", ErrArchiveCorrupt, got, rec.ArchiveSHA256)
	}
	return rec, nil
}

// ImportArchive verifies an immutable archive before atomically adding its
// original snapshot identity to this catalog. It never reads a live volume and
// never replaces an existing snapshot or name binding.
func (m *Manager) ImportArchive(ctx context.Context, rec Record, r io.Reader) (Record, error) {
	if r == nil || validateImportedRecord(rec) != nil {
		return Record{}, fmt.Errorf("%w: invalid snapshot import", ErrInvalidRequest)
	}
	stagedPath, err := stageVerifiedArchive(ctx, r, rec, m.archivesDir)
	if err != nil {
		return Record{}, err
	}
	defer os.Remove(stagedPath)

	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.byID[rec.SnapshotID]; ok {
		if sameCatalogRecord(existing, rec) {
			return existing, nil
		}
		return Record{}, ErrRestoreConflict
	}
	if existingID, ok := m.byName[rec.Name]; ok && existingID != rec.SnapshotID {
		return Record{}, ErrNameConflict
	}
	finalArchive := m.archivePath(rec.SnapshotID)
	if _, err := os.Stat(finalArchive); err == nil {
		return Record{}, fmt.Errorf("%w: snapshot archive already exists", ErrRestoreConflict)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Record{}, fmt.Errorf("snapshot: stat import archive: %w", err)
	}
	if err := os.Rename(stagedPath, finalArchive); err != nil {
		return Record{}, fmt.Errorf("snapshot: publish imported archive: %w", err)
	}
	archivePublished := true
	defer func() {
		if archivePublished {
			_ = os.Remove(finalArchive)
		}
	}()
	if err := syncDir(m.archivesDir); err != nil {
		return Record{}, err
	}
	if err := m.writeRecord(rec); err != nil {
		_ = os.Remove(m.recordPath(rec.SnapshotID))
		_ = syncDir(m.recordsDir)
		return Record{}, err
	}
	archivePublished = false
	m.byID[rec.SnapshotID] = rec
	m.byName[rec.Name] = rec.SnapshotID
	return rec, nil
}

func (m *Manager) beginRead(snapshotID string) (Record, func(), error) {
	m.mu.Lock()
	rec, ok := m.byID[snapshotID]
	if !ok {
		m.mu.Unlock()
		return Record{}, nil, ErrNotFound
	}
	m.activeReaders[snapshotID]++
	m.mu.Unlock()
	release := func() {
		m.mu.Lock()
		m.activeReaders[snapshotID]--
		if m.activeReaders[snapshotID] == 0 {
			delete(m.activeReaders, snapshotID)
		}
		m.mu.Unlock()
	}
	return rec, release, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func (m *Manager) Delete(snapshotID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.byID[snapshotID]
	if !ok {
		return nil
	}
	if m.activeReaders[snapshotID] > 0 {
		return ErrInUse
	}
	if err := os.Remove(m.recordPath(snapshotID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("snapshot: remove record: %w", err)
	}
	if err := syncDir(m.recordsDir); err != nil {
		return err
	}
	delete(m.byID, snapshotID)
	delete(m.byName, rec.Name)
	if err := os.Remove(m.archivePath(snapshotID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("snapshot: remove archive: %w", err)
	}
	return syncDir(m.archivesDir)
}

func (m *Manager) recoverCatalog() error {
	if err := removeTemporaryFiles(m.archivesDir); err != nil {
		return err
	}
	if err := removeTemporaryFiles(m.recordsDir); err != nil {
		return err
	}
	entries, err := os.ReadDir(m.recordsDir)
	if err != nil {
		return fmt.Errorf("snapshot: read records: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(m.recordsDir, entry.Name()))
		if err != nil {
			return fmt.Errorf("snapshot: read record %q: %w", entry.Name(), err)
		}
		var rec Record
		if err := json.Unmarshal(raw, &rec); err != nil {
			return fmt.Errorf("snapshot: parse record %q: %w", entry.Name(), err)
		}
		if err := validateRecord(rec); err != nil {
			return fmt.Errorf("snapshot: invalid record %q: %w", entry.Name(), err)
		}
		if entry.Name() != rec.SnapshotID+".json" {
			return fmt.Errorf("snapshot: record filename %q does not match id %q", entry.Name(), rec.SnapshotID)
		}
		if _, exists := m.byID[rec.SnapshotID]; exists {
			return fmt.Errorf("snapshot: duplicate id %q", rec.SnapshotID)
		}
		if _, exists := m.byName[rec.Name]; exists {
			return fmt.Errorf("snapshot: duplicate name %q", rec.Name)
		}
		cut, err := readArchive(context.Background(), m.archivePath(rec.SnapshotID), rec.ArchiveSHA256, nil)
		if err != nil {
			return fmt.Errorf("snapshot: validate archive %q: %w", rec.SnapshotID, err)
		}
		if cut.Frontier != rec.Frontier || cut.NumBlocks != rec.NumBlocks || cut.BlockSize != rec.BlockSize || cut.BlockCount != rec.RecordCount || cut.DataBytes != rec.DataBytes {
			return fmt.Errorf("%w: catalog/archive mismatch for %q", ErrArchiveCorrupt, rec.SnapshotID)
		}
		info, err := os.Stat(m.archivePath(rec.SnapshotID))
		if err != nil || info.Size() != rec.ArchiveBytes {
			return fmt.Errorf("%w: archive size mismatch for %q", ErrArchiveCorrupt, rec.SnapshotID)
		}
		m.byID[rec.SnapshotID] = rec
		m.byName[rec.Name] = rec.SnapshotID
	}
	return m.removeOrphanArchives()
}

func (m *Manager) removeOrphanArchives() error {
	entries, err := os.ReadDir(m.archivesDir)
	if err != nil {
		return fmt.Errorf("snapshot: read archives: %w", err)
	}
	removed := false
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sbsnap" {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".sbsnap")
		if _, ok := m.byID[id]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(m.archivesDir, entry.Name())); err != nil {
			return fmt.Errorf("snapshot: remove orphan archive %q: %w", entry.Name(), err)
		}
		removed = true
	}
	if removed {
		return syncDir(m.archivesDir)
	}
	return nil
}

func (m *Manager) writeRecord(rec Record) error {
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: marshal record: %w", err)
	}
	tmp, err := os.CreateTemp(m.recordsDir, ".tmp-"+rec.SnapshotID+"-*")
	if err != nil {
		return fmt.Errorf("snapshot: create record temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("snapshot: write record: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("snapshot: fsync record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("snapshot: close record: %w", err)
	}
	if err := os.Rename(tmpPath, m.recordPath(rec.SnapshotID)); err != nil {
		return fmt.Errorf("snapshot: publish record: %w", err)
	}
	return syncDir(m.recordsDir)
}

func (m *Manager) archivePath(id string) string {
	return filepath.Join(m.archivesDir, id+".sbsnap")
}

func (m *Manager) recordPath(id string) string {
	return filepath.Join(m.recordsDir, id+".json")
}

func snapshotID(name, source string) string {
	digest := sha256.Sum256([]byte(source + "\x00" + name))
	return "snap-" + hex.EncodeToString(digest[:16])
}

func validateCreateRequest(req CreateRequest) error {
	if req.Name == "" || len(req.Name) > 128 {
		return fmt.Errorf("%w: name must contain 1-128 bytes", ErrInvalidRequest)
	}
	if req.SourceVolumeID == "" || len(req.SourceVolumeID) > 128 {
		return fmt.Errorf("%w: source volume id must contain 1-128 bytes", ErrInvalidRequest)
	}
	return nil
}

func validateRecord(rec Record) error {
	if rec.SnapshotID == "" || rec.Name == "" || rec.SourceVolumeID == "" || rec.State != StateReady || rec.BlockSize <= 0 || rec.BlockSize > maxArchiveBlockSize || rec.NumBlocks == 0 || rec.RecordCount > uint64(rec.NumBlocks) || rec.SizeBytes != uint64(rec.NumBlocks)*uint64(rec.BlockSize) || rec.DataBytes != rec.RecordCount*uint64(rec.BlockSize) || rec.ArchiveBytes <= 0 || len(rec.ArchiveSHA256) != sha256.Size*2 {
		return fmt.Errorf("invalid ready snapshot record")
	}
	if _, err := hex.DecodeString(rec.ArchiveSHA256); err != nil {
		return fmt.Errorf("invalid ready snapshot record")
	}
	wantArchiveBytes, err := archiveSize(storage.SnapshotCut{BlockSize: rec.BlockSize, NumBlocks: rec.NumBlocks, BlockCount: rec.RecordCount, DataBytes: rec.DataBytes})
	if err != nil || wantArchiveBytes != rec.ArchiveBytes {
		return fmt.Errorf("invalid ready snapshot record")
	}
	return nil
}

func sameCatalogRecord(a, b Record) bool {
	return sameRestoreRecord(a, b) && a.Name == b.Name && a.State == b.State && a.CreatedAt.Equal(b.CreatedAt)
}

func validateImportedRecord(rec Record) error {
	if validateRecord(rec) != nil || validateCreateRequest(CreateRequest{Name: rec.Name, SourceVolumeID: rec.SourceVolumeID}) != nil || rec.SnapshotID != snapshotID(rec.Name, rec.SourceVolumeID) {
		return fmt.Errorf("invalid imported snapshot record")
	}
	return nil
}

// ValidatePortableRecord applies the complete catalog/import contract to a
// record received across a trusted transport boundary.
func ValidatePortableRecord(rec Record) error {
	return validateImportedRecord(rec)
}

// SamePortableRecord compares every persisted snapshot catalog field.
func SamePortableRecord(a, b Record) bool {
	return sameCatalogRecord(a, b)
}

func removeTemporaryFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("snapshot: read temp dir %q: %w", dir, err)
	}
	removed := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".tmp-") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("snapshot: remove temp %q: %w", entry.Name(), err)
		}
		removed = true
	}
	if removed {
		return syncDir(dir)
	}
	return nil
}

func syncDir(dir string) error {
	// Windows does not provide the directory fsync handle semantics used by
	// the Linux product path. File fsync and atomic rename are still tested
	// locally; the release gate verifies directory durability on Linux.
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("snapshot: open directory for fsync %q: %w", dir, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("snapshot: fsync directory %q: %w", dir, err)
	}
	return nil
}
