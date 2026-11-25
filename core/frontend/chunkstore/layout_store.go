package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

const (
	layoutGen     = "swb.layout.gen"
	layoutVolume  = "swb.layout.volume"
	layoutSession = "swb.layout.session"
	layoutMark    = "swb.layout.mark"
	layoutKind    = "swb.layout.kind"
)

// ErrLegacyStore refuses the removed key/intent format rather than converting it.
var ErrLegacyStore = errors.New("chunkstore: legacy store format; recreate the volume")

// LayoutStore maps the kernel's single-entry writes to ordinary filer files.
// Objects carry their immutable bytes as file chunks; metadata updates preserve
// those chunks, and deletion delegates their reclamation to the filer.
type LayoutStore struct {
	f    Filer
	b    Blobs
	root string
	id   layout.VolumeID
}

func NewLayoutStore(f Filer, b Blobs, root string, id layout.VolumeID) (*LayoutStore, error) {
	if f == nil || b == nil || id.IsZero() || !path.IsAbs(root) || path.Clean(root) != root || root == "/" {
		return nil, errors.New("chunkstore: layout store needs filer, blobs, a clean volume root and volume id")
	}
	return &LayoutStore{f: f, b: b, root: root, id: id}, nil
}

var _ layout.HeadStore = (*LayoutStore)(nil)
var _ layout.ObjectStore = (*LayoutStore)(nil)

func (s *LayoutStore) ReadHead(ctx context.Context) (layout.Head, bool, error) {
	e, err := s.f.LookupEntry(ctx, s.root, headName)
	if errors.Is(err, ErrNotFound) {
		return layout.Head{}, false, nil
	}
	if err != nil {
		return layout.Head{}, false, err
	}
	if string(e.Extended[layoutKind]) != "head" {
		return layout.Head{}, false, ErrLegacyStore
	}
	if string(e.Extended[layoutVolume]) != s.id.String() {
		return layout.Head{}, false, layout.ErrIdentity
	}
	gen, err := strconv.ParseUint(string(e.Extended[layoutGen]), 10, 64)
	if err != nil || gen == 0 || strconv.FormatUint(gen, 10) != string(e.Extended[layoutGen]) || len(e.Chunks) != 0 {
		return layout.Head{}, false, fmt.Errorf("%w: invalid HEAD entry", layout.ErrCorrupt)
	}
	h, err := layout.DecodeHead(e.Content, gen, s.id)
	return h, err == nil, err
}

func (s *LayoutStore) UpdateHead(ctx context.Context, expectedGen uint64, next layout.Head) error {
	if next.VolumeID != s.id {
		return layout.ErrIdentity
	}
	if expectedGen == ^uint64(0) {
		return errors.New("chunkstore: HEAD generation exhausted")
	}
	raw, err := next.Encode()
	if err != nil {
		return err
	}
	e := layoutEntry(headName, raw)
	e.Extended = map[string][]byte{layoutKind: []byte("head"), layoutGen: []byte(strconv.FormatUint(expectedGen+1, 10)), layoutVolume: []byte(s.id.String())}
	if expectedGen == 0 {
		err = s.f.CreateEntry(ctx, s.root, e, true)
	} else {
		err = s.f.UpdateEntry(ctx, s.root, e, map[string][]byte{layoutGen: []byte(strconv.FormatUint(expectedGen, 10)), layoutVolume: []byte(s.id.String())})
	}
	return layoutError(err)
}

// Fetch reads a normal filer file, including a source volume's file reached by
// a restore root. The layout Reader validates the referenced volume identity.
func (s *LayoutStore) Fetch(ctx context.Context, p string) ([]byte, bool, error) {
	if !path.IsAbs(p) || path.Clean(p) != p || !strings.HasPrefix(p, path.Dir(s.root)+"/") {
		return nil, false, errors.New("chunkstore: object path outside the store base")
	}
	e, err := s.f.LookupEntry(ctx, path.Dir(p), path.Base(p))
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if e.IsDirectory {
		return nil, false, fmt.Errorf("%w: %s is a directory", layout.ErrCorrupt, p)
	}
	if len(e.Chunks) == 0 {
		return append([]byte(nil), e.Content...), true, nil
	}
	if len(e.Content) != 0 || len(e.Chunks) != 1 || e.Chunks[0].Offset != 0 || e.Chunks[0].FileId == "" {
		return nil, false, fmt.Errorf("%w: invalid file chunks for %s", layout.ErrCorrupt, p)
	}
	raw, err := s.b.Get(ctx, e.Chunks[0].FileId)
	if err != nil {
		return nil, false, err
	}
	if uint64(len(raw)) != e.Chunks[0].Size || e.GetAttributes().GetFileSize() != uint64(len(raw)) {
		return nil, false, fmt.Errorf("%w: short file %s", layout.ErrCorrupt, p)
	}
	return raw, true, nil
}

func (s *LayoutStore) object(rel string) (string, string, error) {
	if rel == "" || path.IsAbs(rel) || path.Clean(rel) != rel || strings.Contains(rel, "\\") {
		return "", "", layout.ErrMalformed
	}
	parts := strings.Split(rel, "/")
	valid := false
	switch {
	case len(parts) == 3 && parts[0] == layout.PoolDir:
		n, err := layout.ParseChunkName(parts[2])
		valid = err == nil && n.Path() == rel
	case len(parts) == 2 && parts[0] == layout.SegDir:
		n, err := layout.ParseSegName(parts[1])
		valid = err == nil && n.Path() == rel
	case len(parts) == 2 && parts[0] == layout.RootDir:
		n, err := layout.ParseRootName(parts[1])
		valid = err == nil && n.Path() == rel
	}
	if !valid {
		return "", "", layout.ErrMalformed
	}
	p := path.Join(s.root, rel)
	return path.Dir(p), path.Base(p), nil
}

func (s *LayoutStore) Stat(ctx context.Context, rel string) (layout.ObjectMeta, bool, error) {
	dir, name, err := s.object(rel)
	if err != nil {
		return layout.ObjectMeta{}, false, err
	}
	e, err := s.f.LookupEntry(ctx, dir, name)
	if errors.Is(err, ErrNotFound) {
		return layout.ObjectMeta{}, false, nil
	}
	if err != nil {
		return layout.ObjectMeta{}, false, err
	}
	id, err := layout.ParseVolumeID(string(e.Extended[layoutVolume]))
	if err != nil || id != s.id {
		return layout.ObjectMeta{}, false, layout.ErrIdentity
	}
	if e.IsDirectory || string(e.Extended[layoutKind]) != "object" || len(e.Extended[layoutSession]) == 0 {
		return layout.ObjectMeta{}, false, layout.ErrCorrupt
	}
	return layout.ObjectMeta{Volume: id, Session: string(e.Extended[layoutSession]), Mark: string(e.Extended[layoutMark])}, true, nil
}

func (s *LayoutStore) Create(ctx context.Context, rel string, raw []byte, session string) error {
	dir, name, err := s.object(rel)
	if err != nil {
		return err
	}
	if session == "" {
		return errors.New("chunkstore: object needs a writer session")
	}
	// All object names end in their bytes' SHA-256; do not create a name
	// for bytes it does not identify.
	if !strings.HasSuffix(name, layout.Sum(raw).String()) {
		return layout.ErrCorrupt
	}
	fid, err := s.b.Put(ctx, raw)
	if err != nil {
		return err
	}
	if fid == "" {
		return errors.New("chunkstore: empty uploaded file id")
	}
	e := layoutEntry(name, nil)
	e.Attributes.FileSize = uint64(len(raw))
	e.Chunks = []*filerpb.FileChunk{{FileId: fid, Offset: 0, Size: uint64(len(raw))}}
	e.Extended = map[string][]byte{layoutKind: []byte("object"), layoutVolume: []byte(s.id.String()), layoutSession: []byte(session), layoutMark: []byte("")}
	err = s.f.CreateEntry(ctx, dir, e, true)
	if errors.Is(err, ErrExists) {
		return layout.ErrExists
	}
	return err
}

func objectExpected(meta layout.ObjectMeta) map[string][]byte {
	return map[string][]byte{layoutVolume: []byte(meta.Volume.String()), layoutSession: []byte(meta.Session), layoutMark: []byte(meta.Mark)}
}

func (s *LayoutStore) changeMeta(ctx context.Context, rel string, expect layout.ObjectMeta, session, mark string) error {
	if expect.Volume != s.id {
		return layout.ErrIdentity
	}
	dir, name, err := s.object(rel)
	if err != nil {
		return err
	}
	e, err := s.f.LookupEntry(ctx, dir, name)
	if err != nil {
		return layoutError(err)
	}
	if !extendedMatch(e, objectExpected(expect)) || string(e.Extended[layoutKind]) != "object" {
		return layout.ErrPrecondition
	}
	e.Extended[layoutSession] = []byte(session)
	e.Extended[layoutMark] = []byte(mark)
	// Preserve the original FileChunks: an UpdateEntry dropping them
	// would tell the filer to delete the object's immutable data.
	return layoutError(s.f.UpdateEntry(ctx, dir, e, objectExpected(expect)))
}

func (s *LayoutStore) Reuse(ctx context.Context, rel string, expect layout.ObjectMeta, session string) error {
	if session == "" {
		return errors.New("chunkstore: reuse needs a session")
	}
	return s.changeMeta(ctx, rel, expect, session, "")
}

func (s *LayoutStore) Mark(ctx context.Context, rel string, expect layout.ObjectMeta, mark string) error {
	if mark == "" || expect.Mark != "" {
		return layout.ErrPrecondition
	}
	return s.changeMeta(ctx, rel, expect, expect.Session, mark)
}

func (s *LayoutStore) deleteObject(ctx context.Context, rel string, expected map[string][]byte) error {
	dir, name, err := s.object(rel)
	if err != nil {
		return err
	}
	err = s.f.DeleteEntry(ctx, dir, name, expected)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if errors.Is(err, ErrPrecondition) {
		if _, lookupErr := s.f.LookupEntry(ctx, dir, name); errors.Is(lookupErr, ErrNotFound) {
			return nil
		}
	}
	return layoutError(err)
}

func (s *LayoutStore) DeleteMarked(ctx context.Context, rel, mark string) error {
	if mark == "" {
		return layout.ErrPrecondition
	}
	return s.deleteObject(ctx, rel, map[string][]byte{layoutVolume: []byte(s.id.String()), layoutMark: []byte(mark)})
}

func (s *LayoutStore) DeleteOfVolume(ctx context.Context, rel string, id layout.VolumeID) error {
	if id != s.id {
		return layout.ErrIdentity
	}
	return s.deleteObject(ctx, rel, map[string][]byte{layoutVolume: []byte(id.String())})
}

func layoutEntry(name string, raw []byte) *filerpb.Entry {
	now := time.Now().Unix()
	return &filerpb.Entry{Name: name, Content: raw, Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(raw)), FileMode: 0644, Mtime: now, Crtime: now}}
}

func layoutError(err error) error {
	if errors.Is(err, ErrPrecondition) || errors.Is(err, ErrExists) || errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: %v", layout.ErrPrecondition, err)
	}
	return err
}
