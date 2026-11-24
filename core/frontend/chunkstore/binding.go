package chunkstore

// A key binding is a standard filer file: <root>/keys/<key> names its needle
// as the entry's one chunk (Chunks[0], with fid and file_id set), so the filer
// and every stock tool (volume.fsck, fs.verify) see the needle as referenced.
// It has exactly two shapes:
//   - live: one chunk {file_id = the canonical fid, fid, offset 0, size > 0},
//     no content, no swb.gc.deleted;
//   - done: swb.gc.deleted=done, no chunk, no content (GC reclaimed it: the
//     filer deleted the dropped needle).
// Anything else fails closed. A key exists or once existed for ever: the
// binding is created exclusively and never removed, so a key is never rebound.

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// ErrBindingReclaimed: the key's binding is the done tombstone; its needle
// was handed to the filer for deletion. Never empty data.
var ErrBindingReclaimed = errors.New("chunkstore: chunk key binding reclaimed")

// canonicalFid returns the stock form of a needle id and its parts. A batch
// offset "<base>_<i>" (the upload form) names needle key base+i with the
// base's cookie; stock tools parse only the plain form
// "<vid>,<key><cookie>" (key bytes with leading zero bytes dropped, then the
// 4-byte cookie, lowercase hex), which the volume server reads and deletes as
// the same needle.
func canonicalFid(fid string) (string, *filerpb.FileId, error) {
	base, delta := fid, uint64(0)
	if b, d, ok := strings.Cut(fid, "_"); ok {
		n, err := strconv.ParseUint(d, 10, 32)
		if err != nil || n == 0 {
			return "", nil, fmt.Errorf("chunkstore: fid %q: bad offset", fid)
		}
		base, delta = b, n
	}
	vidS, kc, ok := strings.Cut(base, ",")
	if !ok || len(kc) <= 8 || len(kc) > 16+8 {
		return "", nil, fmt.Errorf("chunkstore: fid %q: want <vid>,<key><cookie>", fid)
	}
	vid, err := strconv.ParseUint(vidS, 10, 32)
	if err != nil {
		return "", nil, fmt.Errorf("chunkstore: fid %q: volume id: %v", fid, err)
	}
	key, err := strconv.ParseUint(kc[:len(kc)-8], 16, 64)
	if err != nil {
		return "", nil, fmt.Errorf("chunkstore: fid %q: key: %v", fid, err)
	}
	cookie, err := strconv.ParseUint(kc[len(kc)-8:], 16, 32)
	if err != nil {
		return "", nil, fmt.Errorf("chunkstore: fid %q: cookie: %v", fid, err)
	}
	if key > ^uint64(0)-delta {
		return "", nil, fmt.Errorf("chunkstore: fid %q: key + offset overflows", fid)
	}
	key += delta
	var b [12]byte
	binary.BigEndian.PutUint64(b[:8], key)
	binary.BigEndian.PutUint32(b[8:], uint32(cookie))
	i := 0
	for i < 8 && b[i] == 0 {
		i++
	}
	s := strconv.FormatUint(vid, 10) + "," + hex.EncodeToString(b[i:])
	return s, &filerpb.FileId{VolumeId: uint32(vid), FileKey: key, Cookie: uint32(cookie)}, nil
}

// bindingChunk is a binding's one chunk: the stock FileChunk of a needle of
// size bytes, as a stock client would write it.
func bindingChunk(fid string, size int) (*filerpb.FileChunk, error) {
	if size <= 0 {
		return nil, fmt.Errorf("chunkstore: a binding names a needle of %d bytes; want at least 1 (a size-0 chunk is garbage to the filer)", size)
	}
	canon, id, err := canonicalFid(fid)
	if err != nil {
		return nil, err
	}
	return &filerpb.FileChunk{FileId: canon, Fid: id, Offset: 0, Size: uint64(size), ModifiedTsNs: time.Now().UnixNano()}, nil
}

// binding is a decoded key entry.
type binding struct {
	fid     string // canonical; empty when done
	size    uint64
	done    bool
	segment bool
}

// decodeBinding reads a key entry in one of its two shapes; anything else is
// an error naming the shape it found.
func decodeBinding(e *filerpb.Entry) (binding, error) {
	if e.GetIsDirectory() || string(e.GetExtended()[extKind]) != kindChunkKey {
		return binding{}, errors.New("not a chunk key binding")
	}
	if len(e.GetContent()) != 0 {
		return binding{}, errors.New("a chunk key binding with content")
	}
	if _, old := e.GetExtended()["swb.fid"]; old {
		return binding{}, errors.New("an old-form binding (swb.fid): this store predates volume format 2")
	}
	b := binding{segment: string(e.GetExtended()["swb.object"]) == "segment"}
	switch del := string(e.GetExtended()[extGCDeleted]); {
	case del == "done":
		if len(e.GetChunks()) != 0 {
			return binding{}, fmt.Errorf("a reclaimed binding still names %d chunks", len(e.GetChunks()))
		}
		b.done = true
		return b, nil
	case del != "":
		return binding{}, fmt.Errorf("a binding in reclaim state %q", del)
	}
	if n := len(e.GetChunks()); n != 1 {
		return binding{}, fmt.Errorf("a live binding with %d chunks; want 1", n)
	}
	c := e.GetChunks()[0]
	if c.GetOffset() != 0 || c.GetSize() == 0 || c.GetIsChunkManifest() || c.GetIsCompressed() || len(c.GetCipherKey()) != 0 || c.GetFid() == nil {
		return binding{}, errors.New("a binding chunk that is not one plain needle at offset 0 with a size and a fid")
	}
	id := c.GetFid()
	canon, _, err := canonicalFid(fmt.Sprintf("%d,%x%08x", id.GetVolumeId(), id.GetFileKey(), id.GetCookie()))
	if err != nil {
		return binding{}, err
	}
	if c.GetFileId() != "" && c.GetFileId() != canon {
		return binding{}, fmt.Errorf("a binding chunk whose file_id %q is not its fid %q", c.GetFileId(), canon)
	}
	b.fid, b.size = canon, c.GetSize()
	return b, nil
}
