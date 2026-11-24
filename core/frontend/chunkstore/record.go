package chunkstore

// A control record (HEAD, VOLUME, a snapshot attempt or name record, an
// intent, the GC mark, the restore marker) is a standard filer file, written
// the way the filer itself writes one with -saveToFilerLimit=64KiB -maxMB=4:
// a payload under recordInlineMax is the entry's content, a larger one is
// uploaded as needles of at most recordChunkMax bytes named in the entry's
// chunk list. The CAS tokens stay extended attributes of the same entry, so
// one conditional UpdateEntry replaces token and payload together; the filer
// deletes the needles an update or a delete drops. A payload needle is never
// rewritten (every Put is a fresh fid).

import (
	"context"
	"errors"
	"fmt"
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	recordInlineMax = 64 << 10 // payloads below this stay inline
	recordChunkMax  = 4 << 20  // the needle size of a chunked payload
	recordMaxBytes  = 1 << 30  // no record payload is larger
	recordLookups   = 3        // R-READ: lookups of one record before "busy"
)

// recordInline is the inline threshold; tests set it to 0 to chunk every
// record.
var recordInline = recordInlineMax

// objectUploadTimeout bounds an upload and the write that names it
// (storage.ObjectUploadTimeout; tests shorten it).
var objectUploadTimeout = storage.ObjectUploadTimeout

// ErrRecordBusy: a record was rewritten under every one of recordLookups
// reads. Retryable.
var ErrRecordBusy = errors.New("chunkstore: record rewritten while being read; retry")

// ErrRecordLost: a record's entry names a payload needle the store reports
// absent, and a re-read names the same needle: the payload is lost (never an
// absent or empty record).
var ErrRecordLost = errors.New("chunkstore: record payload lost")

// errRecordGone: the record vanished between a read and its re-read.
var errRecordGone = errors.New("chunkstore: record removed while being read")

// recordFiler writes every record's payload in its stock form: an entry
// with inline content at or over the threshold is uploaded as chunks before
// it is created or updated. Reads go through readRecord.
type recordFiler struct {
	Filer
	blobs Blobs // nil: only inline records can be written or read
}

// A chunked record's uploads and the write that names them run under the
// object upload bound, whatever the caller's context.
func (f recordFiler) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, oExcl bool) error {
	if e.GetName() == headName && ctx.Value(headCreateKey{}) == nil {
		return fmt.Errorf("%w: %s/%s", errHeadCreateBypass, dir, headName)
	}
	ctx, cancel := f.bound(ctx, e)
	defer cancel()
	e, err := f.chunked(ctx, e)
	if err != nil {
		return err
	}
	return f.Filer.CreateEntry(ctx, dir, e, oExcl)
}

func (f recordFiler) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	ctx, cancel := f.bound(ctx, e)
	defer cancel()
	e, err := f.chunked(ctx, e)
	if err != nil {
		return err
	}
	return f.Filer.UpdateEntry(ctx, dir, e, expected)
}

func (f recordFiler) bound(ctx context.Context, e *filerpb.Entry) (context.Context, context.CancelFunc) {
	if n := len(e.GetContent()); n == 0 || n < recordInline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, objectUploadTimeout)
}

// chunked returns e, or a copy whose content is uploaded as chunks when it
// is at or over the threshold. Uploads come first: a refused or lost update
// leaves needles no entry names, which stock volume.fsck reclaims.
func (f recordFiler) chunked(ctx context.Context, e *filerpb.Entry) (*filerpb.Entry, error) {
	body := e.GetContent()
	if len(body) == 0 || len(body) < recordInline {
		return e, nil
	}
	if len(e.GetChunks()) != 0 {
		return nil, fmt.Errorf("chunkstore: record %s has both content and chunks", e.GetName())
	}
	if f.blobs == nil {
		return nil, fmt.Errorf("chunkstore: record %s of %d bytes needs a blob store", e.GetName(), len(body))
	}
	out := &filerpb.Entry{Name: e.Name, IsDirectory: e.IsDirectory, Attributes: e.Attributes, Extended: e.Extended}
	for off := 0; off < len(body); off += recordChunkMax {
		end := min(off+recordChunkMax, len(body))
		fid, err := f.blobs.Put(ctx, body[off:end])
		if err != nil {
			return nil, fmt.Errorf("chunkstore: record %s: upload: %w", e.GetName(), err)
		}
		c, err := bindingChunk(fid, end-off)
		if err != nil {
			return nil, err
		}
		c.Offset = int64(off)
		out.Chunks = append(out.Chunks, c)
	}
	if out.Attributes != nil {
		attrs := proto.Clone(out.Attributes).(*filerpb.FuseAttributes)
		attrs.FileSize = uint64(len(body))
		out.Attributes = attrs
	}
	return out, nil
}

// recordChunks checks a record entry's shape: inline (no chunks), or chunks
// (no content) contiguous from 0 to FileSize, each a plain needle.
func recordChunks(e *filerpb.Entry) ([]*filerpb.FileChunk, error) {
	cs := e.GetChunks()
	if len(cs) == 0 {
		return nil, nil
	}
	if len(e.GetContent()) != 0 {
		return nil, errors.New("both content and chunks")
	}
	size := e.GetAttributes().GetFileSize()
	if size > recordMaxBytes {
		return nil, fmt.Errorf("%d bytes, over the %d limit", size, recordMaxBytes)
	}
	var at uint64
	for _, c := range cs {
		if c.GetOffset() != int64(at) || c.GetSize() == 0 || c.GetIsChunkManifest() || c.GetIsCompressed() || len(c.GetCipherKey()) != 0 || c.GetFileId() == "" {
			return nil, fmt.Errorf("chunk at %d is not a plain needle continuing at %d", c.GetOffset(), at)
		}
		at += c.GetSize()
	}
	if at != size {
		return nil, fmt.Errorf("chunks cover %d bytes; file size %d", at, size)
	}
	return cs, nil
}

// recordBody returns e's payload. A needle the store reports absent wraps
// ErrBlobNotFound (readRecord tells a race from a loss).
func recordBody(ctx context.Context, blobs Blobs, p string, e *filerpb.Entry) ([]byte, error) {
	cs, err := recordChunks(e)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: record %s: %v", p, err)
	}
	if cs == nil {
		return e.GetContent(), nil
	}
	if blobs == nil {
		return nil, fmt.Errorf("chunkstore: record %s is chunked; no blob reader", p)
	}
	body := make([]byte, 0, e.GetAttributes().GetFileSize())
	for _, c := range cs {
		d, err := blobs.Get(ctx, c.GetFileId())
		if err != nil {
			return nil, fmt.Errorf("chunkstore: record %s: chunk %s: %w", p, c.GetFileId(), err)
		}
		if uint64(len(d)) != c.GetSize() {
			return nil, fmt.Errorf("chunkstore: record %s: chunk %s holds %d bytes, want %d", p, c.GetFileId(), len(d), c.GetSize())
		}
		body = append(body, d...)
	}
	return body, nil
}

// readRecord returns the payload of e, the entry just read at dir/e.Name,
// by rule R-READ. When a payload needle is absent the entry is read again:
// the same chunk list means the payload is lost (an error, never an absent
// or empty record); a different one means a writer replaced the record, and
// that version is read, up to recordLookups lookups in all, then
// ErrRecordBusy. An entry gone on a re-read is errRecordGone. It returns the
// entry whose payload it read.
func readRecord(ctx context.Context, f Filer, blobs Blobs, dir string, e *filerpb.Entry) (*filerpb.Entry, []byte, error) {
	p := path.Join(dir, e.GetName())
	for lookups := 1; ; lookups++ {
		body, err := recordBody(ctx, blobs, p, e)
		if err == nil || !errors.Is(err, ErrBlobNotFound) {
			return e, body, err
		}
		if lookups == recordLookups {
			return nil, nil, fmt.Errorf("%w: %s", ErrRecordBusy, p)
		}
		again, lerr := f.LookupEntry(ctx, dir, e.GetName())
		if errors.Is(lerr, ErrNotFound) {
			return nil, nil, fmt.Errorf("%w: %s", errRecordGone, p)
		}
		if lerr != nil {
			return nil, nil, lerr
		}
		if sameChunks(e, again) {
			return nil, nil, fmt.Errorf("%w: %s (its needle is absent and the entry still names it): %v", ErrRecordLost, p, err)
		}
		e = again
	}
}

// lookupRecord reads dir/name and its payload by R-READ. An absent entry is
// ErrNotFound (as LookupEntry), one that vanishes during the read is not.
func lookupRecord(ctx context.Context, f Filer, blobs Blobs, dir, name string) (*filerpb.Entry, []byte, error) {
	e, err := f.LookupEntry(ctx, dir, name)
	if err != nil {
		return nil, nil, err
	}
	return readRecord(ctx, f, blobs, dir, e)
}

func sameChunks(a, b *filerpb.Entry) bool {
	if len(a.GetChunks()) != len(b.GetChunks()) || len(b.GetContent()) != 0 {
		return false
	}
	for i, c := range a.GetChunks() {
		if c.GetFileId() != b.GetChunks()[i].GetFileId() {
			return false
		}
	}
	return true
}
