package chunkstore

// Catalog: the chunk control plane's filer metadata and Fork (B9 W2 spec
// §3-§5). Under the chunk base directory, per volume:
//
//	<base>/<vol>/HEAD          the published HEAD (ChunkRemote)
//	<base>/<vol>/keys/<key>    chunk key bindings (ChunkRemote)
//	<base>/<vol>/intents/<id>  live intents (ChunkRemote; GC roots)
//	<base>/<vol>/VOLUME        identity, geometry and lineage; write-once
//	<base>/<vol>/snapnames/<name>         a snapshot name: the attempt it names
//	<base>/<vol>/snaps/<name>/<attempt>   a snapshot attempt: its own copy of
//	                                      one HEAD root; never removed
//
// Every record is a standard filer file (record.go): its payload is inline
// below 64 KiB, else filer chunks the record alone names. Every entry
// carries swb.kind and is checked on read, and an entry that does not check
// out is an error, never "absent".
//
// Snapshot and fork are read-then-write. Each writes an intent naming the
// keys it references before its final manifest write, and refuses that
// write once OpDeadline has passed since it read its source (spec §2.3).
// The operation's intents are removed only once its outcome is settled: the
// manifest is confirmed published (re-read shows it), or the attempt
// cleanly aborted (it never sent the write, or another entry already holds
// the name, so its exclusive create can never land). An uncertain outcome
// keeps them and reports outcome_unknown; a retry with the same request id
// settles it and removes every intent of the operation.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/geometry"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	kindChunkVolume = "chunkvolume"
	kindChunkSnap   = "chunksnap"

	volumeName = "VOLUME"
	snapsDir   = "snaps"
	// snapNamesDir holds one name record per snapshot name: the request and
	// the attempt that own the name (W5).
	snapNamesDir = "snapnames"
	kindSnapName = "chunksnapname"

	extReq           = "swb.req"
	extAttempt       = "swb.attempt" // the id of the intent of the attempt that wrote the entry
	extSnapGen       = "swb.snap.gen"
	extSnapEpoch     = "swb.snap.epoch"
	extSnapLSN       = "swb.snap.lsn"
	extSnapState     = "swb.snap.state"
	extSnapDeletedNs = "swb.snap.deleted_ns"

	// volumeFormat 2: key bindings and control records are standard filer
	// files (binding.go, record.go). Format 1 stores are refused, not read.
	volumeFormat = 2
	snapFormat   = 1

	// SnapActive and SnapDeleting are a snapshot's states.
	SnapActive   = "active"
	SnapDeleting = "deleting"
	// SnapAbsent is DeleteSnapshot's answer for a snapshot that is not there.
	SnapAbsent = "absent"

	// OriginCreate, OriginFork and OriginRestore are a volume's origins. A
	// restore is a fork whose volume later becomes independent of its source
	// (own chunks, restoreIndependentName written); until then it is a fork.
	OriginCreate  = "create"
	OriginFork    = "fork"
	OriginRestore = "restore"

	// DefaultOpDeadline bounds a snapshot or fork from reading its source to
	// its final write (spec §2.3).
	DefaultOpDeadline = 5 * time.Minute

	defaultListLimit = 100
	maxListLimit     = 1000
	maxLabels        = 32
	maxLabelKey      = 64
	maxLabelValue    = 256
	maxSnapName      = 128
)

// Error codes of the control plane (spec §2.2), as the HTTP API and the CLI
// report them.
const (
	CodeNotChunkVolume    = "not_chunk_volume"
	CodeInvalidName       = "invalid_name"
	CodeSnapshotExists    = "snapshot_exists"
	CodeSnapshotNotFound  = "snapshot_not_found"
	CodeSnapshotDeleting  = "snapshot_deleting"
	CodeTargetExists      = "target_exists"
	CodeGeometryMismatch  = "geometry_mismatch"
	CodeForkIncomplete    = "fork_incomplete"
	CodeDeadlineExceeded  = "deadline_exceeded"
	CodeRemoteUnavailable = "remote_unavailable"
	CodeOutcomeUnknown    = "outcome_unknown"
	CodeInvalidLabel      = "invalid_label"
)

// CatalogError is a typed control-plane failure.
type CatalogError struct {
	Code      string
	Message   string
	Retryable bool
	Err       error
}

func (e *CatalogError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("chunkstore: %s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("chunkstore: %s: %s", e.Code, e.Message)
}

func (e *CatalogError) Unwrap() error { return e.Err }

// Detail is the error without its code: the message, and the cause if any
// (an error body carries the code in its own field).
func (e *CatalogError) Detail() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// CodeOf returns the control-plane code of err, or "" when it has none.
func CodeOf(err error) string {
	var ce *CatalogError
	if errors.As(err, &ce) {
		return ce.Code
	}
	return ""
}

func catErr(code string, retryable bool, err error, format string, a ...any) error {
	return &CatalogError{Code: code, Message: fmt.Sprintf(format, a...), Retryable: retryable, Err: err}
}

// remoteErr wraps a filer failure as remote_unavailable, keeping the cause.
func remoteErr(err error, format string, a ...any) error {
	return catErr(CodeRemoteUnavailable, true, err, format, a...)
}

// Geometry is a volume's shape; a chunk index means nothing without it.
type Geometry struct {
	NumBlocks   uint64 `json:"num_blocks"`
	BlockSize   uint32 `json:"block_size"`
	ChunkBlocks uint32 `json:"chunk_blocks"`
}

// Parent is a forked volume's source.
type Parent struct {
	VolumeID string `json:"volume_id"`
	Snapshot string `json:"snapshot"`
	Gen      uint64 `json:"gen"`
}

// VolumeInfo is the VOLUME entry (spec §4.2).
type VolumeInfo struct {
	Format   int    `json:"format"`
	VolumeID string `json:"volume_id"`
	Geometry
	Origin    string  `json:"origin"`
	CreatedNs int64   `json:"created_ns"`
	Parent    *Parent `json:"parent"`
	// Fork records the fork operation that made this volume (origin fork):
	// a retry is bound to it, and a completed fork replays it.
	Fork *ForkRecord `json:"fork,omitempty"`
	// State is VolumeDeleted for a deleted volume, read from the entry's
	// extended attributes (never from the stored document).
	State string `json:"state,omitempty"`
}

// ForkRecord is the operation identity and original result of a fork.
type ForkRecord struct {
	RequestID     string `json:"request_id"`
	Source        string `json:"source"`
	Snapshot      string `json:"snapshot"`
	SourceGen     uint64 `json:"source_gen"`
	HeadSHA256    string `json:"head_sha256"` // SHA-256 of the snapshot's HEAD document
	RemoteLSN     uint64 `json:"remote_lsn"`
	ChunkCount    int    `json:"chunk_count"`
	QualifiedRefs int    `json:"qualified_refs"`
}

type snapDoc struct {
	Format      int               `json:"format"`
	VolumeID    string            `json:"volume_id"`
	Name        string            `json:"name"`
	RequestID   string            `json:"request_id"`
	CreatedNs   int64             `json:"created_ns"`
	CreatedBy   string            `json:"created_by"`
	Consistency string            `json:"consistency"`
	Head        json.RawMessage   `json:"head"`
	Geometry    Geometry          `json:"geometry"`
	Parent      *Parent           `json:"parent"`
	Labels      map[string]string `json:"labels"`
	ExpiresAtNs int64             `json:"expires_at_ns"`
	// Class is the retention class: "semantic", "pinned", or "" for a
	// snapshot made before classes existed (see retention.go).
	Class string `json:"class,omitempty"`
}

// SnapshotInfo describes one snapshot, with the field names of the HTTP API
// (spec §3.2-§3.3).
type SnapshotInfo struct {
	VolumeID      string            `json:"volumeID"`
	Name          string            `json:"name"`
	HeadGen       uint64            `json:"headGen"`
	HeadEpoch     uint64            `json:"headEpoch"`
	CutLSN        uint64            `json:"cutLSN"`
	CreatedNs     int64             `json:"createdNs"`
	State         string            `json:"state"`
	ChunkCount    int               `json:"chunkCount"`
	ManifestBytes int               `json:"manifestBytes"`
	Consistency   string            `json:"consistency"`
	Labels        map[string]string `json:"labels"`
	Parent        *Parent           `json:"parent"`
	ExpiresAtNs   int64             `json:"expiresAtNs"`
	Replayed      bool              `json:"replayed"`
	Class         string            `json:"class"`

	attempt string // the attempt the name resolved to, when read by name

	head   storage.ChunkHead
	geo    Geometry
	digest string // identity uses the embedded writer version, not today's serializer
}

// Head returns the HEAD the snapshot copied.
func (s SnapshotInfo) Head() storage.ChunkHead { return s.head }

// SnapshotRequest names a snapshot of the HEAD a cut installed.
type SnapshotRequest struct {
	Name        string
	RequestID   string // required; a retry with the same id replays
	Labels      map[string]string
	Consistency string // "crash" (default) or "app"
	ExpiresAtNs int64
	CreatedBy   string
	// Class is the retention class: "semantic" or "pinned" ("" leaves it
	// unset, which retention treats as unpinned).
	Class string
	// SourceReadAt is when the caller obtained the HEAD it snapshots (the
	// cut's return); OpDeadline counts from it. Zero means now.
	SourceReadAt time.Time
	// ShareToken is the ShareGuard token of a caller that began the share
	// before it obtained the HEAD (the owner's seal: Begin, then the cut).
	// Empty: CreateSnapshot begins the share itself and refuses a HEAD that
	// is no longer current. Unused without a guard; never stored.
	ShareToken string
}

// ListOptions pages a snapshot listing.
type ListOptions struct {
	Prefix          string
	StartAfter      string
	Limit           int
	IncludeDeleting bool
}

// ForkRequest makes a new volume from a snapshot.
type ForkRequest struct {
	Source    string
	Snapshot  string
	Target    string
	RequestID string // required; a retry with the same id replays
	Labels    map[string]string
}

// ForkResult reports a fork, with the field names of the CLI output
// (spec §3.5).
type ForkResult struct {
	Target        string `json:"target"`
	Source        string `json:"source"`
	Snapshot      string `json:"snapshot"`
	SourceGen     uint64 `json:"sourceGen"`
	HeadGen       uint64 `json:"headGen"`
	HeadEpoch     uint64 `json:"headEpoch"`
	RemoteLSN     uint64 `json:"remoteLSN"`
	ChunkCount    int    `json:"chunkCount"`
	QualifiedRefs int    `json:"qualifiedRefs"`
	Replayed      bool   `json:"replayed"`
}

// Catalog reads and writes the control-plane entries under one chunk base.
type Catalog struct {
	blobs Blobs // optional immutable-object reader; SWFS supplies it directly
	f     Filer
	base  string
	// OpDeadline bounds a snapshot or fork (DefaultOpDeadline when zero).
	OpDeadline time.Duration
	now        func() time.Time
	// retainHook, when set, runs at the named stages of a Retain pass (a
	// test seam for interleavings; see hookPlanned).
	retainHook func(stage, name string)
	// guard, when set, is told about every share of a volume's chunks.
	guard ShareGuard
}

// NewCatalog returns the catalog for the chunk base directory base (the
// blockvolume --chunk-path), a clean absolute directory other than /.
func NewCatalog(f Filer, base string) (*Catalog, error) {
	if f == nil {
		return nil, errors.New("chunkstore: catalog needs a filer")
	}
	if !path.IsAbs(base) || path.Clean(base) != base || base == "/" {
		return nil, fmt.Errorf("chunkstore: chunk base %q must be a clean absolute directory other than /", base)
	}
	blobs, _ := f.(Blobs)
	return &Catalog{f: recordFiler{f, blobs}, blobs: blobs, base: base, now: time.Now}, nil
}

// SetClock replaces the catalog's clock (tests).
func (c *Catalog) SetClock(now func() time.Time) { c.now = now }

func (c *Catalog) deadline() time.Duration {
	if c.OpDeadline > 0 {
		return c.OpDeadline
	}
	return DefaultOpDeadline
}

func (c *Catalog) volRoot(vol string) (string, error) {
	if err := CheckVolumeID(vol); err != nil {
		return "", catErr(CodeInvalidName, false, err, "volume %q", vol)
	}
	return path.Join(c.base, vol), nil
}

// CheckSnapshotName enforces the snapshot name rule (spec §4.4).
func CheckSnapshotName(name string) error {
	if name == "" || len(name) > maxSnapName || strings.HasPrefix(name, ".") {
		return catErr(CodeInvalidName, false, nil, "snapshot name %q: 1-%d characters, not starting with '.'", name, maxSnapName)
	}
	switch name {
	case headName, volumeName, keysDir, snapsDir, snapNamesDir, intentsDir:
		return catErr(CodeInvalidName, false, nil, "snapshot name %q is reserved", name)
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return catErr(CodeInvalidName, false, nil, "snapshot name %q: only [A-Za-z0-9._-]", name)
		}
	}
	return nil
}

func checkLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return catErr(CodeInvalidLabel, false, nil, "%d labels, at most %d", len(labels), maxLabels)
	}
	for k, v := range labels {
		if k == "" || len(k) > maxLabelKey || len(v) > maxLabelValue {
			return catErr(CodeInvalidLabel, false, nil, "label %q: key 1-%d characters, value at most %d bytes", k, maxLabelKey, maxLabelValue)
		}
		for _, r := range k {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
				return catErr(CodeInvalidLabel, false, nil, "label key %q: only [a-z0-9._-]", k)
			}
		}
	}
	return nil
}

// ---- VOLUME ----

func volumeEntry(v VolumeInfo, req string) (*filerpb.Entry, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	ext := map[string][]byte{extKind: []byte(kindChunkVolume)}
	if req != "" {
		ext[extReq] = []byte(req)
	}
	return &filerpb.Entry{
		Name:       volumeName,
		Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(body)), Mtime: now, Crtime: now, FileMode: 0o644, Mime: "application/json"},
		Extended:   ext,
		Content:    body,
	}, nil
}

func decodeVolume(p string, e *filerpb.Entry, body []byte) (VolumeInfo, error) {
	if k := string(e.Extended[extKind]); k != kindChunkVolume {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: kind %q, want %q", p, k, kindChunkVolume)
	}
	var v VolumeInfo
	if err := json.Unmarshal(body, &v); err != nil {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: decode: %v", p, err)
	}
	if v.Format != volumeFormat {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: format %d not supported (this binary reads volume format %d only; a store of another format is not converted, recreate it)", p, v.Format, volumeFormat)
	}
	if v.Origin != OriginCreate && v.Origin != OriginFork && v.Origin != OriginRestore {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: origin %q", p, v.Origin)
	}
	if isForked(v.Origin) && (v.Fork == nil || v.Fork.RequestID == "") {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: a fork without its fork record", p)
	}
	v.State = string(e.Extended[extVolState])
	return v, nil
}

// ReadVolume returns the VOLUME entry; ok is false when it does not exist.
func (c *Catalog) ReadVolume(ctx context.Context, vol string) (VolumeInfo, bool, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return VolumeInfo{}, false, err
	}
	e, body, err := lookupRecord(ctx, c.f, c.blobs, root, volumeName)
	if errors.Is(err, ErrNotFound) {
		return VolumeInfo{}, false, nil
	}
	if err != nil {
		return VolumeInfo{}, false, remoteErr(err, "read VOLUME of %s", vol)
	}
	v, err := decodeVolume(path.Join(root, volumeName), e, body)
	if err != nil {
		return VolumeInfo{}, false, err
	}
	return v, true, nil
}

func (c *Catalog) headExists(ctx context.Context, root string) (bool, error) {
	_, err := c.f.LookupEntry(ctx, root, headName)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ensureInitialHead creates vol's initial empty HEAD unless a HEAD exists.
func (c *Catalog) ensureInitialHead(ctx context.Context, vol, root string) error {
	has, err := c.headExists(ctx, root)
	if err != nil {
		return remoteErr(err, "read HEAD of %s", vol)
	}
	if has {
		return nil
	}
	he, err := headEntry(storage.ChunkHead{Gen: 1, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadActive})
	if err != nil {
		return err
	}
	if err := c.f.CreateEntry(ctx, root, he, true); err != nil && !errors.Is(err, ErrExists) {
		return remoteErr(err, "create the initial HEAD of %s", vol)
	}
	return nil
}

// ReadHead returns vol's HEAD, and false when no HEAD was ever published.
func (c *Catalog) ReadHead(ctx context.Context, vol string) (storage.ChunkHead, bool, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return storage.ChunkHead{}, false, err
	}
	e, body, err := lookupRecord(ctx, c.f, c.blobs, root, headName)
	if errors.Is(err, ErrNotFound) {
		return storage.ChunkHead{}, false, nil
	}
	if err != nil {
		return storage.ChunkHead{}, false, remoteErr(err, "read HEAD of %s", vol)
	}
	h, err := decodeHead(path.Join(root, headName), e, body)
	if err != nil {
		return storage.ChunkHead{}, false, err
	}
	return h, true, nil
}

// EnsureVolume applies the attach rules of spec §4.2 for a volume served
// with geometry g: it creates a missing VOLUME entry (origin create), and
// refuses with geometry_mismatch when VOLUME disagrees with g, or with
// fork_incomplete when VOLUME is a fork whose HEAD is not written yet.
func (c *Catalog) EnsureVolume(ctx context.Context, vol string, g Geometry) (VolumeInfo, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return VolumeInfo{}, err
	}
	v, ok, err := c.ReadVolume(ctx, vol)
	if err != nil {
		return VolumeInfo{}, err
	}
	if !ok {
		if g.BlockSize != 0 && g.NumBlocks > geometry.MaxVolumeBytes/uint64(g.BlockSize) {
			return VolumeInfo{}, catErr(CodeGeometryMismatch, false, nil, "volume size exceeds maximum %d bytes (16 TiB - 4 KiB; uint32 LBA)", geometry.MaxVolumeBytes)
		}
		v = VolumeInfo{Format: volumeFormat, VolumeID: vol, Geometry: g, Origin: OriginCreate, CreatedNs: c.now().UnixNano()}
		e, err := volumeEntry(v, "")
		if err != nil {
			return VolumeInfo{}, err
		}
		if err := c.f.CreateEntry(ctx, root, e, true); err != nil {
			if !errors.Is(err, ErrExists) {
				return VolumeInfo{}, remoteErr(err, "create VOLUME of %s", vol)
			}
			// Another writer created it first: check theirs.
			if v, ok, err = c.ReadVolume(ctx, vol); err != nil || !ok {
				return VolumeInfo{}, remoteErr(err, "re-read VOLUME of %s", vol)
			}
		}
	}
	if v.State == VolumeDeleted {
		return VolumeInfo{}, refuseDeleted(vol, "it cannot be attached or created again")
	}
	if h, has, err := c.ReadHead(ctx, vol); err != nil {
		return VolumeInfo{}, err
	} else if has && headDeletingOrDeleted(h) {
		return VolumeInfo{}, refuseDeleted(vol, "it cannot be attached")
	}
	if v.Geometry != g {
		return VolumeInfo{}, catErr(CodeGeometryMismatch, false, nil, "volume %s: VOLUME geometry %+v, this host serves %+v", vol, v.Geometry, g)
	}
	if v.Origin == OriginCreate {
		// A created volume gets its initial HEAD with VOLUME (integration
		// ruling F1): gen 1, epoch 0, no holder, remote_lsn 0, no chunks
		// (all zero). An exclusive create: an existing HEAD (a volume made
		// before VOLUME entries, or an earlier attach) is kept.
		if err := c.ensureInitialHead(ctx, vol, root); err != nil {
			return VolumeInfo{}, err
		}
	}
	if isForked(v.Origin) {
		// One read of HEAD decides both: absent and aborted never attach.
		// A restore reads only its own HEAD here, never its source: a source
		// deleted meanwhile is a normal state, not a mismatch.
		h, has, err := c.ReadHead(ctx, vol)
		switch {
		case err != nil:
			return VolumeInfo{}, err
		case !has:
			return VolumeInfo{}, catErr(CodeForkIncomplete, false, nil, "volume %s is a fork whose HEAD is not written yet", vol)
		case h.State == storage.ChunkHeadAborted:
			return VolumeInfo{}, catErr(CodeForkIncomplete, false, nil, "volume %s: its fork was aborted (HEAD is a tombstone); it never attaches", vol)
		}
	}
	return v, nil
}

// ---- snapshots ----
//
// A snapshot name is owned by a name record snapnames/<name> {request,
// attempt}, created exclusively; the snapshot itself is published by an
// exclusive create at the attempt's own path snaps/<name>/<attempt>, which
// is never removed (W5). A GC sweep settles a
// dead attempt by claiming its path with an aborted tombstone; a retry of
// the same request publishes to the same attempt path. DeleteSnapshot
// marks the attempt deleting and removes the name record, freeing the name.

// SnapAborted marks an attempt path a GC sweep claimed for a dead attempt.
const SnapAborted = "aborted"

type nameRecord struct {
	Format    int    `json:"format"`
	VolumeID  string `json:"volume_id"`
	Name      string `json:"name"`
	RequestID string `json:"request_id"`
	Attempt   string `json:"attempt"`
	CreatedNs int64  `json:"created_ns"`
}

func nameRecordEntry(rec nameRecord) (*filerpb.Entry, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	return &filerpb.Entry{
		Name:       rec.Name,
		Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(body)), Mtime: now, Crtime: now, FileMode: 0o644, Mime: "application/json"},
		Extended:   map[string][]byte{extKind: []byte(kindSnapName), extReq: []byte(rec.RequestID), extAttempt: []byte(rec.Attempt)},
		Content:    body,
	}, nil
}

func decodeNameRecord(p string, e *filerpb.Entry, body []byte) (nameRecord, error) {
	var rec nameRecord
	if k := string(e.Extended[extKind]); k != kindSnapName {
		return rec, fmt.Errorf("chunkstore: snapshot name %s: kind %q, want %q", p, k, kindSnapName)
	}
	if err := json.Unmarshal(body, &rec); err != nil {
		return rec, fmt.Errorf("chunkstore: snapshot name %s: decode: %v", p, err)
	}
	if rec.Format != snapFormat || rec.Attempt == "" || rec.RequestID == "" ||
		string(e.Extended[extAttempt]) != rec.Attempt || string(e.Extended[extReq]) != rec.RequestID {
		return rec, fmt.Errorf("chunkstore: snapshot name %s: malformed record", p)
	}
	return rec, nil
}

// snapRef is a name resolved through its record: the record, and the
// attempt's entry when it exists (nil: the attempt has not published).
type snapRef struct {
	rec   nameRecord
	entry *filerpb.Entry
	info  SnapshotInfo
}

// resolveSnap reads snapnames/<name> and the attempt it names. ok is false
// when no record holds the name.
func (c *Catalog) resolveSnap(ctx context.Context, vol, name string) (snapRef, bool, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return snapRef{}, false, err
	}
	re, reBody, err := lookupRecord(ctx, c.f, c.blobs, path.Join(root, snapNamesDir), name)
	if errors.Is(err, ErrNotFound) {
		return snapRef{}, false, nil
	}
	if err != nil {
		return snapRef{}, false, remoteErr(err, "read snapshot name %s/%s", vol, name)
	}
	rec, err := decodeNameRecord(path.Join(root, snapNamesDir, name), re, reBody)
	if err != nil {
		return snapRef{}, false, err
	}
	ref := snapRef{rec: rec}
	dir := path.Join(root, snapsDir, name)
	e, body, err := lookupRecord(ctx, c.f, c.blobs, dir, rec.Attempt)
	switch {
	case errors.Is(err, ErrNotFound):
		return ref, true, nil // the attempt has not published (in flight, or dead)
	case err != nil:
		return snapRef{}, false, remoteErr(err, "read snapshot %s/%s", vol, name)
	}
	info, _, err := decodeSnap(path.Join(dir, rec.Attempt), e, body)
	if err != nil {
		return snapRef{}, false, err
	}
	info.attempt = rec.Attempt
	ref.entry, ref.info = e, info
	return ref, true, nil
}

// readSnap returns the published, not aborted snapshot named name, or
// snapshot_not_found.
func (c *Catalog) readSnap(ctx context.Context, vol, name string) (SnapshotInfo, error) {
	ref, ok, err := c.resolveSnap(ctx, vol, name)
	if err != nil {
		return SnapshotInfo{}, err
	}
	if !ok || ref.entry == nil || ref.info.State == SnapAborted {
		return SnapshotInfo{}, catErr(CodeSnapshotNotFound, false, nil, "snapshot %s/%s", vol, name)
	}
	return ref.info, nil
}

func decodeSnap(p string, e *filerpb.Entry, body []byte) (SnapshotInfo, string, error) {
	fail := func(format string, a ...any) (SnapshotInfo, string, error) {
		return SnapshotInfo{}, "", fmt.Errorf("chunkstore: snapshot %s: "+format, append([]any{p}, a...)...)
	}
	if k := string(e.Extended[extKind]); k != kindChunkSnap {
		return fail("kind %q, want %q", k, kindChunkSnap)
	}
	var doc snapDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return fail("decode: %v", err)
	}
	if doc.Format != snapFormat {
		return fail("format %d not supported", doc.Format)
	}
	if string(e.Extended[extSnapState]) == SnapAborted {
		return SnapshotInfo{VolumeID: doc.VolumeID, Name: doc.Name, CreatedNs: doc.CreatedNs, State: SnapAborted}, "", nil
	}
	head, err := decodeHeadDoc(p+"#head", doc.Head)
	if err != nil {
		return fail("%v", err)
	}
	digest, err := snapshotHeadDigest(head, doc.Head)
	if err != nil {
		return fail("HEAD identity: %v", err)
	}
	state := string(e.Extended[extSnapState])
	if state != SnapActive && state != SnapDeleting {
		return fail("state %q", state)
	}
	if string(e.Extended[extSnapGen]) != strconv.FormatUint(head.Gen, 10) {
		return fail("extended gen %q disagrees with the document's HEAD gen %d", e.Extended[extSnapGen], head.Gen)
	}
	return SnapshotInfo{
		VolumeID: doc.VolumeID, Name: doc.Name, HeadGen: head.Gen, HeadEpoch: head.Epoch, CutLSN: head.RemoteLSN,
		CreatedNs: doc.CreatedNs, State: state, ChunkCount: manifestCount(head), ManifestBytes: len(body),
		Consistency: doc.Consistency, Labels: doc.Labels, Parent: doc.Parent, ExpiresAtNs: doc.ExpiresAtNs,
		Class: doc.Class, head: head, geo: doc.Geometry, digest: digest,
	}, doc.RequestID, nil
}

// abortedSnapEntry is the tombstone a sweep writes at an attempt path.
func (c *Catalog) abortedSnapEntry(vol, name, attempt string) (*filerpb.Entry, error) {
	body, err := json.Marshal(snapDoc{Format: snapFormat, VolumeID: vol, Name: name, CreatedNs: c.now().UnixNano()})
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	return &filerpb.Entry{
		Name:       attempt,
		Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(body)), Mtime: now, Crtime: now, FileMode: 0o644, Mime: "application/json"},
		Extended:   map[string][]byte{extKind: []byte(kindChunkSnap), extSnapState: []byte(SnapAborted)},
		Content:    body,
	}, nil
}

// AbortSnapshot settles a dead snapshot attempt for a GC sweep (W5): it
// claims the attempt path snaps/<name>/<attempt>
// with an exclusive create of an aborted tombstone. claimed is true when
// the path holds a tombstone (this one or an earlier one): the attempt can
// never publish. false: the attempt published, and its keys are reachable
// through that snapshot. Either way its copy intent can then be deleted.
// The attempt is the one in the intent's holder "snapshot:<vol>/<name>/<attempt>".
func (c *Catalog) AbortSnapshot(ctx context.Context, vol, name, attempt string) (claimed bool, err error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return false, err
	}
	if err := CheckSnapshotName(name); err != nil {
		return false, err
	}
	if !storage.ValidChunkIntentID(attempt) {
		return false, catErr(CodeInvalidName, false, nil, "attempt %q", attempt)
	}
	e, err := c.abortedSnapEntry(vol, name, attempt)
	if err != nil {
		return false, err
	}
	dir := path.Join(root, snapsDir, name)
	switch err := c.f.CreateEntry(ctx, dir, e, true); {
	case err == nil:
		return true, nil
	case !errors.Is(err, ErrExists):
		return false, remoteErr(err, "aborted tombstone %s/%s/%s", vol, name, attempt)
	}
	got, err := c.f.LookupEntry(ctx, dir, attempt)
	if err != nil {
		return false, remoteErr(err, "read %s/%s/%s", vol, name, attempt)
	}
	return string(got.Extended[extSnapState]) == SnapAborted, nil
}

// dropIntent deletes one attempt's own intent, once that attempt's create
// is confirmed or can never apply. It never touches another attempt's
// intent, even under the same operation: an earlier attempt's create may
// still be held by the server and land later (W5 rule). A failed delete
// leaves the intent, which only over-protects. The delete outlives the
// caller's cancellation (cleanup after a refused or cancelled request) but is
// bounded by dropIntentTimeout, so it can never hang on an unresponsive filer.
func (c *Catalog) dropIntent(ctx context.Context, root, id string) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dropIntentTimeout)
	defer cancel()
	_ = c.f.DeleteEntry(dctx, path.Join(root, intentsDir), id, nil)
}

// dropIntentTimeout bounds one intent delete (a var so tests can shorten it).
var dropIntentTimeout = 30 * time.Second

type createOutcome int

const (
	createdNow      createOutcome = iota // this attempt's create applied (confirmed)
	createRefused                        // the filer refused it (the name exists): it will never apply
	createUnsettled                      // no answer: it may still apply, even later
)

// publishCtx bounds a publishing create by the operation's deadline
// (catalog clock), so the RPC cannot outlive the deadline its intent
// records (W5).
func (c *Catalog) publishCtx(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, deadline.Sub(c.now()))
}

// createResolved makes this attempt's exclusive create of e under dir and
// returns its outcome and the entry now under the name (nil if unread).
// The create runs under publish (bounded by the operation's deadline); the
// re-read that settles an ambiguous failure under ctx.
// attempt, when not empty, is stored in e as extAttempt; a re-read that
// finds it confirms this attempt's create. Only the filer's own "already
// exists" answer settles a failure as refused; any other failure leaves
// the create unsettled, whatever entry the re-read finds (that entry may
// be another attempt's, and this one may still land after it is gone).
func (c *Catalog) createResolved(ctx, publish context.Context, dir string, e *filerpb.Entry, attempt string) (createOutcome, *filerpb.Entry, error) {
	if attempt != "" {
		e.Extended[extAttempt] = []byte(attempt)
	}
	err := c.f.CreateEntry(publish, dir, e, true)
	if err == nil {
		return createdNow, e, nil
	}
	got, lerr := c.f.LookupEntry(ctx, dir, e.GetName())
	if lerr != nil {
		got = nil
	}
	if got != nil && attempt != "" && string(got.Extended[extAttempt]) == attempt {
		return createdNow, got, nil
	}
	if errors.Is(err, ErrExists) {
		return createRefused, got, err
	}
	return createUnsettled, got, err
}

func headDigest(h storage.ChunkHead) (string, error) {
	e, err := headEntry(h)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(e.Content)
	return hex.EncodeToString(sum[:]), nil
}

func headKeys(h storage.ChunkHead) []string {
	if h.Segments != nil {
		keys := make([]string, 0, len(h.Segments))
		for _, ref := range h.Segments {
			keys = append(keys, ref.Key)
		}
		return keys
	}
	keys := make([]string, 0, len(h.Chunks))
	for _, ref := range h.Chunks {
		keys = append(keys, ref.Key)
	}
	return keys
}

// CreateSnapshot stores head, which must be the HEAD a cut of vol installed
// (ChunkCutResult.Head), as the snapshot req.Name. A retry with the same
// RequestID returns the existing snapshot (Replayed); another RequestID
// gets snapshot_exists. The name is taken with an exclusive name record
// {request, attempt}; an intent names the snapshot's keys and the attempt
// (holder "snapshot:<vol>/<name>/<attempt>") before the exclusive create at
// the attempt path snaps/<name>/<attempt>, which only that request's
// attempts ever write.
func (c *Catalog) CreateSnapshot(ctx context.Context, vol string, req SnapshotRequest, head storage.ChunkHead) (SnapshotInfo, error) {
	readAt := req.SourceReadAt
	if readAt.IsZero() {
		readAt = c.now()
	}
	root, err := c.volRoot(vol)
	if err != nil {
		return SnapshotInfo{}, err
	}
	if existing, replayed, err := c.SnapshotReplay(ctx, vol, req); err != nil || replayed {
		return existing, err
	}
	consistency := req.Consistency
	if consistency == "" {
		consistency = "crash"
	}
	if err := ValidateSnapshotClass(req.Class); err != nil {
		return SnapshotInfo{}, err
	}
	if head.Gen == 0 {
		return SnapshotInfo{}, catErr(CodeNotChunkVolume, false, nil, "volume %s has no published HEAD to snapshot", vol)
	}
	v, ok, err := c.ReadVolume(ctx, vol)
	if err != nil {
		return SnapshotInfo{}, err
	}
	if !ok {
		return SnapshotInfo{}, catErr(CodeNotChunkVolume, false, nil, "volume %s has no VOLUME entry", vol)
	}
	if v.Origin == OriginRestore {
		// A restore is independent only once its marker exists: a snapshot
		// taken before would keep naming the source's chunks for its whole
		// retention, so it is refused until then.
		if _, independent, err := c.ReadIndependence(ctx, vol); err != nil {
			return SnapshotInfo{}, err
		} else if !independent {
			return SnapshotInfo{}, catErr(CodeRestoreIncomplete, false, nil, "volume %s is still being restored: snapshots are refused until it is independent", vol)
		}
		// The supplied image need not be the current HEAD: a cut taken before
		// the marker may still name the source's chunks. Check the image itself.
		if q, err := c.foreignRefsOf(ctx, vol, head); err != nil {
			return SnapshotInfo{}, err
		} else if q > 0 {
			return SnapshotInfo{}, catErr(CodeRestoreIncomplete, false, nil, "volume %s: snapshot image gen %d holds %d refs to another volume", vol, head.Gen, q)
		}
	}
	shareToken := req.ShareToken
	if shareToken == "" {
		if shareToken, err = c.beginShare(ctx, vol, head); err != nil {
			return SnapshotInfo{}, err
		}
	}

	// Take the name: this request's record, or an earlier attempt's of it.
	attempt, err := c.claimName(ctx, vol, root, req, head.Gen)
	if err != nil {
		return SnapshotInfo{}, err
	}
	// A volume delete marks HEAD deleting before it checks for snapshot
	// names: either it sees this name (and refuses), or this read sees the
	// mark and nothing is published. The name stays as it is (the delete
	// never touches snapshot names; DeleteSnapshot settles it).
	if gone, err := c.volumeGone(ctx, vol); err != nil {
		return SnapshotInfo{}, err
	} else if gone {
		return SnapshotInfo{}, refuseDeleted(vol, "it cannot be snapshotted")
	}

	head, upgradeIDs, err := c.prepareLegacyHead(ctx, root, head, head, v.Geometry)
	defer c.dropManifestIntents(ctx, root, upgradeIDs)
	if err != nil {
		return SnapshotInfo{}, err
	}
	headEnt, err := headEntry(head)
	if err != nil {
		return SnapshotInfo{}, err
	}
	doc := snapDoc{Format: snapFormat, VolumeID: vol, Name: req.Name, RequestID: req.RequestID, CreatedNs: c.now().UnixNano(),
		CreatedBy: req.CreatedBy, Consistency: consistency, Head: headEnt.Content, Geometry: v.Geometry, Parent: v.Parent,
		Labels: req.Labels, ExpiresAtNs: req.ExpiresAtNs, Class: req.Class}
	body, err := json.Marshal(doc)
	if err != nil {
		return SnapshotInfo{}, err
	}
	now := time.Now().Unix()
	e := &filerpb.Entry{
		Name:       attempt,
		Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(body)), Mtime: now, Crtime: now, FileMode: 0o644, Mime: "application/json"},
		Extended: map[string][]byte{
			extKind:      []byte(kindChunkSnap),
			extSnapGen:   []byte(strconv.FormatUint(head.Gen, 10)),
			extSnapEpoch: []byte(strconv.FormatUint(head.Epoch, 10)),
			extSnapLSN:   []byte(strconv.FormatUint(head.RemoteLSN, 10)),
			extSnapState: []byte(SnapActive),
			extReq:       []byte(req.RequestID),
		},
		Content: body,
	}

	tag := "snapshot:" + vol + "/" + req.Name + "/" + attempt
	deadline := readAt.Add(c.deadline())
	id, err := putIntent(ctx, c.f, root, storage.ChunkIntent{Gen: head.Gen, Epoch: head.Epoch, Holder: tag, Keys: headKeys(head), Segments: head.Segments, ChunkBytes: head.ChunkBytes,
		DeadlineNs: deadline.UnixNano()})
	if err != nil {
		return SnapshotInfo{}, remoteErr(err, "intent for snapshot %s/%s", vol, req.Name)
	}
	if !c.now().Before(deadline) { // re-checked immediately before publishing
		// Clean abort: the snapshot entry was never sent.
		c.dropIntent(ctx, root, id)
		return SnapshotInfo{}, catErr(CodeDeadlineExceeded, true, nil, "snapshot %s/%s: %v since the cut, over the %v deadline; nothing written", vol, req.Name, c.now().Sub(readAt), c.deadline())
	}
	dir := path.Join(root, snapsDir, req.Name)
	if err := c.finishShare(ctx, vol, shareToken, head); err != nil {
		// Clean abort: the snapshot entry was never sent.
		c.dropIntent(ctx, root, id)
		return SnapshotInfo{}, err
	}
	pctx, cancel := c.publishCtx(ctx, deadline)
	outcome, got, cerr := c.createResolved(ctx, pctx, dir, e, attempt)
	cancel()
	switch {
	case outcome == createdNow:
		// This request's snapshot is at the attempt path (this create, or
		// an earlier attempt's of the same request). Judge it from that
		// entry, never by resolving the name again (the name can be freed
		// and reused meanwhile).
		c.dropIntent(ctx, root, id)
		got, body, err := readRecord(ctx, c.f, c.blobs, dir, got)
		if err != nil {
			return SnapshotInfo{}, remoteErr(err, "read snapshot %s/%s", vol, req.Name)
		}
		info, _, err := decodeSnap(path.Join(dir, attempt), got, body)
		if err != nil {
			return SnapshotInfo{}, err
		}
		if info.State == SnapAborted {
			return SnapshotInfo{}, catErr(CodeSnapshotExists, false, nil, "snapshot %s/%s: this request's attempt was aborted; delete the name to reuse it", vol, req.Name)
		}
		if info.State == SnapDeleting {
			return SnapshotInfo{}, catErr(CodeSnapshotDeleting, false, nil, "snapshot %s is being deleted; wait for the delete to finish, then reuse the name", req.Name)
		}
		return info, nil
	case got != nil:
		// The attempt path holds a sweep's tombstone, which is permanent:
		// this create can never land.
		c.dropIntent(ctx, root, id)
		return SnapshotInfo{}, catErr(CodeSnapshotExists, false, nil, "snapshot %s/%s: this request's attempt was aborted; delete the name to reuse it", vol, req.Name)
	case outcome == createRefused:
		c.dropIntent(ctx, root, id)
		return SnapshotInfo{}, remoteErr(cerr, "snapshot %s/%s: the attempt path exists but could not be read; retry with the same request id", vol, req.Name)
	default:
		// Unsettled: the create may still land; its intent stays.
		return SnapshotInfo{}, catErr(CodeOutcomeUnknown, true, cerr, "snapshot %s/%s: create outcome unknown; its intent is kept, retry with the same request id", vol, req.Name)
	}
}

// claimName returns the attempt that publishes req: a new attempt whose
// name record this call created, or the attempt of this request's earlier
// record (a retry publishes to the same attempt path).
func (c *Catalog) claimName(ctx context.Context, vol, root string, req SnapshotRequest, gen uint64) (string, error) {
	attempt, err := storage.NewChunkIntentID(gen)
	if err != nil {
		return "", err
	}
	re, err := nameRecordEntry(nameRecord{Format: snapFormat, VolumeID: vol, Name: req.Name, RequestID: req.RequestID, Attempt: attempt, CreatedNs: c.now().UnixNano()})
	if err != nil {
		return "", err
	}
	cerr := c.f.CreateEntry(ctx, path.Join(root, snapNamesDir), re, true)
	if cerr == nil {
		return attempt, nil
	}
	// Taken, or an ambiguous create: the record now under the name decides.
	ref, ok, err := c.resolveSnap(ctx, vol, req.Name)
	if err != nil || !ok {
		return "", catErr(CodeOutcomeUnknown, true, cerr, "snapshot %s/%s: name record outcome unknown; retry with the same request id", vol, req.Name)
	}
	if ref.rec.RequestID != req.RequestID {
		return "", catErr(CodeSnapshotExists, false, nil, "snapshot %s/%s exists (another request)", vol, req.Name)
	}
	return ref.rec.Attempt, nil
}

// SnapshotReplay resolves a snapshot request against the catalog before
// the caller cuts (B9 W2 §3.2 step 1): when the name is held by req's
// request and its attempt published, it returns that snapshot (Replayed,
// true); held by another request, snapshot_exists; free, or held by req's
// unpublished attempt, (zero, false, nil). It validates the whole request
// first (name, request id, labels, consistency), so a caller can refuse a
// bad request before it cuts.
func (c *Catalog) SnapshotReplay(ctx context.Context, vol string, req SnapshotRequest) (SnapshotInfo, bool, error) {
	if _, err := c.volRoot(vol); err != nil {
		return SnapshotInfo{}, false, err
	}
	if err := CheckSnapshotName(req.Name); err != nil {
		return SnapshotInfo{}, false, err
	}
	if req.RequestID == "" {
		return SnapshotInfo{}, false, catErr(CodeInvalidName, false, nil, "snapshot %s/%s: a request id is required", vol, req.Name)
	}
	if err := checkLabels(req.Labels); err != nil {
		return SnapshotInfo{}, false, err
	}
	if cs := req.Consistency; cs != "" && cs != "crash" && cs != "app" {
		return SnapshotInfo{}, false, catErr(CodeInvalidName, false, nil, "consistency %q: crash or app", cs)
	}
	ref, ok, err := c.resolveSnap(ctx, vol, req.Name)
	switch {
	case err != nil:
		return SnapshotInfo{}, false, err
	case !ok:
		return SnapshotInfo{}, false, nil
	case ref.rec.RequestID != req.RequestID:
		return SnapshotInfo{}, false, catErr(CodeSnapshotExists, false, nil, "snapshot %s/%s exists (another request)", vol, req.Name)
	case ref.entry == nil:
		return SnapshotInfo{}, false, nil // this request's attempt has not published: publish it
	case ref.info.State == SnapAborted:
		return SnapshotInfo{}, false, catErr(CodeSnapshotExists, false, nil, "snapshot %s/%s: this request's attempt was aborted; delete the name to reuse it", vol, req.Name)
	case ref.info.State == SnapDeleting:
		return SnapshotInfo{}, false, catErr(CodeSnapshotDeleting, false, nil, "snapshot %s is being deleted; wait for the delete to finish, then reuse the name", req.Name)
	}
	ref.info.Replayed = true
	return ref.info, true, nil
}

// ListSnapshots lists vol's named snapshots in name order, after
// opts.StartAfter: names whose attempt published and was not aborted. A
// deleted snapshot no longer holds its name (opts.IncludeDeleting adds the
// ones marked deleting that still do). next is the last name returned when
// more may follow, else "".
func (c *Catalog) ListSnapshots(ctx context.Context, vol string, opts ListOptions) ([]SnapshotInfo, string, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return nil, "", err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	dir := path.Join(root, snapNamesDir)
	var out []SnapshotInfo
	after := opts.StartAfter
	for {
		page, err := c.f.ListEntries(ctx, dir, opts.Prefix, after, uint32(limit))
		if err != nil {
			return nil, "", remoteErr(err, "list snapshots of %s", vol)
		}
		for _, e := range page {
			after = e.GetName()
			ref, ok, err := c.resolveSnap(ctx, vol, e.GetName())
			if err != nil {
				return nil, "", err
			}
			if !ok || ref.entry == nil || ref.info.State == SnapAborted || (ref.info.State == SnapDeleting && !opts.IncludeDeleting) {
				continue
			}
			out = append(out, ref.info)
			if len(out) == limit {
				return out, after, nil
			}
		}
		if len(page) < limit {
			return out, "", nil
		}
	}
}

// DeleteSnapshot frees the name (spec §3.4, W5):
// it marks the attempt deleting (or, when the attempt never published,
// claims its path with an aborted tombstone so a late insert cannot land),
// then removes the name record. The attempt path is never removed. It
// returns SnapDeleting, or SnapAbsent when no snapshot holds the name; both
// are success, so a retry is always safe.
func (c *Catalog) DeleteSnapshot(ctx context.Context, vol, name string) (string, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return "", err
	}
	if err := CheckSnapshotName(name); err != nil {
		return "", err
	}
	dir := path.Join(root, snapsDir, name)
	for try := 0; try < 3; try++ {
		ref, ok, err := c.resolveSnap(ctx, vol, name)
		if err != nil {
			return "", err
		}
		if !ok {
			return SnapAbsent, nil
		}
		switch {
		case ref.entry == nil:
			tomb, err := c.abortedSnapEntry(vol, name, ref.rec.Attempt)
			if err != nil {
				return "", err
			}
			if err := c.f.CreateEntry(ctx, dir, tomb, true); err != nil && !errors.Is(err, ErrExists) {
				return "", remoteErr(err, "claim the unpublished attempt of %s/%s", vol, name)
			}
			continue // re-read: the path now holds a tombstone or the late snapshot
		case ref.info.State == SnapActive:
			// The one mark path shared with retention; forks do not hold it.
			m, _, err := c.markDeleting(ctx, vol, name, ref.rec.Attempt)
			if err != nil {
				return "", err
			}
			if m == markGone {
				continue
			}
		}
		// The attempt is deleting or aborted: free the name.
		err = c.f.DeleteEntry(ctx, path.Join(root, snapNamesDir), name, map[string][]byte{extAttempt: []byte(ref.rec.Attempt)})
		if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrPrecondition) {
			return "", remoteErr(err, "remove the name record %s/%s", vol, name)
		}
		return SnapDeleting, nil
	}
	return "", remoteErr(nil, "delete snapshot %s/%s: it kept changing", vol, name)
}

// ListVolumes returns the ids of every volume directory under the base (GC).
func (c *Catalog) ListVolumes(ctx context.Context) ([]string, error) {
	var out []string
	after := ""
	for {
		page, err := c.f.ListEntries(ctx, c.base, "", after, listPage)
		if err != nil {
			return nil, remoteErr(err, "list volumes under %s", c.base)
		}
		for _, e := range page {
			after = e.GetName()
			if e.GetIsDirectory() && CheckVolumeID(e.GetName()) == nil {
				out = append(out, e.GetName())
			}
		}
		if len(page) < listPage {
			return out, nil
		}
	}
}

// ---- fork ----

// Fork makes req.Target a new volume whose content is snapshot
// req.Source/req.Snapshot, in O(1) filer writes and no data copy.
//
// The target is resolved first, independently of the source:
//   - a target VOLUME from another request is target_exists, and so is one
//     from this request id with other parameters;
//   - a completed fork of this request (VOLUME and HEAD exist) replays the
//     result recorded in VOLUME, even if the source has changed since.
//
// Otherwise:
//  1. read the snapshot (snapshot_not_found, snapshot_deleting);
//  2. exclusively create the target's VOLUME (origin fork, the snapshot's
//     geometry, parent, and a fork record binding the request to the
//     snapshot's image digest and the result). An incomplete fork of this
//     request resumes only if the snapshot still has that exact image;
//  3. qualify every chunk ref as "@<owner>/<key>" (a ref already qualified
//     keeps its owner), under an intent in the target;
//  4. exclusively create the target's HEAD: gen 1, epoch 0, no holder, and
//     the snapshot's remote_lsn (LSNs carry forward).
//
// The HEAD write is refused once OpDeadline has passed since step 1. An
// uncertain outcome of step 2 or 4 is outcome_unknown; a retry with the same
// request id completes the fork. An attempt whose HEAD create stays
// unsettled keeps its own intent (that create may still land); no attempt
// or replay deletes another attempt's intent.
func (c *Catalog) Fork(ctx context.Context, req ForkRequest) (ForkResult, error) {
	return c.fork(ctx, req, OriginFork)
}

// Restore starts an independent restore of req.Source's snapshot into
// req.Target: the same steps as Fork (claim, qualified refs, HEAD), recorded
// with Origin restore. The target serves at once (lazy fill); the restore
// driver then hydrates every chunk, re-uploads the qualified ones under the
// target's own keys in bounded batch cuts, and MarkIndependent records the
// end of the lineage once HEAD holds no ref to another volume.
func (c *Catalog) Restore(ctx context.Context, req ForkRequest) (ForkResult, error) {
	return c.fork(ctx, req, OriginRestore)
}

// isForked reports whether origin made the volume from another volume's
// snapshot (a fork record exists and HEAD may hold qualified refs).
func isForked(origin string) bool { return origin == OriginFork || origin == OriginRestore }

func (c *Catalog) fork(ctx context.Context, req ForkRequest, origin string) (ForkResult, error) {
	if _, err := c.volRoot(req.Source); err != nil {
		return ForkResult{}, err
	}
	dstRoot, err := c.volRoot(req.Target)
	if err != nil {
		return ForkResult{}, err
	}
	if req.Source == req.Target {
		return ForkResult{}, catErr(CodeTargetExists, false, nil, "fork target %s is the source", req.Target)
	}
	if err := CheckSnapshotName(req.Snapshot); err != nil {
		return ForkResult{}, err
	}
	if req.RequestID == "" {
		return ForkResult{}, catErr(CodeInvalidName, false, nil, "fork %s: a request id is required", req.Target)
	}
	if err := checkLabels(req.Labels); err != nil {
		return ForkResult{}, err
	}
	tag := "fork:" + req.Target + "/" + req.RequestID

	// The target first: replay or refuse without reading the source.
	existing, ok, err := c.ReadVolume(ctx, req.Target)
	if err != nil {
		return ForkResult{}, err
	}
	if ok && existing.State == VolumeDeleted {
		return ForkResult{}, refuseDeleted(req.Target, "fork to a new volume id")
	}
	if ok {
		if err := c.checkForkRecord(existing, req, origin); err != nil {
			return ForkResult{}, err
		}
		h, has, err := c.ReadHead(ctx, req.Target)
		if err != nil {
			return ForkResult{}, err
		}
		if has && h.State == storage.ChunkHeadAborted {
			return ForkResult{}, catErr(CodeTargetExists, false, nil, "fork %s was aborted (its HEAD is a tombstone); fork to a new volume id", req.Target)
		}
		if has && headDeletingOrDeleted(h) {
			// The target is being (or was) deleted: never a successful
			// replay, also before its VOLUME is marked.
			return ForkResult{}, refuseDeleted(req.Target, "fork to a new volume id")
		}
		if has {
			return forkResult(existing, true), nil
		}
	}

	// The deadline starts before the parent is read, and every attempt
	// (a retry included) reads it again: a snapshot deleted before the read
	// is refused, and one deleted after it stays rooted by GC's grace past
	// readAt + deadline, the last moment this fork can publish
	// (INV-SNAP-DELETE-ANY-ORDER-001). The parent is not claimed.
	readAt := c.now()
	deadline := readAt.Add(c.deadline())
	var shareToken string
	if c.guard != nil { // the shared HEAD is the snapshot's, read below
		if shareToken, err = c.guard.Begin(ctx, req.Source); err != nil {
			return ForkResult{}, remoteErr(err, "share guard for %s", req.Source)
		}
	}
	snap, err := c.readForkParent(ctx, req)
	if err != nil {
		return ForkResult{}, err
	}
	if gone, err := c.volumeGone(ctx, req.Source); err != nil {
		return ForkResult{}, err
	} else if gone {
		return ForkResult{}, refuseDeleted(req.Source, "its snapshots cannot be forked")
	}
	if snap.geo == (Geometry{}) {
		return ForkResult{}, fmt.Errorf("chunkstore: snapshot %s/%s has no geometry", req.Source, req.Snapshot)
	}
	src := snap.Head()
	digest := snap.digest
	head := storage.ChunkHead{Gen: 1, Epoch: 0, Holder: "", RemoteLSN: src.RemoteLSN, Chunks: make(map[uint64]storage.ChunkRef, len(src.Chunks)), State: storage.ChunkHeadActive}
	qualified := 0
	if src.Segments != nil {
		head.Segments = make(map[uint64]storage.ChunkSegment, len(src.Segments))
		head.ChunkBytes, head.ManifestChunkCount = src.ChunkBytes, src.ManifestChunkCount
		for idx, ref := range src.Segments {
			ref.Key = qualifyManifestKey(req.Source, ref.Key)
			head.Segments[idx] = ref
			qualified++
		}
	}
	for idx, ref := range src.Chunks {
		if !strings.HasPrefix(ref.Key, "@") {
			ref.Key = "@" + req.Source + "/" + ref.Key
			qualified++
		}
		head.Chunks[idx] = ref
	}
	record := &ForkRecord{RequestID: req.RequestID, Source: req.Source, Snapshot: req.Snapshot, SourceGen: snap.HeadGen,
		HeadSHA256: digest, RemoteLSN: head.RemoteLSN, ChunkCount: manifestCount(head), QualifiedRefs: qualified}

	if ok {
		// An incomplete fork of this request resumes only on the same image.
		if existing.Fork.HeadSHA256 != digest || existing.Fork.SourceGen != snap.HeadGen || existing.Geometry != snap.geo {
			return ForkResult{}, catErr(CodeTargetExists, false, nil,
				"fork %s: snapshot %s/%s changed since this request's first attempt (gen %d -> %d); not resuming onto another image",
				req.Target, req.Source, req.Snapshot, existing.Fork.SourceGen, snap.HeadGen)
		}
	} else {
		if !c.now().Before(deadline) {
			return ForkResult{}, catErr(CodeDeadlineExceeded, true, nil, "fork %s: deadline passed; VOLUME not written", req.Target)
		}
		vol := VolumeInfo{Format: volumeFormat, VolumeID: req.Target, Geometry: snap.geo, Origin: origin, CreatedNs: c.now().UnixNano(),
			Parent: &Parent{VolumeID: req.Source, Snapshot: req.Snapshot, Gen: snap.HeadGen}, Fork: record}
		ve, err := volumeEntry(vol, req.RequestID)
		if err != nil {
			return ForkResult{}, err
		}
		// VOLUME names no chunk keys, so it needs no intent; a found VOLUME
		// is this request's when it binds the same request and image.
		outcome, got, cerr := c.createResolved(ctx, ctx, dstRoot, ve, "")
		if outcome != createdNow {
			if got == nil {
				return ForkResult{}, catErr(CodeOutcomeUnknown, true, cerr, "fork %s: VOLUME create outcome unknown; retry with the same request id", req.Target)
			}
			var v VolumeInfo
			got, body, err := readRecord(ctx, c.f, c.blobs, dstRoot, got)
			if err == nil {
				v, err = decodeVolume(path.Join(dstRoot, volumeName), got, body)
			}
			if err != nil || c.checkForkRecord(v, req, origin) != nil || v.Fork.HeadSHA256 != digest {
				return ForkResult{}, catErr(CodeTargetExists, false, nil, "volume %s exists (another request, or another image)", req.Target)
			}
		}
		existing = vol
	}

	if !c.now().Before(deadline) {
		return ForkResult{}, catErr(CodeDeadlineExceeded, true, nil, "fork %s: deadline passed; manifest preparation not started", req.Target)
	}
	head, upgradeIDs, err := c.prepareLegacyHead(ctx, dstRoot, storage.ChunkHead{}, head, snap.geo)
	defer c.dropManifestIntents(ctx, dstRoot, upgradeIDs)
	if err != nil {
		return ForkResult{}, err
	}
	he, err := headEntry(head)
	if err != nil {
		return ForkResult{}, err
	}
	he.Extended[extReq] = []byte(req.RequestID)
	if !c.now().Before(deadline) {
		return ForkResult{}, catErr(CodeDeadlineExceeded, true, nil, "fork %s: deadline passed; copy intent not written", req.Target)
	}
	id, err := putIntent(ctx, c.f, dstRoot, storage.ChunkIntent{Gen: 0, Epoch: 0, Holder: tag, Keys: headKeys(head), Segments: head.Segments, ChunkBytes: head.ChunkBytes,
		DeadlineNs: deadline.UnixNano()})
	if err != nil {
		return ForkResult{}, remoteErr(err, "intent for fork %s", req.Target)
	}
	if !c.now().Before(deadline) { // re-checked immediately before publishing
		// Clean abort: HEAD was never sent.
		c.dropIntent(ctx, dstRoot, id)
		return ForkResult{}, catErr(CodeDeadlineExceeded, true, nil, "fork %s: %v since reading the snapshot, over the %v deadline; HEAD not written", req.Target, c.now().Sub(readAt), c.deadline())
	}
	if err := c.finishShare(ctx, req.Source, shareToken, src); err != nil {
		// Clean abort: HEAD was never sent.
		c.dropIntent(ctx, dstRoot, id)
		return ForkResult{}, err
	}
	// Under this request's VOLUME only this request's attempts write HEAD
	// (attach refuses a fork without HEAD), so a HEAD found there completes
	// the fork; if it is another attempt's and this create is unsettled,
	// this attempt's intent stays (the create may land after HEAD is gone).
	pctx, cancel := c.publishCtx(ctx, deadline)
	outcome, got, cerr := c.createResolved(ctx, pctx, dstRoot, he, id)
	cancel()
	// The HEAD now at the target decides, from that one read: a real HEAD
	// completes the fork; an aborted tombstone means a sweep claimed the
	// target first, so this fork lost and its create can never land.
	if got == nil {
		if outcome == createUnsettled {
			return ForkResult{}, catErr(CodeOutcomeUnknown, true, cerr, "fork %s: HEAD create outcome unknown; its intent is kept, retry with the same request id", req.Target)
		}
		c.dropIntent(ctx, dstRoot, id) // refused for good, but HEAD could not be read
		return ForkResult{}, remoteErr(cerr, "fork %s: HEAD exists but could not be read; retry with the same request id", req.Target)
	}
	got, body, err := readRecord(ctx, c.f, c.blobs, dstRoot, got)
	if err != nil {
		return ForkResult{}, remoteErr(err, "fork %s: read the target HEAD; retry with the same request id", req.Target)
	}
	h, err := decodeHead(path.Join(dstRoot, headName), got, body)
	if err != nil {
		return ForkResult{}, err
	}
	if h.State == storage.ChunkHeadAborted || headDeletingOrDeleted(h) {
		c.dropIntent(ctx, dstRoot, id) // the tombstone is permanent: this create never lands
		return ForkResult{}, catErr(CodeTargetExists, false, nil, "fork %s lost to a sweep: its HEAD is an aborted tombstone; fork to a new volume id", req.Target)
	}
	if outcome != createUnsettled {
		c.dropIntent(ctx, dstRoot, id) // confirmed, or refused for good
	}
	return forkResult(existing, outcome != createdNow), nil
}

// readForkParent returns the published snapshot fork req names. A parent
// deleting is refused (snapshot_deleting), an absent or aborted one
// (snapshot_not_found).
func (c *Catalog) readForkParent(ctx context.Context, req ForkRequest) (SnapshotInfo, error) {
	ref, ok, err := c.resolveSnap(ctx, req.Source, req.Snapshot)
	if err != nil {
		return SnapshotInfo{}, err
	}
	if !ok || ref.entry == nil || ref.info.State == SnapAborted {
		return SnapshotInfo{}, catErr(CodeSnapshotNotFound, false, nil, "snapshot %s/%s", req.Source, req.Snapshot)
	}
	if ref.info.State == SnapDeleting {
		return SnapshotInfo{}, catErr(CodeSnapshotDeleting, false, nil, "snapshot %s/%s is being deleted", req.Source, req.Snapshot)
	}
	if ref.info.geo.BlockSize != 0 && ref.info.geo.NumBlocks > geometry.MaxVolumeBytes/uint64(ref.info.geo.BlockSize) {
		return SnapshotInfo{}, catErr(CodeGeometryMismatch, false, nil, "fork size exceeds maximum %d bytes (16 TiB - 4 KiB; uint32 LBA)", geometry.MaxVolumeBytes)
	}
	return ref.info, nil
}

// AbortFork settles a dead fork for a GC sweep (W5): it claims the
// target's HEAD, the fork's publishing entry, with
// an exclusive create of an aborted HEAD tombstone. claimed is true when
// HEAD is a tombstone (this one or an earlier one): the fork can never
// publish, and the target never attaches. false: the fork published, and
// its keys are reachable through that HEAD. Either way the fork's copy
// intent can then be deleted. Volume ids are never reused, so the
// tombstone stays.
func (c *Catalog) AbortFork(ctx context.Context, target string) (claimed bool, err error) {
	root, err := c.volRoot(target)
	if err != nil {
		return false, err
	}
	he, err := headEntry(storage.ChunkHead{Gen: 1, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadAborted})
	if err != nil {
		return false, err
	}
	switch err := c.f.CreateEntry(ctx, root, he, true); {
	case err == nil:
		return true, nil
	case !errors.Is(err, ErrExists):
		return false, remoteErr(err, "aborted HEAD of %s", target)
	}
	h, _, err := c.ReadHead(ctx, target)
	if err != nil {
		return false, err
	}
	return h.State == storage.ChunkHeadAborted, nil
}

// checkForkRecord refuses a target VOLUME that is not this fork request's.
func (c *Catalog) checkForkRecord(v VolumeInfo, req ForkRequest, origin string) error {
	if v.Origin != origin {
		return catErr(CodeTargetExists, false, nil, "volume %s exists (origin %s, not %s)", req.Target, v.Origin, origin)
	}
	if v.Fork == nil || v.Fork.RequestID != req.RequestID {
		return catErr(CodeTargetExists, false, nil, "volume %s exists (another request)", req.Target)
	}
	if v.Fork.Source != req.Source || v.Fork.Snapshot != req.Snapshot {
		return catErr(CodeTargetExists, false, nil, "volume %s: request id %s was used for a fork of %s/%s", req.Target, req.RequestID, v.Fork.Source, v.Fork.Snapshot)
	}
	return nil
}

// forkResult is the result recorded when the fork created VOLUME.
func forkResult(v VolumeInfo, replayed bool) ForkResult {
	r := v.Fork
	return ForkResult{Target: v.VolumeID, Source: r.Source, Snapshot: r.Snapshot, SourceGen: r.SourceGen, HeadGen: 1, HeadEpoch: 0,
		RemoteLSN: r.RemoteLSN, ChunkCount: r.ChunkCount, QualifiedRefs: r.QualifiedRefs, Replayed: replayed}
}
