package chunkstore

// Catalog: the chunk control plane's filer metadata and Fork (B9 W2 spec
// §3-§5). Under the chunk base directory, per volume:
//
//	<base>/<vol>/HEAD          the published HEAD (ChunkRemote)
//	<base>/<vol>/keys/<key>    chunk key bindings (ChunkRemote)
//	<base>/<vol>/intents/<id>  live intents (ChunkRemote; GC roots)
//	<base>/<vol>/VOLUME        identity, geometry and lineage; write-once
//	<base>/<vol>/snaps/<name>  snapshots: a full copy of one HEAD document
//
// No entry carries filer chunks (UpdateEntry would delete chunks a new
// entry drops); every entry carries swb.kind and is checked on read, and an
// entry that does not check out is an error, never "absent".
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

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	kindChunkVolume = "chunkvolume"
	kindChunkSnap   = "chunksnap"

	volumeName = "VOLUME"
	snapsDir   = "snaps"

	extReq           = "swb.req"
	extAttempt       = "swb.attempt" // the id of the intent of the attempt that wrote the entry
	extSnapGen       = "swb.snap.gen"
	extSnapEpoch     = "swb.snap.epoch"
	extSnapLSN       = "swb.snap.lsn"
	extSnapState     = "swb.snap.state"
	extSnapDeletedNs = "swb.snap.deleted_ns"

	volumeFormat = 1
	snapFormat   = 1

	// SnapActive and SnapDeleting are a snapshot's states.
	SnapActive   = "active"
	SnapDeleting = "deleting"
	// SnapAbsent is DeleteSnapshot's answer for a snapshot that is not there.
	SnapAbsent = "absent"

	// OriginCreate and OriginFork are a volume's origins.
	OriginCreate = "create"
	OriginFork   = "fork"

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

	head storage.ChunkHead
	geo  Geometry
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
	// SourceReadAt is when the caller obtained the HEAD it snapshots (the
	// cut's return); OpDeadline counts from it. Zero means now.
	SourceReadAt time.Time
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
	f    Filer
	base string
	// OpDeadline bounds a snapshot or fork (DefaultOpDeadline when zero).
	OpDeadline time.Duration
	now        func() time.Time
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
	return &Catalog{f: f, base: base, now: time.Now}, nil
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
	case headName, volumeName, keysDir, snapsDir, intentsDir:
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
		return catErr(CodeInvalidName, false, nil, "%d labels, at most %d", len(labels), maxLabels)
	}
	for k, v := range labels {
		if k == "" || len(k) > maxLabelKey || len(v) > maxLabelValue {
			return catErr(CodeInvalidName, false, nil, "label %q: key 1-%d characters, value at most %d bytes", k, maxLabelKey, maxLabelValue)
		}
		for _, r := range k {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
				return catErr(CodeInvalidName, false, nil, "label key %q: only [a-z0-9._-]", k)
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

func decodeVolume(p string, e *filerpb.Entry) (VolumeInfo, error) {
	if len(e.GetChunks()) != 0 {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s carries %d filer chunks", p, len(e.Chunks))
	}
	if k := string(e.Extended[extKind]); k != kindChunkVolume {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: kind %q, want %q", p, k, kindChunkVolume)
	}
	var v VolumeInfo
	if err := json.Unmarshal(e.GetContent(), &v); err != nil {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: decode: %v", p, err)
	}
	if v.Format != volumeFormat {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: format %d not supported", p, v.Format)
	}
	if v.Origin != OriginCreate && v.Origin != OriginFork {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: origin %q", p, v.Origin)
	}
	if v.Origin == OriginFork && (v.Fork == nil || v.Fork.RequestID == "") {
		return VolumeInfo{}, fmt.Errorf("chunkstore: VOLUME %s: a fork without its fork record", p)
	}
	return v, nil
}

// ReadVolume returns the VOLUME entry; ok is false when it does not exist.
func (c *Catalog) ReadVolume(ctx context.Context, vol string) (VolumeInfo, bool, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return VolumeInfo{}, false, err
	}
	e, err := c.f.LookupEntry(ctx, root, volumeName)
	if errors.Is(err, ErrNotFound) {
		return VolumeInfo{}, false, nil
	}
	if err != nil {
		return VolumeInfo{}, false, remoteErr(err, "read VOLUME of %s", vol)
	}
	v, err := decodeVolume(path.Join(root, volumeName), e)
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
	if v.Geometry != g {
		return VolumeInfo{}, catErr(CodeGeometryMismatch, false, nil, "volume %s: VOLUME geometry %+v, this host serves %+v", vol, v.Geometry, g)
	}
	if v.Origin == OriginFork {
		has, err := c.headExists(ctx, root)
		if err != nil {
			return VolumeInfo{}, remoteErr(err, "read HEAD of %s", vol)
		}
		if !has {
			return VolumeInfo{}, catErr(CodeForkIncomplete, false, nil, "volume %s is a fork whose HEAD is not written yet", vol)
		}
	}
	return v, nil
}

// ---- snapshots ----

func (c *Catalog) readSnap(ctx context.Context, vol, name string) (SnapshotInfo, string, *filerpb.Entry, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return SnapshotInfo{}, "", nil, err
	}
	e, err := c.f.LookupEntry(ctx, path.Join(root, snapsDir), name)
	if errors.Is(err, ErrNotFound) {
		return SnapshotInfo{}, "", nil, catErr(CodeSnapshotNotFound, false, nil, "snapshot %s/%s", vol, name)
	}
	if err != nil {
		return SnapshotInfo{}, "", nil, remoteErr(err, "read snapshot %s/%s", vol, name)
	}
	info, req, err := decodeSnap(path.Join(root, snapsDir, name), e)
	return info, req, e, err
}

func decodeSnap(p string, e *filerpb.Entry) (SnapshotInfo, string, error) {
	fail := func(format string, a ...any) (SnapshotInfo, string, error) {
		return SnapshotInfo{}, "", fmt.Errorf("chunkstore: snapshot %s: "+format, append([]any{p}, a...)...)
	}
	if len(e.GetChunks()) != 0 {
		return fail("carries %d filer chunks; refusing", len(e.Chunks))
	}
	if k := string(e.Extended[extKind]); k != kindChunkSnap {
		return fail("kind %q, want %q", k, kindChunkSnap)
	}
	var doc snapDoc
	if err := json.Unmarshal(e.GetContent(), &doc); err != nil {
		return fail("decode: %v", err)
	}
	if doc.Format != snapFormat {
		return fail("format %d not supported", doc.Format)
	}
	head, err := decodeHeadDoc(p+"#head", doc.Head)
	if err != nil {
		return fail("%v", err)
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
		CreatedNs: doc.CreatedNs, State: state, ChunkCount: len(head.Chunks), ManifestBytes: len(e.GetContent()),
		Consistency: doc.Consistency, Labels: doc.Labels, Parent: doc.Parent, ExpiresAtNs: doc.ExpiresAtNs,
		head: head, geo: doc.Geometry,
	}, doc.RequestID, nil
}

// dropIntent deletes one attempt's own intent, once that attempt's create
// is confirmed or can never apply. It never touches another attempt's
// intent, even under the same operation: an earlier attempt's create may
// still be held by the server and land later (W5 rule). A failed delete
// leaves the intent, which only over-protects.
func (c *Catalog) dropIntent(ctx context.Context, root, id string) {
	_ = c.f.DeleteEntry(context.WithoutCancel(ctx), path.Join(root, intentsDir), id, nil)
}

type createOutcome int

const (
	createdNow      createOutcome = iota // this attempt's create applied (confirmed)
	createRefused                        // the filer refused it (the name exists): it will never apply
	createUnsettled                      // no answer: it may still apply, even later
)

// createResolved makes this attempt's exclusive create of e under dir and
// returns its outcome and the entry now under the name (nil if unread).
// attempt, when not empty, is stored in e as extAttempt; a re-read that
// finds it confirms this attempt's create. Only the filer's own "already
// exists" answer settles a failure as refused; any other failure leaves
// the create unsettled, whatever entry the re-read finds (that entry may
// be another attempt's, and this one may still land after it is gone).
func (c *Catalog) createResolved(ctx context.Context, dir string, e *filerpb.Entry, attempt string) (createOutcome, *filerpb.Entry, error) {
	if attempt != "" {
		e.Extended[extAttempt] = []byte(attempt)
	}
	err := c.f.CreateEntry(ctx, dir, e, true)
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
	keys := make([]string, 0, len(h.Chunks))
	for _, ref := range h.Chunks {
		keys = append(keys, ref.Key)
	}
	return keys
}

// CreateSnapshot stores head, which must be the HEAD a cut of vol installed
// (ChunkCutResult.Head), as snaps/<req.Name>. A retry with the same
// RequestID returns the existing snapshot (Replayed); another RequestID
// gets snapshot_exists. An intent names the snapshot's keys from before
// the entry is written until after.
func (c *Catalog) CreateSnapshot(ctx context.Context, vol string, req SnapshotRequest, head storage.ChunkHead) (SnapshotInfo, error) {
	readAt := req.SourceReadAt
	if readAt.IsZero() {
		readAt = c.now()
	}
	root, err := c.volRoot(vol)
	if err != nil {
		return SnapshotInfo{}, err
	}
	if err := CheckSnapshotName(req.Name); err != nil {
		return SnapshotInfo{}, err
	}
	if req.RequestID == "" {
		return SnapshotInfo{}, catErr(CodeInvalidName, false, nil, "snapshot %s/%s: a request id is required", vol, req.Name)
	}
	if err := checkLabels(req.Labels); err != nil {
		return SnapshotInfo{}, err
	}
	consistency := req.Consistency
	if consistency == "" {
		consistency = "crash"
	}
	if consistency != "crash" && consistency != "app" {
		return SnapshotInfo{}, catErr(CodeInvalidName, false, nil, "consistency %q: crash or app", consistency)
	}
	if head.Gen == 0 {
		return SnapshotInfo{}, catErr(CodeNotChunkVolume, false, nil, "volume %s has no published HEAD to snapshot", vol)
	}
	if existing, replayed, err := c.SnapshotReplay(ctx, vol, req); err != nil || replayed {
		return existing, err
	}
	v, ok, err := c.ReadVolume(ctx, vol)
	if err != nil {
		return SnapshotInfo{}, err
	}
	if !ok {
		return SnapshotInfo{}, catErr(CodeNotChunkVolume, false, nil, "volume %s has no VOLUME entry", vol)
	}

	headEnt, err := headEntry(head)
	if err != nil {
		return SnapshotInfo{}, err
	}
	doc := snapDoc{Format: snapFormat, VolumeID: vol, Name: req.Name, RequestID: req.RequestID, CreatedNs: c.now().UnixNano(),
		CreatedBy: req.CreatedBy, Consistency: consistency, Head: headEnt.Content, Geometry: v.Geometry, Parent: v.Parent,
		Labels: req.Labels, ExpiresAtNs: req.ExpiresAtNs}
	body, err := json.Marshal(doc)
	if err != nil {
		return SnapshotInfo{}, err
	}
	now := time.Now().Unix()
	e := &filerpb.Entry{
		Name:       req.Name,
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

	tag := "snapshot:" + vol + "/" + req.Name + "/" + req.RequestID
	id, err := putIntent(ctx, c.f, root, storage.ChunkIntent{Gen: head.Gen, Epoch: head.Epoch, Holder: tag, Keys: headKeys(head)})
	if err != nil {
		return SnapshotInfo{}, remoteErr(err, "intent for snapshot %s/%s", vol, req.Name)
	}
	if c.now().Sub(readAt) > c.deadline() {
		// Clean abort: the snapshot entry was never sent.
		c.dropIntent(ctx, root, id)
		return SnapshotInfo{}, catErr(CodeDeadlineExceeded, true, nil, "snapshot %s/%s: %v since the cut, over the %v deadline; nothing written", vol, req.Name, c.now().Sub(readAt), c.deadline())
	}
	dir := path.Join(root, snapsDir)
	outcome, got, cerr := c.createResolved(ctx, dir, e, id)
	if outcome != createUnsettled {
		c.dropIntent(ctx, root, id) // confirmed, or refused for good
	}
	if outcome == createdNow {
		info, _, _, err := c.readSnap(ctx, vol, req.Name)
		return info, err
	}
	// Not this attempt's entry. When unsettled, its intent stays: this
	// create may still land after the entry found here is removed.
	if got == nil && outcome == createUnsettled {
		return SnapshotInfo{}, catErr(CodeOutcomeUnknown, true, cerr, "snapshot %s/%s: create outcome unknown; its intent is kept, retry with the same request id", vol, req.Name)
	}
	if got == nil {
		return SnapshotInfo{}, remoteErr(cerr, "snapshot %s/%s exists but could not be read; retry with the same request id", vol, req.Name)
	}
	if string(got.Extended[extReq]) != req.RequestID {
		return SnapshotInfo{}, catErr(CodeSnapshotExists, false, nil, "snapshot %s/%s exists (another request)", vol, req.Name)
	}
	info, _, err := decodeSnap(path.Join(dir, req.Name), got)
	if err != nil {
		return SnapshotInfo{}, err
	}
	info.Replayed = true // another attempt of this request wrote it
	return info, nil
}

// SnapshotReplay resolves a snapshot request against the catalog before
// the caller cuts (B9 W2 §3.2 step 1): when snaps/<req.Name> exists with
// req's request id it returns that snapshot (Replayed, true); with another
// request id, snapshot_exists; when the name is free, (zero, false, nil).
// It validates the name and the request id first.
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
	existing, prev, _, err := c.readSnap(ctx, vol, req.Name)
	switch {
	case err == nil && prev == req.RequestID:
		existing.Replayed = true
		return existing, true, nil
	case err == nil:
		return SnapshotInfo{}, false, catErr(CodeSnapshotExists, false, nil, "snapshot %s/%s exists (another request)", vol, req.Name)
	case CodeOf(err) == CodeSnapshotNotFound:
		return SnapshotInfo{}, false, nil
	default:
		return SnapshotInfo{}, false, err
	}
}

// ListSnapshots lists vol's snapshots in name order, after opts.StartAfter.
// next is the last name returned when more may follow, else "".
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
	dir := path.Join(root, snapsDir)
	var out []SnapshotInfo
	after := opts.StartAfter
	for {
		page, err := c.f.ListEntries(ctx, dir, opts.Prefix, after, uint32(limit))
		if err != nil {
			return nil, "", remoteErr(err, "list snapshots of %s", vol)
		}
		for _, e := range page {
			info, _, err := decodeSnap(path.Join(dir, e.GetName()), e)
			if err != nil {
				return nil, "", err
			}
			after = e.GetName()
			if info.State == SnapDeleting && !opts.IncludeDeleting {
				continue
			}
			out = append(out, info)
			if len(out) == limit {
				return out, after, nil
			}
		}
		if len(page) < limit {
			return out, "", nil
		}
	}
}

// DeleteSnapshot marks snaps/<name> deleting; GC removes it later (spec
// §3.4). It returns SnapDeleting, or SnapAbsent when there is no such
// snapshot; both are success, so a retry is always safe.
func (c *Catalog) DeleteSnapshot(ctx context.Context, vol, name string) (string, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return "", err
	}
	if err := CheckSnapshotName(name); err != nil {
		return "", err
	}
	for attempt := 0; attempt < 2; attempt++ {
		info, _, e, err := c.readSnap(ctx, vol, name)
		if CodeOf(err) == CodeSnapshotNotFound {
			return SnapAbsent, nil
		}
		if err != nil {
			return "", err
		}
		if info.State == SnapDeleting {
			return SnapDeleting, nil
		}
		e.Extended[extSnapState] = []byte(SnapDeleting)
		e.Extended[extSnapDeletedNs] = []byte(strconv.FormatInt(c.now().UnixNano(), 10))
		err = c.f.UpdateEntry(ctx, path.Join(root, snapsDir), e, map[string][]byte{
			extSnapState: []byte(SnapActive),
			extSnapGen:   []byte(strconv.FormatUint(info.HeadGen, 10)),
		})
		switch {
		case err == nil:
			return SnapDeleting, nil
		case errors.Is(err, ErrPrecondition), errors.Is(err, ErrNotFound):
			continue // re-read once
		default:
			return "", remoteErr(err, "mark snapshot %s/%s deleting", vol, name)
		}
	}
	return "", remoteErr(nil, "mark snapshot %s/%s deleting: it kept changing", vol, name)
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
	if ok {
		if err := c.checkForkRecord(existing, req); err != nil {
			return ForkResult{}, err
		}
		has, err := c.headExists(ctx, dstRoot)
		if err != nil {
			return ForkResult{}, remoteErr(err, "read HEAD of %s", req.Target)
		}
		if has {
			return forkResult(existing, true), nil
		}
	}

	readAt := c.now()
	snap, _, _, err := c.readSnap(ctx, req.Source, req.Snapshot)
	if err != nil {
		return ForkResult{}, err
	}
	if snap.State == SnapDeleting {
		return ForkResult{}, catErr(CodeSnapshotDeleting, false, nil, "snapshot %s/%s is being deleted", req.Source, req.Snapshot)
	}
	if snap.geo == (Geometry{}) {
		return ForkResult{}, fmt.Errorf("chunkstore: snapshot %s/%s has no geometry", req.Source, req.Snapshot)
	}
	src := snap.Head()
	digest, err := headDigest(src)
	if err != nil {
		return ForkResult{}, err
	}
	head := storage.ChunkHead{Gen: 1, Epoch: 0, Holder: "", RemoteLSN: src.RemoteLSN, Chunks: make(map[uint64]storage.ChunkRef, len(src.Chunks)), State: storage.ChunkHeadActive}
	qualified := 0
	for idx, ref := range src.Chunks {
		if !strings.HasPrefix(ref.Key, "@") {
			ref.Key = "@" + req.Source + "/" + ref.Key
			qualified++
		}
		head.Chunks[idx] = ref
	}
	record := &ForkRecord{RequestID: req.RequestID, Source: req.Source, Snapshot: req.Snapshot, SourceGen: snap.HeadGen,
		HeadSHA256: digest, RemoteLSN: head.RemoteLSN, ChunkCount: len(head.Chunks), QualifiedRefs: qualified}

	if ok {
		// An incomplete fork of this request resumes only on the same image.
		if existing.Fork.HeadSHA256 != digest || existing.Fork.SourceGen != snap.HeadGen || existing.Geometry != snap.geo {
			return ForkResult{}, catErr(CodeTargetExists, false, nil,
				"fork %s: snapshot %s/%s changed since this request's first attempt (gen %d -> %d); not resuming onto another image",
				req.Target, req.Source, req.Snapshot, existing.Fork.SourceGen, snap.HeadGen)
		}
	} else {
		vol := VolumeInfo{Format: volumeFormat, VolumeID: req.Target, Geometry: snap.geo, Origin: OriginFork, CreatedNs: c.now().UnixNano(),
			Parent: &Parent{VolumeID: req.Source, Snapshot: req.Snapshot, Gen: snap.HeadGen}, Fork: record}
		ve, err := volumeEntry(vol, req.RequestID)
		if err != nil {
			return ForkResult{}, err
		}
		// VOLUME names no chunk keys, so it needs no intent; a found VOLUME
		// is this request's when it binds the same request and image.
		outcome, got, cerr := c.createResolved(ctx, dstRoot, ve, "")
		if outcome != createdNow {
			if got == nil {
				return ForkResult{}, catErr(CodeOutcomeUnknown, true, cerr, "fork %s: VOLUME create outcome unknown; retry with the same request id", req.Target)
			}
			v, err := decodeVolume(path.Join(dstRoot, volumeName), got)
			if err != nil || c.checkForkRecord(v, req) != nil || v.Fork.HeadSHA256 != digest {
				return ForkResult{}, catErr(CodeTargetExists, false, nil, "volume %s exists (another request, or another image)", req.Target)
			}
		}
		existing = vol
	}

	he, err := headEntry(head)
	if err != nil {
		return ForkResult{}, err
	}
	he.Extended[extReq] = []byte(req.RequestID)
	id, err := putIntent(ctx, c.f, dstRoot, storage.ChunkIntent{Gen: 0, Epoch: 0, Holder: tag, Keys: headKeys(head)})
	if err != nil {
		return ForkResult{}, remoteErr(err, "intent for fork %s", req.Target)
	}
	if c.now().Sub(readAt) > c.deadline() {
		// Clean abort: HEAD was never sent.
		c.dropIntent(ctx, dstRoot, id)
		return ForkResult{}, catErr(CodeDeadlineExceeded, true, nil, "fork %s: %v since reading the snapshot, over the %v deadline; HEAD not written", req.Target, c.now().Sub(readAt), c.deadline())
	}
	// Under this request's VOLUME only this request's attempts write HEAD
	// (attach refuses a fork without HEAD), so a HEAD found there completes
	// the fork; if it is another attempt's and this create is unsettled,
	// this attempt's intent stays (the create may land after HEAD is gone).
	outcome, got, cerr := c.createResolved(ctx, dstRoot, he, id)
	if outcome != createUnsettled {
		c.dropIntent(ctx, dstRoot, id) // confirmed, or refused for good
	}
	if outcome == createUnsettled && got == nil {
		return ForkResult{}, catErr(CodeOutcomeUnknown, true, cerr, "fork %s: HEAD create outcome unknown; its intent is kept, retry with the same request id", req.Target)
	}
	return forkResult(existing, outcome != createdNow), nil // refused: the filer says HEAD exists
}

// checkForkRecord refuses a target VOLUME that is not this fork request's.
func (c *Catalog) checkForkRecord(v VolumeInfo, req ForkRequest) error {
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
