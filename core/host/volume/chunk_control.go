package volume

// /chunk/*: the walstore-chunk control routes on the status listener
// (B9 W2 spec §2.1-§2.2, §3.1-§3.4, §3.6). They are off by default
// (EnableChunkControlEndpoint, flag --chunk-control-endpoint), share the
// listener's loopback rule, take JSON request bodies (POST) or a
// ?volume= query (GET), and answer failures with a typed JSON body
// {"code","message","retryable"}.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ChunkControl is what the /chunk/* routes call for the served volume.
type ChunkControl interface {
	// Cut takes one quiesced cut; with minLSN > 0 it fails unless the cut
	// reaches minLSN (MakeRemoteDurable's contract).
	Cut(ctx context.Context, volumeID string, minLSN uint64) (ChunkCutResponse, error)
	Snapshot(ctx context.Context, volumeID string, req ChunkSnapshotRequest) (chunkstore.SnapshotInfo, error)
	ListSnapshots(ctx context.Context, volumeID string, opts chunkstore.ListOptions) ([]chunkstore.SnapshotInfo, string, error)
	DeleteSnapshot(ctx context.Context, volumeID, name string) (string, error)
	Detach(ctx context.Context, volumeID string) (ChunkDetachResponse, error)
	// Usage is the volume's space accounting and retention policy
	// (GET /status/chunk).
	Usage(ctx context.Context, volumeID string) (chunkstore.VolumeUsage, error)
}

// ChunkCutResponse is the body of a successful POST /chunk/cut (§3.1).
type ChunkCutResponse struct {
	VolumeID  string `json:"volumeID"`
	CutLSN    uint64 `json:"cutLSN"`
	HeadGen   uint64 `json:"headGen"`
	HeadEpoch uint64 `json:"headEpoch"`
	Holder    string `json:"holder"`
	Noop      bool   `json:"noop"`
	FullHash  bool   `json:"fullHash"`
	// Stats are the statistics the chunk layer measured for this cut
	// (§3.1). A zero field is "not measured" or "none" (a Noop cut
	// uploads nothing).
	Stats ChunkCutStatsBody `json:"stats"`
}

// ChunkCutStatsBody is one cut's statistics in the /chunk/cut body. It
// mirrors storage.ChunkCutStats field for field (a test keeps the two in
// step); every duration is in nanoseconds.
type ChunkCutStatsBody struct {
	PayloadNs          int64  `json:"payloadNs"`
	COWWALReadAttempts uint64 `json:"cowWALReadAttempts"`
	COWLocalReadNs     int64  `json:"cowLocalReadNs"`
	COWSavedChunks     int    `json:"cowSavedChunks"`
	COWSaveNs          int64  `json:"cowSaveNs"`
	COWWaitNs          int64  `json:"cowWaitNs"`
	BarrierPauseNs     int64  `json:"barrierPauseNs"`
	LockWaitNs         int64  `json:"lockWaitNs"`
	SyncNs             int64  `json:"syncNs"`
	CopyNs             int64  `json:"copyNs"`
	CandidateChunks    int    `json:"candidateChunks"`
	UploadedChunks     int    `json:"uploadedChunks"`
	UploadedBytes      int64  `json:"uploadedBytes"`
	ZeroedChunks       int    `json:"zeroedChunks"`
	UnchangedChunks    int    `json:"unchangedChunks"`
	UploadNs           int64  `json:"uploadNs"`
	CASNs              int64  `json:"casNs"`
	CASResult          string `json:"casResult"`
	TotalNs            int64  `json:"totalNs"`
	StageRounds        int    `json:"stageRounds"`
	StagedChunks       int    `json:"stagedChunks"`
	StagedChunksUsed   int    `json:"stagedChunksUsed"`
	BarrierCopyChunks  int    `json:"barrierCopyChunks"`
	BarrierOverBound   bool   `json:"barrierOverBound"`
}

func cutStatsBody(s storage.ChunkCutStats) ChunkCutStatsBody {
	return ChunkCutStatsBody{
		PayloadNs:          s.PayloadNs,
		COWWALReadAttempts: s.COWWALReadAttempts,
		COWLocalReadNs:     s.COWLocalReadNs,
		BarrierPauseNs:     s.BarrierPauseNs,
		LockWaitNs:         s.LockWaitNs,
		SyncNs:             s.SyncNs,
		CopyNs:             s.CopyNs,
		COWSavedChunks:     s.COWSavedChunks,
		COWSaveNs:          s.COWSaveNs,
		COWWaitNs:          s.COWWaitNs,
		CandidateChunks:    s.CandidateChunks,
		UploadedChunks:     s.UploadedChunks,
		UploadedBytes:      s.UploadedBytes,
		ZeroedChunks:       s.ZeroedChunks,
		UnchangedChunks:    s.UnchangedChunks,
		UploadNs:           s.UploadNs,
		CASNs:              s.CASNs,
		CASResult:          s.CASResult,
		TotalNs:            s.TotalNs,
		StageRounds:        s.StageRounds,
		StagedChunks:       s.StagedChunks,
		StagedChunksUsed:   s.StagedChunksUsed,
		BarrierCopyChunks:  s.BarrierCopyChunks,
		BarrierOverBound:   s.BarrierOverBound,
	}
}

// ChunkSnapshotRequest is POST /chunk/snapshot's request (§3.2), without
// the volume and the timeout.
type ChunkSnapshotRequest struct {
	Name        string
	RequestID   string
	Labels      map[string]string
	Consistency string
	ExpiresAtNs int64
	Class       string // retention class: "semantic" or "pinned"
}

// ChunkDetachResponse is the body of a successful POST /chunk/detach (§3.6).
type ChunkDetachResponse struct {
	Precopy        *storage.PrecopyReport `json:"precopy,omitempty"`
	VolumeID       string                 `json:"volumeID"`
	CutLSN         uint64                 `json:"cutLSN"`
	HeadGen        uint64                 `json:"headGen"`
	HeadEpoch      uint64                 `json:"headEpoch"`
	LastWrittenLSN uint64                 `json:"lastWrittenLSN"`
}

// snapshotBody is POST /chunk/snapshot's success body, exactly §3.2's fields.
type snapshotBody struct {
	VolumeID      string            `json:"volumeID"`
	Name          string            `json:"name"`
	HeadGen       uint64            `json:"headGen"`
	HeadEpoch     uint64            `json:"headEpoch"`
	CutLSN        uint64            `json:"cutLSN"`
	CreatedNs     int64             `json:"createdNs"`
	ChunkCount    int               `json:"chunkCount"`
	ManifestBytes int               `json:"manifestBytes"`
	Consistency   string            `json:"consistency"`
	Labels        map[string]string `json:"labels"`
	Replayed      bool              `json:"replayed"`
}

type chunkErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// The codes the routes add to the catalog's (B9 §2.2 plus the 2026-11-15
// rulings: awaiting_assignment, reconciling, internal, lsn_behind, and the
// transport errors bad_request, method_not_allowed, forbidden; not_found
// for an unknown /chunk path).
const (
	codeVolumeNotServed    = "volume_not_served"
	codeNotPrimary         = "not_primary"
	codeAwaitingAssignment = "awaiting_assignment"
	codeEpochStale         = "epoch_stale"
	codeBaseInstalling     = "base_installing"
	codeReconciling        = "reconciling"
	codeFenced             = "fenced"
	codeHeadConflict       = "head_conflict"
	codeClosing            = "closing"
	codeDetachAborted      = "detach_aborted" // the detach did not happen; the volume keeps serving here
	codeLSNBehind          = "lsn_behind"
	codeInternal           = "internal"
	codeBadRequest         = "bad_request"
	codeMethodNotAllowed   = "method_not_allowed"
	codeForbidden          = "forbidden"
	codeNotFound           = "not_found"
)

const chunkBodyLimit = 64 << 10

// EnableChunkControlEndpoint mounts the /chunk/* routes. Must be called
// BEFORE Start; the routes answer not_chunk_volume until SetChunkControl.
func (s *StatusServer) EnableChunkControlEndpoint() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunkEnabled = true
}

// SetChunkControl wires the routes to the volume's chunk control. It may be
// called before or after Start (blockvolume binds the listener before the
// durable provider exists).
func (s *StatusServer) SetChunkControl(c ChunkControl) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunkControl = c
}

func (s *StatusServer) chunkMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/chunk/cut", s.handleChunkCut)
	mux.HandleFunc("/chunk/snapshot", s.handleChunkSnapshot)
	mux.HandleFunc("/chunk/snapshots", s.handleChunkSnapshots)
	mux.HandleFunc("/chunk/snapshot/delete", s.handleChunkSnapshotDelete)
	mux.HandleFunc("/chunk/detach", s.handleChunkDetach)
	// Any other /chunk path: a typed JSON 404, like every other /chunk
	// error, not net/http's plain-text page.
	mux.HandleFunc("/chunk/", func(w http.ResponseWriter, r *http.Request) {
		writeChunkErr(w, http.StatusNotFound, codeNotFound, false, "no /chunk route %q", r.URL.Path)
	})
	return mux
}

func writeChunkJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeChunkErr(w http.ResponseWriter, status int, code string, retryable bool, format string, a ...any) {
	writeChunkJSON(w, status, chunkErrorBody{Code: code, Message: fmt.Sprintf(format, a...), Retryable: retryable})
}

// writeChunkFailure answers err with its B9 code and HTTP status.
func writeChunkFailure(w http.ResponseWriter, err error) {
	status, code, retryable := chunkErrorCode(err)
	msg := err.Error()
	var ce *chunkstore.CatalogError
	if errors.As(err, &ce) {
		msg = ce.Detail() // the code is in its own field
	}
	writeChunkJSON(w, status, chunkErrorBody{Code: code, Message: msg, Retryable: retryable})
}

// chunkErrorCode maps an operation's error to (HTTP status, code,
// retryable). An unresolved CAS reports outcome_unknown even when the
// attempt was also refused or the re-read failed on the remote; otherwise
// a refused capture reports why it was refused.
func chunkErrorCode(err error) (int, string, bool) {
	var ce *chunkstore.CatalogError
	switch {
	case errors.Is(err, storage.ErrPrecopyUnknown):
		return http.StatusConflict, "precopy_unknown", true
	case errors.Is(err, storage.ErrPrecopyNotConverged):
		return http.StatusConflict, "precopy_not_converged", true
	case errors.Is(err, durable.ErrDetachAborted):
		// Before any cause (a catalog error included): nothing was
		// published and serving resumed here.
		return http.StatusConflict, codeDetachAborted, true
	case errors.Is(err, durable.ErrDetachOutcomeUnknown):
		// Before any cause: the detach may still have landed; I/O stays stopped.
		return http.StatusServiceUnavailable, chunkstore.CodeOutcomeUnknown, true
	case errors.As(err, &ce):
		return catalogStatus(ce.Code), ce.Code, ce.Retryable
	case errors.Is(err, storage.ErrChunkLayerFenced):
		return http.StatusConflict, codeFenced, false
	case errors.Is(err, storage.ErrChunkCASOutcomeUnknown):
		// An earlier CAS may still land: report that even when this
		// attempt was also refused (the re-proposal re-checks admission).
		return http.StatusServiceUnavailable, chunkstore.CodeOutcomeUnknown, true
	case errors.Is(err, storage.ErrChunkLSNBehind):
		return http.StatusConflict, codeLSNBehind, false
	case errors.Is(err, durable.ErrChunkAwaitingAuthority):
		return http.StatusConflict, codeAwaitingAssignment, true
	case errors.Is(err, durable.ErrChunkNotPrimary):
		return http.StatusConflict, codeNotPrimary, false
	case errors.Is(err, durable.ErrChunkEpochStale):
		return http.StatusConflict, codeEpochStale, false
	case errors.Is(err, storage.ErrChunkBaseInstalling):
		return http.StatusConflict, codeBaseInstalling, true
	case errors.Is(err, storage.ErrChunkReconciling):
		// HEAD does not move until the dirty set is reconciled; retry.
		return http.StatusConflict, codeReconciling, true
	case errors.Is(err, storage.ErrChunkHeadPrecondition):
		return http.StatusConflict, codeHeadConflict, true
	case errors.Is(err, storage.ErrChunkLayerDetached):
		return http.StatusConflict, codeClosing, false // stays closed until a new attach
	case errors.Is(err, storage.ErrChunkLayerClosed):
		return http.StatusServiceUnavailable, codeClosing, false // the process is shutting down
	// A local window abort cancels its uploads; that cancellation is not
	// a remote outage. Keep authority and uncertain-publication precedence.
	case errors.Is(err, storage.ErrCutCOWAborted):
		return http.StatusServiceUnavailable, "cut_aborted", true
	case errors.Is(err, storage.ErrChunkRemoteUnavailable):
		return http.StatusServiceUnavailable, chunkstore.CodeRemoteUnavailable, true
	case errors.Is(err, durable.ErrNotChunkVolume), errors.Is(err, durable.ErrVolumeNotOpen):
		return http.StatusConflict, chunkstore.CodeNotChunkVolume, false
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, chunkstore.CodeDeadlineExceeded, true
	case errors.Is(err, storage.ErrChunkCaptureRefused):
		return http.StatusServiceUnavailable, "capture_refused", true
	default:
		return http.StatusInternalServerError, codeInternal, false
	}
}

func catalogStatus(code string) int {
	switch code {
	case chunkstore.CodeInvalidName, chunkstore.CodeInvalidLabel, chunkstore.CodeInvalidClass, chunkstore.CodeInvalidPolicy:
		return http.StatusBadRequest
	case chunkstore.CodeSnapshotNotFound:
		return http.StatusNotFound
	case chunkstore.CodeDeadlineExceeded, chunkstore.CodeRemoteUnavailable, chunkstore.CodeOutcomeUnknown:
		return http.StatusServiceUnavailable
	default: // snapshot_exists, snapshot_deleting, target_exists, geometry_mismatch, fork_incomplete, not_chunk_volume
		return http.StatusConflict
	}
}

// chunkAdmit runs the transport checks every route shares: method, then
// client. Like every /chunk/* response, they answer in JSON (ruling of
// 2026-11-15); the rest of the listener keeps plain text.
func (s *StatusServer) chunkAdmit(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		w.Header().Set("Allow", method)
		writeChunkErr(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, false, "%s requires %s", r.URL.Path, method)
		return false
	}
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
		writeChunkErr(w, http.StatusForbidden, codeForbidden, false, "chunk control restricted to loopback")
		return false
	}
	return true
}

// chunkTarget resolves the control for volumeID, or answers
// volume_not_served / not_chunk_volume.
func (s *StatusServer) chunkTarget(w http.ResponseWriter, volumeID string) ChunkControl {
	if p := s.statusProjection(); volumeID == "" || volumeID != p.VolumeID {
		writeChunkErr(w, http.StatusNotFound, codeVolumeNotServed, false, "volume %q not served by this host (serves %q)", volumeID, p.VolumeID)
		return nil
	}
	s.mu.Lock()
	ctl := s.chunkControl
	s.mu.Unlock()
	if ctl == nil {
		writeChunkErr(w, http.StatusConflict, chunkstore.CodeNotChunkVolume, false, "volume %q has no chunk layer here", volumeID)
		return nil
	}
	return ctl
}

func decodeChunkBody(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, chunkBodyLimit))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeChunkErr(w, http.StatusBadRequest, codeBadRequest, false, "decode %s request: %v", r.URL.Path, err)
		return false
	}
	// Exactly one JSON value: trailing data refuses the whole request.
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeChunkErr(w, http.StatusBadRequest, codeBadRequest, false, "decode %s request: trailing data after the JSON object", r.URL.Path)
		return false
	}
	return true
}

// chunkContext bounds a long operation by min(timeoutMs, OpDeadline).
func chunkContext(r *http.Request, timeoutMs int64) (context.Context, context.CancelFunc) {
	d := chunkstore.DefaultOpDeadline
	if timeoutMs > 0 && timeoutMs < d.Milliseconds() { // compare before converting: no overflow
		d = time.Duration(timeoutMs) * time.Millisecond
	}
	return context.WithTimeout(r.Context(), d)
}

func (s *StatusServer) handleChunkCut(w http.ResponseWriter, r *http.Request) {
	if !s.chunkAdmit(w, r, http.MethodPost) {
		return
	}
	var req struct {
		VolumeID  string `json:"volumeID"`
		MinLSN    uint64 `json:"minLSN"`
		TimeoutMs int64  `json:"timeoutMs"`
	}
	if !decodeChunkBody(w, r, &req) {
		return
	}
	ctl := s.chunkTarget(w, req.VolumeID)
	if ctl == nil {
		return
	}
	ctx, cancel := chunkContext(r, req.TimeoutMs)
	defer cancel()
	res, err := ctl.Cut(ctx, req.VolumeID, req.MinLSN)
	if err != nil {
		writeChunkFailure(w, err)
		return
	}
	writeChunkJSON(w, http.StatusOK, res)
}

func (s *StatusServer) handleChunkSnapshot(w http.ResponseWriter, r *http.Request) {
	if !s.chunkAdmit(w, r, http.MethodPost) {
		return
	}
	var req struct {
		VolumeID    string            `json:"volumeID"`
		Name        string            `json:"name"`
		RequestID   string            `json:"requestID"`
		Labels      map[string]string `json:"labels"`
		Consistency string            `json:"consistency"`
		ExpiresAtNs int64             `json:"expiresAtNs"`
		Class       string            `json:"class"`
		TimeoutMs   int64             `json:"timeoutMs"`
	}
	if !decodeChunkBody(w, r, &req) {
		return
	}
	ctl := s.chunkTarget(w, req.VolumeID)
	if ctl == nil {
		return
	}
	ctx, cancel := chunkContext(r, req.TimeoutMs)
	defer cancel()
	info, err := ctl.Snapshot(ctx, req.VolumeID, ChunkSnapshotRequest{Name: req.Name, RequestID: req.RequestID,
		Labels: req.Labels, Consistency: req.Consistency, ExpiresAtNs: req.ExpiresAtNs, Class: req.Class})
	if err != nil {
		writeChunkFailure(w, err)
		return
	}
	writeChunkJSON(w, http.StatusOK, snapshotBody{VolumeID: info.VolumeID, Name: info.Name, HeadGen: info.HeadGen,
		HeadEpoch: info.HeadEpoch, CutLSN: info.CutLSN, CreatedNs: info.CreatedNs, ChunkCount: info.ChunkCount,
		ManifestBytes: info.ManifestBytes, Consistency: info.Consistency, Labels: info.Labels, Replayed: info.Replayed})
}

func (s *StatusServer) handleChunkSnapshots(w http.ResponseWriter, r *http.Request) {
	if !s.chunkAdmit(w, r, http.MethodGet) {
		return
	}
	q := r.URL.Query()
	opts := chunkstore.ListOptions{Prefix: q.Get("prefix"), StartAfter: q.Get("startAfter")}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeChunkErr(w, http.StatusBadRequest, codeBadRequest, false, "limit=%q: a non-negative integer", v)
			return
		}
		opts.Limit = n
	}
	if v := q.Get("includeDeleting"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			writeChunkErr(w, http.StatusBadRequest, codeBadRequest, false, "includeDeleting=%q: true or false", v)
			return
		}
		opts.IncludeDeleting = b
	}
	vol := q.Get("volume")
	ctl := s.chunkTarget(w, vol)
	if ctl == nil {
		return
	}
	list, next, err := ctl.ListSnapshots(r.Context(), vol, opts)
	if err != nil {
		writeChunkFailure(w, err)
		return
	}
	writeChunkJSON(w, http.StatusOK, chunkstore.NewSnapshotListBody(vol, list, next))
}

func (s *StatusServer) handleChunkSnapshotDelete(w http.ResponseWriter, r *http.Request) {
	if !s.chunkAdmit(w, r, http.MethodPost) {
		return
	}
	var req struct {
		VolumeID string `json:"volumeID"`
		Name     string `json:"name"`
	}
	if !decodeChunkBody(w, r, &req) {
		return
	}
	ctl := s.chunkTarget(w, req.VolumeID)
	if ctl == nil {
		return
	}
	state, err := ctl.DeleteSnapshot(r.Context(), req.VolumeID, req.Name)
	if err != nil {
		writeChunkFailure(w, err)
		return
	}
	writeChunkJSON(w, http.StatusOK, chunkstore.SnapshotDeleteBody{VolumeID: req.VolumeID, Name: req.Name, State: state})
}

func (s *StatusServer) handleChunkDetach(w http.ResponseWriter, r *http.Request) {
	if !s.chunkAdmit(w, r, http.MethodPost) {
		return
	}
	var req struct {
		VolumeID  string `json:"volumeID"`
		TimeoutMs int64  `json:"timeoutMs"`
	}
	if !decodeChunkBody(w, r, &req) {
		return
	}
	ctl := s.chunkTarget(w, req.VolumeID)
	if ctl == nil {
		return
	}
	ctx, cancel := chunkContext(r, req.TimeoutMs)
	defer cancel()
	res, err := ctl.Detach(ctx, req.VolumeID)
	if err != nil {
		writeChunkFailure(w, err)
		return
	}
	writeChunkJSON(w, http.StatusOK, res)
}

// ---- the control over a durable provider and a catalog ----

// ChunkProvider is the durable provider's chunk surface the control uses.
type ChunkProvider interface {
	// CutVolume takes one quiesced cut and returns what it installed; with
	// minLSN > 0 it refuses an LSN never written before publishing, and
	// fails unless the cut reaches minLSN.
	CutVolume(ctx context.Context, volumeID string, minLSN uint64) (storage.ChunkCutResult, error)
	// DetachVolume stops the volume's I/O and periodic cuts and publishes
	// the detached final cut; it also returns the last written LSN.
	DetachVolume(ctx context.Context, volumeID string) (storage.ChunkCutResult, uint64, error)
}

type chunkControl struct {
	p   ChunkProvider
	cat *chunkstore.Catalog
}

// NewChunkControl serves the routes from a durable provider (cut, detach)
// and the catalog under the provider's chunk base (snapshots).
func NewChunkControl(p ChunkProvider, cat *chunkstore.Catalog) ChunkControl {
	return &chunkControl{p: p, cat: cat}
}

func (c *chunkControl) Cut(ctx context.Context, volumeID string, minLSN uint64) (ChunkCutResponse, error) {
	r, err := c.p.CutVolume(ctx, volumeID, minLSN)
	if err != nil {
		return ChunkCutResponse{}, err
	}
	return ChunkCutResponse{VolumeID: volumeID, CutLSN: r.H, HeadGen: r.Head.Gen, HeadEpoch: r.Head.Epoch, Holder: r.Head.Holder,
		Noop: r.Noop, FullHash: r.FullHash, Stats: cutStatsBody(r.Stats)}, nil
}

// Snapshot resolves the request against the catalog first (§3.2 step 1):
// a retry of a recorded request returns that snapshot and another request
// for the name gets snapshot_exists, both without cutting. Only a free name
// is cut, and the HEAD that cut installed is stored.
func (c *chunkControl) Snapshot(ctx context.Context, volumeID string, req ChunkSnapshotRequest) (chunkstore.SnapshotInfo, error) {
	// Admit the request before anything can cut: an invalid class must
	// not advance HEAD (review of #142).
	if err := chunkstore.ValidateSnapshotClass(req.Class); err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	sreq := chunkstore.SnapshotRequest{Name: req.Name, RequestID: req.RequestID, Labels: req.Labels,
		Consistency: req.Consistency, ExpiresAtNs: req.ExpiresAtNs, CreatedBy: "blockvolume", Class: req.Class}
	if info, replayed, err := c.cat.SnapshotReplay(ctx, volumeID, sreq); err != nil || replayed {
		return info, err
	}
	r, err := c.p.CutVolume(ctx, volumeID, 0)
	if err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	sreq.SourceReadAt = time.Now()
	return c.cat.CreateSnapshot(ctx, volumeID, sreq, r.Head)
}

func (c *chunkControl) ListSnapshots(ctx context.Context, volumeID string, opts chunkstore.ListOptions) ([]chunkstore.SnapshotInfo, string, error) {
	return c.cat.ListSnapshots(ctx, volumeID, opts)
}

func (c *chunkControl) DeleteSnapshot(ctx context.Context, volumeID, name string) (string, error) {
	return c.cat.DeleteSnapshot(ctx, volumeID, name)
}

func (c *chunkControl) Usage(ctx context.Context, volumeID string) (chunkstore.VolumeUsage, error) {
	return c.cat.Usage(ctx, volumeID)
}

// handleStatusChunk answers GET /status/chunk?volume=<id>: the served
// volume's space accounting and retention policy, computed from manifests
// and filer listings (no chunk data reads).
func (s *StatusServer) handleStatusChunk(w http.ResponseWriter, r *http.Request) {
	if !s.chunkAdmit(w, r, http.MethodGet) {
		return
	}
	volumeID := r.URL.Query().Get("volume")
	ctl := s.chunkTarget(w, volumeID)
	if ctl == nil {
		return
	}
	ctx, cancel := chunkContext(r, 0)
	defer cancel()
	u, err := ctl.Usage(ctx, volumeID)
	if err != nil {
		writeChunkFailure(w, err)
		return
	}
	writeChunkJSON(w, http.StatusOK, u)
}

func (c *chunkControl) Detach(ctx context.Context, volumeID string) (ChunkDetachResponse, error) {
	r, last, err := c.p.DetachVolume(ctx, volumeID)
	if err != nil {
		return ChunkDetachResponse{}, err
	}
	response := ChunkDetachResponse{VolumeID: volumeID, CutLSN: r.H, HeadGen: r.Head.Gen, HeadEpoch: r.Head.Epoch, LastWrittenLSN: last}
	if r.Precopy.Frozen {
		response.Precopy = &r.Precopy
	}
	return response, nil
}
