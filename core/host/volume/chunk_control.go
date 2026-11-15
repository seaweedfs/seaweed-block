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
}

// ChunkCutResponse is the body of a successful POST /chunk/cut (§3.1).
type ChunkCutResponse struct {
	VolumeID        string `json:"volumeID"`
	CutLSN          uint64 `json:"cutLSN"`
	HeadGen         uint64 `json:"headGen"`
	HeadEpoch       uint64 `json:"headEpoch"`
	Holder          string `json:"holder"`
	Noop            bool   `json:"noop"`
	FullHash        bool   `json:"fullHash"`
	CandidateChunks int    `json:"candidateChunks"`
	UploadedChunks  int    `json:"uploadedChunks"`
	UploadedBytes   int64  `json:"uploadedBytes"`
	ZeroedChunks    int    `json:"zeroedChunks"`
	BarrierPauseNs  int64  `json:"barrierPauseNs"`
	UploadNs        int64  `json:"uploadNs"`
	CasNs           int64  `json:"casNs"`
	TotalNs         int64  `json:"totalNs"`
}

// ChunkSnapshotRequest is POST /chunk/snapshot's request (§3.2), without
// the volume and the timeout.
type ChunkSnapshotRequest struct {
	Name        string
	RequestID   string
	Labels      map[string]string
	Consistency string
	ExpiresAtNs int64
}

// ChunkDetachResponse is the body of a successful POST /chunk/detach (§3.6).
type ChunkDetachResponse struct {
	VolumeID       string `json:"volumeID"`
	CutLSN         uint64 `json:"cutLSN"`
	HeadGen        uint64 `json:"headGen"`
	HeadEpoch      uint64 `json:"headEpoch"`
	LastWrittenLSN uint64 `json:"lastWrittenLSN"`
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

// snapshotItem is one entry of GET /chunk/snapshots, exactly §3.3's fields.
type snapshotItem struct {
	Name        string             `json:"name"`
	HeadGen     uint64             `json:"headGen"`
	HeadEpoch   uint64             `json:"headEpoch"`
	CutLSN      uint64             `json:"cutLSN"`
	CreatedNs   int64              `json:"createdNs"`
	State       string             `json:"state"`
	ChunkCount  int                `json:"chunkCount"`
	Labels      map[string]string  `json:"labels"`
	Consistency string             `json:"consistency"`
	Parent      *chunkstore.Parent `json:"parent"`
}

type snapshotsBody struct {
	VolumeID  string         `json:"volumeID"`
	Snapshots []snapshotItem `json:"snapshots"`
	Next      string         `json:"next"`
}

type snapshotDeleteBody struct {
	VolumeID string `json:"volumeID"`
	Name     string `json:"name"`
	State    string `json:"state"`
}

type chunkErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// The codes the routes add to the catalog's (B9 §2.2 plus the 2026-11-15
// rulings: awaiting_assignment, internal).
const (
	codeVolumeNotServed    = "volume_not_served"
	codeNotPrimary         = "not_primary"
	codeAwaitingAssignment = "awaiting_assignment"
	codeEpochStale         = "epoch_stale"
	codeBaseInstalling     = "base_installing"
	codeFenced             = "fenced"
	codeHeadConflict       = "head_conflict"
	codeClosing            = "closing"
	codeInternal           = "internal"
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
	writeChunkJSON(w, status, chunkErrorBody{Code: code, Message: err.Error(), Retryable: retryable})
}

// chunkErrorCode maps an operation's error to (HTTP status, code,
// retryable). The most specific cause wins: a refused capture reports why
// it was refused, an unresolved CAS reports outcome_unknown even when the
// re-read failed on the remote.
func chunkErrorCode(err error) (int, string, bool) {
	var ce *chunkstore.CatalogError
	switch {
	case errors.As(err, &ce):
		return catalogStatus(ce.Code), ce.Code, ce.Retryable
	case errors.Is(err, storage.ErrChunkLayerFenced):
		return http.StatusConflict, codeFenced, false
	case errors.Is(err, durable.ErrChunkAwaitingAuthority):
		return http.StatusConflict, codeAwaitingAssignment, true
	case errors.Is(err, durable.ErrChunkNotPrimary):
		return http.StatusConflict, codeNotPrimary, false
	case errors.Is(err, durable.ErrChunkEpochStale):
		return http.StatusConflict, codeEpochStale, false
	case errors.Is(err, storage.ErrChunkBaseInstalling):
		return http.StatusConflict, codeBaseInstalling, true
	case errors.Is(err, storage.ErrChunkCASOutcomeUnknown):
		return http.StatusServiceUnavailable, chunkstore.CodeOutcomeUnknown, true
	case errors.Is(err, storage.ErrChunkHeadPrecondition):
		return http.StatusConflict, codeHeadConflict, true
	case errors.Is(err, storage.ErrChunkRemoteUnavailable):
		return http.StatusServiceUnavailable, chunkstore.CodeRemoteUnavailable, true
	case errors.Is(err, storage.ErrChunkLayerClosed), errors.Is(err, storage.ErrChunkLayerDetached):
		return http.StatusServiceUnavailable, codeClosing, false
	case errors.Is(err, durable.ErrNotChunkVolume), errors.Is(err, durable.ErrVolumeNotOpen):
		return http.StatusConflict, chunkstore.CodeNotChunkVolume, false
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, chunkstore.CodeDeadlineExceeded, true
	default:
		return http.StatusInternalServerError, codeInternal, false
	}
}

func catalogStatus(code string) int {
	switch code {
	case chunkstore.CodeInvalidName:
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
// client. Like a malformed request, these are answered in plain text as
// on the rest of the listener; the typed JSON body is for the operation's
// own outcome (B9 §2.2 has no codes for them).
func (s *StatusServer) chunkAdmit(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		w.Header().Set("Allow", method)
		http.Error(w, r.URL.Path+" requires "+method, http.StatusMethodNotAllowed)
		return false
	}
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
		http.Error(w, "chunk control restricted to loopback", http.StatusForbidden)
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
		http.Error(w, "decode "+r.URL.Path+" request: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

// chunkContext bounds a long operation by min(timeoutMs, OpDeadline).
func chunkContext(r *http.Request, timeoutMs int64) (context.Context, context.CancelFunc) {
	d := chunkstore.DefaultOpDeadline
	if t := time.Duration(timeoutMs) * time.Millisecond; timeoutMs > 0 && t < d {
		d = t
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
		Labels: req.Labels, Consistency: req.Consistency, ExpiresAtNs: req.ExpiresAtNs})
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
			http.Error(w, fmt.Sprintf("limit=%q: a non-negative integer", v), http.StatusBadRequest)
			return
		}
		opts.Limit = n
	}
	if v := q.Get("includeDeleting"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			http.Error(w, fmt.Sprintf("includeDeleting=%q: true or false", v), http.StatusBadRequest)
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
	body := snapshotsBody{VolumeID: vol, Snapshots: make([]snapshotItem, 0, len(list)), Next: next}
	for _, s := range list {
		body.Snapshots = append(body.Snapshots, snapshotItem{Name: s.Name, HeadGen: s.HeadGen, HeadEpoch: s.HeadEpoch,
			CutLSN: s.CutLSN, CreatedNs: s.CreatedNs, State: s.State, ChunkCount: s.ChunkCount, Labels: s.Labels,
			Consistency: s.Consistency, Parent: s.Parent})
	}
	writeChunkJSON(w, http.StatusOK, body)
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
	writeChunkJSON(w, http.StatusOK, snapshotDeleteBody{VolumeID: req.VolumeID, Name: req.Name, State: state})
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
	// CutVolume takes one quiesced cut and returns what it installed.
	CutVolume(ctx context.Context, volumeID string) (storage.ChunkCutResult, error)
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
	r, err := c.p.CutVolume(ctx, volumeID)
	if err != nil {
		return ChunkCutResponse{}, err
	}
	if r.H < minLSN {
		return ChunkCutResponse{}, fmt.Errorf("chunk cut of %s reached LSN %d, below minLSN %d (never written)", volumeID, r.H, minLSN)
	}
	st := r.Stats
	return ChunkCutResponse{VolumeID: volumeID, CutLSN: r.H, HeadGen: r.Head.Gen, HeadEpoch: r.Head.Epoch, Holder: r.Head.Holder,
		Noop: r.Noop, FullHash: r.FullHash, CandidateChunks: st.CandidateChunks, UploadedChunks: st.UploadedChunks,
		UploadedBytes: st.UploadedBytes, ZeroedChunks: st.ZeroedChunks, BarrierPauseNs: st.BarrierPauseNs,
		UploadNs: st.UploadNs, CasNs: st.CASNs, TotalNs: st.TotalNs}, nil
}

// Snapshot validates the request, cuts, and stores the HEAD that cut
// installed (§3.2). A retry with the same request id returns the first
// snapshot even though it cut again.
func (c *chunkControl) Snapshot(ctx context.Context, volumeID string, req ChunkSnapshotRequest) (chunkstore.SnapshotInfo, error) {
	if err := chunkstore.CheckSnapshotName(req.Name); err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	if req.RequestID == "" {
		return chunkstore.SnapshotInfo{}, &chunkstore.CatalogError{Code: chunkstore.CodeInvalidName, Message: "a request id is required"}
	}
	r, err := c.p.CutVolume(ctx, volumeID)
	if err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	return c.cat.CreateSnapshot(ctx, volumeID, chunkstore.SnapshotRequest{Name: req.Name, RequestID: req.RequestID,
		Labels: req.Labels, Consistency: req.Consistency, ExpiresAtNs: req.ExpiresAtNs, CreatedBy: "blockvolume",
		SourceReadAt: time.Now()}, r.Head)
}

func (c *chunkControl) ListSnapshots(ctx context.Context, volumeID string, opts chunkstore.ListOptions) ([]chunkstore.SnapshotInfo, string, error) {
	return c.cat.ListSnapshots(ctx, volumeID, opts)
}

func (c *chunkControl) DeleteSnapshot(ctx context.Context, volumeID, name string) (string, error) {
	return c.cat.DeleteSnapshot(ctx, volumeID, name)
}

func (c *chunkControl) Detach(ctx context.Context, volumeID string) (ChunkDetachResponse, error) {
	r, last, err := c.p.DetachVolume(ctx, volumeID)
	if err != nil {
		return ChunkDetachResponse{}, err
	}
	return ChunkDetachResponse{VolumeID: volumeID, CutLSN: r.H, HeadGen: r.Head.Gen, HeadEpoch: r.Head.Epoch, LastWrittenLSN: last}, nil
}
