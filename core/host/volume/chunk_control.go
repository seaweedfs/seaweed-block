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
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
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
	// Usage counts snapshots, dependents, and current own/foreign chunks
	// (GET /status/chunk).
	Usage(ctx context.Context, volumeID string) (chunkstore.Usage, error)
}

// ChunkCutResponse is the body of a successful POST /chunk/cut (§3.1).
type ChunkCutResponse struct {
	CandidateTail *storage.ChunkCandidateTail `json:"candidateTail,omitempty"`
	VolumeID      string                      `json:"volumeID"`
	CutLSN        uint64                      `json:"cutLSN"`
	HeadGen       uint64                      `json:"headGen"`
	HeadEpoch     uint64                      `json:"headEpoch"`
	Holder        string                      `json:"holder"`
	Noop          bool                        `json:"noop"`
	FullHash      bool                        `json:"fullHash"`
	// Stats are the statistics the chunk layer measured for this cut
	// (§3.1). A zero field is "not measured" or "none" (a Noop cut
	// uploads nothing).
	Stats ChunkCutStatsBody `json:"stats"`
}

// ChunkCutStatsBody is one cut's statistics in the /chunk/cut body. It
// mirrors storage.ChunkCutStats field for field (a test keeps the two in
// step); every duration is in nanoseconds.
type ChunkCutStatsBody struct {
	SyncPhases       storage.SyncPhaseStats `json:"syncPhases"`
	CandidateNs      int64                  `json:"candidateNs"`
	WindowSetupNs    int64                  `json:"windowSetupNs"`
	PublishPrepareNs int64                  `json:"publishPrepareNs"`
	ManifestNs       int64                  `json:"manifestNs"`
	CommitStateNs    int64                  `json:"commitStateNs"`
	NonPayloadCopyNs int64                  `json:"nonPayloadCopyNs"`

	PayloadNs          int64  `json:"payloadNs"`
	COWWALReadAttempts uint64 `json:"cowWALReadAttempts"`
	COWLocalReadNs     int64  `json:"cowLocalReadNs"`
	COWSavedChunks     int    `json:"cowSavedChunks"`
	COWSaveNs          int64  `json:"cowSaveNs"`
	COWWaitNs          int64  `json:"cowWaitNs"`
	BarrierPauseNs     int64  `json:"barrierPauseNs"`
	LockWaitNs         int64  `json:"lockWaitNs"`
	DrainNs            int64  `json:"drainNs"`
	CASWriteNs         int64  `json:"casWriteNs"`
	MapPersistNs       int64  `json:"mapPersistNs"`
	HeadReadbackNs     int64  `json:"headReadbackNs"`
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
	BarrierOverBound   bool   `json:"barrierOverBound"` // always false in production: its bound is set only by legacy test fixtures
}

func cutStatsBody(s storage.ChunkCutStats) ChunkCutStatsBody {
	return ChunkCutStatsBody{
		SyncPhases:       s.SyncPhases,
		CandidateNs:      s.CandidateNs,
		WindowSetupNs:    s.WindowSetupNs,
		PublishPrepareNs: s.PublishPrepareNs,
		ManifestNs:       s.ManifestNs,
		CommitStateNs:    s.CommitStateNs,
		NonPayloadCopyNs: s.NonPayloadCopyNs,

		PayloadNs:          s.PayloadNs,
		COWWALReadAttempts: s.COWWALReadAttempts,
		COWLocalReadNs:     s.COWLocalReadNs,
		BarrierPauseNs:     s.BarrierPauseNs,
		LockWaitNs:         s.LockWaitNs,
		DrainNs:            s.DrainNs,
		CASWriteNs:         s.CASWriteNs,
		MapPersistNs:       s.MapPersistNs,
		HeadReadbackNs:     s.HeadReadbackNs,
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

type chunkFrozenRefusalBody struct {
	storage.FrozenBudget
	RemainingFreeze    time.Duration `json:"remainingFreezeNs"`
	GuestFreezeElapsed time.Duration `json:"guestFreezeElapsedNs"`
}

type chunkErrorBody struct {
	*chunkFrozenRefusalBody
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
	// Only freeze-option validation before provider admission emits this code.
	codeInvalidFreezeBudget = "invalid_freeze_budget"
	codeMethodNotAllowed    = "method_not_allowed"
	codeForbidden           = "forbidden"
	codeNotFound            = "not_found"
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

// codeHostSlotBusy refuses a cut, snapshot or detach because every host
// cut slot is held (retryable).
const codeHostSlotBusy = "host_slot_busy"

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
	body := chunkErrorBody{Code: code, Message: msg, Retryable: retryable}
	var refusal *storage.FrozenRefusal
	// INV-WS-FREEZE-ACCOUNTING-001: transport observation, never a second forecast.
	if code == "frozen_tail_too_large" && errors.As(err, &refusal) {
		r := refusal.Report
		body.chunkFrozenRefusalBody = &chunkFrozenRefusalBody{FrozenBudget: r.FrozenBudget, RemainingFreeze: r.RemainingFreeze, GuestFreezeElapsed: r.GuestFreezeElapsed}
	}
	writeChunkJSON(w, status, body)
}

// chunkErrorCode maps an operation's error to (HTTP status, code,
// retryable). An unresolved CAS reports outcome_unknown even when the
// attempt was also refused or the re-read failed on the remote; otherwise
// a refused capture reports why it was refused.
func chunkErrorCode(err error) (int, string, bool) {
	var ce *chunkstore.CatalogError
	switch {
	case errors.Is(err, durable.ErrPlainDetachBusy):
		return http.StatusConflict, "plain_detach_busy", true
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
	case errors.Is(err, layout.ErrGeometryMismatch):
		return http.StatusConflict, chunkstore.CodeGeometryMismatch, false
	case errors.Is(err, layout.ErrTooManySnapshots):
		return http.StatusConflict, chunkstore.CodeSnapshotLimit, false
	case errors.Is(err, layout.ErrSnapshotExists):
		return http.StatusConflict, chunkstore.CodeSnapshotExists, false
	case errors.Is(err, layout.ErrHeadAbsent), errors.Is(err, layout.ErrNoSnapshot):
		return http.StatusNotFound, "not_found", false
	case errors.Is(err, layout.ErrHasDependents), errors.Is(err, layout.ErrVolumeDeleting), errors.Is(err, layout.ErrDetached), errors.Is(err, layout.ErrIdentity):
		return http.StatusConflict, "layout_refused", false
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
	case errors.Is(err, errFrozenDetachUnsupported):
		return http.StatusConflict, "frozen_detach_unsupported", false
	case errors.Is(err, storage.ErrInvalidFreezeBudget):
		return http.StatusBadRequest, codeInvalidFreezeBudget, false
	case errors.Is(err, storage.ErrFrozenTailTooLarge):
		return http.StatusConflict, "frozen_tail_too_large", true
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
	// Every host cut slot held (host admission, --host-limit-dir): a wait
	// that ends when another cut releases its slot. Its own code, so a
	// caller can resend on exactly this refusal without reading messages.
	case errors.Is(err, hostlimit.ErrSlotsHeld):
		return http.StatusServiceUnavailable, codeHostSlotBusy, true
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
		VolumeID          string `json:"volumeID"`
		TimeoutMs         int64  `json:"timeoutMs"`
		Plain             bool   `json:"plain,omitempty"`
		Frozen            bool   `json:"frozen,omitempty"`
		FreezeStartedAtNs int64  `json:"freezeStartedAtNs,omitempty"`
		AllowOverBudget   bool   `json:"allowOverBudget,omitempty"`
	}
	if !decodeChunkBody(w, r, &req) {
		return
	}
	if (req.Frozen && (req.Plain || req.FreezeStartedAtNs <= 0 || time.Unix(0, req.FreezeStartedAtNs).After(time.Now()))) || (!req.Frozen && (req.FreezeStartedAtNs != 0 || req.AllowOverBudget)) {
		writeChunkErr(w, http.StatusBadRequest, codeInvalidFreezeBudget, false, "frozen requires a non-future freezeStartedAtNs and cannot combine with plain; allowOverBudget requires frozen")
		return
	}
	// M5 PlainStop -> Quiesce: the offline caller owns a finite wait.
	if req.Plain && req.TimeoutMs <= 0 {
		writeChunkErr(w, http.StatusBadRequest, codeBadRequest, false, "plain detach requires positive timeoutMs")
		return
	}
	ctl := s.chunkTarget(w, req.VolumeID)
	if ctl == nil {
		return
	}
	ctx, cancel := chunkContext(r, req.TimeoutMs)
	if req.Plain { // M5 PlainStop -> Quiesce: offline work is bounded by its caller.
		cancel()
		const maxTimeoutMs = int64((1<<63 - 1) / time.Millisecond)
		ctx, cancel = context.WithTimeout(r.Context(), time.Duration(min(req.TimeoutMs, maxTimeoutMs))*time.Millisecond)
	}
	defer cancel()
	var res ChunkDetachResponse
	var err error
	if req.Frozen {
		// Frozen detach is not supported in this release: refuse before any
		// provider call. The request fields stay so the strict decoder keeps
		// answering this typed refusal rather than an unknown-field 400.
		writeChunkErr(w, http.StatusConflict, "frozen_detach_unsupported", false, "frozen detach is not supported in this release; stop the guest and send a plain detach")
		return
	} else if req.Plain {
		plain, ok := ctl.(interface {
			DetachPlain(context.Context, string) (ChunkDetachResponse, error)
		})
		if !ok {
			writeChunkErr(w, http.StatusConflict, "plain_detach_unsupported", false, "plain detach unsupported")
			return
		}
		res, err = plain.DetachPlain(ctx, req.VolumeID)
	} else {
		res, err = ctl.Detach(ctx, req.VolumeID)
	}
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
	// KickMaintenance asks the volume's maintenance loop for one pass now
	// (its cleaning), as after a snapshot delete; false when no loop runs.
	KickMaintenance(volumeID string) bool
}

var errFrozenDetachUnsupported = errors.New("frozen detach unsupported by provider")

type chunkControl struct {
	p      ChunkProvider
	cat    *chunkstore.BCatalog
	volume chunkstore.VolumeRef
}

// NewChunkControl binds catalog operations to the master's immutable volume identity.
func NewChunkControl(p ChunkProvider, cat *chunkstore.BCatalog, volume chunkstore.VolumeRef) ChunkControl {
	return &chunkControl{p: p, cat: cat, volume: volume}
}

func (c *chunkControl) catalogVolume(name string) (chunkstore.VolumeRef, error) {
	if c.cat == nil || name != c.volume.Name || c.volume.ID.IsZero() {
		return chunkstore.VolumeRef{}, layout.ErrIdentity
	}
	return c.volume, nil
}

func (c *chunkControl) Cut(ctx context.Context, volumeID string, minLSN uint64) (ChunkCutResponse, error) {
	r, err := c.p.CutVolume(ctx, volumeID, minLSN)
	if err != nil {
		return ChunkCutResponse{}, err
	}
	return ChunkCutResponse{VolumeID: volumeID, CutLSN: r.H, HeadGen: r.Head.Gen, HeadEpoch: r.Head.Epoch, Holder: r.Head.Holder,
		Noop: r.Noop, FullHash: r.FullHash, CandidateTail: r.CandidateTail, Stats: cutStatsBody(r.Stats)}, nil
}

// Snapshot belongs to the data path: its commit names the snapshot in HEAD.
// The control never records a second catalog entry after an ordinary cut.
func (c *chunkControl) Snapshot(ctx context.Context, volumeID string, req ChunkSnapshotRequest) (chunkstore.SnapshotInfo, error) {
	if _, err := c.catalogVolume(volumeID); err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	if err := chunkstore.ValidateSnapshotClass(req.Class); err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	if len(req.Labels) > layout.MaxSnapshotLabels {
		return chunkstore.SnapshotInfo{}, &chunkstore.CatalogError{Code: chunkstore.CodeInvalidLabel, Message: fmt.Sprintf("snapshot has %d labels; at most %d", len(req.Labels), layout.MaxSnapshotLabels)}
	}
	p, ok := c.p.(interface {
		SnapshotVolume(context.Context, string, string, layout.SnapshotMeta) (layout.Head, error)
	})
	if !ok {
		return chunkstore.SnapshotInfo{}, errors.New("chunk snapshot data path is not configured")
	}
	// Sample once for this capture. The provider carries this value unchanged in
	// CommitRequest.Meta; a replay returns the facts already recorded in HEAD.
	meta := layout.SnapshotMeta{CreatedNs: time.Now().UnixNano(), Class: req.Class, ExpiresNs: req.ExpiresAtNs, Labels: maps.Clone(req.Labels)}
	h, err := p.SnapshotVolume(hostlimit.WithClass(ctx, hostlimit.Urgent), volumeID, req.Name, meta)
	if err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	return chunkstore.SnapshotInfoFromHead(volumeID, req.Name, h), nil
}

func (c *chunkControl) ListSnapshots(ctx context.Context, volumeID string, opts chunkstore.ListOptions) ([]chunkstore.SnapshotInfo, string, error) {
	v, err := c.catalogVolume(volumeID)
	if err != nil {
		return nil, "", err
	}
	h, found, err := c.cat.ReadHead(ctx, v)
	if err != nil {
		return nil, "", err
	}
	if !found {
		return nil, "", layout.ErrHeadAbsent
	}
	names := make([]string, 0, len(h.Snapshots))
	for name := range h.Snapshots {
		if strings.HasPrefix(name, opts.Prefix) && name > opts.StartAfter {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	next := ""
	if opts.Limit > 0 && len(names) > opts.Limit {
		names = names[:opts.Limit]
		next = names[len(names)-1]
	}
	out := make([]chunkstore.SnapshotInfo, 0, len(names))
	for _, name := range names {
		sn := h.Snapshots[name]
		out = append(out, chunkstore.SnapshotInfo{VolumeID: volumeID, Name: name, Root: sn.Root.String(), Dependents: sn.Dependents, CreatedNs: sn.Meta.CreatedNs, Class: sn.Meta.Class, ExpiresAtNs: sn.Meta.ExpiresNs, Labels: maps.Clone(sn.Meta.Labels)})
	}
	return out, next, nil
}

func (c *chunkControl) DeleteSnapshot(ctx context.Context, volumeID, name string) (string, error) {
	v, err := c.catalogVolume(volumeID)
	if err != nil {
		return "", err
	}
	_, err = c.cat.DeleteSnapshot(ctx, v, name)
	if err != nil {
		return "", err
	}
	// The deleted snapshot's chunks are removed by the holder's next
	// cleaning: ask for it now rather than at the next interval.
	c.p.KickMaintenance(volumeID)
	return "deleted", nil
}

func (c *chunkControl) Usage(ctx context.Context, volumeID string) (chunkstore.Usage, error) {
	v, err := c.catalogVolume(volumeID)
	if err != nil {
		return chunkstore.Usage{}, err
	}
	return c.cat.Usage(ctx, v)
}

// handleStatusChunk answers GET /status/chunk?volume=<id> from HEAD and
// its current root (no chunk data reads).
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
	return chunkDetachResponse(volumeID, r, last, err)
}

func (c *chunkControl) DetachFrozen(ctx context.Context, volumeID string, opts storage.FrozenDetachOptions) (ChunkDetachResponse, error) {
	p, ok := c.p.(interface {
		DetachVolumeFrozen(context.Context, string, storage.FrozenDetachOptions) (storage.ChunkCutResult, uint64, error)
	})
	if !ok {
		return ChunkDetachResponse{}, errFrozenDetachUnsupported
	}
	r, last, err := p.DetachVolumeFrozen(ctx, volumeID, opts)
	return chunkDetachResponse(volumeID, r, last, err)
}

func (c *chunkControl) DetachPlain(ctx context.Context, volumeID string) (ChunkDetachResponse, error) {
	p, ok := c.p.(interface {
		DetachVolumePlain(context.Context, string) (storage.ChunkCutResult, uint64, error)
	})
	if !ok {
		return ChunkDetachResponse{}, fmt.Errorf("plain detach unsupported by provider")
	}
	r, last, err := p.DetachVolumePlain(ctx, volumeID)
	return chunkDetachResponse(volumeID, r, last, err)
}

func chunkDetachResponse(volumeID string, r storage.ChunkCutResult, last uint64, err error) (ChunkDetachResponse, error) {
	if err != nil {
		return ChunkDetachResponse{}, err
	}
	response := ChunkDetachResponse{VolumeID: volumeID, CutLSN: r.H, HeadGen: r.Head.Gen, HeadEpoch: r.Head.Epoch, LastWrittenLSN: last}
	if r.Precopy.Frozen {
		response.Precopy = &r.Precopy
	}
	return response, nil
}
