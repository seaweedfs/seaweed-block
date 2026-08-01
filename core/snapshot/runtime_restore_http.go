package snapshot

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	runtimeRestoreApplyPath    = "/v1/snapshot/restore/apply"
	runtimeRestoreActivatePath = "/v1/snapshot/restore/activate"
	maxRestoreMetadataBytes    = 16 << 10
)

type runtimeRestoreWireRequest struct {
	Snapshot        Record `json:"snapshot"`
	TargetVolumeID  string `json:"targetVolumeID"`
	TargetReplicaID string `json:"targetReplicaID"`
	TargetStorageID string `json:"targetStorageID"`
	TargetNumBlocks uint32 `json:"targetNumBlocks"`
	TargetBlockSize int    `json:"targetBlockSize"`
}

type RuntimeRestoreRequest struct {
	Endpoint        string
	Snapshot        Record
	TargetVolumeID  string
	TargetReplicaID string
	TargetStorageID string
	TargetNumBlocks uint32
	TargetBlockSize int
}

type RestoreRuntimeHandler struct {
	target           *RestoreTarget
	releaseReadiness func() error
	token            string
}

func NewRuntimeMux(capture *RuntimeHandler, restore *RestoreRuntimeHandler) (http.Handler, error) {
	if capture == nil && restore == nil {
		return nil, fmt.Errorf("snapshot: runtime mux requires at least one handler")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case runtimeCapturePath:
			if capture == nil {
				http.NotFound(w, r)
				return
			}
			capture.ServeHTTP(w, r)
		case runtimeRestoreApplyPath, runtimeRestoreActivatePath:
			if restore == nil {
				http.NotFound(w, r)
				return
			}
			restore.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	}), nil
}

func NewRestoreRuntimeHandler(target *RestoreTarget, releaseReadiness func() error, token string) (*RestoreRuntimeHandler, error) {
	if target == nil || releaseReadiness == nil || token == "" {
		return nil, fmt.Errorf("snapshot: restore runtime requires target, readiness callback, and token")
	}
	return &RestoreRuntimeHandler{target: target, releaseReadiness: releaseReadiness, token: token}, nil
}

func (h *RestoreRuntimeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !runtimeBearerAuthorized(r, h.token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case runtimeRestoreApplyPath:
		h.serveApply(w, r)
	case runtimeRestoreActivatePath:
		h.serveActivate(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *RestoreRuntimeHandler) serveApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	req, metadataBytes, err := readRestoreWireRequest(r.Body)
	if err != nil {
		http.Error(w, "invalid restore request", http.StatusBadRequest)
		return
	}
	if !h.matchesTarget(req) {
		http.Error(w, ErrRestoreConflict.Error(), http.StatusConflict)
		return
	}
	if validateRecord(req.Snapshot) != nil {
		http.Error(w, "invalid snapshot record", http.StatusBadRequest)
		return
	}
	wantContentLength := int64(4+metadataBytes) + req.Snapshot.ArchiveBytes
	if r.ContentLength >= 0 && r.ContentLength != wantContentLength {
		http.Error(w, "invalid restore content length", http.StatusBadRequest)
		return
	}
	result, err := h.target.Apply(r.Context(), r.Body, req.Snapshot)
	if err != nil {
		writeRestoreRuntimeError(w, err)
		return
	}
	writeRestoreRuntimeJSON(w, result)
}

func (h *RestoreRuntimeHandler) serveActivate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.ContentLength > maxRestoreMetadataBytes {
		http.Error(w, "activation request too large", http.StatusBadRequest)
		return
	}
	var req runtimeRestoreWireRequest
	if err := decodeSingleJSON(http.MaxBytesReader(w, r.Body, maxRestoreMetadataBytes), &req); err != nil || !h.matchesTarget(req) {
		http.Error(w, "invalid activation request", http.StatusBadRequest)
		return
	}
	if err := h.target.Activate(h.releaseReadiness); err != nil {
		writeRestoreRuntimeError(w, err)
		return
	}
	writeRestoreRuntimeJSON(w, h.target.Marker())
}

func (h *RestoreRuntimeHandler) matchesTarget(req runtimeRestoreWireRequest) bool {
	marker := h.target.Marker()
	return req.Snapshot.SnapshotID == marker.SnapshotID &&
		req.TargetVolumeID == marker.TargetVolumeID &&
		req.TargetReplicaID == marker.TargetReplicaID &&
		req.TargetStorageID == marker.TargetStorageID &&
		req.TargetNumBlocks == marker.TargetNumBlocks &&
		req.TargetBlockSize == marker.TargetBlockSize
}

type ArchiveStreamer interface {
	StreamArchive(ctx context.Context, snapshotID string, w io.Writer) (Record, error)
}

type HTTPSRestoreRuntime struct {
	client *http.Client
	token  string
}

func NewHTTPSRestoreRuntime(client *http.Client, token string) (*HTTPSRestoreRuntime, error) {
	if client == nil || token == "" {
		return nil, fmt.Errorf("snapshot: HTTPS restore runtime requires client and token")
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &HTTPSRestoreRuntime{client: &clientCopy, token: token}, nil
}

func (c *HTTPSRestoreRuntime) Apply(ctx context.Context, req RuntimeRestoreRequest, source ArchiveStreamer) (RestoreApplyResult, error) {
	if source == nil || !validRuntimeRestoreRequest(req) {
		return RestoreApplyResult{}, fmt.Errorf("%w: invalid runtime restore request", ErrInvalidRequest)
	}
	endpoint, err := runtimeURL(req.Endpoint, runtimeRestoreApplyPath)
	if err != nil {
		return RestoreApplyResult{}, err
	}
	wire := restoreWireRequest(req)
	metadata, err := json.Marshal(wire)
	if err != nil {
		return RestoreApplyResult{}, err
	}
	if len(metadata) > maxRestoreMetadataBytes {
		return RestoreApplyResult{}, fmt.Errorf("%w: restore metadata too large", ErrInvalidRequest)
	}
	reader, writer := io.Pipe()
	streamDone := make(chan error, 1)
	go func() {
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(len(metadata)))
		if _, err := writer.Write(length[:]); err == nil {
			_, err = writer.Write(metadata)
			if err == nil {
				var streamed Record
				streamed, err = source.StreamArchive(ctx, req.Snapshot.SnapshotID, writer)
				if err == nil && !sameRestoreRecord(streamed, req.Snapshot) {
					err = fmt.Errorf("%w: streamed catalog changed", ErrRestoreConflict)
				}
			}
			_ = writer.CloseWithError(err)
			streamDone <- err
			return
		} else {
			_ = writer.CloseWithError(err)
			streamDone <- err
		}
	}()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, reader)
	if err != nil {
		_ = reader.Close()
		return RestoreApplyResult{}, err
	}
	httpReq.ContentLength = int64(4+len(metadata)) + req.Snapshot.ArchiveBytes
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Content-Type", "application/vnd.seaweed-block.snapshot-restore")
	resp, requestErr := c.client.Do(httpReq)
	streamErr := <-streamDone
	if requestErr != nil {
		return RestoreApplyResult{}, errors.Join(fmt.Errorf("snapshot: restore runtime request: %w", requestErr), streamErr)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, maxRuntimeErrorBytes))
		return RestoreApplyResult{}, fmt.Errorf("snapshot: restore runtime status %d: %s", resp.StatusCode, strings.TrimSpace(string(detail)))
	}
	if streamErr != nil {
		return RestoreApplyResult{}, streamErr
	}
	var result RestoreApplyResult
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxRestoreMetadataBytes)).Decode(&result); err != nil {
		return RestoreApplyResult{}, fmt.Errorf("snapshot: decode restore result: %w", err)
	}
	return result, nil
}

func (c *HTTPSRestoreRuntime) Activate(ctx context.Context, req RuntimeRestoreRequest) (RestoreMarker, error) {
	if !validRuntimeRestoreRequest(req) {
		return RestoreMarker{}, fmt.Errorf("%w: invalid runtime restore request", ErrInvalidRequest)
	}
	endpoint, err := runtimeURL(req.Endpoint, runtimeRestoreActivatePath)
	if err != nil {
		return RestoreMarker{}, err
	}
	body, err := json.Marshal(restoreWireRequest(req))
	if err != nil {
		return RestoreMarker{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return RestoreMarker{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return RestoreMarker{}, fmt.Errorf("snapshot: activate runtime request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, maxRuntimeErrorBytes))
		return RestoreMarker{}, fmt.Errorf("snapshot: activate runtime status %d: %s", resp.StatusCode, strings.TrimSpace(string(detail)))
	}
	var marker RestoreMarker
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxRestoreMetadataBytes)).Decode(&marker); err != nil {
		return RestoreMarker{}, fmt.Errorf("snapshot: decode activation result: %w", err)
	}
	return marker, nil
}

func readRestoreWireRequest(r io.Reader) (runtimeRestoreWireRequest, int, error) {
	var rawLength [4]byte
	if _, err := io.ReadFull(r, rawLength[:]); err != nil {
		return runtimeRestoreWireRequest{}, 0, err
	}
	length := binary.LittleEndian.Uint32(rawLength[:])
	if length == 0 || length > maxRestoreMetadataBytes {
		return runtimeRestoreWireRequest{}, 0, fmt.Errorf("invalid metadata length")
	}
	raw := make([]byte, int(length))
	if _, err := io.ReadFull(r, raw); err != nil {
		return runtimeRestoreWireRequest{}, 0, err
	}
	var req runtimeRestoreWireRequest
	if err := decodeSingleJSON(bytes.NewReader(raw), &req); err != nil {
		return runtimeRestoreWireRequest{}, 0, err
	}
	return req, int(length), nil
}

func validRuntimeRestoreRequest(req RuntimeRestoreRequest) bool {
	return req.Endpoint != "" && req.TargetVolumeID != "" && req.TargetReplicaID != "" && req.TargetStorageID != "" && req.TargetNumBlocks != 0 && req.TargetBlockSize > 0 && req.TargetNumBlocks == req.Snapshot.NumBlocks && req.TargetBlockSize == req.Snapshot.BlockSize && validateRecord(req.Snapshot) == nil
}

func restoreWireRequest(req RuntimeRestoreRequest) runtimeRestoreWireRequest {
	return runtimeRestoreWireRequest{
		Snapshot:        req.Snapshot,
		TargetVolumeID:  req.TargetVolumeID,
		TargetReplicaID: req.TargetReplicaID,
		TargetStorageID: req.TargetStorageID,
		TargetNumBlocks: req.TargetNumBlocks,
		TargetBlockSize: req.TargetBlockSize,
	}
}

func decodeSingleJSON(r io.Reader, value any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func runtimeBearerAuthorized(r *http.Request, token string) bool {
	want := "Bearer " + token
	got := r.Header.Get("Authorization")
	return len(got) == len(want) && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func writeRestoreRuntimeError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrInvalidRequest):
		code = http.StatusBadRequest
	case errors.Is(err, ErrRestoreConflict), errors.Is(err, ErrRestoreNotApplied), errors.Is(err, ErrRestoreUnsafe):
		code = http.StatusConflict
	case errors.Is(err, ErrArchiveCorrupt):
		code = http.StatusUnprocessableEntity
	}
	http.Error(w, err.Error(), code)
}

func writeRestoreRuntimeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}
