package snapshot

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	runtimeCapturePath = "/v1/snapshot/capture"
	runtimeStreamMagic = "SWBSRT01"

	runtimeFrameBlock    = byte(1)
	runtimeFrameTerminal = byte(2)
	runtimeFrameError    = byte(3)

	maxRuntimeRequestBytes = 16 << 10
	maxRuntimeBlockBytes   = 16 << 20
	maxRuntimeErrorBytes   = 4 << 10
)

type runtimeCaptureWireRequest struct {
	SnapshotName    string `json:"snapshotName"`
	VolumeID        string `json:"volumeID"`
	ReplicaID       string `json:"replicaID"`
	Epoch           uint64 `json:"epoch"`
	EndpointVersion uint64 `json:"endpointVersion"`
	SizeBytes       uint64 `json:"sizeBytes"`
}

type RuntimeHandler struct {
	source storage.SnapshotSource
	view   frontend.ProjectionView
	token  string
}

func NewRuntimeHandler(source storage.SnapshotSource, view frontend.ProjectionView, token string) (*RuntimeHandler, error) {
	if source == nil || view == nil || token == "" {
		return nil, fmt.Errorf("snapshot: runtime requires source, projection view, and token")
	}
	return &RuntimeHandler{source: source, view: view, token: token}, nil
}

func (h *RuntimeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != runtimeCapturePath {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	wantAuth := "Bearer " + h.token
	gotAuth := r.Header.Get("Authorization")
	if len(gotAuth) != len(wantAuth) || subtle.ConstantTimeCompare([]byte(gotAuth), []byte(wantAuth)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req runtimeCaptureWireRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRuntimeRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.SnapshotName == "" || req.VolumeID == "" || req.ReplicaID == "" || req.Epoch == 0 || req.EndpointVersion == 0 || req.SizeBytes == 0 {
		http.Error(w, "missing required identity", http.StatusBadRequest)
		return
	}
	if !projectionMatches(h.view.Projection(), req) {
		http.Error(w, ErrSourceNotReady.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.seaweed-block.snapshot-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, runtimeStreamMagic); err != nil {
		return
	}
	cut, err := h.source.CaptureSnapshot(r.Context(), func(lba uint32, data []byte) error {
		return writeRuntimeBlock(w, lba, data)
	})
	if err != nil {
		_ = writeRuntimeError(w, err)
		return
	}
	if uint64(cut.NumBlocks)*uint64(cut.BlockSize) != req.SizeBytes {
		_ = writeRuntimeError(w, fmt.Errorf("snapshot: source geometry changed"))
		return
	}
	if !projectionMatches(h.view.Projection(), req) {
		_ = writeRuntimeError(w, ErrAuthorityChanged)
		return
	}
	_ = writeRuntimeTerminal(w, cut)
}

func projectionMatches(proj frontend.Projection, req runtimeCaptureWireRequest) bool {
	return proj.Healthy &&
		proj.VolumeID == req.VolumeID &&
		proj.ReplicaID == req.ReplicaID &&
		proj.Epoch == req.Epoch &&
		proj.EndpointVersion == req.EndpointVersion
}

func writeRuntimeBlock(w io.Writer, lba uint32, data []byte) error {
	if len(data) == 0 || len(data) > maxRuntimeBlockBytes {
		return fmt.Errorf("snapshot: runtime invalid block size %d", len(data))
	}
	header := make([]byte, 13)
	header[0] = runtimeFrameBlock
	binary.LittleEndian.PutUint32(header[1:5], lba)
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(data)))
	binary.LittleEndian.PutUint32(header[9:13], crc32.ChecksumIEEE(data))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func writeRuntimeTerminal(w io.Writer, cut storage.SnapshotCut) error {
	if cut.BlockSize <= 0 || cut.BlockSize > maxRuntimeBlockBytes || cut.NumBlocks == 0 {
		return fmt.Errorf("snapshot: runtime invalid terminal geometry")
	}
	frame := make([]byte, 33)
	frame[0] = runtimeFrameTerminal
	binary.LittleEndian.PutUint32(frame[1:5], uint32(cut.BlockSize))
	binary.LittleEndian.PutUint32(frame[5:9], cut.NumBlocks)
	binary.LittleEndian.PutUint64(frame[9:17], cut.Frontier)
	binary.LittleEndian.PutUint64(frame[17:25], cut.BlockCount)
	binary.LittleEndian.PutUint64(frame[25:33], cut.DataBytes)
	_, err := w.Write(frame)
	return err
}

func writeRuntimeError(w io.Writer, cause error) error {
	message := []byte(cause.Error())
	if len(message) > maxRuntimeErrorBytes {
		message = message[:maxRuntimeErrorBytes]
	}
	header := make([]byte, 5)
	header[0] = runtimeFrameError
	binary.LittleEndian.PutUint32(header[1:5], uint32(len(message)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(message)
	return err
}

type HTTPSCaptureRuntime struct {
	client *http.Client
	token  string
}

func NewHTTPSCaptureRuntime(client *http.Client, token string) (*HTTPSCaptureRuntime, error) {
	if client == nil || token == "" {
		return nil, fmt.Errorf("snapshot: HTTPS runtime requires client and token")
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPSCaptureRuntime{client: &clientCopy, token: token}, nil
}

func (c *HTTPSCaptureRuntime) CaptureSnapshot(ctx context.Context, req RuntimeCaptureRequest, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	if sink == nil || !req.Source.valid() || req.SnapshotName == "" || req.Source.SizeBytes == 0 {
		return storage.SnapshotCut{}, fmt.Errorf("snapshot: invalid runtime capture request")
	}
	endpoint, err := runtimeCaptureURL(req.Source.RuntimeEndpoint)
	if err != nil {
		return storage.SnapshotCut{}, err
	}
	body, err := json.Marshal(runtimeCaptureWireRequest{
		SnapshotName:    req.SnapshotName,
		VolumeID:        req.Source.VolumeID,
		ReplicaID:       req.Source.ReplicaID,
		Epoch:           req.Source.Epoch,
		EndpointVersion: req.Source.EndpointVersion,
		SizeBytes:       req.Source.SizeBytes,
	})
	if err != nil {
		return storage.SnapshotCut{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return storage.SnapshotCut{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return storage.SnapshotCut{}, fmt.Errorf("snapshot: runtime request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, maxRuntimeErrorBytes))
		return storage.SnapshotCut{}, fmt.Errorf("snapshot: runtime status %d: %s", resp.StatusCode, strings.TrimSpace(string(detail)))
	}
	return readRuntimeStream(resp.Body, req.Source.SizeBytes, sink)
}

func runtimeCaptureURL(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("snapshot: invalid HTTPS runtime endpoint")
	}
	if u.Path != "" && u.Path != "/" {
		return "", fmt.Errorf("snapshot: runtime endpoint must not contain a path")
	}
	u.Path = runtimeCapturePath
	return u.String(), nil
}

func ValidateRuntimeEndpoint(endpoint string) error {
	_, err := runtimeCaptureURL(endpoint)
	return err
}

func readRuntimeStream(r io.Reader, maxDataBytes uint64, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	if maxDataBytes == 0 {
		return storage.SnapshotCut{}, fmt.Errorf("snapshot: runtime stream has no size limit")
	}
	magic := make([]byte, len(runtimeStreamMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != runtimeStreamMagic {
		return storage.SnapshotCut{}, fmt.Errorf("snapshot: invalid runtime stream")
	}
	var count, dataBytes uint64
	var previousLBA uint32
	var observedBlockSize uint32
	havePrevious := false
	for {
		var frameType [1]byte
		if _, err := io.ReadFull(r, frameType[:]); err != nil {
			return storage.SnapshotCut{}, fmt.Errorf("snapshot: runtime stream ended before terminal: %w", err)
		}
		switch frameType[0] {
		case runtimeFrameBlock:
			header := make([]byte, 12)
			if _, err := io.ReadFull(r, header); err != nil {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: read runtime block header: %w", err)
			}
			lba := binary.LittleEndian.Uint32(header[0:4])
			length := binary.LittleEndian.Uint32(header[4:8])
			wantCRC := binary.LittleEndian.Uint32(header[8:12])
			if length < 512 || length > maxRuntimeBlockBytes || (havePrevious && lba <= previousLBA) {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: invalid runtime block frame")
			}
			if observedBlockSize == 0 {
				observedBlockSize = length
			} else if length != observedBlockSize {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: runtime block size changed")
			}
			if uint64(length) > maxDataBytes || dataBytes > maxDataBytes-uint64(length) || count >= (maxDataBytes+511)/512 {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: runtime stream exceeds source size")
			}
			data := make([]byte, int(length))
			if _, err := io.ReadFull(r, data); err != nil {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: read runtime block: %w", err)
			}
			if crc32.ChecksumIEEE(data) != wantCRC {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: runtime block CRC mismatch at LBA %d", lba)
			}
			if err := sink(lba, data); err != nil {
				return storage.SnapshotCut{}, err
			}
			count++
			dataBytes += uint64(length)
			previousLBA = lba
			havePrevious = true
		case runtimeFrameTerminal:
			terminal := make([]byte, 32)
			if _, err := io.ReadFull(r, terminal); err != nil {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: read runtime terminal: %w", err)
			}
			cut := storage.SnapshotCut{
				BlockSize:  int(binary.LittleEndian.Uint32(terminal[0:4])),
				NumBlocks:  binary.LittleEndian.Uint32(terminal[4:8]),
				Frontier:   binary.LittleEndian.Uint64(terminal[8:16]),
				BlockCount: binary.LittleEndian.Uint64(terminal[16:24]),
				DataBytes:  binary.LittleEndian.Uint64(terminal[24:32]),
			}
			if cut.BlockSize <= 0 || cut.NumBlocks == 0 || uint64(cut.NumBlocks)*uint64(cut.BlockSize) != maxDataBytes || cut.BlockCount != count || cut.DataBytes != dataBytes || (observedBlockSize != 0 && uint32(cut.BlockSize) != observedBlockSize) || (havePrevious && previousLBA >= cut.NumBlocks) {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: runtime terminal does not reconcile stream")
			}
			return cut, nil
		case runtimeFrameError:
			var rawLength [4]byte
			if _, err := io.ReadFull(r, rawLength[:]); err != nil {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: read runtime error: %w", err)
			}
			length := binary.LittleEndian.Uint32(rawLength[:])
			if length == 0 || length > maxRuntimeErrorBytes {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: invalid runtime error frame")
			}
			message := make([]byte, int(length))
			if _, err := io.ReadFull(r, message); err != nil {
				return storage.SnapshotCut{}, fmt.Errorf("snapshot: read runtime error: %w", err)
			}
			return storage.SnapshotCut{}, fmt.Errorf("snapshot: remote capture failed: %s", message)
		default:
			return storage.SnapshotCut{}, fmt.Errorf("snapshot: unknown runtime frame %d", frameType[0])
		}
	}
}

type RuntimeServerConfig struct {
	Listen            string
	AdvertiseEndpoint string
	TLSCertFile       string
	TLSKeyFile        string
	ClientCAFile      string
	Handler           http.Handler
}

type RuntimeServer struct {
	ln       net.Listener
	server   *http.Server
	endpoint string
}

func StartRuntimeServer(cfg RuntimeServerConfig) (*RuntimeServer, error) {
	if cfg.Listen == "" || cfg.AdvertiseEndpoint == "" || cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.ClientCAFile == "" || cfg.Handler == nil {
		return nil, fmt.Errorf("snapshot: runtime server requires listen, advertise endpoint, TLS cert/key, client CA, and handler")
	}
	endpointURL, err := runtimeCaptureURL(cfg.AdvertiseEndpoint)
	if err != nil {
		return nil, err
	}
	certificate, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("snapshot: load runtime TLS identity: %w", err)
	}
	clientCAPEM, err := os.ReadFile(cfg.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("snapshot: read runtime client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, fmt.Errorf("snapshot: runtime client CA contains no certificates")
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("snapshot: runtime listen: %w", err)
	}
	tlsLn := tls.NewListener(ln, &tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs,
	})
	srv := &RuntimeServer{
		ln:       tlsLn,
		server:   &http.Server{Handler: cfg.Handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second},
		endpoint: strings.TrimSuffix(endpointURL, runtimeCapturePath),
	}
	go func() { _ = srv.server.Serve(tlsLn) }()
	return srv, nil
}

func (s *RuntimeServer) Endpoint() string { return s.endpoint }

func (s *RuntimeServer) Close(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

var _ CaptureRuntime = (*HTTPSCaptureRuntime)(nil)
