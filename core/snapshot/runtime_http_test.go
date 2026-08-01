package snapshot

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPhase175HTTPSRuntimeCapturesExactLineage(t *testing.T) {
	authority := SourceAuthority{VolumeID: "vol-a", ReplicaID: "r2", Epoch: 7, EndpointVersion: 3, SizeBytes: 4 * 4096}
	view := &mutableProjectionView{projection: frontend.Projection{
		VolumeID: authority.VolumeID, ReplicaID: authority.ReplicaID,
		Epoch: authority.Epoch, EndpointVersion: authority.EndpointVersion, Healthy: true,
	}}
	source := &runtimeTestSource{blocks: map[uint32][]byte{0: testBlock(0x31), 2: testBlock(0x33)}, frontier: 21, numBlocks: 4}
	handler, err := NewRuntimeHandler(source, view, "token-a")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	authority.RuntimeEndpoint = server.URL
	client, err := NewHTTPSCaptureRuntime(server.Client(), "token-a")
	if err != nil {
		t.Fatal(err)
	}
	gotBlocks := map[uint32][]byte{}
	cut, err := client.CaptureSnapshot(context.Background(), RuntimeCaptureRequest{SnapshotName: "snap-a", Source: authority}, func(lba uint32, data []byte) error {
		gotBlocks[lba] = append([]byte(nil), data...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if source.calls != 1 || cut.Frontier != 21 || cut.BlockCount != 2 || len(gotBlocks) != 2 || gotBlocks[2][0] != 0x33 {
		t.Fatalf("source calls=%d cut=%+v blocks=%v", source.calls, cut, gotBlocks)
	}
}

func TestPhase175HTTPSRuntimeRejectsUnauthorizedAndStaleLineage(t *testing.T) {
	view := &mutableProjectionView{projection: frontend.Projection{VolumeID: "vol-a", ReplicaID: "r1", Epoch: 2, EndpointVersion: 1, Healthy: true}}
	source := &runtimeTestSource{numBlocks: 1}
	handler, err := NewRuntimeHandler(source, view, "right-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()

	for _, tc := range []struct {
		name  string
		token string
		epoch uint64
	}{
		{name: "wrong token", token: "wrong-token", epoch: 2},
		{name: "stale epoch", token: "right-token", epoch: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewHTTPSCaptureRuntime(server.Client(), tc.token)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.CaptureSnapshot(context.Background(), RuntimeCaptureRequest{
				SnapshotName: "snap-a",
				Source:       SourceAuthority{VolumeID: "vol-a", ReplicaID: "r1", Epoch: tc.epoch, EndpointVersion: 1, RuntimeEndpoint: server.URL, SizeBytes: 4096},
			}, func(uint32, []byte) error { return nil })
			if err == nil {
				t.Fatal("expected refusal")
			}
		})
	}
	if source.calls != 0 {
		t.Fatalf("source calls=%d want 0", source.calls)
	}
}

func TestPhase175HTTPSRuntimeFailsWhenAuthorityChangesDuringCut(t *testing.T) {
	view := &mutableProjectionView{projection: frontend.Projection{VolumeID: "vol-a", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2, Healthy: true}}
	source := &runtimeTestSource{
		blocks:    map[uint32][]byte{0: testBlock(0x41)},
		frontier:  8,
		numBlocks: 1,
		afterCapture: func() {
			view.set(frontend.Projection{VolumeID: "vol-a", ReplicaID: "r2", Epoch: 5, EndpointVersion: 1, Healthy: true})
		},
	}
	handler, err := NewRuntimeHandler(source, view, "token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	client, err := NewHTTPSCaptureRuntime(server.Client(), "token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CaptureSnapshot(context.Background(), RuntimeCaptureRequest{
		SnapshotName: "failover-cut",
		Source:       SourceAuthority{VolumeID: "vol-a", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2, RuntimeEndpoint: server.URL, SizeBytes: 4096},
	}, func(uint32, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), ErrAuthorityChanged.Error()) {
		t.Fatalf("authority change error=%v", err)
	}
}

func TestPhase175HTTPSRuntimeRejectsPlainHTTP(t *testing.T) {
	client, err := NewHTTPSCaptureRuntime(&http.Client{}, "token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CaptureSnapshot(context.Background(), RuntimeCaptureRequest{
		SnapshotName: "snap-a",
		Source:       SourceAuthority{VolumeID: "vol-a", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, RuntimeEndpoint: "http://127.0.0.1:1234", SizeBytes: 4096},
	}, func(uint32, []byte) error { return nil })
	if err == nil {
		t.Fatal("expected non-HTTPS endpoint rejection")
	}
}

func TestPhase175HTTPSRuntimeDoesNotForwardTokenOnRedirect(t *testing.T) {
	redirected := false
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == runtimeCapturePath {
			http.Redirect(w, r, "/token-sink", http.StatusTemporaryRedirect)
			return
		}
		redirected = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewHTTPSCaptureRuntime(server.Client(), "secret-token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CaptureSnapshot(context.Background(), RuntimeCaptureRequest{
		SnapshotName: "snap-a",
		Source: SourceAuthority{
			VolumeID: "vol-a", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, RuntimeEndpoint: server.URL, SizeBytes: 4096,
		},
	}, func(uint32, []byte) error { return nil })
	if err == nil || redirected {
		t.Fatalf("redirect error=%v redirected=%v", err, redirected)
	}
}

func TestPhase175RuntimeStreamCannotExceedSourceCapacity(t *testing.T) {
	var stream bytes.Buffer
	stream.WriteString(runtimeStreamMagic)
	if err := writeRuntimeBlock(&stream, 0, testBlock(0x61)); err != nil {
		t.Fatal(err)
	}
	if _, err := readRuntimeStream(&stream, 512, func(uint32, []byte) error { return nil }); err == nil || !strings.Contains(err.Error(), "exceeds source size") {
		t.Fatalf("error=%v", err)
	}
}

func TestPhase175RuntimeServerStreamsIntoDurableCatalogOverTLS(t *testing.T) {
	identity := writeRuntimeTLSIdentity(t)
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	authority := SourceAuthority{VolumeID: "vol-a", ReplicaID: "r1", Epoch: 9, EndpointVersion: 2, RuntimeEndpoint: "https://" + addr, SizeBytes: 8 * 4096}
	view := &mutableProjectionView{projection: frontend.Projection{
		VolumeID: authority.VolumeID, ReplicaID: authority.ReplicaID, Epoch: authority.Epoch, EndpointVersion: authority.EndpointVersion, Healthy: true,
	}}
	handler, err := NewRuntimeHandler(&runtimeTestSource{
		blocks: map[uint32][]byte{3: testBlock(0x77)}, frontier: 31, numBlocks: 8,
	}, view, "catalog-token")
	if err != nil {
		t.Fatal(err)
	}
	server, err := StartRuntimeServer(RuntimeServerConfig{
		Listen: addr, AdvertiseEndpoint: authority.RuntimeEndpoint, TLSCertFile: identity.serverCertFile, TLSKeyFile: identity.serverKeyFile, ClientCAFile: identity.caFile, Handler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close(context.Background())
	unauthenticated, err := NewHTTPSCaptureRuntime(&http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: identity.roots, MinVersion: tls.VersionTLS12,
	}}}, "catalog-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unauthenticated.CaptureSnapshot(context.Background(), RuntimeCaptureRequest{SnapshotName: "denied", Source: authority}, func(uint32, []byte) error { return nil }); err == nil {
		t.Fatal("runtime accepted a client without the blockmaster mTLS identity")
	}
	runtime, err := NewHTTPSCaptureRuntime(&http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: identity.roots, Certificates: []tls.Certificate{identity.clientCertificate}, MinVersion: tls.VersionTLS12,
	}}}, "catalog-token")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(manager, fixedRuntimeResolver{authority: authority}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	record, err := coordinator.Create(context.Background(), CreateRequest{Name: "snap-tls", SourceVolumeID: "vol-a"})
	if err != nil {
		t.Fatal(err)
	}
	if record.State != StateReady || record.Frontier != 31 || record.RecordCount != 1 || record.ArchiveSHA256 == "" {
		t.Fatalf("record=%+v", record)
	}
}

type fixedRuntimeResolver struct {
	authority SourceAuthority
}

func (r fixedRuntimeResolver) ResolveSnapshotSource(_ context.Context, volumeID string) (SourceAuthority, error) {
	if volumeID != r.authority.VolumeID {
		return SourceAuthority{}, ErrSourceNotReady
	}
	return r.authority, nil
}

type runtimeTLSIdentity struct {
	caFile            string
	serverCertFile    string
	serverKeyFile     string
	clientCertificate tls.Certificate
	roots             *x509.CertPool
}

func writeRuntimeTLSIdentity(t *testing.T) runtimeTLSIdentity {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "phase175-ca"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:     true, BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	writeLeaf := func(name string, serial int64, usage x509.ExtKeyUsage, ips []net.IP) (string, string) {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: ips,
		}
		certDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certFile := filepath.Join(dir, name+".crt")
		keyFile := filepath.Join(dir, name+".key")
		if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
			t.Fatal(err)
		}
		return certFile, keyFile
	}
	serverCertFile, serverKeyFile := writeLeaf("server", 2, x509.ExtKeyUsageServerAuth, []net.IP{net.ParseIP("127.0.0.1")})
	clientCertFile, clientKeyFile := writeLeaf("client", 3, x509.ExtKeyUsageClientAuth, nil)
	clientCertificate, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append generated certificate")
	}
	return runtimeTLSIdentity{
		caFile: caFile, serverCertFile: serverCertFile, serverKeyFile: serverKeyFile,
		clientCertificate: clientCertificate, roots: roots,
	}
}

type mutableProjectionView struct {
	mu         sync.Mutex
	projection frontend.Projection
}

func (v *mutableProjectionView) Projection() frontend.Projection {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.projection
}

func (v *mutableProjectionView) set(projection frontend.Projection) {
	v.mu.Lock()
	v.projection = projection
	v.mu.Unlock()
}

type runtimeTestSource struct {
	blocks       map[uint32][]byte
	frontier     uint64
	numBlocks    uint32
	calls        int
	afterCapture func()
}

func (s *runtimeTestSource) CaptureSnapshot(ctx context.Context, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	s.calls++
	cut := storage.SnapshotCut{Frontier: s.frontier, NumBlocks: s.numBlocks, BlockSize: 4096}
	for lba := uint32(0); lba < s.numBlocks; lba++ {
		if err := ctx.Err(); err != nil {
			return storage.SnapshotCut{}, err
		}
		data, ok := s.blocks[lba]
		if !ok {
			continue
		}
		if err := sink(lba, data); err != nil {
			return storage.SnapshotCut{}, err
		}
		cut.BlockCount++
		cut.DataBytes += uint64(len(data))
	}
	if s.afterCapture != nil {
		s.afterCapture()
	}
	return cut, nil
}
