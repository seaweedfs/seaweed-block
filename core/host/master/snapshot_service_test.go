package master

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestPhase175SnapshotServiceLifecycle(t *testing.T) {
	manager, err := snapshot.OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authority := snapshot.SourceAuthority{
		VolumeID: "vol-a", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2, RuntimeEndpoint: "https://snapshot.example:9443",
	}
	coordinator, err := snapshot.NewCoordinator(manager, fixedSnapshotResolver{authority: authority}, fixedSnapshotRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	svc := newServices(&Host{snapshotCoordinator: coordinator, snapshotAPIToken: "api-token", snapshotCaptureTimeout: time.Minute})
	ctx := snapshotIncomingContext("api-token")

	created, err := svc.CreateSnapshot(ctx, &control.CreateSnapshotRequest{Name: "snap-a", SourceVolumeId: "vol-a"})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetSnapshotId() == "" || created.GetState() != snapshot.StateReady || created.GetFrontier() != 17 || created.GetRecordCount() != 1 {
		t.Fatalf("created=%+v", created)
	}
	listed, err := svc.ListSnapshots(ctx, &control.ListSnapshotsRequest{SourceVolumeId: "vol-a"})
	if err != nil || len(listed.GetSnapshots()) != 1 || listed.GetSnapshots()[0].GetSnapshotId() != created.GetSnapshotId() {
		t.Fatalf("listed=%+v err=%v", listed, err)
	}
	got, err := svc.GetSnapshot(ctx, &control.GetSnapshotRequest{SnapshotId: created.GetSnapshotId()})
	if err != nil || got.GetArchiveSha256() == "" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if _, err := svc.DeleteSnapshot(ctx, &control.DeleteSnapshotRequest{SnapshotId: created.GetSnapshotId()}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetSnapshot(ctx, &control.GetSnapshotRequest{SnapshotId: created.GetSnapshotId()}); status.Code(err) != codes.NotFound {
		t.Fatalf("get deleted error=%v", err)
	}
}

func TestPhase175SnapshotServiceFailsClosedWhenDisabledOrInvalid(t *testing.T) {
	svc := newServices(&Host{})
	if _, err := svc.ListSnapshots(context.Background(), &control.ListSnapshotsRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("disabled error=%v", err)
	}

	manager, err := snapshot.OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := snapshot.NewCoordinator(manager, fixedSnapshotResolver{}, fixedSnapshotRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	svc = newServices(&Host{snapshotCoordinator: coordinator, snapshotAPIToken: "api-token", snapshotCaptureTimeout: time.Minute})
	if _, err := svc.CreateSnapshot(snapshotIncomingContext("api-token"), &control.CreateSnapshotRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid create error=%v", err)
	}
	if _, err := svc.ListSnapshots(context.Background(), &control.ListSnapshotsRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated list error=%v", err)
	}
}

func TestPhase175SnapshotServiceOnlyRegisteredOnDedicatedMTLSGRPC(t *testing.T) {
	tlsFiles := writeSnapshotAPITestIdentity(t)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("grpc-api-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := New(Config{
		AuthorityStoreDir:             filepath.Join(t.TempDir(), "authority"),
		LifecycleStoreDir:             filepath.Join(t.TempDir(), "lifecycle"),
		Listen:                        "127.0.0.1:0",
		SnapshotRoot:                  filepath.Join(t.TempDir(), "snapshots"),
		SnapshotRuntimeCAFile:         tlsFiles.caFile,
		SnapshotRuntimeTokenFile:      tokenFile,
		SnapshotRuntimeClientCertFile: tlsFiles.clientCertFile,
		SnapshotRuntimeClientKeyFile:  tlsFiles.clientKeyFile,
		SnapshotAPIListen:             "127.0.0.1:0",
		SnapshotAPITLSCertFile:        tlsFiles.serverCertFile,
		SnapshotAPITLSKeyFile:         tlsFiles.serverKeyFile,
		SnapshotAPIClientCAFile:       tlsFiles.caFile,
		SnapshotAPITokenFile:          tokenFile,
		SnapshotCaptureTimeout:        time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	defer closeTestMaster(t, h)
	manager, err := snapshot.OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authority := snapshot.SourceAuthority{
		VolumeID: "vol-grpc", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, RuntimeEndpoint: "https://snapshot.example:9443",
	}
	h.snapshotCoordinator, err = snapshot.NewCoordinator(manager, fixedSnapshotResolver{authority: authority}, fixedSnapshotRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	plainConn, err := grpc.DialContext(ctx, h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer plainConn.Close()
	if _, err := control.NewSnapshotServiceClient(plainConn).ListSnapshots(ctx, &control.ListSnapshotsRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("plaintext control listener exposed SnapshotService: %v", err)
	}

	clientCertificate, err := tls.LoadX509KeyPair(tlsFiles.clientCertFile, tlsFiles.clientKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	caPEM, err := os.ReadFile(tlsFiles.caFile)
	if err != nil || !roots.AppendCertsFromPEM(caPEM) {
		t.Fatalf("load roots: %v", err)
	}
	noCertConn, err := grpc.NewClient(h.SnapshotAPIAddr(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs: roots, MinVersion: tls.VersionTLS12,
	})))
	if err != nil {
		t.Fatal(err)
	}
	noCertCtx, noCertCancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer grpc-api-token"), 2*time.Second)
	_, noCertErr := control.NewSnapshotServiceClient(noCertConn).ListSnapshots(noCertCtx, &control.ListSnapshotsRequest{})
	noCertCancel()
	_ = noCertConn.Close()
	if status.Code(noCertErr) != codes.Unavailable {
		t.Fatalf("snapshot API accepted a client without an mTLS identity: %v", noCertErr)
	}
	conn, err := grpc.DialContext(ctx, h.SnapshotAPIAddr(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs: roots, Certificates: []tls.Certificate{clientCertificate}, MinVersion: tls.VersionTLS12,
	})), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer grpc-api-token")
	created, err := control.NewSnapshotServiceClient(conn).CreateSnapshot(authCtx, &control.CreateSnapshotRequest{
		Name: "snap-grpc", SourceVolumeId: "vol-grpc",
	})
	if err != nil || created.GetSnapshotId() == "" || created.GetState() != snapshot.StateReady {
		t.Fatalf("created=%+v err=%v", created, err)
	}
}

type snapshotAPITestIdentity struct {
	caFile, serverCertFile, serverKeyFile, clientCertFile, clientKeyFile string
}

func writeSnapshotAPITestIdentity(t *testing.T) snapshotAPITestIdentity {
	t.Helper()
	dir := t.TempDir()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute)
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "phase175-api-ca"},
		NotBefore: now, NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	writeLeaf := func(name string, serial int64, usage x509.ExtKeyUsage, ips []net.IP) (string, string) {
		t.Helper()
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			NotBefore: now, NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		certFile := filepath.Join(dir, name+".crt")
		keyFile := filepath.Join(dir, name+".key")
		if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
			t.Fatal(err)
		}
		return certFile, keyFile
	}
	serverCert, serverKey := writeLeaf("api-server", 2, x509.ExtKeyUsageServerAuth, []net.IP{net.ParseIP("127.0.0.1")})
	clientCert, clientKey := writeLeaf("api-client", 3, x509.ExtKeyUsageClientAuth, nil)
	return snapshotAPITestIdentity{caFile: caFile, serverCertFile: serverCert, serverKeyFile: serverKey, clientCertFile: clientCert, clientKeyFile: clientKey}
}

func TestPhase175SnapshotServiceEnforcesCaptureDeadline(t *testing.T) {
	manager, err := snapshot.OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authority := snapshot.SourceAuthority{
		VolumeID: "vol-slow", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, RuntimeEndpoint: "https://snapshot.example:9443",
	}
	coordinator, err := snapshot.NewCoordinator(manager, fixedSnapshotResolver{authority: authority}, blockingSnapshotRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	svc := newServices(&Host{snapshotCoordinator: coordinator, snapshotAPIToken: "api-token", snapshotCaptureTimeout: 10 * time.Millisecond})
	_, err = svc.CreateSnapshot(snapshotIncomingContext("api-token"), &control.CreateSnapshotRequest{Name: "slow", SourceVolumeId: "vol-slow"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline error=%v", err)
	}
}

func snapshotIncomingContext(token string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

type fixedSnapshotResolver struct {
	authority snapshot.SourceAuthority
}

func (r fixedSnapshotResolver) ResolveSnapshotSource(_ context.Context, volumeID string) (snapshot.SourceAuthority, error) {
	if r.authority.VolumeID != volumeID {
		return snapshot.SourceAuthority{}, snapshot.ErrSourceNotReady
	}
	return r.authority, nil
}

type fixedSnapshotRuntime struct{}

func (fixedSnapshotRuntime) CaptureSnapshot(_ context.Context, _ snapshot.RuntimeCaptureRequest, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	block := make([]byte, 4096)
	block[0] = 0x5a
	if err := sink(2, block); err != nil {
		return storage.SnapshotCut{}, err
	}
	return storage.SnapshotCut{BlockSize: 4096, NumBlocks: 8, Frontier: 17, BlockCount: 1, DataBytes: 4096}, nil
}

type blockingSnapshotRuntime struct{}

func (blockingSnapshotRuntime) CaptureSnapshot(ctx context.Context, _ snapshot.RuntimeCaptureRequest, _ storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	<-ctx.Done()
	return storage.SnapshotCut{}, ctx.Err()
}
