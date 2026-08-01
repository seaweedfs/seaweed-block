package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

const (
	phase174RemoteReplicasEnv   = "SW_BLOCK_PHASE174_REMOTE_REPLICAS"
	phase174ReplicaStoreEnv     = "SW_BLOCK_PHASE174_REPLICA_STORE"
	phase174ReplicaListenEnv    = "SW_BLOCK_PHASE174_REPLICA_LISTEN"
	phase174ReplicaReadyFileEnv = "SW_BLOCK_PHASE174_REPLICA_READY_FILE"
	phase174ReplicaStopFileEnv  = "SW_BLOCK_PHASE174_REPLICA_STOP_FILE"
	phase174ReplicaResultEnv    = "SW_BLOCK_PHASE174_REPLICA_RESULT_FILE"
	phase174ReplicaFinalSetEnv  = "SW_BLOCK_PHASE174_REPLICA_FINAL_SET"
	phase174ReplicaFinalRunEnv  = "SW_BLOCK_PHASE174_REPLICA_FINAL_RUN"
)

type phase174RemoteReplicaResult struct {
	Status             string `json:"status"`
	ListenAddress      string `json:"listen_address"`
	StableLSN          uint64 `json:"stable_lsn"`
	HeadLSN            uint64 `json:"head_lsn"`
	ExpectedHeadLSN    uint64 `json:"expected_head_lsn"`
	CorrectnessSamples int    `json:"correctness_samples"`
}

func phase174RemoteReplicaTargets() ([]ReplicaTarget, error) {
	raw := strings.TrimSpace(os.Getenv(phase174RemoteReplicasEnv))
	if raw == "" {
		return nil, nil
	}
	addresses := strings.Split(raw, ",")
	if len(addresses) != 2 {
		return nil, fmt.Errorf("phase174: %s must contain exactly two addresses", phase174RemoteReplicasEnv)
	}
	targets := make([]ReplicaTarget, 0, len(addresses))
	hosts := make(map[string]struct{}, len(addresses))
	for index, value := range addresses {
		address := strings.TrimSpace(value)
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("phase174: remote replica address %q: %w", address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
			return nil, fmt.Errorf("phase174: remote replica host %q must be a non-loopback IP", host)
		}
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return nil, fmt.Errorf("phase174: remote replica port %q: %w", port, err)
		}
		if _, exists := hosts[ip.String()]; exists {
			return nil, fmt.Errorf("phase174: remote replicas must use distinct hosts")
		}
		hosts[ip.String()] = struct{}{}
		targets = append(targets, ReplicaTarget{
			ReplicaID: fmt.Sprintf("remote-r%d", index+1), DataAddr: address,
			ControlAddr: address, Epoch: 1, EndpointVersion: 1,
		})
	}
	return targets, nil
}

func TestPhase174RemoteReplicaTargetContract(t *testing.T) {
	t.Setenv(phase174RemoteReplicasEnv, "192.0.2.1:17411,192.0.2.2:17412")
	targets, err := phase174RemoteReplicaTargets()
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].DataAddr != "192.0.2.1:17411" || targets[1].DataAddr != "192.0.2.2:17412" {
		t.Fatalf("targets=%+v", targets)
	}

	for _, invalid := range []string{
		"192.0.2.1:17411",
		"127.0.0.1:17411,192.0.2.2:17412",
		"192.0.2.1:17411,192.0.2.1:17412",
	} {
		t.Setenv(phase174RemoteReplicasEnv, invalid)
		if _, err := phase174RemoteReplicaTargets(); err == nil {
			t.Fatalf("accepted remote replica addresses %q", invalid)
		}
	}
}

func TestPhase174RemoteReplicaProcess(t *testing.T) {
	storePath := os.Getenv(phase174ReplicaStoreEnv)
	if storePath == "" {
		t.Skip("remote replica process requires " + phase174ReplicaStoreEnv)
	}
	listenAddress := os.Getenv(phase174ReplicaListenEnv)
	readyFile := os.Getenv(phase174ReplicaReadyFileEnv)
	stopFile := os.Getenv(phase174ReplicaStopFileEnv)
	resultFile := os.Getenv(phase174ReplicaResultEnv)
	if listenAddress == "" || readyFile == "" || stopFile == "" || resultFile == "" {
		t.Fatal("remote replica process requires listen, ready, stop, and result paths")
	}
	finalSet := phase174RequiredPositiveInt(t, phase174ReplicaFinalSetEnv)
	finalRun := phase174RequiredPositiveInt(t, phase174ReplicaFinalRunEnv)

	if err := os.MkdirAll(filepath.Dir(storePath), 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := storage.CreateWALStore(storePath, phase174NumBlocks, phase174BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	storeOpen := true
	defer func() {
		if storeOpen {
			_ = store.Close()
		}
	}()

	listener, err := transport.NewReplicaListener(listenAddress, store)
	if err != nil {
		t.Fatal(err)
	}
	listenerRunning := true
	defer func() {
		if listenerRunning {
			listener.Stop()
		}
	}()
	listener.Serve()
	if err := os.WriteFile(readyFile, []byte(listener.Addr()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Minute)
	for {
		if _, err := os.Stat(stopFile); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for remote replica stop file")
		}
		time.Sleep(100 * time.Millisecond)
	}

	listener.Stop()
	listenerRunning = false
	if _, err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	storeOpen = false

	reopened, err := storage.OpenWALStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	stable, _, head := reopened.Boundaries()
	expectedHead := uint64((phase174Runs + 1) * (phase174WarmupOperations + phase174APIOperations))
	if stable != head || head != expectedHead {
		t.Fatalf("remote replica frontier stable=%d head=%d want=%d", stable, head, expectedHead)
	}

	payloads := phase174Payloads(phase174APIOperations, finalSet, finalRun, 0x1741)
	samples := 0
	for _, operation := range phase174SampleOperations(len(payloads)) {
		got, err := reopened.Read(phase174OperationLBA(operation, 0))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payloads[operation]) {
			t.Fatalf("operation=%d data mismatch", operation)
		}
		samples++
	}
	result := phase174RemoteReplicaResult{
		Status: "ok", ListenAddress: listener.Addr(), StableLSN: stable,
		HeadLSN: head, ExpectedHeadLSN: expectedHead, CorrectnessSamples: samples,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resultFile, append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("phase174_remote_replica_result=%s\n", encoded)
}

func phase174RequiredPositiveInt(t *testing.T, name string) int {
	t.Helper()
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return value
}
