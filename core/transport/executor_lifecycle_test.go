package transport

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestBlockExecutorStopWaitsForSessionsAndResidentWalShippers(t *testing.T) {
	primary := storage.NewBlockStore(8, 4096)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	exec := NewBlockExecutor(primary, listener.Addr().String())
	shipper := exec.WalShipperFor("r1")
	if err := exec.StartCatchUp("r1", 1, 1, 1, 1, 0); err != nil {
		t.Fatal(err)
	}

	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
		defer serverConn.Close()
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for catch-up connection")
	}

	deadline := time.Now().Add(time.Second)
	for {
		exec.mu.Lock()
		session := exec.sessions[1]
		attached := session != nil && session.conn != nil
		exec.mu.Unlock()
		if attached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("catch-up connection was not attached to the session")
		}
		time.Sleep(time.Millisecond)
	}

	stopped := make(chan struct{})
	go func() {
		exec.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("executor Stop did not wait for and terminate background work")
	}

	select {
	case <-shipper.timerDone:
	default:
		t.Fatal("resident WalShipper timer still running after executor Stop")
	}
	exec.Stop()

	err = exec.StartCatchUp("r1", 2, 1, 1, 1, 0)
	if !errors.Is(err, ErrExecutorStopped) {
		t.Fatalf("StartCatchUp after Stop error=%v, want ErrExecutorStopped", err)
	}
	err = exec.RegisterLiveShipSession(RecoveryLineage{
		SessionID:       3,
		Epoch:           1,
		EndpointVersion: 1,
	})
	if !errors.Is(err, ErrExecutorStopped) {
		t.Fatalf("RegisterLiveShipSession after Stop error=%v, want ErrExecutorStopped", err)
	}
}

func TestBlockExecutorStopTerminatesFenceWithoutLateCallback(t *testing.T) {
	primary := storage.NewBlockStore(8, 4096)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	exec := NewBlockExecutor(primary, listener.Addr().String())
	callback := make(chan struct{}, 1)
	exec.SetOnFenceComplete(func(adapter.FenceResult) {
		callback <- struct{}{}
	})
	if err := exec.Fence("r1", 1, 1, 1); err != nil {
		t.Fatal(err)
	}

	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
		defer serverConn.Close()
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for fence connection")
	}

	deadline := time.Now().Add(time.Second)
	for {
		exec.mu.Lock()
		tracked := len(exec.transientConns) == 1
		exec.mu.Unlock()
		if tracked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fence connection was not tracked")
		}
		time.Sleep(time.Millisecond)
	}

	stopped := make(chan struct{})
	go func() {
		exec.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("executor Stop did not terminate the active fence")
	}
	select {
	case <-callback:
		t.Fatal("fence callback fired after executor Stop began")
	default:
	}
}
