package volume

import (
	"context"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

type heartbeatSink struct {
	control.UnimplementedObservationServiceServer
	got chan struct{}
}

func (s *heartbeatSink) ReportHeartbeat(context.Context, *control.HeartbeatReport) (*control.HeartbeatAck, error) {
	select {
	case s.got <- struct{}{}:
	default:
	}
	return &control.HeartbeatAck{}, nil
}

// A volume process now waits for an unreachable master instead of exiting,
// so its one master channel lives through the whole outage. With gRPC's
// default reconnect back-off (1 s growing x1.6 to 120 s) the step is
// already about 17 s after a 45 s outage, and the first heartbeat after
// the master returns can lag by that much. The master link must reconnect
// within about 2 s of the master's return, however long the outage was.
func TestMasterLinkReconnectsWithinTwoSeconds(t *testing.T) {
	if testing.Short() {
		t.Skip("45 s outage")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close() // the master is down: connects are refused

	h, err := New(Config{MasterAddr: addr, ServerID: "s1", VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304",
		HeartbeatInterval: 200 * time.Millisecond, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() { _ = h.Close() })

	time.Sleep(45 * time.Second) // let the connect failures run up the back-off

	l, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("relisten %s: %v", addr, err)
	}
	srv := grpc.NewServer()
	sink := &heartbeatSink{got: make(chan struct{}, 1)}
	control.RegisterObservationServiceServer(srv, sink)
	back := time.Now()
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)

	select {
	case <-sink.got:
		lag := time.Since(back)
		t.Logf("first heartbeat %v after the master returned", lag.Round(10*time.Millisecond))
		if lag > 2400*time.Millisecond {
			t.Fatalf("master link reconnected %v after the master returned; want <= 2.4 s", lag.Round(10*time.Millisecond))
		}
	case <-time.After(60 * time.Second):
		t.Fatal("no heartbeat within 60 s of the master's return")
	}
}
