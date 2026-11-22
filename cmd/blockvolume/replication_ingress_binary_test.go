//go:build subprocess && !windows

package main_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/recovery"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
	"google.golang.org/grpc"
)

// Controlled master stream exercises the real binary's normal assignment path.
// It does not model the master's placement or epoch-minting policy.
type ingressAssignments struct {
	control.UnimplementedAssignmentServiceServer
	control.UnimplementedObservationServiceServer
	facts chan *control.AssignmentFact
}

func (m *ingressAssignments) ReportHeartbeat(context.Context, *control.HeartbeatReport) (*control.HeartbeatAck, error) {
	return &control.HeartbeatAck{}, nil
}
func (m *ingressAssignments) SubscribeAssignments(_ *control.SubscribeRequest, s control.AssignmentService_SubscribeAssignmentsServer) error {
	for {
		select {
		case <-s.Context().Done():
			return s.Context().Err()
		case f := <-m.facts:
			if err := s.Send(f); err != nil {
				return err
			}
		}
	}
}
func ingressPortPair(t *testing.T) (string, string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		a, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		port := a.Addr().(*net.TCPAddr).Port
		b, e := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port + 1})
		if e == nil {
			x, y := a.Addr().String(), b.Addr().String()
			a.Close()
			b.Close()
			return x, y
		}
		a.Close()
	}
	t.Fatal("no adjacent free ports")
	return "", ""
}
func ingressWait(t *testing.T, what string, fn func() bool) {
	t.Helper()
	end := time.Now().Add(10 * time.Second)
	for !fn() {
		if time.Now().After(end) {
			t.Fatal(what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
func ingressRefused(t *testing.T, addr string) {
	t.Helper()
	c, e := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if c != nil {
		_ = c.SetDeadline(time.Now().Add(300 * time.Millisecond))
		_ = transport.WriteMsg(c, transport.MsgProbeReq, transport.EncodeProbeReq(transport.ProbeRequest{Lineage: transport.RecoveryLineage{Epoch: 999, SessionID: 1, EndpointVersion: 1, FrontierHint: 1}}))
		c.Close()
	}
	if !errors.Is(e, syscall.ECONNREFUSED) {
		t.Fatalf("replication connection to %s: want connection refused, got %v", addr, e)
	}
}
func TestBinaryReplicationIngressFollowsAssignedSlots(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "blockvolume")
	cmd := exec.Command("go", "build", "-race", "-p", "4", "-o", exe, ".")
	if b, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("build: %v %s", e, b)
	}
	for _, mode := range []string{"legacy", "dual-lane"} {
		t.Run(mode, func(t *testing.T) {
			m := &ingressAssignments{facts: make(chan *control.AssignmentFact, 4)}
			ln, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			server := grpc.NewServer()
			control.RegisterAssignmentServiceServer(server, m)
			control.RegisterObservationServiceServer(server, m)
			go server.Serve(ln)
			t.Cleanup(server.Stop)
			data, dual := ingressPortPair(t)
			status := pickAddr(t)
			ctrl := pickAddr(t)
			root := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "volume.log")
			log, e := os.Create(logPath)
			if e != nil {
				t.Fatal(e)
			}
			child := exec.Command(exe, "--master", ln.Addr().String(), "--server-id", "s1", "--volume-id", "v1", "--replica-id", "r1", "--data-addr", data, "--ctrl-addr", ctrl, "--status-addr", status, "--t1-readiness", "--durable-root", root, "--volume-lock-dir-test-only", t.TempDir(), "--durable-impl", "walstore", "--durable-blocks", "64", "--heartbeat-interval", "50ms", "--recovery-mode", mode)
			child.Stdout, child.Stderr = log, log
			if e = child.Start(); e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			go func() { done <- child.Wait() }()
			exited := false
			t.Cleanup(func() {
				if !exited {
					child.Process.Signal(syscall.SIGTERM)
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						child.Process.Kill()
						select {
						case <-done:
						case <-time.After(3 * time.Second):
							t.Error("child reap timeout")
						}
					}
				}
				log.Close()
				if t.Failed() {
					b, _ := os.ReadFile(logPath)
					t.Logf("binary log: %s", b)
				}
			})
			client := &http.Client{Timeout: 300 * time.Millisecond}
			epoch := func(want float64) bool {
				resp, e := client.Get("http://" + status + "/status?volume=v1")
				if e != nil {
					return false
				}
				defer resp.Body.Close()
				var p map[string]any
				if json.NewDecoder(resp.Body).Decode(&p) != nil {
					return false
				}
				if want == 0 {
					return true
				}
				return p["Epoch"] == want
			}
			ingressWait(t, "binary status not ready", func() bool { return epoch(0) })
			// No assignment: do not open an unauthorised replication socket.
			ingressRefused(t, data)
			ingressRefused(t, dual)
			fact := func(e uint64, peers []*control.ReplicaDescriptor) *control.AssignmentFact {
				return &control.AssignmentFact{ServerId: "s1", VolumeId: "v1", ReplicaId: "r1", Epoch: e, EndpointVersion: e, DataAddr: data, CtrlAddr: ctrl, PeerSetGeneration: e, Peers: peers}
			}
			m.facts <- fact(1, nil)
			ingressWait(t, "RF1 assignment not applied", func() bool { return epoch(1) })
			// An attempted higher-epoch frame cannot even establish a connection.
			ingressRefused(t, data)
			ingressRefused(t, dual)
			// RF can grow only through the authoritative assignment stream.
			peer := &control.ReplicaDescriptor{ReplicaId: "r2", Epoch: 2, EndpointVersion: 2, DataAddr: pickAddr(t), CtrlAddr: pickAddr(t)}
			m.facts <- fact(2, []*control.ReplicaDescriptor{peer})
			ingressWait(t, "RF2 assignment not applied", func() bool { return epoch(2) })
			for _, addr := range []string{data, dual} {
				if addr == dual && mode != "dual-lane" {
					ingressRefused(t, addr)
					continue
				}
				c, e := net.DialTimeout("tcp", addr, time.Second)
				if e != nil {
					t.Fatalf("RF2 listener %s: %v", addr, e)
				}
				c.Close()
			}
			// A withdrawn role must close existing idle connections as well as
			// the listening socket, without waiting for the peer to disconnect.
			held, e := net.DialTimeout("tcp", data, time.Second)
			if e != nil {
				t.Fatal(e)
			}
			defer held.Close()
			m.facts <- fact(3, nil)
			ingressWait(t, "RF1 shrink not applied", func() bool { return epoch(3) })
			_ = held.SetReadDeadline(time.Now().Add(time.Second))
			var one [1]byte
			if _, err := held.Read(one[:]); err == nil {
				t.Fatal("withdrawn connection still usable")
			} else if n, ok := err.(net.Error); ok && n.Timeout() {
				t.Fatal("withdrawn connection not closed")
			}
			ingressRefused(t, data)
			ingressRefused(t, dual)
			// A supporting slot also opens both configured lanes. Removal from
			// the authoritative set closes them, even without a primary role.
			supporting := fact(4, []*control.ReplicaDescriptor{{ReplicaId: "r1", Epoch: 4, EndpointVersion: 4, DataAddr: data, CtrlAddr: ctrl}})
			supporting.ReplicaId = "r2"
			m.facts <- supporting
			ingressWait(t, "supporting assignment not applied", func() bool { return epoch(4) })
			for _, addr := range []string{data, dual} {
				if addr == dual && mode != "dual-lane" {
					continue
				}
				c, e := net.DialTimeout("tcp", addr, time.Second)
				if e != nil {
					t.Fatalf("supporting listener %s: %v", addr, e)
				}
				c.Close()
			}
			want := ingressProtocolPositive(t, data, dual, mode)
			excluded := fact(5, nil)
			excluded.ReplicaId = "r2"
			m.facts <- excluded
			ingressWait(t, "excluded slot still listens", func() bool {
				c, e := net.DialTimeout("tcp", data, 200*time.Millisecond)
				if c != nil {
					c.Close()
				}
				return errors.Is(e, syscall.ECONNREFUSED)
			})
			ingressRefused(t, dual)
			// Reopen through the public recovery API only after the binary has
			// fully stopped. Exact bytes must survive the wire barrier and close.
			_ = child.Process.Signal(syscall.SIGTERM)
			select {
			case err := <-done:
				exited = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("binary close did not settle")
			}
			store, err := storage.OpenWALStore(filepath.Join(root, "v1.bin"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, err = store.Recover(); err != nil {
				t.Fatal(err)
			}
			for lba, payload := range want {
				got, err := store.Read(lba)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatalf("recovered LBA%d bytes differ: %v", lba, err)
				}
			}
			t.Logf("%s supporting protocol/barrier and exact recovered bytes PASS", mode)
		})
	}
}

func ingressProtocolPositive(t *testing.T, data, dual, mode string) map[uint32][]byte {
	t.Helper()
	a := bytes.Repeat([]byte{0xA7}, 4096)
	want := map[uint32][]byte{0: a, 1: make([]byte, 4096), 2: make([]byte, 4096)}
	c, err := net.DialTimeout("tcp", data, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	lineage := transport.RecoveryLineage{Epoch: 4, SessionID: 1, EndpointVersion: 4, FrontierHint: 1}
	if err = transport.WriteMsg(c, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: lineage, LBA: 0, LSN: 1, Data: a})); err != nil {
		t.Fatal(err)
	}
	if err = transport.WriteMsg(c, transport.MsgBarrierReq, transport.EncodeLineage(lineage)); err != nil {
		t.Fatal(err)
	}
	kind, payload, err := transport.ReadMsg(c)
	if err != nil || kind != transport.MsgBarrierResp {
		t.Fatalf("legacy barrier kind=%d err=%v", kind, err)
	}
	ack, err := transport.DecodeBarrierResp(payload)
	if err != nil || !ack.Lineage.Equivalent(lineage) || ack.AchievedLSN != 1 {
		t.Fatalf("legacy barrier %+v %v", ack, err)
	}
	if mode != "dual-lane" {
		return want
	}
	d, err := net.DialTimeout("tcp", dual, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	_ = d.SetDeadline(time.Now().Add(5 * time.Second))
	start := make([]byte, 36)
	binary.BigEndian.PutUint64(start, 2)
	binary.BigEndian.PutUint64(start[8:], 1)
	binary.BigEndian.PutUint64(start[16:], 2)
	binary.BigEndian.PutUint32(start[24:], 64)
	binary.BigEndian.PutUint64(start[28:], 4)
	ingressDualWrite(t, d, 1, start)
	want[1] = bytes.Repeat([]byte{0xB8}, 4096)
	want[2] = bytes.Repeat([]byte{0xC9}, 4096)
	for lba := uint32(0); lba < 64; lba++ {
		block := make([]byte, 4096)
		if lba == 0 {
			copy(block, a)
		}
		if lba == 1 {
			copy(block, want[1])
		}
		p := make([]byte, 4+4096)
		binary.BigEndian.PutUint32(p, lba)
		copy(p[4:], block)
		ingressDualWrite(t, d, 2, p)
	}
	ingressDualWrite(t, d, 3, nil)
	if err = recovery.WriteWALEntryFrame(d, recovery.WALKindSessionLive, 2, 2, want[2]); err != nil {
		t.Fatal(err)
	}
	ingressDualWrite(t, d, 5, nil)
	for i := 0; i < 4; i++ {
		var hdr [5]byte
		if _, err = io.ReadFull(d, hdr[:]); err != nil {
			t.Fatal(err)
		}
		n := binary.BigEndian.Uint32(hdr[1:])
		if n > 1024 {
			t.Fatal("oversize recovery response")
		}
		p := make([]byte, n)
		if _, err = io.ReadFull(d, p); err != nil {
			t.Fatal(err)
		}
		if hdr[0] == 7 {
			if len(p) != 20 || binary.BigEndian.Uint64(p) != 2 {
				t.Fatal("invalid base batch ack")
			}
			continue
		}
		if hdr[0] != 6 || len(p) != 8 || binary.BigEndian.Uint64(p) != 2 {
			t.Fatalf("invalid recovery completion %d %x", hdr[0], p)
		}
		return want
	}
	t.Fatal("no recovery completion")
	return nil
}
func ingressDualWrite(t *testing.T, c net.Conn, kind byte, p []byte) {
	t.Helper()
	b := make([]byte, 5+len(p))
	b[0] = kind
	binary.BigEndian.PutUint32(b[1:], uint32(len(p)))
	copy(b[5:], p)
	if _, err := c.Write(b); err != nil {
		t.Fatal(err)
	}
}
