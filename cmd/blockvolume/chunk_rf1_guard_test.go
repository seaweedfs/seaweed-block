package main

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A walstore-chunk volume is RF1 only: a chunk cut takes its LSN from the local
// WAL head, which replicas may not hold. A direct start must refuse replication
// (the master's workspace RPCs already do).
func TestChunkVolumeRefusesReplicationAckAtStart(t *testing.T) {
	base := []string{"--master", "127.0.0.1:1", "--server-id", "node-a", "--volume-id", "v1", "--replica-id", "r1", "--data-addr", "127.0.0.1:9100", "--ctrl-addr", "127.0.0.1:9101"}
	chunk := append(append([]string{}, base...), "--durable-root", t.TempDir(), "--durable-impl", "walstore-chunk", "--chunk-filer", "127.0.0.1:18888")
	for _, ack := range []string{"sync-quorum", "sync-all"} {
		_, err := parseFlags(append(append([]string{}, chunk...), "--replication-ack", ack))
		if err == nil || !strings.Contains(err.Error(), "single-replica") || !strings.Contains(err.Error(), "RF1 only") {
			t.Fatalf("walstore-chunk with --replication-ack=%s: %v", ack, err)
		}
	}
	if _, err := parseFlags(append(append([]string{}, chunk...), "--replication-ack", "best-effort")); err != nil {
		t.Fatalf("walstore-chunk best-effort refused: %v", err)
	}
	if _, err := parseFlags(append(append([]string{}, base...), "--durable-root", t.TempDir(), "--durable-impl", "walstore", "--replication-ack", "sync-quorum")); err != nil {
		t.Fatalf("walstore sync-quorum refused: %v", err)
	}
}

// The replication factor arrives with the master's assignment, so the guard is
// also on the role gate: every role with replica slots is refused and no socket
// is opened; a primary role without peers (RF1) is admitted.
func TestChunkVolumeIngressRefusesReplicaSlots(t *testing.T) {
	for _, impl := range []string{"walstore", "walstore-chunk"} {
		t.Run(impl, func(t *testing.T) {
			addr, _ := ingressPair(t)
			r := newReplicationIngress(flags{dataAddr: addr, volumeID: "v1", durableImpl: impl, recoveryMode: "legacy"}, storage.NewBlockStore(4, 4096))
			defer r.Stop()
			admitErr := r.AdmitAsReplica(1)
			if impl == "walstore" {
				if admitErr != nil || r.listener == nil {
					t.Fatalf("walstore replica role: %v", admitErr)
				}
				return
			}
			if admitErr == nil || !strings.Contains(admitErr.Error(), "refuses a replica role") || !strings.Contains(admitErr.Error(), "RF1 only") {
				t.Fatalf("replica role admitted: %v", admitErr)
			}
			if err := r.FenceAsPrimaryWithPeers(2, true); err == nil || !strings.Contains(err.Error(), "refuses a primary role with replica peers") {
				t.Fatalf("primary with peers admitted: %v", err)
			}
			if err := r.FenceAsPrimary(3); err == nil {
				t.Fatal("legacy primary fence (peers assumed) admitted")
			}
			if r.listener != nil {
				t.Fatal("a replica listener was created")
			}
			if c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond); err == nil {
				c.Close()
				t.Fatal("the replication address accepts connections")
			}
			if err := r.FenceAsPrimaryWithPeers(4, false); err != nil {
				t.Fatalf("RF1 primary refused: %v", err)
			}
		})
	}
}
