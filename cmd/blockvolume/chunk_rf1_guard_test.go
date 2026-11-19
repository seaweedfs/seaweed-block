package main

import (
	"strings"
	"testing"

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

// The replication factor arrives with the master's assignment. The host refuses
// roles with replica slots (core/host/volume RF1Only, from hostConfig); the
// ingress gate refuses them again and opens neither inbound lane (legacy and
// dual-lane ports). A primary role without peers (RF1) is admitted.
func TestChunkVolumeIngressRefusesReplicaSlots(t *testing.T) {
	for _, row := range []struct{ impl, mode string }{{"walstore", "legacy"}, {"walstore", "dual-lane"}, {"walstore-chunk", "legacy"}, {"walstore-chunk", "dual-lane"}} {
		impl, dualLane := row.impl, row.mode == "dual-lane"
		t.Run(row.impl+"/"+row.mode, func(t *testing.T) {
			addr, dual := ingressPair(t)
			f := flags{dataAddr: addr, volumeID: "v1", durableImpl: impl, recoveryMode: row.mode}
			if got, want := hostConfig(f, nil).RF1Only, map[string]string{"walstore": "", "walstore-chunk": chunkRF1Reason}[impl]; got != want {
				t.Fatalf("host config RF1Only = %q, want %q", got, want)
			}
			r := newReplicationIngress(f, storage.NewBlockStore(4, 4096))
			defer r.Stop()
			admitErr := r.AdmitAsReplica(1)
			if impl == "walstore" {
				if admitErr != nil || r.listener == nil || (r.dual != nil) != dualLane {
					t.Fatalf("walstore replica role (%s): %v", row.mode, admitErr)
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
			if r.listener != nil || r.dual != nil {
				t.Fatal("an inbound listener was created")
			}
			for _, a := range []string{addr, dual} {
				if err := ingressPortReleased(a); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.FenceAsPrimaryWithPeers(4, false); err != nil {
				t.Fatalf("RF1 primary refused: %v", err)
			}
			if r.listener != nil || r.dual != nil {
				t.Fatal("the RF1 primary role opened an inbound listener")
			}
		})
	}
}
