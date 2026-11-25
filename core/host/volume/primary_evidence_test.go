package volume

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// One row per guard of the target's adoption check.
func TestCheckPrimaryEvidence(t *testing.T) {
	peers := []*control.ReplicaDescriptor{{ReplicaId: "r2"}}
	rows := func(r ...transport.EpochRow) replication.EpochTableStatus {
		return replication.EpochTableStatus{Rows: r, Present: true}
	}
	local := storage.StoreID{1, 2, 3}
	promo := func(lsn, ep, first uint64) *control.AssignmentFact {
		return &control.AssignmentFact{Epoch: 3, Peers: peers, PrimaryKind: control.PrimaryKind_PRIMARY_KIND_PROMOTION, EvidenceDurableLsn: lsn, EvidenceEpoch: ep, EvidenceFirstLsn: first, EvidenceStoreId: local[:]}
	}
	onStore := func(f *control.AssignmentFact, id []byte) *control.AssignmentFact { f.EvidenceStoreId = id; return f }
	for _, c := range []struct {
		name    string
		fact    *control.AssignmentFact
		st      replication.EpochTableStatus
		end     uint64
		floor   uint64
		refused string // "" = adopted; else a word of the reason
	}{
		{"replay of an adopted epoch", &control.AssignmentFact{Epoch: 3, Peers: peers}, rows(transport.EpochRow{Epoch: 3, FirstLSN: 10}), 12, 3, ""},
		{"replay over an invalid table", &control.AssignmentFact{Epoch: 3, Peers: peers}, replication.EpochTableStatus{Rows: []transport.EpochRow{{Epoch: 3, FirstLSN: 10}}, Present: true, Invalid: true}, 12, 3, "invalid"},
		{"bootstrap on an empty node", &control.AssignmentFact{Epoch: 1, Peers: peers, PrimaryKind: control.PrimaryKind_PRIMARY_KIND_BOOTSTRAP}, rows(), 0, 0, ""},
		{"bootstrap on a node with data", &control.AssignmentFact{Epoch: 1, Peers: peers, PrimaryKind: control.PrimaryKind_PRIMARY_KIND_BOOTSTRAP}, rows(), 5, 0, "bootstrap"},
		{"promotion as probed", promo(8, 2, 5), rows(transport.EpochRow{Epoch: 1, FirstLSN: 1}, transport.EpochRow{Epoch: 2, FirstLSN: 5}), 8, 2, ""},
		{"promotion, end grown since the probe", promo(8, 2, 5), rows(transport.EpochRow{Epoch: 1, FirstLSN: 1}, transport.EpochRow{Epoch: 2, FirstLSN: 5}), 11, 2, ""},
		{"promotion, store lower than probed", promo(8, 2, 5), rows(transport.EpochRow{Epoch: 1, FirstLSN: 1}, transport.EpochRow{Epoch: 2, FirstLSN: 5}), 6, 2, "durable end"},
		{"promotion, another row at the probed LSN", promo(8, 2, 5), rows(transport.EpochRow{Epoch: 1, FirstLSN: 1}, transport.EpochRow{Epoch: 2, FirstLSN: 7}), 8, 2, "row"},
		{"promotion over an invalid table", promo(8, 2, 5), replication.EpochTableStatus{Present: true, Invalid: true}, 8, 2, "invalid"},
		{"promotion without the probed store id", onStore(promo(8, 2, 5), nil), rows(transport.EpochRow{Epoch: 1, FirstLSN: 1}, transport.EpochRow{Epoch: 2, FirstLSN: 5}), 8, 2, "store"},
		{"promotion probed on another store", onStore(promo(8, 2, 5), []byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}), rows(transport.EpochRow{Epoch: 1, FirstLSN: 1}, transport.EpochRow{Epoch: 2, FirstLSN: 5}), 8, 2, "store"},
		{"promotion replayed after adoption", promo(8, 2, 5), rows(transport.EpochRow{Epoch: 1, FirstLSN: 1}, transport.EpochRow{Epoch: 2, FirstLSN: 5}, transport.EpochRow{Epoch: 3, FirstLSN: 9}), 12, 3, ""},
		{"promotion replayed, another store", onStore(promo(8, 2, 5), []byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}), rows(transport.EpochRow{Epoch: 2, FirstLSN: 5}, transport.EpochRow{Epoch: 3, FirstLSN: 9}), 12, 3, "store"},
		{"promotion replayed, no store id", onStore(promo(8, 2, 5), nil), rows(transport.EpochRow{Epoch: 2, FirstLSN: 5}, transport.EpochRow{Epoch: 3, FirstLSN: 9}), 12, 3, "store"},
		{"promotion replayed, end below the evidence", promo(20, 3, 9), rows(transport.EpochRow{Epoch: 2, FirstLSN: 5}, transport.EpochRow{Epoch: 3, FirstLSN: 9}), 12, 3, "durable end"},
		{"promotion to a store replaced empty", promo(8, 2, 5), rows(), 0, 0, "durable end"},
		{"forced", &control.AssignmentFact{Epoch: 3, Peers: peers, PrimaryKind: control.PrimaryKind_PRIMARY_KIND_FORCED}, rows(transport.EpochRow{Epoch: 1, FirstLSN: 1}), 5, 2, ""},
		{"no kind to a node with data", &control.AssignmentFact{Epoch: 3, Peers: peers}, rows(transport.EpochRow{Epoch: 1, FirstLSN: 1}), 5, 2, "without a primary kind"},
		{"upgrade: a pre-table store replays its own line", &control.AssignmentFact{Epoch: 3, Peers: peers}, rows(), 5, 3, ""},
		{"upgrade: a pre-table store, another epoch", &control.AssignmentFact{Epoch: 4, Peers: peers}, rows(), 5, 3, "without a primary kind"},
		{"a single-replica line (no peers)", &control.AssignmentFact{Epoch: 3}, rows(), 5, 0, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := checkPrimaryEvidence(c.fact, c.st, c.end, c.floor, c.floor > 0, local)
			switch {
			case c.refused == "" && got != "":
				t.Fatalf("refused: %s", got)
			case c.refused != "" && !strings.Contains(got, c.refused):
				t.Fatalf("got %q; want a refusal naming %q", got, c.refused)
			}
		})
	}
}
