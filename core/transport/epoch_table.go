package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// The epoch table (leader-epoch reconciliation, as Kafka's KIP-101): one row
// per epoch at which this volume holds data, with the first LSN written at
// that epoch. An entry's identity is (epoch, LSN), never the LSN alone; the
// table lets a primary find the prefix a rejoining replica shares with it
// without a per-entry epoch. Kept beside the epoch floor record and across
// roles. No record (a store from before the table) = no rows.

const epochTableSchema = "seaweed-block.epoch-table/v1"

// epochTableMaxBytes bounds the record read; a row is under 60 bytes.
const epochTableMaxBytes = 64 << 10

// EpochRow is one epoch's first LSN.
type EpochRow struct {
	Epoch    uint64 `json:"epoch"`
	FirstLSN uint64 `json:"first_lsn"`
}

type epochTableRecord struct {
	Schema   string     `json:"schema"`
	VolumeID string     `json:"volume_id"`
	Rows     []EpochRow `json:"rows"`
}

// EpochTable is the durable table of one volume. The zero path keeps it in
// memory only (tests and non-durable stores).
type EpochTable struct {
	path, volumeID string

	mu   sync.Mutex
	rows []EpochRow
}

// LoadEpochTable reads the table at path; a missing record is an empty table.
func LoadEpochTable(path, volumeID string) (*EpochTable, error) {
	t := &EpochTable{path: path, volumeID: volumeID}
	if path == "" {
		return t, nil
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return t, nil
	}
	if err != nil {
		return nil, fmt.Errorf("epoch table: %w", err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, epochTableMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("epoch table: %w", err)
	}
	if len(raw) > epochTableMaxBytes {
		return nil, errors.New("epoch table record over 64 KiB")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var rec epochTableRecord
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("epoch table record malformed: %w", err)
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return nil, errors.New("epoch table record has trailing data")
	}
	if rec.Schema != epochTableSchema {
		return nil, fmt.Errorf("epoch table record schema %q", rec.Schema)
	}
	if rec.VolumeID != volumeID {
		return nil, fmt.Errorf("epoch table record is for volume %q, not %q", rec.VolumeID, volumeID)
	}
	if err := checkEpochRows(rec.Rows); err != nil {
		return nil, err
	}
	t.rows = rec.Rows
	return t, nil
}

// checkEpochRows: epochs strictly increasing, first LSNs non-decreasing and
// above zero.
func checkEpochRows(rows []EpochRow) error {
	for i, r := range rows {
		if r.Epoch == 0 || r.FirstLSN == 0 {
			return fmt.Errorf("epoch table row %d has a zero epoch or LSN", i)
		}
		if i > 0 && (r.Epoch <= rows[i-1].Epoch || r.FirstLSN < rows[i-1].FirstLSN) {
			return fmt.Errorf("epoch table row %d (epoch %d, lsn %d) is not after row %d", i, r.Epoch, r.FirstLSN, i-1)
		}
	}
	return nil
}

// Note records that the volume holds data at epoch from firstLSN on. A new
// epoch above the last row is appended and made durable before it returns;
// the epoch of the last row is a no-op; a lower epoch is refused (no write).
// Call it before the entry at firstLSN is acknowledged.
func (t *EpochTable) Note(epoch, firstLSN uint64) error {
	if epoch == 0 || firstLSN == 0 {
		return errors.New("epoch table: zero epoch or LSN")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if n := len(t.rows); n > 0 {
		last := t.rows[n-1]
		switch {
		case epoch == last.Epoch:
			return nil
		case epoch < last.Epoch:
			return fmt.Errorf("epoch table: epoch %d below the last row's %d", epoch, last.Epoch)
		case firstLSN < last.FirstLSN:
			return fmt.Errorf("epoch table: epoch %d starts at lsn %d, below epoch %d's %d", epoch, firstLSN, last.Epoch, last.FirstLSN)
		}
	}
	next := append(append([]EpochRow(nil), t.rows...), EpochRow{Epoch: epoch, FirstLSN: firstLSN})
	if err := t.persistLocked(next); err != nil {
		return err
	}
	t.rows = next
	return nil
}

// Replace makes rows the table (after a rebuild from a primary's image).
func (t *EpochTable) Replace(rows []EpochRow) error {
	if err := checkEpochRows(rows); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	next := append([]EpochRow(nil), rows...)
	if err := t.persistLocked(next); err != nil {
		return err
	}
	t.rows = next
	return nil
}

func (t *EpochTable) persistLocked(rows []EpochRow) error {
	if t.path == "" {
		return nil
	}
	body, _ := json.Marshal(epochTableRecord{Schema: epochTableSchema, VolumeID: t.volumeID, Rows: rows})
	tmp := t.path + ".tmp"
	if err := floorFS.writeSynced(tmp, append(body, '\n')); err != nil {
		_ = floorFS.remove(tmp)
		return fmt.Errorf("epoch table write: %w", err)
	}
	if err := floorFS.rename(tmp, t.path); err != nil {
		_ = floorFS.remove(tmp)
		return fmt.Errorf("epoch table rename: %w", err)
	}
	if err := floorFS.syncDir(filepath.Dir(t.path)); err != nil {
		return fmt.Errorf("epoch table directory sync: %w", err)
	}
	return nil
}

// LastEpoch is the epoch of the last row; 0 = no rows.
func (t *EpochTable) LastEpoch() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.rows) == 0 {
		return 0
	}
	return t.rows[len(t.rows)-1].Epoch
}

// Rows is a copy of the table.
func (t *EpochTable) Rows() []EpochRow {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]EpochRow(nil), t.rows...)
}

// SharedEnd answers a replica that reports its last data epoch and end LSN,
// from this (the primary's) table: the highest LSN up to which the replica's
// entries are this table's entries. shared is false when the replica holds
// entries this table never had (a divergent tail or an unknown epoch): it
// must not count until it is rebuilt.
//   - the replica's epoch is this table's last: in sync up to its end;
//   - else entries below the first LSN of the next epoch here are shared: an
//     end below it is a prefix, an end at or above it is divergent;
//   - no row here for the replica's epoch, or a replica with data but no
//     table (epoch 0): divergent. A replica with no data shares the empty prefix.
func (t *EpochTable) SharedEnd(replicaEpoch, replicaEnd uint64) (end uint64, shared bool) {
	if replicaEnd == 0 {
		return 0, true
	}
	if replicaEpoch == 0 {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, r := range t.rows {
		if r.Epoch != replicaEpoch {
			continue
		}
		if i == len(t.rows)-1 {
			return replicaEnd, true
		}
		next := t.rows[i+1].FirstLSN
		if replicaEnd < next {
			return replicaEnd, true
		}
		return next - 1, false
	}
	return 0, false // no row for the replica's epoch: data this table never had
}
