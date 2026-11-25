package transport

import (
	"bytes"
	"encoding/binary"
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

// epochTableMaxBytes bounds the record read.
const epochTableMaxBytes = 64 << 10

// EpochTableMaxRows bounds the table: every write keeps the newest rows
// only, so the record always fits the read bound (a row is at most 62
// bytes of JSON). An epoch whose row was dropped is unknown to SharedEnd,
// which reads as diverged: a rebuild, never a wrong count.
const EpochTableMaxRows = 512

// newestRows keeps the last EpochTableMaxRows rows.
func newestRows(rows []EpochRow) []EpochRow {
	if len(rows) > EpochTableMaxRows {
		return rows[len(rows)-EpochTableMaxRows:]
	}
	return rows
}

// EpochRow is one epoch's first LSN.
type EpochRow struct {
	Epoch    uint64 `json:"epoch"`
	FirstLSN uint64 `json:"first_lsn"`
}

type epochTableRecord struct {
	Schema   string     `json:"schema"`
	VolumeID string     `json:"volume_id"`
	Rows     []EpochRow `json:"rows"`
	// Invalid: a recovery (catch-up or rebuild) began applying history
	// whose epochs arrive only at its end. Until that end replaces the
	// table, the store's data has no known history: no last data row, no
	// note; it reconciles as diverged at every crash point.
	Invalid bool `json:"invalid,omitempty"`
}

// EpochTable is the durable table of one volume. The zero path keeps it in
// memory only (tests and non-durable stores).
type EpochTable struct {
	path, volumeID string

	mu      sync.Mutex
	rows    []EpochRow
	invalid bool
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
	t.rows, t.invalid = rec.Rows, rec.Invalid
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
	if t == nil {
		return nil // no table: nothing recorded (a store from before the table)
	}
	if epoch == 0 || firstLSN == 0 {
		return errors.New("epoch table: zero epoch or LSN")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.invalid {
		return errors.New("epoch table: invalid until the recovery in progress replaces it")
	}
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
	next := newestRows(append(append([]EpochRow(nil), t.rows...), EpochRow{Epoch: epoch, FirstLSN: firstLSN}))
	if err := t.persistLocked(next, false); err != nil {
		return err
	}
	t.rows = next
	return nil
}

// Invalidate durably marks the table invalid, before a recovery applies
// any history (a catch-up's lane start, a rebuild's first base block): the
// history's epochs arrive only at the recovery's end, and until Replace
// installs them the data must read as diverged, whatever crashes between.
func (t *EpochTable) Invalidate() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.invalid {
		return nil
	}
	if err := t.persistLocked(nil, true); err != nil {
		return err
	}
	t.rows, t.invalid = nil, true
	return nil
}

// Replace makes rows the table (after a rebuild from a primary's image).
func (t *EpochTable) Replace(rows []EpochRow) error {
	if err := checkEpochRows(rows); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	next := newestRows(append([]EpochRow(nil), rows...))
	if err := t.persistLocked(next, false); err != nil {
		return err
	}
	t.rows, t.invalid = next, false
	return nil
}

// TrimAbove drops the rows whose first LSN is above next, the store's next
// LSN after its recovery, durably. Such a row names entries the store does
// not hold (a tail lost before it was synced); kept, it would attribute the
// entries written later at those LSNs to the previous epoch. The next Note
// of that epoch appends it again at its real first LSN.
func (t *EpochTable) TrimAbove(next uint64) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	keep := RowsThrough(t.rows, next)
	if t.invalid || len(keep) == len(t.rows) {
		return nil
	}
	if err := t.persistLocked(keep, false); err != nil {
		return err
	}
	t.rows = keep
	return nil
}

func (t *EpochTable) persistLocked(rows []EpochRow, invalid bool) error {
	if t.path == "" {
		return nil
	}
	body, _ := json.Marshal(epochTableRecord{Schema: epochTableSchema, VolumeID: t.volumeID, Rows: rows, Invalid: invalid})
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

// SetEpochTable installs the volume's epoch table on the listener. Call
// before Serve.
func (r *ReplicaListener) SetEpochTable(t *EpochTable) { r.epochs = t }

// SetEpochTable installs the primary's epoch table on the executor, whose
// rebuild-done frame then carries its rows.
func (e *BlockExecutor) SetEpochTable(t *EpochTable) { e.epochs = t }

// Invalid reports whether a recovery left the table without a known history.
func (t *EpochTable) Invalid() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.invalid
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

// LastDataEpochAt is the epoch of the last row whose first LSN is at or
// below end (the durable end). A row is made durable BEFORE the first entry
// of its epoch, so a crash between the two leaves a row with no entry; it is
// not counted here. 0 = no such row.
func (t *EpochTable) LastDataEpochAt(end uint64) uint64 { return t.LastDataRowAt(end).Epoch }

// LastDataRowAt is the last row whose first LSN is at or below end, with that
// first LSN; the zero row = none. The barrier reply carries both: two tables
// can agree on an epoch number and still disagree on where it starts.
func (t *EpochTable) LastDataRowAt(end uint64) EpochRow {
	if t == nil {
		return EpochRow{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.invalid {
		return EpochRow{} // history unknown: data without a table reconciles as diverged
	}
	for i := len(t.rows) - 1; i >= 0; i-- {
		if t.rows[i].FirstLSN <= end {
			return t.rows[i]
		}
	}
	return EpochRow{}
}

// EncodeEpochRows is the wire form of rows: [4B count][count x (8B epoch,
// 8B first LSN)]. It follows the lineage in a rebuild-done frame.
func EncodeEpochRows(rows []EpochRow) []byte {
	buf := make([]byte, 4+16*len(rows))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(rows)))
	for i, r := range rows {
		binary.BigEndian.PutUint64(buf[4+16*i:], r.Epoch)
		binary.BigEndian.PutUint64(buf[12+16*i:], r.FirstLSN)
	}
	return buf
}

// DecodeEpochRows parses EncodeEpochRows strictly: the exact length and
// ordered rows.
func DecodeEpochRows(buf []byte) ([]EpochRow, error) {
	if len(buf) < 4 {
		return nil, errors.New("epoch rows: short")
	}
	n := binary.BigEndian.Uint32(buf[0:4])
	if n > EpochTableMaxRows {
		return nil, fmt.Errorf("epoch rows: %d rows, at most %d", n, EpochTableMaxRows)
	}
	if uint64(len(buf)) != 4+16*uint64(n) {
		return nil, fmt.Errorf("epoch rows: %d bytes for %d rows", len(buf), n)
	}
	rows := make([]EpochRow, n)
	for i := range rows {
		rows[i] = EpochRow{Epoch: binary.BigEndian.Uint64(buf[4+16*i:]), FirstLSN: binary.BigEndian.Uint64(buf[12+16*i:])}
	}
	if err := checkEpochRows(rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// RowsThrough is the rows whose first LSN is at or below end: the primary's
// history cut at a rebuild end.
func RowsThrough(rows []EpochRow, end uint64) []EpochRow {
	var out []EpochRow
	for _, r := range rows {
		if r.FirstLSN <= end {
			out = append(out, r)
		}
	}
	return out
}

// Rows is a copy of the table.
func (t *EpochTable) Rows() []EpochRow {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]EpochRow(nil), t.rows...)
}

// SharedEnd answers a replica that reports its last data row (epoch and that
// epoch's first LSN) and its end LSN, from this (the primary's) table: the
// highest LSN up to which the replica's entries are this table's entries.
// shared is false when the replica holds entries this table never had (a
// divergent tail, an unknown epoch, or the same epoch starting elsewhere):
// it must not count until it is rebuilt.
//   - the replica's epoch starts at another LSN here: diverged (an epoch
//     number alone is not a history);
//   - the replica's epoch is this table's last: in sync up to its end;
//   - else entries below the first LSN of the next epoch here are shared: an
//     end below it is a prefix, an end at or above it is divergent;
//   - no row here for the replica's epoch, or a replica with data but no
//     table (epoch 0): divergent. A replica with no data shares the empty prefix.
func (t *EpochTable) SharedEnd(replicaEpoch, replicaFirst, replicaEnd uint64) (end uint64, shared bool) {
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
		if r.FirstLSN != replicaFirst {
			if lo := min(r.FirstLSN, replicaFirst); lo > 1 {
				return lo - 1, false // the same epoch begins elsewhere: shared at most below either start
			}
			return 0, false
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
