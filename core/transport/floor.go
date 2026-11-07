package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

// The replica epoch floor record (fencing gap G1): the highest epoch this
// replica has taken a role at or acknowledged a mutation at, kept beside the
// volume's durable files so it survives a restart.
//
// Epoch 0 is a valid bootstrap floor (written when the volume is created),
// never an assignment: a listener that loads it stays unassigned and refuses
// every mutating frame until a successful role call.

const floorRecordSchema = "seaweed-block.replica-floor/v1"

// floorRecordMaxBytes bounds the record read; a real record is under 200 bytes.
const floorRecordMaxBytes = 4 << 10

type floorRecord struct {
	Schema   string `json:"schema"`
	VolumeID string `json:"volume_id"`
	Epoch    uint64 `json:"epoch"`
}

// ErrFloorRecordMissing: an existing volume has no floor record. It is never
// read as floor 0; only InitReplicaFloor, at volume creation, writes epoch 0.
var ErrFloorRecordMissing = errors.New("replica floor record missing for an existing volume")

// floorOps is the file-system seam of the floor record; tests replace it to
// inject a failure at each step. Production uses osFloorOps.
type floorOps struct {
	writeSynced func(path string, data []byte) error // create, write, fsync, close
	rename      func(from, to string) error
	link        func(from, to string) error
	syncDir     func(dir string) error
	remove      func(path string) error
}

var osFloorOps = floorOps{
	writeSynced: func(path string, data []byte) error {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	},
	rename: os.Rename,
	link:   os.Link,
	syncDir: func(dir string) error {
		if runtime.GOOS == "windows" {
			return nil // the OS has no directory fsync; production runs on Linux
		}
		d, err := os.Open(dir)
		if err != nil {
			return err
		}
		if err := d.Sync(); err != nil {
			_ = d.Close()
			return err
		}
		return d.Close()
	},
	remove: os.Remove,
}

// floorFS is the active seam (package variable, swapped only by tests).
var floorFS = osFloorOps

func encodeFloor(volumeID string, epoch uint64) []byte {
	body, _ := json.Marshal(floorRecord{Schema: floorRecordSchema, VolumeID: volumeID, Epoch: epoch})
	return append(body, '\n')
}

// decodeFloor strictly parses a floor record bound to volumeID: no unknown,
// duplicate or missing fields, no trailing data, the exact schema and volume.
func decodeFloor(raw []byte, volumeID string) (uint64, error) {
	if err := rejectDuplicateKeys(raw); err != nil {
		return 0, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var rec struct {
		Schema   *string `json:"schema"`
		VolumeID *string `json:"volume_id"`
		Epoch    *uint64 `json:"epoch"`
	}
	if err := dec.Decode(&rec); err != nil {
		return 0, fmt.Errorf("replica floor record malformed: %w", err)
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return 0, errors.New("replica floor record has trailing data")
	}
	if rec.Schema == nil || rec.VolumeID == nil || rec.Epoch == nil {
		return 0, errors.New("replica floor record incomplete")
	}
	if *rec.Schema != floorRecordSchema {
		return 0, fmt.Errorf("replica floor record schema %q", *rec.Schema)
	}
	if *rec.VolumeID != volumeID {
		return 0, fmt.Errorf("replica floor record is for volume %q, not %q", *rec.VolumeID, volumeID)
	}
	return *rec.Epoch, nil
}

// floorRecordKeys is the record's closed key set, matched exactly (encoding/json
// struct decoding alone would also accept case variants such as "EPOCH").
var floorRecordKeys = map[string]bool{"schema": true, "volume_id": true, "epoch": true}

// rejectDuplicateKeys checks the record's top-level keys BEFORE struct
// decoding: exactly the closed key set, case-sensitive, none repeated.
func rejectDuplicateKeys(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return errors.New("replica floor record is not a JSON object")
	}
	seen := map[string]bool{}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return fmt.Errorf("replica floor record malformed: %w", err)
		}
		key, _ := k.(string)
		if !floorRecordKeys[key] {
			return fmt.Errorf("replica floor record has unexpected key %q", key)
		}
		if seen[key] {
			return fmt.Errorf("replica floor record repeats %q", key)
		}
		seen[key] = true
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return fmt.Errorf("replica floor record malformed: %w", err)
		}
	}
	return nil
}

// loadFloor reads the floor record at path. A missing record is
// ErrFloorRecordMissing; any other read or parse problem is an error.
func loadFloor(path, volumeID string) (uint64, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("%w: %s", ErrFloorRecordMissing, path)
	}
	if err != nil {
		return 0, fmt.Errorf("replica floor record: %w", err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, floorRecordMaxBytes+1))
	if err != nil {
		return 0, fmt.Errorf("replica floor record: %w", err)
	}
	if len(raw) > floorRecordMaxBytes {
		return 0, errors.New("replica floor record over 4 KiB")
	}
	return decodeFloor(raw, volumeID)
}

// persistFloor durably replaces the record with epoch: temp file written and
// fsynced, renamed over the record, then the directory fsynced. Every step's
// error is returned; the caller treats any error as "not durable".
func persistFloor(path, volumeID string, epoch uint64) error {
	tmp := path + ".tmp"
	if err := floorFS.writeSynced(tmp, encodeFloor(volumeID, epoch)); err != nil {
		_ = floorFS.remove(tmp)
		return fmt.Errorf("replica floor record write: %w", err)
	}
	if err := floorFS.rename(tmp, path); err != nil {
		_ = floorFS.remove(tmp)
		return fmt.Errorf("replica floor record rename: %w", err)
	}
	if err := floorFS.syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("replica floor record directory sync: %w", err)
	}
	return nil
}

// InitReplicaFloor creates the bootstrap floor record (epoch 0) for a volume
// that is about to be created, BEFORE its volume file. If the record already
// exists (a crash between this call and the volume's creation) it is kept.
func InitReplicaFloor(path, volumeID string) error {
	tmp := path + ".init"
	if err := floorFS.writeSynced(tmp, encodeFloor(volumeID, 0)); err != nil {
		_ = floorFS.remove(tmp)
		return fmt.Errorf("replica floor init write: %w", err)
	}
	err := floorFS.link(tmp, path) // never replaces an existing record
	_ = floorFS.remove(tmp)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("replica floor init link: %w", err)
	}
	if err := floorFS.syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("replica floor init directory sync: %w", err)
	}
	if errors.Is(err, os.ErrExist) {
		_, lerr := loadFloor(path, volumeID) // the kept record must still be ours and valid
		return lerr
	}
	return nil
}
