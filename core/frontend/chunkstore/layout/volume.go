package layout

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// VolumeName is the entry, under the volume root, that holds the volume's
// identity: written once, never changed.
const VolumeName = "VOLUME"

// HeadName is the entry, under the volume root, that holds the volume's
// HEAD; it is never removed (a deleted volume keeps its deleted HEAD).
const HeadName = "HEAD"

// VolumeID is a volume's identity: random, chosen when the volume is
// created. A volume's name is a path and can be created again after a
// delete; its id cannot, so a volume created again satisfies no reference
// to the old one.
type VolumeID [16]byte

// NewVolumeID returns a random volume id.
func NewVolumeID() (VolumeID, error) {
	var id VolumeID
	if _, err := rand.Read(id[:]); err != nil {
		return VolumeID{}, fmt.Errorf("layout: volume id: %w", err)
	}
	return id, nil
}

func (id VolumeID) String() string { return hex.EncodeToString(id[:]) }

// IsZero reports whether id is unset; the zero id names no volume.
func (id VolumeID) IsZero() bool { return id == VolumeID{} }

// ParseVolumeID parses 32 lower-case hex digits; the zero id is refused.
func ParseVolumeID(s string) (VolumeID, error) {
	var id VolumeID
	if len(s) != 2*len(id) || s != string(bytes.ToLower([]byte(s))) {
		return VolumeID{}, fmt.Errorf("%w: volume id %q is not 32 lower-case hex digits", ErrMalformed, s)
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return VolumeID{}, fmt.Errorf("%w: volume id %q: %v", ErrMalformed, s, err)
	}
	if id.IsZero() {
		return VolumeID{}, fmt.Errorf("%w: zero volume id", ErrMalformed)
	}
	return id, nil
}

// VolumeDir is the directory of a volume under the store's base:
// <name>-<first 8 hex of the volume id>. It is created exclusively and
// never reused, so a volume created again under a name gets another
// directory; it is resolved only from the master's volume record.
func VolumeDir(name string, id VolumeID) string {
	return name + "-" + id.String()[:8]
}

// VolumeFormat is the layout format a VOLUME record declares.
const VolumeFormat = 1

// Volume is the VOLUME record: the volume's id, its name when created, and
// the layout format.
type Volume struct {
	ID     VolumeID
	Name   string
	Format int
}

type volumeDoc struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Format int    `json:"format"`
}

// Encode returns the record's canonical bytes.
func (v Volume) Encode() ([]byte, error) {
	if v.ID.IsZero() {
		return nil, fmt.Errorf("%w: VOLUME without an id", ErrMalformed)
	}
	if err := checkOwner(v.Name); err != nil {
		return nil, err
	}
	if err := validName("volume name", v.Name); err != nil {
		return nil, err
	}
	if v.Format != VolumeFormat {
		return nil, fmt.Errorf("%w: VOLUME format %d, want %d", ErrMalformed, v.Format, VolumeFormat)
	}
	return json.Marshal(volumeDoc{ID: v.ID.String(), Name: v.Name, Format: v.Format})
}

// DecodeVolume parses canonical VOLUME bytes.
func DecodeVolume(b []byte) (Volume, error) {
	var d volumeDoc
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return Volume{}, fmt.Errorf("%w: VOLUME: %v", ErrMalformed, err)
	}
	id, err := ParseVolumeID(d.ID)
	if err != nil {
		return Volume{}, err
	}
	v := Volume{ID: id, Name: d.Name, Format: d.Format}
	again, err := v.Encode()
	if err != nil {
		return Volume{}, err
	}
	if !bytes.Equal(again, b) {
		return Volume{}, fmt.Errorf("%w: VOLUME bytes are not canonical", ErrMalformed)
	}
	return v, nil
}
