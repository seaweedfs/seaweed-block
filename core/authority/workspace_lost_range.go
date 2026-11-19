package authority

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WorkspaceLostRange records what a forced move abandoned: the old holder's
// writes after LostAfterLSN (the HEAD frontier the move keeps) are lost, and
// none of them was acknowledged later than LostUntilNS (master time: the
// forced barrier, the lease horizon or restart deadline plus skew). The
// lease carries no write frontier, so the upper bound is a time.
type WorkspaceLostRange struct {
	RequestID    string
	NewEpoch     uint64
	OldReplicaID string
	OldEpoch     uint64
	LostAfterLSN uint64
	LostUntilNS  int64
}

// WorkspaceLostRangeStore is an optional AuthorityStore capability. The
// ranges live beside the authority record, never in it, so the record body
// and its checksum stay readable by binaries that predate them (those load
// only *.v3auth.json). A store without it records no lost range.
type WorkspaceLostRangeStore interface {
	PutWorkspaceLostRanges(volumeID string, ranges []WorkspaceLostRange) error
	LoadWorkspaceLostRanges() (map[string][]WorkspaceLostRange, []error, error)
}

// lostRangeFileExtension does not end in storeFileExtension: authority
// loaders, old and new, never read these files.
const lostRangeFileExtension = ".lostrange.json"

type lostRangeEnvelope struct {
	Version  int                  `json:"v"`
	VolumeID string               `json:"volume"`
	Ranges   []WorkspaceLostRange `json:"ranges"`
	Checksum string               `json:"checksum"`
}

func lostRangeChecksum(volumeID string, ranges []WorkspaceLostRange) string {
	body, err := json.Marshal(struct {
		VolumeID string
		Ranges   []WorkspaceLostRange
	}{volumeID, ranges})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// PutWorkspaceLostRanges replaces volumeID's lost ranges atomically (temp
// file, fsync, rename, directory fsync).
func (s *FileAuthorityStore) PutWorkspaceLostRanges(volumeID string, ranges []WorkspaceLostRange) error {
	if volumeID == "" {
		return fmt.Errorf("authority_store: lost range with empty VolumeID")
	}
	payload, err := json.Marshal(lostRangeEnvelope{Version: 1, VolumeID: volumeID, Ranges: ranges, Checksum: lostRangeChecksum(volumeID, ranges)})
	if err != nil {
		return fmt.Errorf("authority_store: encode lost range: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("authority_store: lost range Put on closed store")
	}
	final := filepath.Join(s.dir, encodeVolumeIDForFilename(volumeID)+lostRangeFileExtension)
	temp := final + tempSuffix
	f, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("authority_store: open lost range temp: %w", err)
	}
	_, werr := f.Write(payload)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("authority_store: write lost range temp: %w", werr)
	}
	if err := renameWithRetry(temp, final); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("authority_store: rename lost range: %w", err)
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("authority_store: lost range directory open: %w", err)
	}
	serr := d.Sync()
	if cerr := d.Close(); serr == nil {
		serr = cerr
	}
	if serr != nil {
		return fmt.Errorf("authority_store: lost range directory fsync: %w", serr)
	}
	return nil
}

// LoadWorkspaceLostRanges returns every volume's lost ranges. A file that
// fails its checks is skipped with an error (these are records of loss, not
// authority); only an unreadable directory fails the load.
func (s *FileAuthorityStore) LoadWorkspaceLostRanges() (map[string][]WorkspaceLostRange, []error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, nil, fmt.Errorf("authority_store: readdir: %w", err)
	}
	out := map[string][]WorkspaceLostRange{}
	var skips []error
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, lostRangeFileExtension) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.dir, name))
		if err != nil {
			skips = append(skips, fmt.Errorf("%s: read: %w", name, err))
			continue
		}
		var env lostRangeEnvelope
		switch {
		case json.Unmarshal(raw, &env) != nil:
			err = errors.New("decode")
		case env.Version != 1:
			err = fmt.Errorf("version %d unsupported", env.Version)
		case env.Checksum != lostRangeChecksum(env.VolumeID, env.Ranges):
			err = errors.New("checksum mismatch")
		case encodeVolumeIDForFilename(env.VolumeID)+lostRangeFileExtension != name:
			err = fmt.Errorf("filename does not match VolumeID %q", env.VolumeID)
		}
		if err != nil {
			skips = append(skips, fmt.Errorf("%s: %w: lost range %v", name, ErrCorruptRecord, err))
			continue
		}
		out[env.VolumeID] = env.Ranges
	}
	return out, skips, nil
}

// recordLostRangeLocked durably records r for volume before the move record
// that issues it is written; a store without the capability records nothing.
// An entry whose move never issues is an orphan, ignored on read. Called
// under p.mu.
func (p *Publisher) recordLostRangeLocked(volume string, r WorkspaceLostRange) error {
	ls, ok := p.store.(WorkspaceLostRangeStore)
	if !ok {
		return nil
	}
	next := make([]WorkspaceLostRange, 0, len(p.lostRanges[volume])+1)
	for _, e := range p.lostRanges[volume] {
		if e.RequestID != r.RequestID || e.NewEpoch != r.NewEpoch {
			next = append(next, e)
		}
	}
	next = append(next, r)
	if err := ls.PutWorkspaceLostRanges(volume, next); err != nil {
		return fmt.Errorf("workspace forced move: record lost range: %w", err)
	}
	p.lostRanges[volume] = next
	return nil
}

// WorkspaceLostRanges returns the lost ranges of volume's issued forced
// moves (an entry with no matching issued move is ignored).
func (p *Publisher) WorkspaceLostRanges(volume string) []WorkspaceLostRange {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.workspace[volume]
	if s == nil {
		return nil
	}
	var out []WorkspaceLostRange
	for _, e := range p.lostRanges[volume] {
		for _, m := range s.Moves {
			if m.Issued && m.Kind == "forced" && m.RequestID == e.RequestID && m.NewEpoch == e.NewEpoch {
				out = append(out, e)
				break
			}
		}
	}
	return out
}
