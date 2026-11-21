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
//
// HeadTimeNS is the kept HEAD entry's filer mtime, read with that HEAD at the
// move; 0 means unknown (the filer gave none, or the record predates the
// field). It is stored apart from the checksummed v1 range body (see
// lostRangeEnvelope) so the encoding of the ranges themselves is unchanged.
type WorkspaceLostRange struct {
	RequestID    string
	NewEpoch     uint64
	OldReplicaID string
	OldEpoch     uint64
	LostAfterLSN uint64
	LostUntilNS  int64
	HeadTimeNS   int64 `json:"-"`
}

// lostRangeFileExtension does not end in storeFileExtension: authority
// loaders, old and new, never read these files.
const lostRangeFileExtension = ".lostrange.json"

// lostRangeEnvelope: Checksum covers VolumeID and Ranges exactly as before
// HEAD times existed, so a binary that predates them still verifies and
// loads the file (it ignores the two fields after it). HeadTimes carries the
// non-zero HEAD times; HeadTimesChecksum covers VolumeID, Ranges and
// HeadTimes together, and a file with HeadTimes but without a matching
// HeadTimesChecksum fails its checks.
type lostRangeEnvelope struct {
	Version           int                  `json:"v"`
	VolumeID          string               `json:"volume"`
	Ranges            []WorkspaceLostRange `json:"ranges"`
	Checksum          string               `json:"checksum"`
	HeadTimes         []lostRangeHeadTime  `json:"head_times,omitempty"`
	HeadTimesChecksum string               `json:"head_times_checksum,omitempty"`
}

type lostRangeHeadTime struct {
	RequestID  string
	NewEpoch   uint64
	HeadTimeNS int64
}

func lostRangeHeadTimesChecksum(volumeID string, ranges []WorkspaceLostRange, times []lostRangeHeadTime) string {
	body, err := json.Marshal(struct {
		VolumeID  string
		Ranges    []WorkspaceLostRange
		HeadTimes []lostRangeHeadTime
	}{volumeID, ranges, times})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func encodeLostRangeEnvelope(volumeID string, ranges []WorkspaceLostRange) lostRangeEnvelope {
	env := lostRangeEnvelope{Version: 1, VolumeID: volumeID, Ranges: ranges, Checksum: lostRangeChecksum(volumeID, ranges)}
	for _, r := range ranges {
		if r.HeadTimeNS != 0 {
			env.HeadTimes = append(env.HeadTimes, lostRangeHeadTime{RequestID: r.RequestID, NewEpoch: r.NewEpoch, HeadTimeNS: r.HeadTimeNS})
		}
	}
	if len(env.HeadTimes) > 0 {
		env.HeadTimesChecksum = lostRangeHeadTimesChecksum(volumeID, ranges, env.HeadTimes)
	}
	return env
}

// decodeLostRangeHeadTimes checks env's HEAD times and copies them onto its
// ranges.
func decodeLostRangeHeadTimes(env *lostRangeEnvelope) error {
	if len(env.HeadTimes) == 0 && env.HeadTimesChecksum == "" {
		return nil
	}
	if env.HeadTimesChecksum != lostRangeHeadTimesChecksum(env.VolumeID, env.Ranges, env.HeadTimes) {
		return errors.New("head times checksum mismatch")
	}
	for _, t := range env.HeadTimes {
		found := false
		for i := range env.Ranges {
			if env.Ranges[i].RequestID == t.RequestID && env.Ranges[i].NewEpoch == t.NewEpoch {
				env.Ranges[i].HeadTimeNS = t.HeadTimeNS
				found = true
			}
		}
		if !found {
			return fmt.Errorf("head time for unknown range %s/%d", t.RequestID, t.NewEpoch)
		}
	}
	return nil
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
	payload, err := json.Marshal(encodeLostRangeEnvelope(volumeID, ranges))
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
	if werr == nil {
		werr = testLostRangeFault("before-rename")
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
	if serr == nil {
		serr = testLostRangeFault("directory-sync")
	}
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
		default:
			err = decodeLostRangeHeadTimes(&env)
		}
		if err != nil {
			skips = append(skips, fmt.Errorf("%s: %w: lost range %v", name, ErrCorruptRecord, err))
			continue
		}
		out[env.VolumeID] = env.Ranges
	}
	return out, skips, nil
}

// testLostRangeFaultHook, test-only, fails PutWorkspaceLostRanges at a named
// step ("before-rename", "directory-sync") when it returns an error.
var testLostRangeFaultHook func(step string) error

func testLostRangeFault(step string) error {
	if h := testLostRangeFaultHook; h != nil {
		return h(step)
	}
	return nil
}

// lostRangeLocked returns volume's recorded range for (requestID, newEpoch).
// Called under p.mu.
func (p *Publisher) lostRangeLocked(volume, requestID string, newEpoch uint64) (WorkspaceLostRange, bool) {
	for _, e := range p.lostRanges[volume] {
		if e.RequestID == requestID && e.NewEpoch == newEpoch {
			return e, true
		}
	}
	return WorkspaceLostRange{}, false
}

// recordLostRangeLocked durably records r for volume before the move record
// that issues it is written. An entry whose move never issues is an orphan,
// ignored on read. Called under p.mu.
func (p *Publisher) recordLostRangeLocked(volume string, r WorkspaceLostRange) error {
	if p.store == nil {
		return fmt.Errorf("workspace forced move: no store to record the lost range")
	}
	next := make([]WorkspaceLostRange, 0, len(p.lostRanges[volume])+1)
	for _, e := range p.lostRanges[volume] {
		if e.RequestID != r.RequestID || e.NewEpoch != r.NewEpoch {
			next = append(next, e)
		}
	}
	next = append(next, r)
	if err := p.store.PutWorkspaceLostRanges(volume, next); err != nil {
		return fmt.Errorf("workspace forced move: record lost range: %w", err)
	}
	p.lostRanges[volume] = next
	return nil
}

// ErrWorkspaceLostRangeMissing: an issued forced move has no readable lost
// range (never written, or its file failed its checks at load and was
// skipped). The readout is unproven; it must never be reported as no loss.
var ErrWorkspaceLostRangeMissing = errors.New("workspace lost range missing or unreadable")

// ErrWorkspaceNoForcedMove: the request id names no issued forced move.
var ErrWorkspaceNoForcedMove = errors.New("workspace: no issued forced move with that request id")

// WorkspaceLostRangeReadout is one stored lost range with the stored move
// record it belongs to (the HEAD generation the move kept, and the hosts).
type WorkspaceLostRangeReadout struct {
	WorkspaceLostRange
	VolumeID         string
	HeadGen          uint64
	OldHost, NewHost string
}

// WorkspaceLostRangeReadouts returns the stored lost range of every issued
// forced move of volume, in journal (issue) order, or only requestID's when
// it is not empty. Nothing is recomputed. Any issued forced move in scope
// whose range is not loaded fails the whole read with
// ErrWorkspaceLostRangeMissing; an empty result means the volume has no
// issued forced move.
func (p *Publisher) WorkspaceLostRangeReadouts(volume, requestID string) ([]WorkspaceLostRangeReadout, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []WorkspaceLostRangeReadout
	if s := p.workspace[volume]; s != nil {
		for _, m := range s.Moves {
			if !m.Issued || m.Kind != "forced" || (requestID != "" && m.RequestID != requestID) {
				continue
			}
			r, ok := p.lostRangeLocked(volume, m.RequestID, m.NewEpoch)
			if !ok {
				return nil, fmt.Errorf("%w: forced move %q (epoch %d -> %d) of volume %q has no loaded lost range record", ErrWorkspaceLostRangeMissing, m.RequestID, m.OldEpoch, m.NewEpoch, volume)
			}
			out = append(out, WorkspaceLostRangeReadout{WorkspaceLostRange: r, VolumeID: volume, HeadGen: m.HeadGen, OldHost: m.OldHost, NewHost: m.NewHost})
		}
	}
	if requestID != "" && len(out) == 0 {
		return nil, fmt.Errorf("%w: %q on volume %q", ErrWorkspaceNoForcedMove, requestID, volume)
	}
	return out, nil
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

// lostRangeForRequestLocked returns the recorded range of requestID's
// journal record, if any. Called under p.mu.
func (p *Publisher) lostRangeForRequestLocked(volume, requestID string) (WorkspaceLostRange, bool) {
	if s := p.workspace[volume]; s != nil {
		for _, m := range s.Moves {
			if m.RequestID == requestID {
				return p.lostRangeLocked(volume, requestID, m.NewEpoch)
			}
		}
	}
	return WorkspaceLostRange{}, false
}
