package master

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// storeAttestationFile holds every volume's last primary attestation, in
// the authority store directory; it is replaced as a whole by one atomic
// write (temp file, fsync, rename, directory fsync).
const storeAttestationFile = "store-attestations.json"

// attestedStores is one volume's last attestation: the primary line that
// made it and, per replica, the store that counted toward its write ACKs.
type attestedStores struct {
	Primary string            `json:"primary"`
	Epoch   uint64            `json:"epoch"`
	Stores  map[string]string `json:"stores"` // replica id -> store id (hex)
	// Initial is a new volume's replica set at its first bind; non-empty
	// while the volume is creating (waiting for its replicas' stores). The
	// wait itself follows the volume's current replicas (a replica replaced
	// meanwhile is not waited for); it is cleared, for good, the first time
	// every current replica has a recorded store.
	Initial []string `json:"initial,omitempty"`
}

// storeAttestations is the master's record of which store each replica
// counted toward write ACKs with, as the volume's primary attested it. It
// is the reference a promotion's probed store must equal: a replica's own
// report of its store never sets it, so a replaced store (an empty disk
// with a new superblock) is not taken for the store that acknowledged.
type storeAttestations struct {
	mu   sync.Mutex
	path string // "" = memory only
	vols map[string]attestedStores
	// changed[vol] is closed (and dropped) when vol's record changes.
	changed map[string]chan struct{}
}

func loadStoreAttestations(dir string) (*storeAttestations, error) {
	s := &storeAttestations{vols: map[string]attestedStores{}, changed: map[string]chan struct{}{}}
	if dir == "" {
		return s, nil
	}
	s.path = filepath.Join(dir, storeAttestationFile)
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store attestations: %w", err)
	}
	if err := json.Unmarshal(b, &s.vols); err != nil {
		return nil, fmt.Errorf("store attestations %s: %w", s.path, err)
	}
	if s.vols == nil {
		s.vols = map[string]attestedStores{}
	}
	return s, nil
}

// lookup returns the store a primary of the volume last attested for
// replica; false when none ever did. A replica dropped from the write-ACK
// set since keeps it.
func (s *storeAttestations) lookup(volumeID, replicaID string) (storage.StoreID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.vols[volumeID].Stores[replicaID]
	if !ok {
		return storage.StoreID{}, false
	}
	var id storage.StoreID
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != len(id) {
		return storage.StoreID{}, false
	}
	copy(id[:], b)
	return id, !id.IsZero()
}

// watch returns a channel closed at the next change of volumeID's record.
func (s *storeAttestations) watch(volumeID string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.changed[volumeID]
	if !ok {
		ch = make(chan struct{})
		s.changed[volumeID] = ch
	}
	return ch
}

func (s *storeAttestations) notifyLocked(volumeID string) {
	if ch, ok := s.changed[volumeID]; ok {
		close(ch)
		delete(s.changed, volumeID)
	}
}

// open records a new volume's initial replicas at its first bind, so its
// primary serves nothing until each has an attested store. A volume that
// already has a record (or an attestation) is left as it is: only a volume
// the master binds for the first time waits.
func (s *storeAttestations) open(volumeID string, replicas []string) error {
	if len(replicas) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, had := s.vols[volumeID]; had {
		return nil
	}
	vols := make(map[string]attestedStores, len(s.vols)+1)
	for k, v := range s.vols {
		vols[k] = v
	}
	vols[volumeID] = attestedStores{Stores: map[string]string{}, Initial: append([]string(nil), replicas...)}
	if err := s.persist(vols); err != nil {
		return err
	}
	s.vols = vols
	s.notifyLocked(volumeID)
	return nil
}

// awaited reports, for a volume still creating, how many of its current
// replicas have no recorded store yet and how many it has; 0, 0 once the
// volume left creating (or never waited). When nothing is awaited any more
// the creation wait is cleared durably, so a later replacement of a
// replica never closes the volume again.
func (s *storeAttestations) awaited(volumeID string, current []string) (waiting, replicas int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, had := s.vols[volumeID]
	if !had || len(v.Initial) == 0 {
		return 0, 0
	}
	for _, id := range current {
		if _, ok := v.Stores[id]; !ok {
			waiting++
		}
	}
	if waiting > 0 || len(current) == 0 {
		return waiting, len(current)
	}
	vols := make(map[string]attestedStores, len(s.vols))
	for k, x := range s.vols {
		vols[k] = x
	}
	v.Initial = nil
	vols[volumeID] = v
	if err := s.persist(vols); err != nil {
		// Not cleared: the volume keeps answering from its stores (nothing
		// awaited); the next call retries the write.
		return 0, len(current)
	}
	s.vols = vols
	s.notifyLocked(volumeID)
	return 0, len(current)
}

// record merges att into the volume's record, in one write: each replica it
// names gets that store as its reference; a replica it does not name keeps
// its last one. An attestation from a lower epoch than the recorded one is
// ignored (a deposed primary's late report); an unchanged one writes
// nothing.
func (s *storeAttestations) record(att *control.StoreAttestation) error {
	next := attestedStores{Primary: att.GetReplicaId(), Epoch: att.GetEpoch(), Stores: map[string]string{}}
	for _, st := range att.GetStores() {
		var id storage.StoreID
		if st.GetReplicaId() == "" || len(st.GetStoreId()) != len(id) {
			return fmt.Errorf("store attestation for %s: malformed entry %q (%d bytes)", att.GetVolumeId(), st.GetReplicaId(), len(st.GetStoreId()))
		}
		copy(id[:], st.GetStoreId())
		if id.IsZero() {
			continue
		}
		next.Stores[st.GetReplicaId()] = hex.EncodeToString(id[:])
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, had := s.vols[att.GetVolumeId()]
	if had && next.Epoch < cur.Epoch {
		return nil
	}
	// A replica the primary no longer attests keeps its last reference: it
	// may be the only holder of an acknowledged write, and only that store
	// can still prove it. A new attestation (after a rebuild) replaces it.
	for id, sid := range cur.Stores {
		if _, again := next.Stores[id]; !again {
			next.Stores[id] = sid
		}
	}
	next.Initial = cur.Initial // cleared only by awaited
	if had && sameAttestation(cur, next) {
		return nil
	}
	vols := make(map[string]attestedStores, len(s.vols)+1)
	for k, v := range s.vols {
		vols[k] = v
	}
	vols[att.GetVolumeId()] = next
	if err := s.persist(vols); err != nil {
		return err
	}
	s.vols = vols
	s.notifyLocked(att.GetVolumeId())
	return nil
}

func sameAttestation(a, b attestedStores) bool {
	if a.Primary != b.Primary || a.Epoch != b.Epoch || len(a.Stores) != len(b.Stores) || len(a.Initial) != len(b.Initial) {
		return false
	}
	for k, v := range a.Stores {
		if b.Stores[k] != v {
			return false
		}
	}
	return true
}

func (s *storeAttestations) persist(vols map[string]attestedStores) error {
	if s.path == "" {
		return nil
	}
	payload, err := json.Marshal(vols)
	if err != nil {
		return err
	}
	temp := s.path + ".tmp"
	f, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("store attestations: %w", err)
	}
	_, werr := f.Write(payload)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(temp, s.path)
	}
	if werr != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("store attestations: %w", werr)
	}
	if runtime.GOOS == "windows" {
		return nil // a directory cannot be fsynced there; the rename is the commit
	}
	d, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("store attestations: directory: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("store attestations: directory fsync: %w", err)
	}
	return nil
}

// ingestStoreAttestation records a heartbeat's attestation when the
// reporting server holds the volume's published primary line at the
// attested epoch (the line's replica and owner). Anything else is not a
// primary's word and is dropped.
func (h *Host) ingestStoreAttestation(server string, att *control.StoreAttestation) error {
	if att == nil || h.attest == nil {
		return nil
	}
	vol := att.GetVolumeId()
	line, published := h.Publisher().PublishedAuthorityLine(vol)
	if !published || line.ReplicaID != att.GetReplicaId() || line.Epoch != att.GetEpoch() ||
		h.workspaceLineOwner(line, vol, line.ReplicaID, line.Epoch) != server {
		return nil
	}
	return h.attest.record(att)
}

// openStoreRecord records the initial replicas of a volume the master binds
// for the first time (no authority line yet): its primary serves nothing
// until each has an attested store.
func (h *Host) openStoreRecord(volumeID string) error {
	if h == nil || h.attest == nil {
		return nil
	}
	if _, bound := h.Publisher().VolumeAuthorityLine(volumeID); bound {
		return nil
	}
	return h.attest.open(volumeID, h.replicaSlotsFor(volumeID))
}

// AttestedStore is the store the primary of volumeID last attested for
// replicaID (see storeAttestations).
func (h *Host) AttestedStore(volumeID, replicaID string) (storage.StoreID, bool) {
	if h == nil || h.attest == nil {
		return storage.StoreID{}, false
	}
	return h.attest.lookup(volumeID, replicaID)
}
