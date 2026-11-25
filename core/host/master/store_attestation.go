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
}

func loadStoreAttestations(dir string) (*storeAttestations, error) {
	s := &storeAttestations{vols: map[string]attestedStores{}}
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

// lookup returns the store the volume's primary last attested for replica;
// false when that replica is not in the attestation (never attested, or
// dropped from the write-ACK set since).
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

// record replaces the volume's attestation with att, in one write. An
// attestation from a lower epoch than the recorded one is ignored (a
// deposed primary's late report); an unchanged one writes nothing.
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
	return nil
}

func sameAttestation(a, b attestedStores) bool {
	if a.Primary != b.Primary || a.Epoch != b.Epoch || len(a.Stores) != len(b.Stores) {
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

// AttestedStore is the store the primary of volumeID last attested for
// replicaID (see storeAttestations).
func (h *Host) AttestedStore(volumeID, replicaID string) (storage.StoreID, bool) {
	if h == nil || h.attest == nil {
		return storage.StoreID{}, false
	}
	return h.attest.lookup(volumeID, replicaID)
}
