package storage

// StoreID is a local store's identity: the 16 random bytes its superblock
// was given when the store was created. A store re-created at the same path
// has a new one, so equal ids mean the same store, never a copy or a
// replacement. The zero value means unknown.
type StoreID [16]byte

// IsZero reports whether the id is unknown.
func (id StoreID) IsZero() bool { return id == StoreID{} }

// StoreIDFromBytes decodes a wire store id; anything but 16 bytes is zero.
func StoreIDFromBytes(b []byte) StoreID {
	var id StoreID
	if len(b) == len(id) {
		copy(id[:], b)
	}
	return id
}

// storeIdentified is implemented by stores that carry a superblock UUID.
type storeIdentified interface {
	StoreID() StoreID
}

// StoreIDOf returns s's store id, or the zero id when s does not carry one.
func StoreIDOf(s LogicalStorage) StoreID {
	if si, ok := s.(storeIdentified); ok {
		return si.StoreID()
	}
	return StoreID{}
}

// StoreID returns the superblock UUID written when the store was created
// (both on-disk formats carry it).
func (s *WALStore) StoreID() StoreID { return StoreID(s.sb.UUID) }
