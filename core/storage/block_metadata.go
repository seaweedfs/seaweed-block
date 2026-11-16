package storage

import (
	"fmt"
	"io"
	"math"
	"os"
)

// BlockMetadata is one optional companion to extent data. Write and verify
// run under the existing extent stripe; the checkpoint barrier runs under
// checkpoint locking. Hooks must not re-enter the store or acquire its locks.
// A successful barrier promises metadata is durable before WAL recycling.
type BlockMetadata interface {
	OnExtentWrite(lba uint32, data []byte) error
	VerifyExtent(lba uint32, data []byte) error
	BeforeCheckpointAdvance(lsn uint64) error
}

// BlockMetadataFile is the store's own descriptor, not a separately opened
// file. In read-only mode writes and Sync are forbidden by the caller contract
// (the underlying descriptor is also O_RDONLY). The factory must not close it.
type BlockMetadataFile interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
}

type BlockMetadataLayout struct {
	ImplVersion uint32
	BlockSize   uint32
	NumBlocks   uint32
	ExtentEnd   int64
	Creating    bool
	ReadOnly    bool
}

type BlockMetadataBinding struct {
	ImplVersion   uint32
	AppendedBytes uint64
	Hook          BlockMetadata
}

// BlockMetadataFactory binds a single implementation before background work
// or recovery begins. On create it declares a layout without writing bytes;
// on open it must accept exactly the stored version. No registry or hook chain.
type BlockMetadataFactory func(BlockMetadataFile, BlockMetadataLayout) (BlockMetadataBinding, error)
type WALStoreOptions struct{ MetadataFactory BlockMetadataFactory }

func bindBlockMetadata(f *os.File, sb *superblock, opts WALStoreOptions, creating, readonly bool) (BlockMetadata, int64, error) {
	if sb.WALOffset > math.MaxInt64 || sb.WALSize > math.MaxInt64-sb.WALOffset || sb.VolumeSize > math.MaxInt64-sb.WALOffset-sb.WALSize {
		return nil, 0, fmt.Errorf("storage: extent size overflow")
	}
	base := sb.WALOffset + sb.WALSize + sb.VolumeSize
	binding := BlockMetadataBinding{ImplVersion: WALStoreImplVersion}
	if opts.MetadataFactory != nil {
		var err error
		binding, err = opts.MetadataFactory(f, BlockMetadataLayout{ImplVersion: sb.ImplVersion, BlockSize: sb.BlockSize, NumBlocks: uint32(sb.VolumeSize / uint64(sb.BlockSize)), ExtentEnd: int64(base), Creating: creating, ReadOnly: readonly})
		if err != nil {
			return nil, 0, fmt.Errorf("storage: bind block metadata: %w", err)
		}
	}
	if binding.ImplVersion == 0 || (!creating && binding.ImplVersion != sb.ImplVersion) || (binding.ImplVersion != WALStoreImplVersion && binding.Hook == nil) {
		return nil, 0, fmt.Errorf("storage: unsupported walstore implementation version %d without accepting metadata factory", sb.ImplVersion)
	}
	if binding.AppendedBytes > math.MaxInt64-base {
		return nil, 0, fmt.Errorf("storage: metadata region size overflow")
	}
	size := int64(base + binding.AppendedBytes)
	if creating {
		sb.ImplVersion = binding.ImplVersion
	} else {
		info, err := f.Stat()
		if err != nil {
			return nil, 0, err
		}
		if info.Size() < size {
			return nil, 0, fmt.Errorf("storage: incomplete metadata layout: %w", io.ErrUnexpectedEOF)
		}
	}
	return binding.Hook, size, nil
}
