package parallelwal

import (
	"fmt"
	"io"
	"sync"
)

type segmentDurableFile interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
}

type segmentDurableEngine struct {
	file   segmentDurableFile
	owner  *segmentOwner
	syncMu sync.Mutex
	header segmentDurableHeader
	slot   int
}

func newSegmentDurableEngine(file segmentDurableFile, config segmentOwnerConfig) (*segmentDurableEngine, error) {
	if file == nil {
		return nil, fmt.Errorf("parallelwal: nil segmented durable file")
	}
	config.LogOffset = segmentDurableLogOffset
	if err := config.validate(); err != nil {
		return nil, err
	}
	header := segmentDurableHeader{
		Generation:  1,
		BlockSize:   config.BlockSize,
		NumBlocks:   config.NumBlocks,
		LogOffset:   config.LogOffset,
		MaxLogBytes: config.MaxLogBytes,
	}
	if err := writeSegmentDurableHeaderAt(file, 0, header); err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("parallelwal: sync initial segmented header: %w", err)
	}
	owner, err := newSegmentOwner(file, config)
	if err != nil {
		return nil, err
	}
	return &segmentDurableEngine{
		file:   file,
		owner:  owner,
		header: header,
		slot:   0,
	}, nil
}

func (e *segmentDurableEngine) Submit(lba uint32, data []byte) (uint64, error) {
	return e.owner.Submit(lba, data)
}

func (e *segmentDurableEngine) Sync() (uint64, error) {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()

	targetLSN, err := e.owner.Fence()
	if err != nil {
		return 0, err
	}
	snapshot, err := e.owner.BeginDurability(targetLSN)
	if err != nil {
		return 0, err
	}
	if snapshot.PublishedLSN <= e.header.LastLSN {
		e.owner.EndDurability(nil)
		return e.header.LastLSN, nil
	}
	if err := e.file.Sync(); err != nil {
		return 0, e.failSync(fmt.Errorf("parallelwal: sync segmented WAL data: %w", err))
	}
	if e.header.Generation == ^uint64(0) {
		return 0, e.failSync(fmt.Errorf("parallelwal: segmented header generation exhausted"))
	}
	next := segmentDurableHeader{
		Generation:     e.header.Generation + 1,
		BlockSize:      e.header.BlockSize,
		NumBlocks:      e.header.NumBlocks,
		LogOffset:      e.header.LogOffset,
		MaxLogBytes:    e.header.MaxLogBytes,
		CommittedBytes: snapshot.CommittedBytes,
		SegmentCount:   snapshot.SegmentCount,
		FirstSequence:  snapshot.FirstSequence,
		FirstLSN:       snapshot.FirstLSN,
		LastLSN:        snapshot.LastLSN,
	}
	nextSlot := (e.slot + 1) % segmentDurableHeaderSlots
	if err := writeSegmentDurableHeaderAt(e.file, nextSlot, next); err != nil {
		return 0, e.failSync(err)
	}
	if err := e.file.Sync(); err != nil {
		return 0, e.failSync(fmt.Errorf("parallelwal: sync segmented durable header: %w", err))
	}
	e.header = next
	e.slot = nextSlot
	e.owner.EndDurability(nil)
	return next.LastLSN, nil
}

func (e *segmentDurableEngine) failSync(err error) error {
	e.owner.EndDurability(err)
	return err
}

func (e *segmentDurableEngine) Close() error {
	return e.owner.Close()
}
