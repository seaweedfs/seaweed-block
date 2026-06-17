package smartwal

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
)

// Layout describes the on-disk SmartWAL geometry parsed from the file header.
// It is intentionally read-only; fault injection must use these offsets instead
// of legacy WALStore constants.
type Layout struct {
	HeaderSize  int64
	RecordSize  int64
	WALOffset   int64
	WALLength   int64
	WALEnd      int64
	ExtentStart int64
	ExtentBytes int64
	FileSize    int64

	BlockSize   uint32
	NumBlocks   uint32
	WALSlots    uint64
	ImplKind    uint8
	ImplVersion uint32
}

// RecordInfo describes one valid record currently present in the SmartWAL ring.
type RecordInfo struct {
	Slot      uint64
	Offset    int64
	LSN       uint64
	LBA       uint32
	Flags     uint8
	DataCRC32 uint32
	BytesHex  string
}

// InspectLayout opens path read-only and returns the SmartWAL layout described
// by its header. It fails closed for non-SmartWAL files or truncated files.
func InspectLayout(path string) (Layout, error) {
	f, err := os.Open(path)
	if err != nil {
		return Layout{}, fmt.Errorf("smartwal: inspect open %s: %w", path, err)
	}
	defer f.Close()

	hdr, err := readHeader(f)
	if err != nil {
		return Layout{}, err
	}
	if err := hdr.validate(); err != nil {
		return Layout{}, err
	}
	st, err := f.Stat()
	if err != nil {
		return Layout{}, fmt.Errorf("smartwal: inspect stat %s: %w", path, err)
	}

	walOffset := int64(headerSize)
	walLength := int64(hdr.WALSlots) * int64(recordSize)
	extentStart := walOffset + walLength
	extentBytes := int64(hdr.NumBlocks) * int64(hdr.BlockSize)
	required := extentStart + extentBytes
	if st.Size() < required {
		return Layout{}, fmt.Errorf("smartwal: truncated file: size=%d required=%d", st.Size(), required)
	}

	return Layout{
		HeaderSize:  int64(headerSize),
		RecordSize:  int64(recordSize),
		WALOffset:   walOffset,
		WALLength:   walLength,
		WALEnd:      walOffset + walLength,
		ExtentStart: extentStart,
		ExtentBytes: extentBytes,
		FileSize:    st.Size(),
		BlockSize:   hdr.BlockSize,
		NumBlocks:   hdr.NumBlocks,
		WALSlots:    hdr.WALSlots,
		ImplKind:    hdr.ImplKind,
		ImplVersion: hdr.ImplVersion,
	}, nil
}

// RecordOffset returns the absolute file offset for the slot used by lsn.
func (l Layout) RecordOffset(lsn uint64) (int64, error) {
	if l.WALSlots == 0 || l.RecordSize == 0 {
		return 0, fmt.Errorf("smartwal: invalid layout: WALSlots=%d RecordSize=%d", l.WALSlots, l.RecordSize)
	}
	slot := lsn % l.WALSlots
	return l.WALOffset + int64(slot)*l.RecordSize, nil
}

// ContainsWALOffset reports whether off lands inside the SmartWAL ring.
func (l Layout) ContainsWALOffset(off int64) bool {
	return off >= l.WALOffset && off < l.WALEnd
}

// ContainsExtentOffset reports whether off lands inside the extent region.
func (l Layout) ContainsExtentOffset(off int64) bool {
	return off >= l.ExtentStart && off < l.ExtentStart+l.ExtentBytes
}

// InspectRecords returns all valid records found in the SmartWAL ring, sorted
// by LSN ascending. Torn/empty records are intentionally omitted; corruption
// tools use this to pick a real record before mutating it.
func InspectRecords(path string) (Layout, []RecordInfo, error) {
	layout, err := InspectLayout(path)
	if err != nil {
		return Layout{}, nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return Layout{}, nil, fmt.Errorf("smartwal: inspect records open %s: %w", path, err)
	}
	defer f.Close()

	buf := make([]byte, layout.RecordSize)
	records := make([]RecordInfo, 0)
	for slot := uint64(0); slot < layout.WALSlots; slot++ {
		offset := layout.WALOffset + int64(slot)*layout.RecordSize
		if _, err := f.ReadAt(buf, offset); err != nil && err != io.EOF {
			return Layout{}, nil, fmt.Errorf("smartwal: inspect records read slot %d: %w", slot, err)
		}
		rec, ok := decode(buf)
		if !ok {
			continue
		}
		raw := make([]byte, len(buf))
		copy(raw, buf)
		records = append(records, RecordInfo{
			Slot:      slot,
			Offset:    offset,
			LSN:       rec.LSN,
			LBA:       rec.LBA,
			Flags:     rec.Flags,
			DataCRC32: rec.DataCRC32,
			BytesHex:  hex.EncodeToString(raw),
		})
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].LSN < records[j].LSN
	})
	return layout, records, nil
}

// LatestRecord returns the highest-LSN valid record in the SmartWAL ring.
func LatestRecord(path string) (Layout, RecordInfo, error) {
	layout, records, err := InspectRecords(path)
	if err != nil {
		return Layout{}, RecordInfo{}, err
	}
	if len(records) == 0 {
		return Layout{}, RecordInfo{}, fmt.Errorf("smartwal: no valid records in WAL ring")
	}
	return layout, records[len(records)-1], nil
}
