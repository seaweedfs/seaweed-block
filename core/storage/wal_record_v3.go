package storage

// WALRecordV3 is the checked header of one impl-3 WAL record, for read-only
// tools (crash-image analysis, diagnostics). Recovery does not use it.
type WALRecordV3 struct {
	LSN     uint64
	Type    uint8
	LBA     uint64
	Length  uint32 // the payload bytes the record covers
	Size    uint64 // the whole record, header included
	Padding bool
}

// WALRecordHeaderBytesV3 is what a record adds to its payload (prefix, CRC32C
// and EntrySize): the same constant the encoder and decoder use.
const WALRecordHeaderBytesV3 = walEntryHeaderSizeV3

// DecodeWALRecordV3 decodes the record at the start of buf with the decoder
// recovery uses: the same geometry, size and CRC checks and errors. It adds
// no parsing of its own; nonce, cursor and lap admission stay the scanner's.
func DecodeWALRecordV3(buf []byte, blockSize uint32, numBlocks uint64) (WALRecordV3, error) {
	e, err := decodeWALEntryV3(buf, blockSize, numBlocks)
	if err != nil {
		return WALRecordV3{}, err
	}
	return WALRecordV3{
		LSN:     e.LSN,
		Type:    e.Type,
		LBA:     e.LBA,
		Length:  e.Length,
		Size:    uint64(walEntryHeaderSizeV3 + len(e.Data)),
		Padding: e.Type == walEntryPadding,
	}, nil
}
