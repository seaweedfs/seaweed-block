package storage

import "errors"

const (
	walEntryWrite      = 0x01
	walEntryTrim       = 0x02
	walEntryBarrier    = 0x03
	walEntryWriteBatch = 0x04
	walEntryPadding    = 0xFF
)

var (
	errCRCMismatch    = errors.New("storage: WAL entry CRC mismatch")
	errInvalidEntry   = errors.New("storage: invalid WAL entry")
	errEntryTruncated = errors.New("storage: WAL entry truncated")
	errWALFull        = errors.New("storage: WAL region full")
	errWALPwrite      = errors.New("storage: WAL pwrite failed")
)
