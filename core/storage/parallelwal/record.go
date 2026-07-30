package parallelwal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	recordHeaderSize = 32
	recordMagic      = "PWR1"
	recordVersion    = 1
	flagWrite        = uint16(1)
)

type walRecord struct {
	LSN   uint64
	LBA   uint32
	Flags uint16
	Data  []byte
}

func encodeRecord(rec walRecord, blockSize int) ([]byte, error) {
	buf := make([]byte, recordHeaderSize+blockSize)
	if err := encodeRecordInto(buf, rec, blockSize); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeRecordInto(buf []byte, rec walRecord, blockSize int) error {
	if len(rec.Data) != blockSize {
		return fmt.Errorf("parallelwal: record data size %d != block size %d", len(rec.Data), blockSize)
	}
	if len(buf) != recordHeaderSize+blockSize {
		return fmt.Errorf("parallelwal: encode buffer size %d != record size %d", len(buf), recordHeaderSize+blockSize)
	}
	clear(buf)
	copy(buf[0:4], recordMagic)
	binary.LittleEndian.PutUint16(buf[4:6], recordVersion)
	binary.LittleEndian.PutUint16(buf[6:8], rec.Flags)
	binary.LittleEndian.PutUint64(buf[8:16], rec.LSN)
	binary.LittleEndian.PutUint32(buf[16:20], rec.LBA)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(blockSize))
	binary.LittleEndian.PutUint32(buf[24:28], crc32.ChecksumIEEE(rec.Data))
	copy(buf[recordHeaderSize:], rec.Data)
	recordCRC := crc32.NewIEEE()
	_, _ = recordCRC.Write(buf[:28])
	_, _ = recordCRC.Write(buf[recordHeaderSize : recordHeaderSize+blockSize])
	binary.LittleEndian.PutUint32(buf[28:32], recordCRC.Sum32())
	return nil
}

func decodeRecord(buf []byte, blockSize int) (walRecord, error) {
	if len(buf) != recordHeaderSize+blockSize {
		return walRecord{}, fmt.Errorf("parallelwal: record size %d != %d", len(buf), recordHeaderSize+blockSize)
	}
	if string(buf[0:4]) != recordMagic {
		return walRecord{}, fmt.Errorf("parallelwal: record bad magic")
	}
	if version := binary.LittleEndian.Uint16(buf[4:6]); version != recordVersion {
		return walRecord{}, fmt.Errorf("parallelwal: record version %d", version)
	}
	if size := binary.LittleEndian.Uint32(buf[20:24]); size != uint32(blockSize) {
		return walRecord{}, fmt.Errorf("parallelwal: record payload size %d != %d", size, blockSize)
	}
	data := make([]byte, blockSize)
	copy(data, buf[recordHeaderSize:recordHeaderSize+blockSize])
	if got, want := crc32.ChecksumIEEE(data), binary.LittleEndian.Uint32(buf[24:28]); got != want {
		return walRecord{}, fmt.Errorf("parallelwal: data CRC got=%08x want=%08x", got, want)
	}
	recordCRC := crc32.NewIEEE()
	_, _ = recordCRC.Write(buf[:28])
	_, _ = recordCRC.Write(data)
	if got, want := recordCRC.Sum32(), binary.LittleEndian.Uint32(buf[28:32]); got != want {
		return walRecord{}, fmt.Errorf("parallelwal: record CRC got=%08x want=%08x", got, want)
	}
	return walRecord{
		LSN:   binary.LittleEndian.Uint64(buf[8:16]),
		LBA:   binary.LittleEndian.Uint32(buf[16:20]),
		Flags: binary.LittleEndian.Uint16(buf[6:8]),
		Data:  data,
	}, nil
}
