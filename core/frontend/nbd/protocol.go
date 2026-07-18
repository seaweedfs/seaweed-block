package nbd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"syscall"
)

const (
	nbdRequestMagic = uint32(0x25609513)
	nbdReplyMagic   = uint32(0x67446698)

	nbdCmdRead  = uint32(0)
	nbdCmdWrite = uint32(1)
	nbdCmdDisc  = uint32(2)
	nbdCmdFlush = uint32(3)
	nbdCmdMask  = uint32(0xffff)
	nbdCmdFUA   = uint32(1 << 16)

	maxNBDRequestSize = uint32(64 << 20)
)

// Backend is the minimum block contract required by the kernel NBD bridge.
type Backend interface {
	Read(context.Context, int64, []byte) (int, error)
	Write(context.Context, int64, []byte) (int, error)
	Sync(context.Context) error
}

type protocolServer struct {
	backend Backend
	size    uint64
}

func (s protocolServer) serve(ctx context.Context, rw io.ReadWriter) error {
	header := make([]byte, 28)
	for {
		if _, err := io.ReadFull(rw, header); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(header[0:4]) != nbdRequestMagic {
			return errors.New("nbd: invalid request magic")
		}

		command := binary.BigEndian.Uint32(header[4:8])
		handle := header[8:16]
		offset := binary.BigEndian.Uint64(header[16:24])
		length := binary.BigEndian.Uint32(header[24:28])
		if command&nbdCmdMask == nbdCmdDisc {
			return nil
		}
		if length > maxNBDRequestSize {
			if command&nbdCmdMask == nbdCmdWrite {
				return fmt.Errorf("nbd: write request length %d exceeds limit %d", length, maxNBDRequestSize)
			}
			if err := writeReply(rw, handle, syscall.EINVAL, nil); err != nil {
				return err
			}
			continue
		}

		if offset > s.size || uint64(length) > s.size-offset {
			if command&nbdCmdMask == nbdCmdWrite {
				if _, err := io.CopyN(io.Discard, rw, int64(length)); err != nil {
					return err
				}
			}
			if err := writeReply(rw, handle, syscall.EINVAL, nil); err != nil {
				return err
			}
			continue
		}

		switch command & nbdCmdMask {
		case nbdCmdRead:
			data := make([]byte, int(length))
			n, err := s.backend.Read(ctx, int64(offset), data)
			if err != nil || n != len(data) {
				if err := writeReply(rw, handle, syscall.EIO, nil); err != nil {
					return err
				}
				continue
			}
			if err := writeReply(rw, handle, 0, data); err != nil {
				return err
			}

		case nbdCmdWrite:
			data := make([]byte, int(length))
			if _, err := io.ReadFull(rw, data); err != nil {
				return err
			}
			n, err := s.backend.Write(ctx, int64(offset), data)
			if err == nil && n == len(data) && command&nbdCmdFUA != 0 {
				err = s.backend.Sync(ctx)
			}
			if err != nil || n != len(data) {
				if err := writeReply(rw, handle, syscall.EIO, nil); err != nil {
					return err
				}
				continue
			}
			if err := writeReply(rw, handle, 0, nil); err != nil {
				return err
			}

		case nbdCmdFlush:
			if err := s.backend.Sync(ctx); err != nil {
				if err := writeReply(rw, handle, syscall.EIO, nil); err != nil {
					return err
				}
				continue
			}
			if err := writeReply(rw, handle, 0, nil); err != nil {
				return err
			}

		default:
			if err := writeReply(rw, handle, syscall.EOPNOTSUPP, nil); err != nil {
				return err
			}
		}
	}
}

func writeReply(w io.Writer, handle []byte, errno syscall.Errno, data []byte) error {
	header := make([]byte, 16)
	binary.BigEndian.PutUint32(header[0:4], nbdReplyMagic)
	binary.BigEndian.PutUint32(header[4:8], uint32(errno))
	copy(header[8:16], handle)
	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}
