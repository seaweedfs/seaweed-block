package nbd

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"testing"
)

type memoryBackend struct {
	mu     sync.Mutex
	data   []byte
	syncs  int
	failIO bool
}

func (b *memoryBackend) Read(_ context.Context, offset int64, p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failIO {
		return 0, io.ErrUnexpectedEOF
	}
	return copy(p, b.data[offset:int(offset)+len(p)]), nil
}

func (b *memoryBackend) Write(_ context.Context, offset int64, p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failIO {
		return 0, io.ErrUnexpectedEOF
	}
	return copy(b.data[offset:int(offset)+len(p)], p), nil
}

func (b *memoryBackend) Sync(context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.syncs++
	return nil
}

func TestProtocolServerWriteReadFlushAndDisconnect(t *testing.T) {
	backend := &memoryBackend{data: make([]byte, 4096)}
	serverSide, clientSide := io.Pipe()
	replyRead, replyWrite := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- protocolServer{backend: backend, size: uint64(len(backend.data))}.serve(
			context.Background(), struct {
				io.Reader
				io.Writer
			}{Reader: serverSide, Writer: replyWrite},
		)
	}()

	handle := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	payload := []byte("phase163-rdma")
	writeRequest(t, clientSide, nbdCmdWrite|nbdCmdFUA, handle, 512, payload)
	readReply(t, replyRead, handle, nil)
	writeRequest(t, clientSide, nbdCmdRead, handle, 512, make([]byte, len(payload)))
	readReply(t, replyRead, handle, payload)
	writeRequest(t, clientSide, nbdCmdFlush, handle, 0, nil)
	readReply(t, replyRead, handle, nil)
	writeRequest(t, clientSide, nbdCmdDisc, handle, 0, nil)

	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
	if backend.syncs != 2 {
		t.Fatalf("syncs=%d want 2 (FUA + flush)", backend.syncs)
	}
}

func TestProtocolServerRejectsOutOfRangeWithoutBackendIO(t *testing.T) {
	backend := &memoryBackend{data: make([]byte, 1024)}
	request := new(bytes.Buffer)
	handle := [8]byte{9}
	writeRequest(t, request, nbdCmdRead, handle, 1000, make([]byte, 64))
	writeRequest(t, request, nbdCmdDisc, handle, 0, nil)
	reply := new(bytes.Buffer)
	if err := (protocolServer{backend: backend, size: 1024}).serve(context.Background(), struct {
		io.Reader
		io.Writer
	}{Reader: request, Writer: reply}); err != nil {
		t.Fatalf("serve: %v", err)
	}
	header := reply.Next(16)
	if got := binary.BigEndian.Uint32(header[4:8]); got == 0 {
		t.Fatal("out-of-range request unexpectedly succeeded")
	}
}

func TestProtocolServerConsumesRejectedWritePayload(t *testing.T) {
	backend := &memoryBackend{data: make([]byte, 1024)}
	request := new(bytes.Buffer)
	handle := [8]byte{7}
	writeRequest(t, request, nbdCmdWrite, handle, 1000, make([]byte, 64))
	writeRequest(t, request, nbdCmdFlush, handle, 0, nil)
	writeRequest(t, request, nbdCmdDisc, handle, 0, nil)
	reply := new(bytes.Buffer)
	if err := (protocolServer{backend: backend, size: 1024}).serve(context.Background(), struct {
		io.Reader
		io.Writer
	}{Reader: request, Writer: reply}); err != nil {
		t.Fatalf("serve: %v", err)
	}
	first := reply.Next(16)
	if got := binary.BigEndian.Uint32(first[4:8]); got == 0 {
		t.Fatal("out-of-range write unexpectedly succeeded")
	}
	second := reply.Next(16)
	if got := binary.BigEndian.Uint32(second[4:8]); got != 0 {
		t.Fatalf("flush after rejected write errno=%d; request stream lost alignment", got)
	}
}

func writeRequest(t *testing.T, w io.Writer, command uint32, handle [8]byte, offset uint64, payload []byte) {
	t.Helper()
	header := make([]byte, 28)
	binary.BigEndian.PutUint32(header[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint32(header[4:8], command)
	copy(header[8:16], handle[:])
	binary.BigEndian.PutUint64(header[16:24], offset)
	binary.BigEndian.PutUint32(header[24:28], uint32(len(payload)))
	if _, err := w.Write(header); err != nil {
		t.Fatalf("write request header: %v", err)
	}
	if command&nbdCmdMask == nbdCmdWrite {
		if _, err := w.Write(payload); err != nil {
			t.Fatalf("write request payload: %v", err)
		}
	}
}

func readReply(t *testing.T, r io.Reader, handle [8]byte, data []byte) {
	t.Helper()
	header := make([]byte, 16)
	if _, err := io.ReadFull(r, header); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if got := binary.BigEndian.Uint32(header[0:4]); got != nbdReplyMagic {
		t.Fatalf("reply magic=%#x", got)
	}
	if got := binary.BigEndian.Uint32(header[4:8]); got != 0 {
		t.Fatalf("reply errno=%d", got)
	}
	if !bytes.Equal(header[8:16], handle[:]) {
		t.Fatalf("reply handle=%x want %x", header[8:16], handle)
	}
	if len(data) > 0 {
		got := make([]byte, len(data))
		if _, err := io.ReadFull(r, got); err != nil {
			t.Fatalf("read reply data: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("reply data=%q want %q", got, data)
		}
	}
}
