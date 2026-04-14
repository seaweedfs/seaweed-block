package storage

// simpleByteBuf is a tiny io.Writer that accumulates bytes in a slice.
// Used by persistCheckpoint to encode the superblock without seeking
// the underlying file.
type simpleByteBuf struct {
	b []byte
}

func newSimpleByteBuf() *simpleByteBuf { return &simpleByteBuf{} }

func (w *simpleByteBuf) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

func (w *simpleByteBuf) bytes() []byte { return w.b }
