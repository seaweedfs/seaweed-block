package ublk

import (
	"errors"
	"fmt"
	"math/bits"
)

// Config describes one ublk device.
type Config struct {
	Backend Backend
	// Size is the device size in bytes, a multiple of BlockSize.
	Size uint64
	// BlockSize is the logical block size: a power of two, 512..4096.
	BlockSize uint32
	// DevID selects /dev/ublkbN; negative lets the kernel choose.
	DevID int
	// Queues is the number of hardware queues (one server thread each).
	Queues int
	// QueueDepth is the number of in-flight requests per queue.
	QueueDepth int
	// MaxIOBytes is the largest single request, a multiple of 4096.
	MaxIOBytes uint32
}

// Defaults for the optional Config fields.
const (
	DefaultQueues     = 1
	DefaultQueueDepth = 64
	DefaultMaxIOBytes = 512 << 10
)

func (c *Config) applyDefaults() {
	if c.Queues == 0 {
		c.Queues = DefaultQueues
	}
	if c.QueueDepth == 0 {
		c.QueueDepth = DefaultQueueDepth
	}
	if c.MaxIOBytes == 0 {
		c.MaxIOBytes = DefaultMaxIOBytes
	}
}

func (c *Config) validate() error {
	c.applyDefaults()
	switch {
	case c.Backend == nil:
		return errors.New("ublk: backend required")
	case c.BlockSize < 512 || c.BlockSize > 4096 || bits.OnesCount32(c.BlockSize) != 1:
		return fmt.Errorf("ublk: block size %d must be a power of two in 512..4096", c.BlockSize)
	case c.Size == 0 || c.Size%uint64(c.BlockSize) != 0:
		return fmt.Errorf("ublk: size %d must be a non-zero multiple of the block size %d", c.Size, c.BlockSize)
	case c.Queues < 1 || c.Queues > 64:
		return fmt.Errorf("ublk: %d queues, want 1..64", c.Queues)
	case c.QueueDepth < 1 || c.QueueDepth > maxQueueDepth:
		return fmt.Errorf("ublk: queue depth %d, want 1..%d", c.QueueDepth, maxQueueDepth)
	case c.MaxIOBytes < 4096 || c.MaxIOBytes%4096 != 0 || c.MaxIOBytes > maxIOBufBytesLimit:
		return fmt.Errorf("ublk: max IO %d bytes, want a multiple of 4096 up to %d", c.MaxIOBytes, maxIOBufBytesLimit)
	case c.DevID > 1<<20:
		return fmt.Errorf("ublk: device id %d out of range", c.DevID)
	}
	return nil
}
