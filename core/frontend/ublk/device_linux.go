//go:build linux

package ublk

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Implemented reports whether this platform has the ublk server.
func Implemented() bool { return true }

// Device is a running ublk device: /dev/ublkbN served from a Backend.
type Device struct {
	devID  uint32
	path   string
	ctrlFD int
	ctrl   *uring
	charFD int
	queues []*queue
	stats  *counters

	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
}

type queue struct {
	id      int
	depth   int
	ioBytes int
	descs   []byte // mmap'd descriptor array (read-only)
	bufs    []byte // anonymous mmap: depth buffers of ioBytes each
	h       handler
	charFD  int

	fetched chan error    // nil once every tag has been fetched
	done    chan struct{} // closed when the queue thread exits
	err     error
}

// Start creates /dev/ublkbN (N = cfg.DevID, or the kernel's choice when
// DevID < 0) backed by cfg.Backend and serves it until Close. A device
// left at the same id by a dead server is stopped and deleted first.
func Start(cfg Config) (*Device, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	ctrlFD, err := unix.Open(ctrlDevicePath, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("ublk: open %s (ublk_drv loaded, root?): %w", ctrlDevicePath, err)
	}
	ctrl, err := newURing(8)
	if err != nil {
		_ = unix.Close(ctrlFD)
		return nil, err
	}
	d := &Device{ctrlFD: ctrlFD, ctrl: ctrl, charFD: -1}
	fail := func(err error) (*Device, error) {
		d.teardown(false)
		return nil, err
	}

	id := devIDAuto
	if cfg.DevID >= 0 {
		id = uint32(cfg.DevID)
		_, statErr := os.Stat(charDevicePrefix + strconv.Itoa(cfg.DevID))
		if err := reclaimStale(d, id, statErr == nil, processAlive); err != nil {
			return fail(err)
		}
	}

	info := devInfo{
		NrHWQueues:    uint16(cfg.Queues),
		QueueDepth:    uint16(cfg.QueueDepth),
		MaxIOBufBytes: cfg.MaxIOBytes,
		DevID:         id,
		Flags:         flagCmdIoctlEncode,
	}
	if _, err := d.ctrlCommand(uCmdAddDev, id, bytesOf(&info), 0); err != nil {
		return fail(fmt.Errorf("ublk: add device: %w", err))
	}
	d.devID = info.DevID
	d.path = blockDevicePrefix + strconv.FormatUint(uint64(d.devID), 10)

	shift := uint8(bits.TrailingZeros32(cfg.BlockSize))
	p := params{
		Len:   uint32(unsafe.Sizeof(params{})),
		Types: paramTypeBasic,
		Basic: paramBasic{
			Attrs:           attrVolatileCache | attrFUA,
			LogicalBSShift:  shift,
			PhysicalBSShift: shift,
			IOMinShift:      shift,
			IOOptShift:      shift,
			MaxSectors:      cfg.MaxIOBytes >> sectorShift,
			DevSectors:      cfg.Size >> sectorShift,
		},
	}
	if _, err := d.ctrlCommand(uCmdSetParams, d.devID, bytesOf(&p), 0); err != nil {
		return fail(fmt.Errorf("ublk: set params: %w", err))
	}

	charPath := charDevicePrefix + strconv.FormatUint(uint64(d.devID), 10)
	if d.charFD, err = openWhenPresent(charPath, 5*time.Second); err != nil {
		return fail(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.stats = &counters{}
	h := handler{backend: cfg.Backend, size: cfg.Size, stats: d.stats}
	for q := 0; q < cfg.Queues; q++ {
		qu, err := newQueue(q, cfg, d.charFD, h)
		if err != nil {
			return fail(err)
		}
		d.queues = append(d.queues, qu)
		go qu.run(ctx)
		if err := <-qu.fetched; err != nil {
			return fail(fmt.Errorf("ublk: queue %d: %w", q, err))
		}
	}
	// Every tag of every queue has a FETCH outstanding: the device can go
	// live. START_DEV names this server process.
	if _, err := d.ctrlCommand(uCmdStartDev, d.devID, nil, uint64(os.Getpid())); err != nil {
		return fail(fmt.Errorf("ublk: start device: %w", err))
	}
	return d, nil
}

// Path is the block device, /dev/ublkbN.
func (d *Device) Path() string { return d.path }

// Stats returns the request counters since the device started.
func (d *Device) Stats() Stats { return d.stats.snapshot() }

// Close stops the device (outstanding fetches abort), waits for the queue
// threads, and deletes the device.
func (d *Device) Close() error {
	d.closeOnce.Do(func() { d.closeErr = d.teardown(true) })
	return d.closeErr
}

func (d *Device) teardown(added bool) error {
	var firstErr error
	note := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if d.path != "" {
		if _, err := d.ctrlCommand(uCmdStopDev, d.devID, nil, 0); err != nil && added {
			note(fmt.Errorf("ublk: stop device: %w", err))
		}
	}
	if d.cancel != nil {
		d.cancel()
	}
	qs := make([]settler, len(d.queues))
	for i, q := range d.queues {
		qs[i] = q
	}
	note(settleQueues(qs, queueStopTimeout))
	if d.charFD >= 0 {
		note(unix.Close(d.charFD))
		d.charFD = -1
	}
	if d.path != "" {
		if _, err := d.ctrlCommand(uCmdDelDev, d.devID, nil, 0); err != nil {
			note(fmt.Errorf("ublk: delete device: %w", err))
		}
	}
	note(d.ctrl.close())
	note(unix.Close(d.ctrlFD))
	return firstErr
}

// queueStopTimeout bounds how long Close waits for the queue threads.
const queueStopTimeout = 10 * time.Second

// command implements controller for reclaimStale.
func (d *Device) command(cmdOp uint32, devID uint32, buf []byte, data uint64) (int32, error) {
	return d.ctrlCommand(cmdOp, devID, buf, data)
}

// processAlive reports whether pid names a live process: signal 0 is
// delivered (or refused with EPERM: it exists), ESRCH means it is gone.
func processAlive(pid int32) (bool, error) {
	switch err := unix.Kill(int(pid), 0); err {
	case nil, unix.EPERM:
		return true, nil
	case unix.ESRCH:
		return false, nil
	default:
		return false, fmt.Errorf("ublk: probe pid %d: %w", pid, err)
	}
}

// ctrlCommand runs one control command on /dev/ublk-control and returns
// the CQE result. buf (may be nil) is the command's in/out buffer.
func (d *Device) ctrlCommand(cmdOp uint32, devID uint32, buf []byte, data uint64) (int32, error) {
	c := ctrlCmd{DevID: devID, QueueID: queueIDNone, Data: data}
	if len(buf) > 0 {
		c.Addr = uint64(uintptr(unsafe.Pointer(&buf[0])))
		c.Len = uint16(len(buf))
	}
	if err := d.ctrl.prepare(opURingCmd, d.ctrlFD, cmdOp, bytesOf(&c), uint64(cmdOp)); err != nil {
		return 0, err
	}
	if err := d.ctrl.submitAndWait(1); err != nil {
		return 0, err
	}
	cq := d.ctrl.reap(nil)
	runtime.KeepAlive(buf)
	if len(cq) != 1 {
		return 0, fmt.Errorf("ublk: control command %#x: %d completions", cmdOp, len(cq))
	}
	if cq[0].Res < 0 {
		return cq[0].Res, unix.Errno(-cq[0].Res)
	}
	return cq[0].Res, nil
}

func openWhenPresent(path string, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC, 0)
		if err == nil {
			return fd, nil
		}
		if !errors.Is(err, unix.ENOENT) || time.Now().After(deadline) {
			return -1, fmt.Errorf("ublk: open %s: %w", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newQueue(id int, cfg Config, charFD int, h handler) (*queue, error) {
	page := os.Getpagesize()
	descLen := (cfg.QueueDepth*ioDescSize + page - 1) / page * page
	descs, err := unix.Mmap(charFD, queueDescOffset(id), descLen, unix.PROT_READ, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return nil, fmt.Errorf("ublk: mmap queue %d descriptors: %w", id, err)
	}
	bufs, err := unix.Mmap(-1, 0, cfg.QueueDepth*int(cfg.MaxIOBytes), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		_ = unix.Munmap(descs)
		return nil, fmt.Errorf("ublk: map queue %d buffers: %w", id, err)
	}
	return &queue{
		id: id, depth: cfg.QueueDepth, ioBytes: int(cfg.MaxIOBytes),
		descs: descs, bufs: bufs, h: h, charFD: charFD,
		fetched: make(chan error, 1), done: make(chan struct{}),
	}, nil
}

func (q *queue) buf(tag int) []byte { return q.bufs[tag*q.ioBytes : (tag+1)*q.ioBytes] }

func (q *queue) desc(tag int) ioDesc {
	return *(*ioDesc)(unsafe.Pointer(&q.descs[tag*ioDescSize]))
}

func (q *queue) ioCommand(r *uring, cmdOp uint32, tag int, result int32) error {
	c := ioCmd{QID: uint16(q.id), Tag: uint16(tag), Result: result, Addr: uint64(uintptr(unsafe.Pointer(&q.buf(tag)[0])))}
	return r.prepare(opURingCmd, q.charFD, cmdOp, bytesOf(&c), uint64(tag))
}

// run is the queue's server thread. ublk binds a queue to the task that
// issued its FETCH commands, so the ring lives and is driven on one
// locked OS thread.
func (q *queue) run(ctx context.Context) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(q.done)

	r, err := newURing(uint32(q.depth))
	if err == nil {
		for tag := 0; tag < q.depth && err == nil; tag++ {
			err = q.ioCommand(r, uIOFetchReq, tag, 0)
		}
		if err == nil {
			err = r.submitAndWait(0)
		}
	}
	q.fetched <- err
	if err != nil {
		if r != nil {
			_ = r.close()
		}
		q.err = err
		return
	}
	defer r.close()

	live := q.depth // tags with a fetch outstanding
	var cq []cqe
	for live > 0 {
		if err := r.submitAndWait(1); err != nil {
			q.err = err
			return
		}
		cq = r.reap(cq[:0])
		for _, c := range cq {
			tag := int(c.UserData)
			if c.Res != ioResOK { // ABORT (-ENODEV) or a failure: no re-fetch
				live--
				continue
			}
			result := q.h.handle(ctx, q.desc(tag).request(), q.buf(tag))
			if err := q.ioCommand(r, uIOCommitAndFetchReq, tag, result); err != nil {
				q.err = err
				return
			}
		}
	}
}

func (q *queue) doneCh() <-chan struct{} { return q.done }
func (q *queue) name() string            { return "queue " + strconv.Itoa(q.id) }

func (q *queue) release() {
	if q.descs != nil {
		_ = unix.Munmap(q.descs)
		q.descs = nil
	}
	if q.bufs != nil {
		_ = unix.Munmap(q.bufs)
		q.bufs = nil
	}
}
