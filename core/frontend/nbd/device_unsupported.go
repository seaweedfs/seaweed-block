//go:build !linux

package nbd

import "errors"

type Config struct {
	Backend   Backend
	BlockSize uint32
	Size      uint64
	Device    string
}

type Device struct{}

func Start(Config) (*Device, error) {
	return nil, errors.New("nbd: Linux required")
}

func (d *Device) Path() string { return "" }

func (d *Device) Close() error { return nil }
