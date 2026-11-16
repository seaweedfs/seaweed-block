//go:build !linux

package ublk

import "errors"

// Implemented reports whether this platform has the ublk server.
func Implemented() bool { return false }

// Device is unavailable off Linux.
type Device struct{}

// Start always fails off Linux: ublk is a Linux kernel driver.
func Start(cfg Config) (*Device, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return nil, errors.New("ublk: requires Linux (ublk_drv)")
}

// Path is empty off Linux.
func (d *Device) Path() string { return "" }

// Stats is empty off Linux.
func (d *Device) Stats() Stats { return Stats{} }

// Close is a no-op off Linux.
func (d *Device) Close() error { return nil }
