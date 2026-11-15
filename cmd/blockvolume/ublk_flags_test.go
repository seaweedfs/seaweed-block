package main

import "testing"

func TestUblkFlags(t *testing.T) {
	base := func(extra ...string) []string {
		return append(phase150RequiredBlockvolumeArgs(), extra...)
	}
	off, err := parseFlags(base())
	if err != nil || off.ublkDevice != -1 {
		t.Fatalf("default: ublk-device=%d err=%v, want -1 (off)", off.ublkDevice, err)
	}
	on, err := parseFlags(base("--ublk-device=3", "--ublk-queues=2", "--ublk-queue-depth=128"))
	if err != nil {
		t.Fatal(err)
	}
	if on.ublkDevice != 3 || on.ublkQueues != 2 || on.ublkQueueDepth != 128 {
		t.Fatalf("parsed ublk flags: device %d queues %d depth %d", on.ublkDevice, on.ublkQueues, on.ublkQueueDepth)
	}
	if !on.enableT1Readiness {
		t.Fatal("--ublk-device did not enable the readiness bridge (the device must serve only through a healthy projection)")
	}
	for name, args := range map[string][]string{
		"queues without a device": base("--ublk-queues=2"),
		"depth without a device":  base("--ublk-queue-depth=8"),
		"negative device id":      base("--ublk-device=-2"),
		"too many queues":         base("--ublk-device=0", "--ublk-queues=65"),
		"queue depth too large":   base("--ublk-device=0", "--ublk-queue-depth=4097"),
	} {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("%s: parseFlags succeeded; want an error", name)
		}
	}
}
