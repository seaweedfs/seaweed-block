package main

import "testing"

func phase150RequiredBlockvolumeArgs() []string {
	return []string{
		"--master=127.0.0.1:9333",
		"--server-id=m02",
		"--volume-id=v1",
		"--replica-id=r1",
		"--data-addr=127.0.0.1:19101",
		"--ctrl-addr=127.0.0.1:19102",
	}
}

func TestPhase150_BlockvolumeDurableWALMultiBlockFlagDefaultFalse(t *testing.T) {
	f, err := parseFlags(phase150RequiredBlockvolumeArgs())
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.durableWALMultiBlockRecords {
		t.Fatal("durableWALMultiBlockRecords default=true, want false")
	}
}

func TestPhase150_BlockvolumeDurableWALMultiBlockFlagRequiresWalstore(t *testing.T) {
	args := append(phase150RequiredBlockvolumeArgs(),
		"--durable-root=/tmp/sw-block",
		"--durable-impl=smartwal",
		"--durable-wal-multiblock-records",
	)
	if _, err := parseFlags(args); err == nil {
		t.Fatal("parseFlags succeeded; want walstore requirement error")
	}
}

func TestPhase150_BlockvolumeDurableWALMultiBlockFlagParsesOptIn(t *testing.T) {
	args := append(phase150RequiredBlockvolumeArgs(),
		"--durable-root=/tmp/sw-block",
		"--durable-impl=walstore",
		"--durable-wal-multiblock-records",
	)
	f, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !f.durableWALMultiBlockRecords {
		t.Fatal("durableWALMultiBlockRecords=false, want true")
	}
}
