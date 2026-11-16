package durable_test

import (
	"context"
	"encoding/json"
	"testing"
)

func TestDurabilityLatencyStatusPresent(t *testing.T) {
	b, _, _ := newTestBackend(t, logicalStorageFactories()[0], 16, 4096)
	if _, err := b.Write(context.Background(), 0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := b.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(b.WriteProfile())
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["DurabilityLatency"]; !ok {
		t.Fatalf("durability stage histograms missing from existing WriteProfile status: %s", data)
	}
}
