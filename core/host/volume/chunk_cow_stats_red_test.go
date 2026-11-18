package volume

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Keep the new cut-body counters as a caller-visible contract. Reflection
// lets this RED compile on main before the COW fields are implemented.
func TestChunkCutStatsBody_ExposesCOWCounters(t *testing.T) {
	var stats storage.ChunkCutStats
	fields := []struct {
		goName, jsonName string
		value            int64
	}{
		{"COWSavedChunks", "cowSavedChunks", 31},
		{"COWSaveNs", "cowSaveNs", 47},
		{"COWWaitNs", "cowWaitNs", 59},
	}
	value := reflect.ValueOf(&stats).Elem()
	for _, field := range fields {
		target := value.FieldByName(field.goName)
		if !target.IsValid() {
			t.Errorf("RED: storage cut stats lack %s", field.goName)
			continue
		}
		if target.Kind() != reflect.Int && target.Kind() != reflect.Int64 {
			t.Errorf("counter %s has unexpected kind %s", field.goName, target.Kind())
			continue
		}
		target.SetInt(field.value)
	}
	raw, err := json.Marshal(cutStatsBody(stats))
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]int64
	// Other cut-body fields include strings/bools, so decode only the named
	// integer counters from RawMessage rather than accepting a partial decode.
	var members map[string]json.RawMessage
	if err = json.Unmarshal(raw, &members); err != nil {
		t.Fatal(err)
	}
	body = make(map[string]int64)
	for _, field := range fields {
		member, exists := members[field.jsonName]
		if !exists {
			t.Errorf("RED: cut response lacks %s", field.jsonName)
			continue
		}
		var number int64
		if err = json.Unmarshal(member, &number); err != nil {
			t.Fatalf("counter %s: %v", field.jsonName, err)
		}
		body[field.jsonName] = number
		if number != field.value {
			t.Errorf("counter %s=%d want %d", field.jsonName, number, field.value)
		}
	}
}
