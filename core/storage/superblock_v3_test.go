package storage

import (
	"bufio"
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Row 1 (DESIGN r15 section 2) new-API coverage. The vectors under
// testdata/format_v3/slot were produced by gen_slot_vectors.py, an
// independent stdlib-only generator with its own CRC32C (self-checked
// against CRC32C("123456789") = e3069283), not by this codec.

const slotVectorDir = "testdata/format_v3/slot"

func readSlotVector(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(slotVectorDir, name+".bin"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func slotVectorNames(t *testing.T) []string {
	t.Helper()
	f, err := os.Open(filepath.Join(slotVectorDir, "EXPECTED.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var names []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.Index(line, ".bin crc32c="); i > 0 && !strings.Contains(line[:i], " ") {
			names = append(names, line[:i])
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if len(names) != 25 {
		t.Fatalf("EXPECTED.txt lists %d vectors, want 25", len(names))
	}
	return names
}

func TestSlotV3VectorsDecodeAsExpected(t *testing.T) {
	for _, name := range slotVectorNames(t) {
		t.Run(name, func(t *testing.T) {
			raw := readSlotVector(t, name)
			d, err := decodeSlotV3(raw, nil)
			if strings.HasPrefix(name, "neg-") {
				if err == nil {
					t.Fatalf("decoded an invalid slot: %+v", d)
				}
				if !errors.Is(err, errSuperblockInvalid) {
					t.Fatalf("err = %v, want errSuperblockInvalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("valid slot refused: %v", err)
			}
			if got := d.encode(); !bytes.Equal(got, raw) {
				t.Fatal("re-encoding the decoded slot is not byte-exact")
			}
		})
	}
}

func TestSlotV3ReferenceFields(t *testing.T) {
	d, err := decodeSlotV3(readSlotVector(t, "slot-ref"), nil)
	if err != nil {
		t.Fatal(err)
	}
	var uuid, nonce, lineage [16]byte
	for i := 0; i < 16; i++ {
		uuid[i], lineage[i], nonce[i] = byte(i), byte(0x10+i), byte(0x20+i)
	}
	if d.UUID != uuid || d.Nonce != nonce || d.MapLineage != lineage {
		t.Fatal("identity fields differ from the generator")
	}
	if d.ImplKind != ImplKindWALStore || d.ImplVersion != walStoreImplVersion3 ||
		d.WALOffset != walOffsetV3 || d.SlotGen != 1 || d.CheckpointLSN != 7 ||
		d.W != 7 || d.MapCoveredCheckpoint != 7 || d.MapGeneration != 1 ||
		d.BaseState != BaseNone || d.WMode != WModeLagged || d.MigrationPhase != MigrationNormal {
		t.Fatalf("reference fields differ: %+v", d)
	}
	// The slot names the map fixture root (DESIGN 9.5).
	if got := d.MapRootHash; got[0] != 0xb4 || got[1] != 0x7d || got[31] != 0x1b {
		t.Fatalf("map root hash %x is not the 9.5 rootBodyDigest", got)
	}
}

func withSlotGen(t *testing.T, raw []byte, gen uint64, mutate func(*descriptorV3)) []byte {
	t.Helper()
	d, err := decodeSlotV3(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.SlotGen = gen
	if mutate != nil {
		mutate(&d)
	}
	return d.encode()
}

func TestSlotV3Selection(t *testing.T) {
	ref := readSlotVector(t, "slot-ref")
	gen2 := withSlotGen(t, ref, 2, func(d *descriptorV3) { d.W = 9; d.MapCoveredCheckpoint = 9 })
	torn := append([]byte(nil), gen2...)
	torn[4092] ^= 1
	garbage := bytes.Repeat([]byte{0xA5}, slotSizeV3)
	prepared := readSlotVector(t, "slot-prepared")

	cases := []struct {
		name    string
		a, b    []byte
		wantErr error
		want    slotIDV3
		wantGen uint64
	}{
		{"higher generation in B wins", ref, gen2, nil, slotB, 2},
		{"higher generation in A wins", gen2, ref, nil, slotA, 2},
		{"torn newer slot falls back", ref, torn, nil, slotA, 1},
		{"garbage A falls back to B", garbage, gen2, nil, slotB, 2},
		{"both invalid refuse", garbage, torn, errSuperblockInvalid, 0, 0},
		{"identical equal-generation copies are equivalent", ref, append([]byte(nil), ref...), nil, slotA, 1},
		{"equal generation different bytes refuse", readSlotVector(t, "pair-equal-gen-A"), readSlotVector(t, "pair-equal-gen-B"), errSuperblockInvalid, 0, 0},
		{"prepared B is never selected over a valid A", gen2, prepared, nil, slotA, 2},
		{"prepared only refuses as incomplete migration", garbage, prepared, ErrMigrationIncomplete, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, id, err := selectSlotV3(c.a, c.b, nil)
			if c.wantErr != nil {
				if !errors.Is(err, c.wantErr) {
					t.Fatalf("err = %v, want %v", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if id != c.want || d.SlotGen != c.wantGen {
				t.Fatalf("selected slot %d gen %d, want slot %d gen %d", id, d.SlotGen, c.want, c.wantGen)
			}
		})
	}

	t.Run("identity mismatch refuses", func(t *testing.T) {
		other := withSlotGen(t, ref, 2, func(d *descriptorV3) { d.UUID[0] ^= 0xFF })
		if _, _, err := selectSlotV3(ref, other, nil); !errors.Is(err, errSuperblockInvalid) {
			t.Fatalf("err = %v, want errSuperblockInvalid", err)
		}
	})
	t.Run("geometry mismatch refuses", func(t *testing.T) {
		other := withSlotGen(t, ref, 2, func(d *descriptorV3) { d.WALSize *= 2 })
		if _, _, err := selectSlotV3(ref, other, nil); !errors.Is(err, errSuperblockInvalid) {
			t.Fatalf("err = %v, want errSuperblockInvalid", err)
		}
	})
}

func TestSlotV3CounterExhaustion(t *testing.T) {
	if counterMaxV3 != math.MaxUint64-1 {
		t.Fatalf("counter cutoff %d, want 2^64-2", uint64(counterMaxV3))
	}
	if _, err := nextSlotGenV3(counterMaxV3); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("next after 2^64-2: err = %v, want ErrCounterExhausted", err)
	}
	if n, err := nextSlotGenV3(counterMaxV3 - 1); err != nil || n != counterMaxV3 {
		t.Fatalf("next after 2^64-3 = %d, %v; want 2^64-2", n, err)
	}
	ref := readSlotVector(t, "slot-ref")
	if _, err := decodeSlotV3(withSlotGen(t, ref, counterMaxV3, nil), nil); err != nil {
		t.Fatalf("slotGen 2^64-2 refused: %v", err)
	}
	if _, err := decodeSlotV3(withSlotGen(t, ref, math.MaxUint64, nil), nil); !errors.Is(err, errSuperblockInvalid) {
		t.Fatalf("slotGen 2^64-1: err = %v, want errSuperblockInvalid", err)
	}
}

func TestSlotV3InstalledMayHaveCursorBelowTail(t *testing.T) {
	ref := readSlotVector(t, "slot-ref")
	installed := withSlotGen(t, ref, 3, func(d *descriptorV3) {
		d.BaseState = BaseInstalled
		d.BaseLSN = 7
		d.BaseInstallID[0] = 1
		d.RetainedTail, d.RetainedHead, d.BaseCursor = 500, 600, 100
	})
	if _, err := decodeSlotV3(installed, nil); err != nil {
		t.Fatalf("installed slot with BaseCursor below retainedTail refused (r15): %v", err)
	}
	installing := withSlotGen(t, ref, 3, func(d *descriptorV3) {
		d.BaseState = BaseInstalling
		d.BaseLSN = 7
		d.BaseInstallID[0] = 1
		d.RetainedTail, d.RetainedHead, d.BaseCursor = 500, 600, 100
	})
	if _, err := decodeSlotV3(installing, nil); !errors.Is(err, errSuperblockInvalid) {
		t.Fatalf("installing slot with BaseCursor below retainedTail: err = %v, want errSuperblockInvalid", err)
	}
}

func TestSlotV3RejectsOtherVersionsAndMagic(t *testing.T) {
	ref := readSlotVector(t, "slot-ref")
	v2 := append([]byte(nil), ref...)
	v2[4] = 2
	if _, err := decodeSlotV3(v2, nil); !errors.Is(err, errSuperblockBadVer) {
		t.Fatalf("version 2 bytes: err = %v, want errSuperblockBadVer", err)
	}
	bad := append([]byte(nil), ref...)
	bad[0] = 'X'
	if _, err := decodeSlotV3(bad, nil); !errors.Is(err, errSuperblockNotOurs) {
		t.Fatalf("bad magic: err = %v, want errSuperblockNotOurs", err)
	}
	if _, err := decodeSlotV3(ref[:100], nil); !errors.Is(err, errSuperblockInvalid) {
		t.Fatalf("short slot: err = %v, want errSuperblockInvalid", err)
	}
}
