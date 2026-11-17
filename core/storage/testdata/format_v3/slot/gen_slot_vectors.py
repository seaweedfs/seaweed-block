"""Independent generator for DESIGN r12 section 2.1 slot vectors (row 1 fixtures).

Pure stdlib. CRC32C (Castagnoli, reflected poly 0x82F63B78) implemented here and
self-checked against the standard check value CRC32C(b"123456789") = 0xE3069283.
Writes one positive reference slot and named negative variants, plus EXPECTED.txt.
"""
import struct
import sys
import os

POLY = 0x82F63B78
TABLE = []
for i in range(256):
    c = i
    for _ in range(8):
        c = (c >> 1) ^ POLY if c & 1 else c >> 1
    TABLE.append(c)


def crc32c(data: bytes) -> int:
    c = 0xFFFFFFFF
    for b in data:
        c = TABLE[(c ^ b) & 0xFF] ^ (c >> 8)
    return c ^ 0xFFFFFFFF


assert crc32c(b"123456789") == 0xE3069283, "crc32c self-check failed"

UUID = bytes(range(0x00, 0x10))
LINEAGE = bytes(range(0x10, 0x20))
NONCE = bytes(range(0x20, 0x30))
# rootBodyDigest of codex03's reference root (DESIGN 9.5): this slot names that map.
ROOT_HASH = bytes.fromhex("b47d24af8db0e0a2cd2494652c981c48f4d2a861c381f4a04cac4b35e2ac321b")


def slot(**kw) -> bytearray:
    f = dict(
        version=3, flags=0, implKind=1, implVersion=3,
        volumeSize=64 * 4096, extentSize=64 * 1024, blockSize=4096,
        walOffset=8192, walSize=64 * 1024, createdAt=1, slotGen=1, checkpoint=7,
        nonce=NONCE, tail=0, head=0, mapFormatID=1, mapGen=1, rootLocator=0,
        mapCovered=7, rootHash=ROOT_HASH, lineage=LINEAGE,
        W=7, baseLSN=0, baseState=0, baseCursor=0, installID=bytes(16),
        wMode=0, migPhase=0,
    )
    f.update(kw)
    b = bytearray(4096)
    b[0:4] = b"SWBK"
    struct.pack_into("<HH", b, 4, f["version"], f["flags"])
    b[8] = f["implKind"]
    struct.pack_into("<I", b, 12, f["implVersion"])
    b[16:32] = UUID
    struct.pack_into("<QIIQQQQQ", b, 32, f["volumeSize"], f["extentSize"], f["blockSize"],
                     f["walOffset"], f["walSize"], f["createdAt"], f["slotGen"], f["checkpoint"])
    b[128:144] = f["nonce"]
    struct.pack_into("<QQI", b, 144, f["tail"], f["head"], f["mapFormatID"])
    struct.pack_into("<QQQ", b, 168, f["mapGen"], f["rootLocator"], f["mapCovered"])
    b[192:224] = f["rootHash"]
    b[224:240] = f["lineage"]
    struct.pack_into("<QQ", b, 256, f["W"], f["baseLSN"])
    b[272] = f["baseState"]
    struct.pack_into("<Q", b, 280, f["baseCursor"])
    b[288:304] = f["installID"]
    b[304] = f["wMode"]
    b[305] = f["migPhase"]
    return b


def seal(b: bytearray) -> bytes:
    struct.pack_into("<I", b, 4092, crc32c(bytes(b[0:4092])))
    return bytes(b)


IID = bytes(range(0x40, 0x50))
cases = [
    # name, bytes, expected outcome (DESIGN section)
    ("slot-ref", seal(slot()), "valid, selectable (2.1-2.3)"),
    ("slot-installing", seal(slot(baseState=1, baseLSN=7, baseCursor=0, installID=IID)), "valid: installing with BaseCursor within [tail, head], nonzero ID (2.1)"),
    ("slot-strict", seal(slot(wMode=1)), "valid: WMode A (2.1, 6.1)"),
    ("slot-prepared", seal(slot(migPhase=1)), "valid but NEVER selectable: prepared, slotGen 1, BaseState none (2.3, 10.1)"),
]


def neg(name, b, why):
    cases.append((name, b, "INVALID: " + why))


bad = bytearray(seal(slot())); bad[4092] ^= 0x01
neg("neg-crc", bytes(bad), "CRC32C mismatch / torn (2.2.1)")
bad = slot(); bad[100] = 1
neg("neg-reserved-88", seal(bad), "nonzero reserved byte in 88..127 (2.2.1)")
bad = slot(); bad[310] = 1
neg("neg-reserved-306", seal(bad), "nonzero reserved byte in 306..319 (2.1)")
bad = slot(); bad[2000] = 1
neg("neg-reserved-320", seal(bad), "nonzero reserved byte in 320..4091 (2.1)")
neg("neg-flags", seal(slot(flags=1)), "nonzero flags (2.2.1)")
neg("neg-gen0", seal(slot(slotGen=0)), "slotGen 0 (2.2.2)")
neg("neg-nonce0", seal(slot(nonce=bytes(16))), "all-zero nonce (2.2.2)")
neg("neg-walofs", seal(slot(walOffset=4096)), "WALOffset != 8192 (2.2.3)")
neg("neg-head", seal(slot(tail=0, head=64 * 1024 + 1)), "head > tail + WALSize (2.2.3)")
neg("neg-wmode2", seal(slot(wMode=2)), "WMode not 0/1 (2.1)")
neg("neg-phase2", seal(slot(migPhase=2)), "MigrationPhase not 0/1 (2.1)")
neg("neg-phase1-gen2", seal(slot(migPhase=1, slotGen=2)), "prepared slot with slotGen != 1 (2.1)")
neg("neg-phase1-installing", seal(slot(migPhase=1, baseState=1, baseLSN=7, installID=IID)), "prepared slot with BaseState != none (2.1)")
neg("neg-basestate3", seal(slot(baseState=3)), "BaseState not 0/1/2 (2.1)")
neg("neg-none-with-id", seal(slot(installID=IID)), "BaseState none with nonzero BaseInstallID (2.1 BASE rules)")
neg("neg-installing-zero-id", seal(slot(baseState=1, baseLSN=7)), "installing with zero BaseInstallID (2.1 BASE rules)")
neg("neg-implv1", seal(slot(implVersion=1)), "v3 with impl 1 (1, combinations)")
neg("neg-coverage", seal(slot(checkpoint=8)), "CheckpointLSN 8 > mapCoveredCheckpoint 7 (9.1 predicate 2)")
neg("neg-covered-mismatch", seal(slot(mapCovered=6)), "descriptor mapCoveredCheckpoint != root coveredCheckpoint 7 (9.1 predicate 1)")
# Selection pairs (2.3): equal slotGen, different bytes -> refuse.
pairA = seal(slot(slotGen=5))
pairB = seal(slot(slotGen=5, W=8))
cases.append(("pair-equal-gen-A", pairA, "with pair-equal-gen-B: equal slotGen, different bytes -> refuse errSuperblockInvalid (2.3)"))
cases.append(("pair-equal-gen-B", pairB, "see pair-equal-gen-A"))

out = sys.argv[1]
os.makedirs(out, exist_ok=True)
with open(os.path.join(out, "EXPECTED.txt"), "w", encoding="utf-8", newline="\n") as e:
    e.write("Slot vectors for DESIGN r12 (raw c9c652cbe846822dff5c08ce95081b0156e1df0582e6b9766b62e1742ddc1adf), section 2.\n")
    e.write("Each file is one 4096-byte slot; CRC32C (Castagnoli) LE at 4092 unless the case says otherwise.\n")
    e.write("Map-dependent predicates assume the map fixture vectors/reference-root.bin (rootBodyDigest b47d24af..., C=7, gen 1).\n\n")
    for name, data, why in cases:
        assert len(data) == 4096
        with open(os.path.join(out, name + ".bin"), "wb") as fh:
            fh.write(data)
        e.write(f"{name}.bin crc32c={struct.unpack_from('<I', data, 4092)[0]:08x}: {why}\n")
print(len(cases), "vectors")
