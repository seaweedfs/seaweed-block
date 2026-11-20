# Format v3 record vectors

The barrier vector is a worked example of the frozen format byte table (original design SHA in barrier.json; unchanged through the accepted r15 contract). It was constructed independently using reflected Castagnoli polynomial 0x82f63b78 and initial/final XOR 0xffffffff, without a production encoder. Nonce 00..0f is a nonzero 128-bit value. Prefix62 + no payload + CRC32C/footer8 =70 bytes. Literal fields, CRC and raw-file SHA are recorded in barrier.json.

The production dark-codec tests encode to and decode from these fixed bytes. This is new-API contract coverage, not a fabricated parent behavioral RED or a claim that production recovery selects v3. Wrong-nonce/current-cursor, correct-nonce/wrong-cursor, lap and physical ring admission belong to the later writer/scanner integration, not codec CRC acceptance.

Existing-caller REDs (foreign WAL replay and old/equal-LSN capture omission) remain on the separate evidence branch at655091e8; they are intentionally absent from the green integration branch until the corresponding fixes land.

batch.bin uses count2, LBA3 and8192 bytes of0x72; trim.bin uses LBA5 and affected65536 bytes with no payload. Both use LSN7/DurableLSN6/cursor128 and the barrier nonce. Independent literal fields, CRC32C and SHA256 are in additional-vectors.json. These nonzero count/LBA/length vectors prevent a mutually wrong encoder/decoder from passing only by roundtrip.
