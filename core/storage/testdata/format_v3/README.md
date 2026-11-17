# Format v3 test-only fixture custody

The barrier vector is a literal worked example of DESIGN r12 (SHA in barrier.json). It was constructed independently from the byte table, using reflected Castagnoli polynomial 0x82f63b78, initial/final XOR 0xffffffff. It does not use a production encoder. Nonce 00..0f is deliberately nonzero as a 128-bit value. Fixed prefix 62 bytes; no payload; CRC32C then EntrySize gives 70 bytes. Exact values and raw-file SHA are in barrier.json.

The custody test checks bytes against literals and the standard library CRC32C. It is new-format FIXTURE coverage only, not a parent behavioral RED, product codec acceptance, or v3 recovery PASS. After code GO, the real encoder/decoder must consume this same vector; mutating nonce/cursor/type/count/CRC/footer and lap-placement controls must exercise the product path, not this fixture checker.

Existing-caller REDs are in walstore_incarnation_red_test.go and chunk_lsn_map_red_test.go. They intentionally fail on the authorized base. Do not include those failing heads as a green integration claim.
