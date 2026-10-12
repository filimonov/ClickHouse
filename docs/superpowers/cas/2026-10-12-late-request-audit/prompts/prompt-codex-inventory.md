Adversarial completeness check of an inventory. You are read-only. Do not edit files except the output file named below.

Inventory: /home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/cas/2026-10-12-late-request-audit/inventory.md (Russian). It lists every object kind CAS (content-addressed MergeTree storage on S3/GCS) keeps in the store, every read/write/delete/modify operation on it with the actor function, request type, today's protection against a late ("zombie") request, the late-write hazard, and TLA+ coverage. It claims 101 rows, an empty "unmapped call sites" list, and an unsafe set U1–U15.

Zombie = paused process resuming after lease expiry; partition; request applied late by the store; SDK-internal resend of an identical request; second instance with the same server_root_id; stale cached read.

Code: commit d76b59f9295 in /home/mfilimonov/workspace/ClickHouse/cas-janitor-budget. Read with `git -C /home/mfilimonov/workspace/ClickHouse/cas-janitor-budget show d76b59f9295:<path>` and `git -C ... grep -n <pat> d76b59f9295 -- <path>`. CAS code: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/. S3 layer: src/Disks/DiskObjectStorage/ObjectStorages/S3/, src/IO/S3/, src/IO/WriteBufferFromS3.cpp. TLA+ models: /home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/models/.

Your job:
1. Find store call sites or object kinds the inventory missed (any path reaching IObjectStorage / the CAS backend: writes, deletes, copies, multipart, lists used for decisions, reads used for decisions). Run untruncated greps.
2. Find rows marked store-protected that are actually time-protected or unprotected (check the claimed mechanism really changes the TARGET key the late request hits; include ABA on content ETags and SDK resends).
3. Find rows in the unsafe set that are actually safe (overclaims).
4. Check TLA+ coverage claims (does the cited operator really model a late/zombie write?).
5. Check whether the U1–U15 grouping is right: topics that should be merged (same root mechanism, same fix) or split.

Output: /home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/cas/2026-10-12-late-request-audit/inventory-codex.md, English, a table: row id or "NEW" / verdict (MISSED, MISCLASSIFIED, OVERCLAIM, TLA-WRONG, CONFIRMED) / evidence (file:function) / consequence. Then a proposed final unsafe-topic list (merge/split notes). Under 1500 words.
