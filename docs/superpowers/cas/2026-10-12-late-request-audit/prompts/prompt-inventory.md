# Task: complete nomenclature of CAS store objects and their late-write risk {#task-complete-nomenclature-of-cas-store-objects-and-their-la}

You are a read-only researcher. Do not edit source, do not commit, do not push. Your only outputs are the files named below.

## Goal {#goal}

Build the complete list of every object kind CAS keeps in the object store, and for each kind: who reads, writes, deletes and modifies it, with what request type, what protects it today from a late ("zombie") request, and whether a TLA+ model covers it. This list is the input for per-class follow-up research, whose goal is to remove time-based (clock/lease) protection in favour of store-enforced protection. Completeness matters more than prose.

"Zombie" = any of: a paused process (SIGSTOP, GC pause, swap) that resumes after its lease expired; a network partition; a request that the store applies late (after the client gave up); an SDK-internal retry that resends an identical request; a second server instance with the same `server_root_id` (srid); a stale cached read.

## Code base {#code-base}

- Base commit: `d76b59f9295` (branch `altinity/antalya-26.6-20261008`) in the repo `/home/mfilimonov/workspace/ClickHouse/cas-janitor-budget`. Do NOT check out. Read via `git -C /home/mfilimonov/workspace/ClickHouse/cas-janitor-budget show d76b59f9295:<path>` and `git -C ... grep -n <pattern> d76b59f9295 -- <path>`. Never truncate a grep you use for an exhaustiveness claim (no `head`).
- CAS code: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` (Backend, Formats, Gc, Parts, Pool, Primitives, Tools, top-level files). Key layout: `Formats/CasLayout.h`. Request layer: `Backend/CasRequests.*`, `Backend/CasObjectStorageBackend.*`. Also inspect the S3 layer for retries that resend identical bodies: `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp`, `src/IO/S3/Client.cpp`, `src/IO/WriteBufferFromS3.cpp`, `src/IO/S3/copyS3File.cpp` (multipart upload / copy).
- The lease series PR 8–16 (tip `3b4037ae265`, same repo) changed the mount plane only. For mount-plane objects (mount slot, epoch counter, gc_fenced, farewell, observers), add a short note on how `3b4037ae265` differs from the base (`git diff d76b59f9295 3b4037ae265 -- <path>`). Do not otherwise analyse the series.

## Prior work you must take as input (verify, do not trust) {#prior-work-you-must-take-as-input-verify-do-not-trust}

- `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/cas/2026-10-12-late-request-audit/prior/no-clock-fencing.md` (Russian; written against `3b4037ae265`; classes Z1–Z17, G1–G7, time inventory T1–T18).
- `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/cas/2026-10-12-late-request-audit/prior/no-clock-fencing-codex.md` (adversarial check; REFUTED Z8's `404` fix, Z14 blob publication under SDK retries, §4 admission-to-dispatch gap; found missed classes: repeated physical blob PUT/copy, `Tools/CasDecommission.cpp` retirement, startup staging sweep, `Gc/CasGcMetaWriter.cpp:deleteConfirmedMeta`, `gc/hb`, `gc/maintenance_state`, pool-algorithm registration, owner/catalog bootstrap, probe writes).
- Every class from both reports must appear in your table (or be explicitly merged into a row, saying so). Re-check each against the base commit — line numbers and even functions may differ from `3b4037ae265`.

## TLA+ models {#tla-models}

`/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/models/*.tla` (+ `*_RESULTS.md`, `.cfg`). For each row name the model(s) and the action(s)/variable(s) that represent the object and whether a late/zombie write is modelled (e.g. a delayed-apply or stale-writer action), or "not modelled". Cite operator names, not line numbers.

## Method {#method}

1. Enumerate object kinds from the key layout (`CasLayout.h` and every other place that builds a key string). Cross-check by grepping every store-call site: `putIfAbsent`, `replace`, `readModifyWrite*`, `casUpdate`, `publish`, `remove*`, `deleteExact`, `removeMany*`, `copy*`, `writeObject`, `list*`, `get*`/`read*`, `head*`. Every call site must map to a row; list unmapped ones explicitly.
2. For each object kind, find all actors: writer mount (insert/merge/mutation/ALTER/DROP/ATTACH/relink), GC (each phase), recovery, startup/shutdown, tools (decommission, fsck, backup), observers.
3. Classify each operation: conditional create (If-None-Match), conditional replace (If-Match etag), blind put, exact-token delete, blind delete, prefix list+delete, copy, read (cached or fresh).
4. Late-write risk: concrete schedule (who is late, what the successor/GC did meanwhile, what the late request then does) and harm (data loss, resurrection, dangling ref, leak/debris only, liveness wedge, none). Mark each claim **verified** (you traced the code) or **inferred**. Include ABA on content-derived ETags and SDK-internal resends.
5. Today's protection: store-enforced (name the mechanism: key scoping by epoch/life, dense id + seal, monotone merge, If-Match target changed by successor, exact token), or time-based (which time mechanism: admission reserve, observer wait, GC interval, etc.), or none.

## Outputs {#outputs}

1. `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/cas/2026-10-12-late-request-audit/inventory.md` — in Russian, terse. Sections:
   - **Сводка**: counts; list of rows with protection = time or none (the "unsafe" set) with one line each. This list drives the next stage, so make each item a self-contained research topic name (id + object + hazard).
   - **Таблица номенклатуры**: one row per (object kind × actor operation) with columns: id / объект и шаблон ключа / актор и функция (`file:function`) / тип запроса / защита сегодня / риск опоздавшей записи (schedule → harm, verified|inferred) / TLA+ (model:action or "нет") / отличие в серии (mount-plane only).
   - **Непокрытые места вызова**: store call sites not mapped to any row (should be empty; justify any left).
   - **Расхождения с прошлыми отчётами**: each Z*/G*/codex item whose verdict changes on the base commit or after your check.
2. A one-paragraph final message to me with the path, the row count, and the unsafe set ids.

Write the file incrementally (create it early, then extend) so partial work survives. Keep under ~2500 lines; prefer tables over prose.
