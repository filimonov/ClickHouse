---
description: 'Live backlog — operability, release gates, disk-error hardening, fsck/introspection surfaces, and the lazy_load_tables decision.'
sidebar_label: 'Operability & introspection'
sidebar_position: 7
slug: /superpowers/cas/backlog/operability-and-introspection
title: 'CAS Backlog — Operability and introspection'
doc_type: 'guide'
---

# CAS Backlog — Operability and introspection {#operability-and-introspection}

Part of the [CAS live backlog](/superpowers/cas/backlog). Topic file for operability, release gates,
disk-error hardening, fsck/introspection surfaces, and the `lazy_load_tables` decision. Grouped by surface:
system tables and columns, metrics and events, logging, SQL commands and CLI tools, runbooks and docs, and
open design questions.

Historical grab-bag section anchors, dissolved in the 2026-09-26 reorg — their items are redistributed by
surface below, not deleted.

### Operability & release gates {#operability}

Regrouped into the topic headings above.

### fsck surfaces {#fsck-surfaces}

Regrouped into the topic headings above.

### New findings from the 2026-08-04 orphaned-open triage {#orphan-triage-2026-08-04}

Regrouped into the topic headings above.

## System tables and columns {#system-tables-and-columns}

### The GC-health surface cannot express "never led", "GC stopped" or "backlog shed" (2031-triage CAS-098) {#gc-health-zero-is-ambiguous}

Four separate readings of the same per-disk GC-health snapshot
(`Gc/CasGcScheduler.cpp:392-407`, rendered by `StorageSystemContentAddressedMounts.cpp:52-56`,
`:196-209` and by the per-disk asynchronous metrics at
`src/Interpreters/ServerAsynchronousMetrics.cpp:378-392`):

1. **`last_success_age_seconds = 0` means BOTH "never led a round" and "succeeded within the last
   second."** `gcHealth` computes the discriminator — `ever_succeeded = last_ms != 0`
   (`Gc/CasGcScheduler.cpp:475`, field at `Gc/CasGcScheduler.h:143`) — but no surface exposes it: there
   is no `ever_succeeded` column and no `CASGCEverSucceeded_<disk>` metric, and only two gtests read the
   field. `StorageSystemContentAddressedMounts.cpp` already declares the column `Nullable(UInt64)` and
   already inserts NULL on peer rows (`:213`), but the LOCAL row's insert (`:205`) is unconditional — it
   does not check `ever_succeeded` first, so distinguishing "never led" from "just succeeded" on a
   server's own row is a one-line fix at that call site. The operational bite: an alert of the shape
   `CASGCLastSuccessAgeSeconds_<disk> > threshold` can NEVER fire for a disk whose GC never succeeded
   even once — precisely the silent failure the metric exists to catch. A live production audit
   independently reconfirmed this on 2026-09-25: `docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md`{#f25}
   found `CASGCLastSuccessAgeSeconds` reading 0 for over three hours after a restart with no round
   succeeded in that process. Cheapest honest fix: render `NULL` (and skip the metric) when
   `!ever_succeeded`; the alternative is to expose `ever_succeeded` alongside. *(Formerly also tracked
   separately as `[ever-succeeded-unused]` {#ever-succeeded-unused}, opus review M4 — same fact, same
   one-line fix; merged here — this anchor now redirects to this point.)*
2. **`is_leader = 0` conflates "follower", "operator stopped GC here" and "scheduler self-exited".**
   `SYSTEM CAS GC STOP` is STOP-IN-PLACE — the scheduler object is retained deliberately so
   `gcHealth` keeps answering (`ContentAddressedMetadataStorage.cpp:971-1001`) — so a stopped GC
   presents exactly as a follower: `is_leader = 0`, health present. Nothing in `system.cas_mounts`,
   the metrics, or `system.cas_gc_log` says "this node's reclaimer is administratively off"; the only
   trace is the one-shot `LOG_INFO` at `src/Interpreters/InterpreterSystemQuery.cpp:2656-2661`. A
   `gc_running` (or `gc_state`) column, sampled from the scheduler's `stopping` latch, closes it.
   Not a defect: `GC STOP` being node-local and non-durable across restart matches the
   `SYSTEM STOP MERGES` precedent and is documented as such at the same site — the gap is
   observability, not persistence.
3. **`pending_reclaim` sheds nothing but executed deletes, so it drifts upward permanently — and,
   confirmed live, goes NEGATIVE across a restart.** The accumulator is `condemned - redeleted`
   (`Gc/CasGcScheduler.cpp:202-205`); an entry that leaves the retired list as `spared` or `replaced`
   (`Gc/CasGc.h:136-137`, counted at `Gc/CasGc.cpp:995-996`) is never subtracted, satisfying the
   metric's own documented reading ("a persistently growing value indicates GC is not keeping up",
   `ServerAsynchronousMetrics.cpp:387-388`) even on a perfectly healthy pool that spares a lot. Report
   `{#f25}` above (section F13) additionally confirms this went to **-388,242** in production after a
   restart on 2026-09-25, since the counter is process-local and the new process's deletes are not the
   condemns it remembers. The planned fix already has a name but no code: spec
   `docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md` §C5 ("Observability")
   commits to computing `pending_reclaim` from the adopted seal's `CondemnedSummary` (durable,
   restart-safe) INSTEAD OF the process-local counter — replacing its source, not adding a second
   column — but §10/§11 leave "where `CondemnedSummary` totals become available to `cas_mounts`
   without a new read" as an open verification item, and no code routing `CondemnedSummary` to the
   health surface exists yet on either branch. Until §C5 ships: the authoritative per-round gauge
   `RoundReport::pending_condemned`/`pending_candidates`/`pending_retired` (`Gc/CasGc.h:152-155`)
   is rendered ONLY in the `SYSTEM CAS GC RUN` result set (`InterpreterSystemQuery.cpp:2356-2358`,
   `:2380-2382`), not in `system.cas_mounts`, `system.cas_gc_log`, or the metrics.
4. **Our own docs read these columns as something they are not.** `operations/migration.md:203-204`
   tells the operator to check the victim's `state` and `last_success_age_seconds` before
   `SYSTEM CAS DROP POOL MEMBER` — but GC-health columns are `NULL` on every peer row by design
   (stated correctly in `architecture/mounts-and-leases.md:219` and
   `operations/monitoring.md:98-101`), so the victim's row never carries it; the liveness signals
   there are `state`/`expires_at`. `operations/troubleshooting.md` likewise offers
   "`last_success_age_seconds` not climbing" as evidence that the MOUNT LEASE is still renewing —
   two unrelated clocks. Both lines are wrong as written and both are cheap prose fixes.

NOT a defect, checked while triaging (the audit's fifth claim): `CASGCClampSuppressedPasses` no
longer "fires every round by construction". The destructive gate is conditional at HEAD —
`suppress_destructive = anomalies || carried_holds || frontier_incomplete`
(`Gc/CasGc.cpp:3065-3071`) with `UniversePolicy::kDefault = Authoritative`
(`Gc/CasGc.h:42-63`), flipped by `58fd482a800`. What IS stale is the counter's operator-facing
description (`src/Common/ProfileEvents.cpp:803`), still asserting "In the current stage this is EVERY
folding round by construction ... the round's destructive gate is shut unconditionally" — written
before the flip (`e337bb2c87d`) and never revisited. Same class as
`{#fsck-rule-restated-in-unfenceable-prose}`: a rule restated in prose no build can check. Fix the
sentence; the pointer to the fold seal's hold set and `tables_held` stays useful.

### `[gc-enabled-false-silent]` `gc_enabled=false` accumulates garbage silently {#gc-enabled-false-silent}

HARD (user settings-policy direction) — Disabling the background GC scheduler produces no ongoing
signal that reclamation has stopped. Add a periodic warning log line plus a metric while
`gc_enabled=false` and the pool has reclaimable debris, so an operator who disabled GC for a legitimate
reason (or by mistake) finds out before the pool grows unbounded.

### `FsckReport::clean` asserts more than the scan checked, including a `--partial` scan (2031-triage CAS-100 / CAS-049 orphan triage) {#fsck-clean-verdict-has-no-coverage-flag}

P3, verdict-honesty only — no scan misclassifies anything, and every skipped family is skipped for a
stated cost reason. What is missing is the machine-readable "this family did not run" companion.

Four families are conditionally skipped or truncated, and `FsckReport::clean()` accounts for none of them:

0. **A `--partial` deadline-truncated scan can still report `clean() == true`.** `FsckReport` carries
   `partial`/`partial_reason` (shipped `15436aa3e07`, 2026-07-06) precisely so a deadline-truncated scan's
   counts are known to cover only what was walked — but `FsckReport::clean()`
   (`Tools/CasFsck.h:252-256`) iterates only `kFsckHardFindings` and never consults `partial`. So a scan
   that hit its deadline with zero counted findings so far still returns `clean()==true`, exactly the
   "false consistency proof" the `--partial` mode's naive use was warned against before the feature
   ships more broadly. *(Formerly tracked separately as `[fsck-partial-degrade-false-consistency]`,
   2026-08-04 orphan triage; merged here as it is the same class of bug.)*
1. The GC-snapshot run read — and with it the whole-file seal-checksum check that produces
   `corrupted_runs` — runs only when the pool has at least one present-but-unreferenced blob
   (`Tools/CasFsck.cpp:815`, checksum compare at `:877`). On a pool with none, `corrupted_runs` reads
   0 without a single run having been read. Mitigation, and why this is P3 rather than higher: the
   deletion-deriving consumers verify the same checksum fail-closed
   (`Gc/CasBlobInDegree.cpp:130`, `:718`, `Gc/CasGc.cpp:4478` → `SourceEdgeRunReader::verifyAgainst`,
   `Formats/CasRecordStreamFormat.cpp:316-322`), so a corrupt run stops GC loudly whether or not fsck
   looked.
2. `stale_edge` is computed only under `detail` (`Tools/CasFsck.cpp:905`), and the SQL path always
   passes `detail=false` (`src/Interpreters/InterpreterSystemQuery.cpp:2599`), so the `stale_edge`
   column is structurally 0 (acknowledged at `:2456-2458`). This one has a documented exception plus a
   compensating soak gate (`Tools/CasFsck.h:220-226`), so only the report-side "was it checked" bit is
   owed, not the gating decision.
3. A `--namespace`-scoped run skips the whole pool-wide physical/pipeline classification
   (`Tools/CasFsck.cpp:719`, scoped branch `:1048-1087`). The CLI help says so
   (`programs/disks/CommandFsck.cpp:29-31`), but the summary line, the report struct and `clean()`
   carry no scope marker, so a scoped run's `reachable=… dangling=0 …` line is byte-shaped exactly
   like a full run's.

Owed, cheapest first: teach `clean()` to fail when `partial` is set (closing case 0 alone removes the
worst failure mode — a truncated scan reporting healthy), then a per-family coverage bit (or one
`checked_families` bitmask) on `FsckReport` for cases 1-3, rendered on the summary line and the SQL row
next to the counters it qualifies. Related: `{#fsck-meta-body-counters-unrendered}` (counters computed
and rendered nowhere) and `{#lifecycle-verbs-wait-out-uncancellable-scans}` (the SQL FSCK passes none of
the CLI's bounding parameters).

### The fsck meta/body pairing counters are computed and rendered nowhere (2031-triage CAS-062) {#fsck-meta-body-counters-unrendered}

P3, observability only — the two counters are ADVISORY by design and correctly excluded from
`FsckReport::clean` (pinned by `src/Disks/tests/gtest_cas_fsck.cpp:1224-1259`), so nothing here is a
missed hard finding.

`runFsck` counts `meta_without_body` and `body_without_meta`
(`ContentAddressed/Tools/CasFsck.cpp:1043,1046`), but no surface prints either one:
`formatFsckSummary` omits both (`Tools/CasFsck.cpp:1155-1174`), `contentAddressedFsckColumns` /
`appendContentAddressedFsckRow` omit both
(`src/Interpreters/InterpreterSystemQuery.cpp:2433-2478,2482-2504`), `programs/disks/CommandFsck.cpp`
never mentions them, and `detail` mode emits no per-object row for them either (the pairing loop only
increments). Outside the gtest, the only reader of these fields is nobody.

That makes the field comment in `Tools/CasFsck.h` (`meta_without_body`: "Counted and reported;
excluded from `clean()`") wrong on its "reported" half, which is exactly the shape
`{#fsck-rule-restated-in-unfenceable-prose}` is about: prose asserting a rendering that no surface
performs. Owed: either render both counters on the summary line (and, if rendered there, on the SQL
row for the same reason the other non-`clean` counters are on it) plus a `detail` row naming the
offending hash, or delete the counters and the comment together. Decide which — a counter no consumer
can read is not an audit signal.

The rest of CAS-062 is not new: the SQL FSCK's missing deadline / scoping / cancellation is
`{#lifecycle-verbs-wait-out-uncancellable-scans}`, per-object keys from SQL is the documented YAGNI at
`InterpreterSystemQuery.cpp:2426-2429` (the `clickhouse-disks cas-fsck --detail` applet is the
per-object surface), and "no repair path" is `gc.md`{#ckpt-damage-no-repair-path} for `_ckpt` — while
`SYSTEM CAS GC REBUILD` (`InterpreterSystemQuery.cpp:2545`) already is the repair path for the
in-degree/`stale_edge` class.

### Byte accounting covers only blob bodies, and `previewDeletes` reports two different size units in one column (2031-triage CAS-123) {#byte-accounting-blobs-only-and-preview-size-units}

Two P3 observability residuals, neither of them a correctness issue.

**No per-object-class byte accounting.** `FsckReport::physical_bytes` is summed exclusively from the
`blobs/` listing, and even there only from BODY keys — `.meta` siblings are split out of
`present_blobs` before the sum (`Tools/CasFsck.cpp:727-744`, plus the two HEAD top-ups at `:759` and
`:1071`). Nothing anywhere sums the bytes of manifests, ref logs, ref snapshots, `_ckpt`, run files,
fold seals, GC state, or staging keys. The only non-blob byte counter in the whole surface is
`namespace_janitor_pending_bytes` (`Tools/CasFsck.h`, rendered at `Tools/CasFsck.cpp:1166` and exposed
as a SQL column at `src/Interpreters/InterpreterSystemQuery.cpp:2497`), which covers one debris class
only. The GC log has no byte columns at all — every counter in
`ContentAddressedGarbageCollectionLogElement` is an object count
(`src/Interpreters/ContentAddressedGarbageCollectionLog.h:32-43`), so "how many bytes did this round
reclaim" is not answerable from `system.content_addressed_garbage_collection_log` either. Consequence:
a bucket-total-versus-`physical_bytes` gap cannot be attributed to any object class. This is the
general form of the narrow case already named under `{#mpu-and-probe-debris-unaccounted}` (incomplete
MPU parts and `_probe/` debris); that item's owed doc line should say which classes `physical_bytes`
covers, and the cheap fix here is a per-prefix byte breakdown in the fsck report (it already walks
every plane) plus a `bytes_deleted` column on the GC-round row.

Related but distinct, and NOT owed here: there is no per-table reclaim forecast ("how much would
dropping table X free"), and on a deduplicating pool that number is not even well defined without
naming the sharing model (unique-to-this-table bytes vs its share of shared blobs). If it is ever
wanted, it needs a spec first, not a counter.

**`previewDeletes` mixes physical and logical sizes.** `Gc::PreviewEntry::size` carries the raw HEAD
size for zero-in-degree candidates (`Gc/CasGc.cpp:4608-4680`, envelope-inclusive) but the condemned
row's stored size for retired-in-snapshot rows, and that stored value was written through
`retiredLogicalSize` — i.e. already payload-only, `object_size - blob_header_len`
(`Gc/CasGc.cpp:286-294`, applied at `:1881`,`:1913`). `clickhouse-disks cas-gc-dryrun` prints that
column raw (`programs/disks/CommandCaGcDryRun.cpp:47`), so summing it mixes units by
`blob_header_len` (256 by default, `Pool/CasPool.h:54`) per condemned row. Small in absolute terms and
the command is documented diagnostic-only, but it is a one-line fix: either subtract the header in the
zero-in-degree branch too, or carry both fields.

Not defects in the same tool, for the record: rows the round will not delete are labeled in the
`reason` column (`unreachable` / `awaiting_graduation` / `delete_pending`), the non-quiescence
over-report is documented at the API (`Gc/CasGc.h:453-457`), and no blob is double-counted — the fold
emits at most one sentinel row per blob (`Gc/CasBlobInDegree.cpp:573-589`) and `zeroInDegree` skips
`kCondemned` rows (`:706-708`).

### `system.content_addressed_log` stamps time, `thread_id` and `query_id` on the DRAINING thread, not the emitter (2031-triage CAS-131) {#cas-log-drain-thread-attribution}

`CasEvent` is pure data with no time or caller identity fields (`Primitives/CasEvent.h:61-75`), and the
three columns that answer "when, and who did this" are filled inside the sink, at delivery:
`ContentAddressedMetadataStorage.cpp:580-581` (`event_time`/`event_time_microseconds` from
`system_clock::now()`) and `:594-595` (`e.thread_id = getThreadId(); e.query_id =
CurrentThread::getQueryId();`).

Delivery is not guaranteed to run on the emitting thread. `EventDispatcher::emit` enqueues under the
mutex and, if another thread is already draining, returns immediately — the queued event is delivered
by that other thread's loop (`Pool/CasEventDispatcher.cpp:19-30`, and the drain loop at `:31-52`
which releases the lock around the sink call). The same hand-off happens for an emission made from
inside a sink callback (the documented reentrancy case, `Pool/CasEventDispatcher.h:20-23`). So under
any concurrency — two queries writing, a query plus the GC/keeper background threads, the intra-part
upload fan-out the dispatcher was introduced for — a row can carry the `thread_id` of an unrelated
thread and the `query_id` of an unrelated query (or none), while its timestamp is the delivery
instant rather than the decision instant.

Nothing is corrupted and no path fails; the cost is that the audit table's own attribution columns
cannot be trusted for exactly the correlate-with-`system.query_log` triage they exist for. Fix
direction: stamp at emission — capture the three values in `EventDispatcher::emit` (or in the
emission helpers) into new `CasEvent` fields, and have the sink copy them instead of sampling its own
thread. Cheap, and it also makes `event_time` mean "when the decision happened", which is what every
existing analysis assumes.

Checked while triaging, NOT defects: the deliberate skip of the `RefResolve` row on a warm
`CachedForLoad` view-cache hit (`Parts/PartFolderAccess.cpp:161-186`, contract stated in
`Parts/PartFolderAccess.h:394` and `Pool/CasRefLedger.h:33`) — the hit is reported by
`CASPartFolderViewHits`; and the part-folder cache counters, which are all plain counts with
descriptions matching the code (`src/Common/ProfileEvents.cpp:919-924,943`), with the bytes/entries
gauges kept as separate `CurrentMetrics` (`src/Common/CurrentMetrics.cpp:234-235`).

### `[B15/B99/B169/B159]` `system.*` views for pool/blob/part refcounts {#b15-b99-b169-b159-system-views}

HARD (PARTIAL) — GC log + event log + `content_addressed_mounts` + ca-fsck/dryrun/rebuild/ca-inspect
CLI done; per-part/ref `system.*` views + a top-down decode/traversal surface not yet built. Only
`StorageSystemContentAddressedMounts.{h,cpp}` exists today; no new `StorageSystemCas*` file has been
added on either branch. (INTROSPECTION-1/2 close signals.)

### GC observability field list needs an overlap check before anything new is built (2026-08-04 orphan triage) {#gc-observability-field-list}

DESIRABLE — heartbeat lag, B170 event classes, retired-list age, an invariant alert: a concrete
dashboard/metrics gap, partially covered by `system.cas_gc_log` already — check the overlap before
building anything new; only the uncovered fields are the real ask. No new columns have been added to
the GC log since this was filed.

## Metrics and events {#metrics-and-events}

### CAS profile events need a reader each: 184 `CAS*` events out of 1611, audit before release (2026-09-16) {#cas-profile-events-audit}

`src/Common/ProfileEvents.cpp` carries roughly 183 events with the `CAS` prefix (the original count of
184 has drifted by one since filing — immaterial). Many were added as instruments of one investigation
and kept afterwards as if they were a monitoring contract (the six `CASRelinkConfirmRefused*` counters
from F11 were meant to be the worked example — see the closing checklist in `gcs.md`, which proposes
merging them 7 → 4 — but that merge was never executed: `gcs.md:170` still reads "Proposal: 7 → 4", and
the family is unchanged at 5 events after an unrelated rescoping commit, not a count merge). Names
become a contract the day someone builds a dashboard on them, so the cheap moment to prune is before
the first release.

A live production audit independently reconfirms the scale of the problem at the roadmap-tracking
level: `docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md`{#f25} found "99 of 184
counters never moved... 54% of the metric surface is dead weight on a dashboard", and
`docs/superpowers/cas/umbrella-roadmap.md` §3 now carries "99 of 184 counters never move; `CASServer*`
is unreachable code" as a live roadmap bullet. Neither is the systematic family-by-family audit this
item asks for — they're a spot count from live data, not a pruning pass with a named reader per
surviving event. This item, and its audit method, is still the right place to do that work.

**Rule to audit against.** An event stays if it names an operating state that recurs in normal use, is
distinguishable only through it (not from a log line, a system table or an existing counter), and
changes what an operator does. Misconfiguration, protocol errors and bugs are log lines, not counters.
Developer-facing splits (which sub-branch of a refusal, which retry attempt) collapse into one counter
plus a trace line carrying the detail.

**How.** One pass over the ~183 by family (ref ledger, GC phases, requests/retries, relink, cache,
janitor): for each, the named reader (dashboard, alert, test assertion, triage recipe) or the verdict
"merge into X" / "drop". Output: a table in this file, then one PR per family, renames and removals
together so downstream names change once. Check `tests/integration` and `tests/queries` for asserted
names before removing any. Expected result on the order of a third fewer events; the number is not the
goal, the reader-per-event property is.

Related: `gcs.md` F11 closing checklist item 3 (still unexecuted); `BACKLOG/gc.md`'s
`{#janitor-page-hardcoded}` (GC phase events are the other large family); `umbrella-roadmap.md` §3 and
report `{#f25}` for the live-data corroboration.

### ProfileEvents surface residuals {#profileevents-surface-residuals}

One small, non-correctness residual remains in the CAS `ProfileEvents` surface. The former blob
presence-cache/accounting half is closed below for provenance.

**(a) The `CASServer*` row is unreachable, so eleven shipped counters are permanently zero.**
`classifyCasNs` returns only `Blob`, `Manifest`, `Root`, `Gc` and `Other`
(`ContentAddressed/Backend/CasInstrumentedBackend.cpp:113-130`); `CasNs::Server`
(`CasInstrumentedBackend.h:33`) still occupies a row of `cas_event_table`
(`CasInstrumentedBackend.cpp:103-106`), and the eleven `CASServer*` events it points at are declared
with operator-facing descriptions in `src/Common/ProfileEvents.cpp:844-854`. The per-server control
subtree moved under `<prefix>/gc/server-roots/<srid>/` (`Formats/CasLayout.h:388-417`), so owner
claims, epoch bumps, mount-lease claims and heartbeat renewals all classify as `Gc` — deliberately,
per the classifier cleanup in `44e41878ff0`, and documented at `CasInstrumentedBackend.h:18-20` and
pinned by `src/Disks/tests/gtest_cas_backend.cpp:304-308`. The residual is the user-visible half: an
operator reading `system.events` sees eleven documented counters that can never move — independently
reconfirmed live by report `{#f25}` above and now also carried as a roadmap bullet
(`umbrella-roadmap.md` §3: "`CASServer*` is unreachable code"). Close it by either deleting the
`Server` row plus its eleven descriptions, or classifying `/gc/server-roots/` as `Server` before the
`/gc/` rule (which would move mount/lease/epoch traffic out of the `CASGC*` counters — a
dashboard-visible change, so it needs a deliberate call). Mount and lease activity itself is not
unobservable meanwhile: `system.content_addressed_log` carries
`MountClaim`/`MountRelease`/`MountConflict`/`WatermarkRenew`/`GcLease*`
(`Primitives/CasEvent.h:30-33`) and `system.content_addressed_mounts` exposes the slots.

**(b) ✅ CLOSED by the unconditional blob-publication rewrite.** Confirmed at HEAD (`940b1685bf9`):
the presence cache and its cache-only events were deleted. `CASBlobBodyPutAvoided` now increments only
after mandatory blob `HEAD`, size validation, metadata classification, and fence checks have
established a safe present observation (`Pool/CasPartWriteTxn.cpp:399-445`). A `Condemned` body
proceeds to unconditional publication without incrementing the avoided-body event.

### `[blob-reuse-resurrect-no-emitter]` `BlobReuseResurrect` has no emitter, and the condemned-token re-upload has no positive test {#blob-reuse-resurrect-no-emitter}

**Found while triaging an S16 soak failure during the wire-keys proof phase (2026-08-30).**

`CasEventType::BlobReuseResurrect` is still declared in `Primitives/CasEvent.h` and still maps to the
string `blob_reuse_resurrect` in `CasEvent.cpp`, but nothing in `src/` raises it — only
`BlobReuseAdopt` is emitted, from two sites in `Pool/CasPartWriteTxn.cpp`. `git log -S` puts the
removal at `907c3b5ce7d` ("Publish CAS blobs after mandatory `HEAD`"): once publication became
unconditional after a mandatory `HEAD`, a writer no longer splits reuse into adopt versus resurrect,
because it always re-uploads from source.

Two separate things are left over.

**A dead enum member.** It costs nothing at runtime, but it makes the event vocabulary lie: a reader of
`system.cas_log`'s event set will look for a value that can never appear, which is exactly the trap
S16 fell into — its verdict required the event and so could not pass at any scale.

**A real coverage gap, which is the part that matters.** S16 was the only positive check that a
CONDEMNED token specifically forces a re-upload rather than a revival. Its assertion has been replaced
with one that requires reuse to happen at all, so the resurrect invariant is now guarded only by S16's
proxy — correct data on every cycle plus no bad CA events. That proxy is genuine but negative: it
would catch a revival that corrupted data or raised a bad event, and would miss one that happened to
return the right bytes. Restoring a direct check means finding an observable that distinguishes
"re-uploaded from writer-owned source" from "revived from the condemned object" under the current
architecture — a counter, an event, or a fault-injected condemned object that must not be readable.

### `[gc-anomaly-never-emitted]` `CasEventType::GcAnomaly` is defined but never emitted {#gc-anomaly-never-emitted}

MINOR — Found during the deep-verification batch (batch-006): the event type exists in the enum but no
call site constructs one, so any doc or dashboard describing GC-anomaly events as observable is
currently wrong. Either wire an emit site or remove the dead enum value. (An orphaned 2026-08-04-triage
finding on catalog/fold-seal capacity-reservation correctness is adjacent to this GC-observability gap
— folded in as a related note, not a separate item.)

### `[ca-event-log-loses-gc-manifest-deletes]` GC deleted 517 manifests and the CA event log recorded one {#ca-event-log-loses-manifest-deletes}

**Found while auditing S10's leftover-manifest finding in `cas_log` (2026-08-31).** This is why that
audit could not be done.

In the full-scale S10 run, `ch1`'s `gc_log` sums to exactly **517** `manifests_deleted` across its
rounds — round 6 alone deletes 133 — while `ch1`'s `cas_log` holds exactly **one**
`manifest_delete` event. `ch2` deleted none and logged none, so the whole discrepancy sits on one
node.

It is not a semantics mismatch between the two numbers. `CasGc.cpp` PHASE 15/18 emits
`CasEventType::ManifestDelete` **unconditionally, once per attempt**, inside the `mf_cleanup_now`
loop, and increments `report.manifests_deleted` only when the outcome classifies as `Deleted`. So
attempts are greater than or equal to deletions, and the event count must be **at least** 517.
It is 1.

One event did land — round 1, for a namespace owned by the other server root — so the emitter is
wired and reachable. That argues for events being dropped or lost rather than never produced.
Candidates not yet distinguished: a bounded queue in the event sink discarding under burst (round 6's
133 deletions in one phase is exactly a burst), a per-call `EventEmitter{*store}` binding to a sink
that is not the disk's configured log on most calls, or rows buffered in `ca_event_log` and lost when
the scenario tears the cluster down.

**Consequence for anything that reads `cas_log`:** manifest reclaim is effectively invisible there,
so `cas_log` cannot support any claim about whether a manifest was deleted, retried or skipped. The
S10 residual finding (`BACKLOG/performance.md`{#s10-manifest-residual}) must be re-derived from
`gc_log` and `fsck` until this is fixed.

**Update, architecture changed underneath it.** The manifest-delete phase was later rewritten to batch
write-once deletes (`CasGc.cpp:1105-1141`, via `removeChunkWriteOnceOrOneByOne`) and now emits exactly
one `ManifestDelete` event per entry in the same loop that increments `report.manifests_deleted`
(`:1131-1141`) — structurally this removes the described "unconditional per-attempt emit, incremented
only on `Deleted`" defect. No commit or test asserts event-count == `manifests_deleted` parity though.
Recommend one soak rerun comparing `cas_log` `manifest_delete` counts against `gc_log.manifests_deleted`
before calling this DONE.

**First step (if rerun is not yet done):** count events against `attempted` rather than against
`manifests_deleted` — the phase already records `attempted` as a metric — and check whether
`system.ca_event_log` shows drops of its own before looking for a bug in the emitter.

### Terminal counters are undocumented and logged below their own severity (opus review M3) {#terminal-counters-undocumented-and-warned}

Both halves reproduce verbatim at HEAD: none of the seven terminal counters appears anywhere in
`docs/`, and `CASIdentityLost`/`CASDataRootVanished` — states the code itself calls TERMINAL — are
emitted at `LOG_WARNING` (`Pool/CasMountRuntime.cpp:971-972`, `:1045-1046`), while the documented
alerting example keys on ERROR (`CASMountExclusivityViolation`). So the two signals that mean "this
mount is finished" are both invisible in the docs and below the severity an operator is told to alert
on. Fix: document the terminal family in `monitoring.md` and raise those two to ERROR. P2.

## Logging {#logging}

### `putIfAbsentControlled`'s successor discards the exception that decided the attempt's outcome (2031-triage CAS-068) {#putifabsent-swallowed-attempt-cause}

The byte-exact ref/manifest lane's classification point was `Backend/CasRequestControl.cpp:358-361`
at filing time; that file and the `putIfAbsentControlled` function it named were both deleted by
`c3f7b20f8ab` (2026-09-05, "rename Incarnation to Etag/Dialect, migrate the gtest suite onto the
engine, delete the old controller", an ancestor of both branches). The defect class survives in the
successor `Backend/CasRequests.cpp`: the per-attempt classification catches (`catch (const Exception &
e)` / `catch (const std::exception & e)` around lines 756-767, 782-793, and the write-attempt catch
around lines 990-1000) still classify (`isDeterministicLocalFailure`) without logging either way. This
is the DELIBERATE half of the `isDeterministicLocalFailure` decision (`4f4f93c6bc6`: this lane is
"deliberately unchanged", because its resolve-by-identical-bytes makes retrying any unproven error
harmless), and it is not a correctness item: every exit is fail-closed (`resolveByExactGet` throws
`CORRUPTED_DATA` on a genuine different-object conflict). What is missing is the diagnosis: on a lane
that wedges after burning `max_attempts`, the wedge message names only the wedge reason — the actual
per-attempt failure (socket error, timeout, S3 code, or a deterministic local bug the sibling ops would
have rethrown) appears in no log line at all; `describeUnresolvedReason`, the function the wedge
message used to be cited by name, is also gone from `CasRefLedger.cpp` in the same rename. Owed: a
rate-limited `LOG_DEBUG`/`LOG_WARNING` of the classified exception at the classification point — the
reusable rate limiter (`logCasWriteRetryLater`, now at `CasRequests.cpp:85-94`) is wired to a different
"give up entirely" path (`throwCasWriteRetryLater`), not into any of these classification catches, so
reusing it here is still the natural shape. Same class as `{#fsck-meta-body-counters-unrendered}`: a
signal computed and then not shown to anyone.

### The audit-event sink is installed even when `system.cas_log` is disabled, so the "disabled path is free" promise does not hold (2031-triage CAS-104) {#cas-event-sink-installed-when-log-disabled}

`makeCasEventSink` returns a non-empty `std::function` whenever the metadata storage has a `Context` —
its only early exit is `if (!context) return {}`
(`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.cpp:562-571`),
and the "is the log actually there" question is answered INSIDE the sink
(`:573-575`, `auto log = ctx->getContentAddressedLog(); if (!log) return;`). But
`createSystemLog` returns an empty pointer when the config section is missing
(`src/Interpreters/SystemLog.cpp:135-142`), and removing the section is the documented way to turn the
log off (`programs/server/config.xml:1197-1199`, "Remove the section to disable"). So with the log
disabled, `Pool::hasEventSink()` (`Pool/CasPool.h:784`) still answers `true`, every emission site still
builds a full `CasEvent` (7 `String`s plus a `std::map`, `Primitives/CasEvent.h:180-194`), still pays
the dispatcher's queue mutex (`Pool/CasEventDispatcher.cpp:19-22`), and the row is dropped at the very
end.

That contradicts two claims the code makes about itself: "disabled hot path still skips constructing
events entirely" / "a true no-op on the production hot path" (`Pool/CasPool.h:769-770,782-784`) and
"the query-frequency disabled hot path pays no mutex" (`Pool/CasEventDispatcher.h:99-101`). Nothing is
corrupted and no path fails — it is wasted work on paths that also do object-storage round trips, which
is why this is P3 and not a gate.

Fix direction: ask `context->getContentAddressedLog()` when BUILDING the sink and return an empty
`std::function` when the log is absent, so `hasEventSink` is again a truthful "delivery enabled"
predicate. The honest version of that needs the sink to be re-derivable on config reload, which is the
same missing plumbing as `{#cas-settings-not-reloadable-silently}` — a one-shot check at pool open is
still strictly better than today, because the disabled case is a static config choice in practice.

Related, and NOT this item: the per-emit event VOLUME (independent of whether the log is on) is already
tracked as `{#ca-log-tables-restart-cost}` in `gc.md` and in the audit-row note inside
`{#standalone-write-scratch-manifest-cost}` in `performance.md`, and now also as the "`cas_log` volume"
bullet in `umbrella-roadmap.md` §3 (8M rows/day, 10% of write bytes, per report `{#f25}`-adjacent
section F22). The dispatcher itself does not serialize delivery under its mutex — it releases the lock
around the sink call (`Pool/CasEventDispatcher.cpp:36-51`), and the sink is the never-blocking
`SystemLog::add` (`src/Common/SystemLogBase.cpp:93-123`) — so there is no read-path mutex bottleneck to
track here.

### Writer-cleanup duty has exactly one drain path (opus review M12) {#writer-cleanup-single-drain}

The only drain is the `mutateRefsAfterWriterCleanup` seam, taken before the next durable ref mutation
of the SAME namespace (call sites now in `Pool/CasPool.cpp`'s dropRef/publishRef/repointRef delegates,
~lines 1990-2060; line numbers have drifted from the original 893/956 with unrelated code growth).
Neither a GC round, nor mount, nor FSCK, nor the background snapshot publisher drains it; teardown
only OBSERVES the debt (`drained = ref_lanes_drained && !writerCleanupDutiesPending();`, now at
`Pool/CasPool.cpp:1028`,`:1183`). So a namespace that stops being written keeps its cleanup duty
indefinitely — bounded in practice only by a process restart, since `prefixEligible` compares
`writer_epoch` first (`Gc/CasOrphanManifestSweep.cpp:485-489`). Sub-finding: the unclean-farewell
WARNING (now at `Pool/CasMountRuntime.cpp:1144`, was 529-530) still blames "an unresolved ref-log PUT"
even when the real cause is an undrained cleanup duty — misleading at exactly the moment an operator
reads it. P2.

### `deleteFilesFromS3`'s generic per-key error classification still loses the error class {#delete-files-from-s3-generic-error-classification}

**Found 2026-09-08, while fixing the CAS batch delete.** `src/IO/S3/deleteFileFromS3.cpp` classifies
each per-key error of a `DeleteObjects` reply with `S3ErrorMapper::GetErrorForName` alone, which knows
only S3-specific names (`NoSuchKey`, `NoSuchBucket`, ...); a service-wide name such as `AccessDenied`
comes back `UNKNOWN`. Message and code text stay right, only the `S3Errors` enum callers may match on
is wrong. The CAS path got a two-step lookup (S3 mapper, then `Aws::Client::CoreErrorsMapper`, the same
order `S3ErrorMarshaller::Marshall` uses) in `13bf6a92df0`; the generic path is shared upstream code,
out of the CAS PR's scope → separate small fix, upstream-worthy.

### `[control-object-generic-s3-error]` A transient control-object failure surfaces as a generic `S3_ERROR` with no indication of which object or operation was involved {#control-object-generic-s3-error}

DOC/MINOR. A transient failure on a control-object read/write (catalog, `_ckpt`, `gc/state`) surfaces
to the user as a generic `S3_ERROR` with no indication of which CAS control object or operation was
involved. Makes CI/production triage slower than it needs to be (see the `ref_catalog`-unretried
incident this was originally paired with, now fixed on the retry side). Fix: wrap or annotate the
exception at the CAS control-object call sites with the object class and key before it escapes to the
query result.

Source: `docs/superpowers/cas/umbrella-backlog.md` line 4 (untracked draft, file deleted by the u22
consolidation pass). Placement (Logging section, near `[delete-files-from-s3-generic-error-classification]`
above) is a best-fit judgment call by the applier — the source proposal did not state a target file.

## SQL commands and CLI tools {#sql-commands-and-cli-tools}

### `SYSTEM` control surface — `POOL READONLY` is the remaining gap {#b197-system-control-surface}

GATE — `SYSTEM CAS GC STOP`/`GC START`/`FSCK`/`FORGET`/`DROP POOL MEMBER` are real, access-controlled
SQL verbs today (`src/Interpreters/InterpreterSystemQuery.cpp:1045-1096,2649-2705`,
`AccessType.h:357-361`, landed via `4fb813a9988` and `77535622b8c`) — GC stop is no longer only a
soak-harness workaround, and `CHECK` is satisfied by `SYSTEM CAS FSCK`. What remains: no
`POOL READONLY` verb exists on either branch.

### Lifecycle verbs wait out an uncancellable GC round or FSCK scan (2031-triage CAS-049) {#lifecycle-verbs-wait-out-uncancellable-scans}

P2, operability only — nothing is corrupted or lost, and no data path is blocked
(`poolAccess`/`gcHealth` never take these mutexes; the snapshot at
`ContentAddressed/ContentAddressedMetadataStorage.cpp:432` is explicitly forbidden from waiting behind
`gc_scheduler_mutex`). What is missing is cooperative cancellation:

- **Server shutdown and the storage destructor are DONE**
  (`docs/superpowers/cas/history/2026-09-04-cas-gc-teardown-stop-design.md`{#cas-gc-teardown-stop-design},
  status "IMPLEMENTED, rev.5"): both arm the pool's teardown flag before the lock or join they would
  otherwise wait behind, the open request plane carries that flag as its fence, and a round in flight
  is refused at its next request. What remains of this item is `SYSTEM CAS GC STOP` and
  `SYSTEM CAS FORGET`.
- A GC round still has no stop hook for those two verbs, confirmed unchanged at HEAD on both branches:
  `CasGcScheduler::stop` (`Gc/CasGcScheduler.cpp:101-116`) sets `stopping`, notifies `wake`, and then
  `join()`s — an in-flight round runs to completion. Neither verb may use the pool-wide teardown flag:
  `GC STOP` must not refuse an unrelated `system.content_addressed_mounts` query or a running FSCK,
  and FORGET cannot arm at all, because an already-latched self-remount completes one more step whose
  pool-identity probe is admitted on that same plane, and the reclaim FORGET's second `tripMountLost`
  exists to override would then never happen. Both need round-scoped liveness instead. The round's
  destructive/recovery work IS capped (`GcRoundWorkBudget`, `Gc/CasBlobInDegree.h:251`, filled from the
  non-zero defaults at `ContentAddressedSettings.cpp:76-83`), so the wait is finite — but there is no
  time budget at all in the settings, and against a slow bucket the wall-clock wait is whatever the
  bucket makes it.
- `SYSTEM CAS FSCK` still passes none of the bounding parameters the CLI passes and cannot be killed,
  confirmed unchanged: `runFsckNow` calls `Cas::runFsck(*store(), detail)`
  (`ContentAddressedMetadataStorage.cpp:1170-1183`) while holding `lifecycle_mutex` for the whole scan,
  whereas `programs/disks/CommandFsck.cpp:67` passes `on_progress`, `deadline`, `partial` and
  `namespace_prefix` (signature: `Tools/CasFsck.h:269-271`). The scan checks no query cancellation
  either, so the statement (`src/Interpreters/InterpreterSystemQuery.cpp:2599`) ignores `KILL QUERY`
  and `max_execution_time`, and `FORGET`/`GC STOP`/`GC START` block behind it for the duration.
  Serializing them against FSCK is deliberate (see the comment at `:1052-1053`); being unable to bound
  or interrupt the scan is not.

Owed: a stop token threaded through the round phases, and a SQL FSCK that derives a deadline from
`max_execution_time`, sets `partial_on_deadline`, and polls query cancellation — and once it does,
`{#fsck-clean-verdict-has-no-coverage-flag}` case 0 (`clean()` ignoring `partial`) becomes load-bearing
rather than latent. Related and already tracked: `gc.md`{#fsck-scale-timeout} (fsck does not finish on
a ~30 GiB pool).

Corrected while triaging: `shutdown` does NOT serialize behind an in-flight FSCK — it takes
`gc_scheduler_mutex` and `pointer_mutex` only, never `lifecycle_mutex`, and the pool stays alive
through the `shared_ptr` the scan holds. Its only wait was on a GC round; that wait is now bounded by
one request, and the "clean GC completion over fast shutdown" priority it used to document is
reversed.

### `[damaged-object-diagnose-and-repair]` fsck must diagnose AND repair a damaged rebuildable object; the runbook must say how {#damaged-object-repair}

**Found by the T8 criterion-4 injection** (Stage-B soak; evidence pack
`.superpowers/sdd/2026-08-02-cas-stage-b-remaining/crit4-injection-evidence/`): a single namespace
checkpoint (`cas/ns/state/<life>/_ckpt`) was overwritten with garbage under a live writer. The GC fold
behaved exactly as designed — it detected the damage, classified the namespace as an anomaly/hold and
suppressed every irreversible family, round after round — but nothing in the product ever repaired the
object, and the live ref lane went to `CASRefNeedsRecovery` and stayed there for the remaining ~20
minutes of the run, including after the exact original bytes were restored. Byte-level damage to a
durable object is outside the trusted-store fault model this design assumes, so this is not a
correctness defect; it is an OPERABILITY hole: the system fails closed forever and hands the operator
no lever.

**What is missing, in priority order.**

1. **`ca-fsck` should diagnose the class precisely.** Today a damaged object surfaces as a suppressed
   GC round plus a counter; fsck's report has no row that says "namespace N's checkpoint is present but
   undecodable" (as distinct from absent, which is a legal cold-recovery state). Add the distinction:
   *present-and-undecodable* vs *absent* vs *decodable-but-inconsistent*, per affected object kind
   (`_ckpt`, fold seal, `gc/state`, catalog), naming the exact key.
2. **`ca-fsck --repair` (or an explicit sibling verb) should REBUILD what is rebuildable.** The
   checkpoint is a derived accelerator over the durable ref-log, so a damaged one is reconstructible by
   the same recovery walk the writer already implements (`recoverRefTableDetailed` / the recovery-epoch
   seal). The repair verb should: re-derive the object from its authoritative source, publish it by the
   ordinary CAS write path (no new object kinds, no protocol change), and refuse — loudly — for any
   object whose content is NOT derivable (a blob body, a committed ref-log record: those are the real
   data, and their loss is a restore-from-backup situation, not a repair).
3. **The lane must be able to leave `NeedsRecovery` once the source is sound again.** Our single
   observation says it did not, even after byte-identical restore. Whether that is a wedge, a
   remount-only exit, or an artifact of the injected shape is UNVERIFIED — determine it, and if the only
   exit is a remount, say so in the runbook and consider making recovery retry on its own.
4. **Runbook section: "a CAS object is damaged".** Operator-facing, in the numbered doc set, covering:
   how the condition ANNOUNCES itself (suppressed rounds naming the namespace, the fsck row from item 1,
   the `CASRefNeedsRecovery` counter); why there is no urgency (GC has already frozen everything
   irreversible — the pool is safe, it is just not reclaiming); the asymmetry an operator must know
   (an ABSENT checkpoint is a legal state that triggers cold recovery, a CORRUPT one is not — so the
   fallback of last resort is to DELETE the damaged derived object, never to hand-edit it); the repair
   sequence once item 2 exists; what NOT to do (`DROP POOL MEMBER` is for dead members, not damaged
   data; never hand-delete blob bodies or ref-log records; never "restore" bytes from an unofficial
   copy); and when the answer really is backup/restore because the damaged object is authoritative.

**Note on scope.** Items 1, 2 and 4 are operability work and need no protocol change. Item 3 may reveal
a real recovery-path defect; treat its outcome as its own item if so.

### Every CA CLI/DR verb opens the pool through `_pool_meta`, so damage to that one object disables the instruments {#pool-meta-bootstrap-blocks-dr-tools}

Sub-item of [`[damaged-object-diagnose-and-repair]`](#damaged-object-repair) (2031 triage,
CAS-061), naming the one object kind
that item's list (`_ckpt`, fold seal, `gc/state`, catalog) does not: `_pool_meta`. All five CA tools
(`cas-fsck`, `cas-inspect`, `cas-gc-dryrun`, `cas-gc-rebuild`, `cas-drop-member`) reach the pool only
via `ca->store()`, i.e. via `Cas::Pool::open`, which ends at
`PoolMeta::createOrValidate(..., allow_mint=!read_only)` (`Pool/CasPool.cpp:494-496`,
confirmed unchanged at HEAD, both branches). A read-only open — which every tool is required to use
(`programs/disks/CommandFsck.cpp:54`, `CommandCaInspect.cpp:48,51-55`, `CommandCaGcRebuild.cpp:54`,
`CommandCaGcDryRun.cpp:38`, `CommandCaDropMember.cpp:47`) — must never mint, so an absent `_pool_meta`
fails closed (`Pool/CasPoolMeta.cpp:143-146`) and an undecodable one throws out of `decodePoolMeta`
(`Formats/CasPoolMetaFormat.cpp:105-117`). Consequence: the single damaged object locks out even
`cas-inspect`, whose only use of the pool is a raw-key `GET` plus `Layout` (`CommandCaInspect.cpp:52-56`)
and which therefore does not need pool metadata at all. No pool-meta-less "raw open" mode exists yet.

Owed, in increasing cost: (a) let `cas-inspect` (and fsck's diagnose-only mode) work off a
pool-meta-less "raw backend + layout" open so the operator can read the damaged bytes; (b) an fsck
row that distinguishes `_pool_meta` present-and-undecodable from absent; (c) decide whether
`_pool_meta` is repairable at all — `pool_id` is a random u128 minted at creation, so it is
restorable from a backup copy of the object but not derivable, which makes the honest answer for
(c) "restore, not repair" and belongs in the runbook item 4 of `{#damaged-object-repair}`.

### `cas-inspect` decodes 10 of the 17 live object formats, and drops the fold seal's hold (2031-triage CAS-097) {#cas-inspect-format-coverage-and-hold}

P3, observability only — every gap is fail-loud (`BAD_ARGUMENTS`/`CORRUPTED_DATA`), never a silent
mis-read of a decodable object. Confirmed unchanged at HEAD, both branches.

`caInspectToJson`'s dispatch (`ContentAddressed/Tools/CasInspect.cpp:484-548`) has a branch for
`PartManifest`, `RefCkpt`, `RefLog`, `RefSnapshot`, `GcState`, `MountLease`, `FoldSeal`, `RunFile`,
`BlobMeta` and the blob envelope — still 10 branches. Seven live formats have no branch and land on the
closing `BAD_ARGUMENTS`: `PoolMeta` (`_pool_meta`), `RefCatalog` (`cas/ref_catalog`),
`GcMaintenanceState` (`gc/maintenance_state`), `GcHeartbeat` (`gc/hb`), `GcOutcomes`, `Owner` and
`ServerEpoch` (`Formats/CasFormat.h:98-128`; `Roster` is reserved and never written,
`Formats/CasFormat.cpp:83`, so the live set is 17, not 18). Two of them — `cas/ref_catalog` and
`gc/maintenance_state` — are exactly the control objects an operator reaches for when GC or a
namespace lifecycle is stuck.

Two rendering residuals inside the branches that DO exist: (a) `renderRefCoverage`
(`Tools/CasInspect.cpp:309-315`) renders `classification` and `last_folded_ref_id` but still omits
`RefCoverage::hold`, which by the format's own strict grammar is present iff `classification == 4`
(`Formats/CasFoldSealFormat.h:128-146`) — so a fold-seal dump shows `"classification":4` with no
reason and no offending position, dropping the one field that says WHY the fold refuses to advance
past that life; `src/Disks/tests/gtest_cas_inspect.cpp:298-316`
(`RendersCoverageClassificationWireWords`) DOES construct a `Clamped` row with a real `hold` value but
only asserts on the 4 `classification` strings, never on `hold` appearing in the JSON — the gap is
untested, matching the original finding exactly. (b) Sentinel and enumerated values render as bare
numbers — `"classification":4`, and a never-folded cursor as `{"writer_epoch":0,"ref_sequence":0}`
(`renderRefTxnIdObj`, `Tools/CasInspect.cpp:124-129`) — so the reader must know the encoding.

Also real but harmless: a `cas/ns/state/<life>/_files/<name>` key falls through the namespace-state
branch (only `parseRefCkptKey` is tried, `Tools/CasInspect.cpp:501-506`) and, for the two names `mount`
and `fold_seal`, is caught by the suffix branches (`Tools/CasInspect.cpp:527-531`) before the closing
throw, so it is decoded as the wrong format. The outcome is a `CORRUPTED_DATA` decode error instead of "unrecognized key layout" —
a worse message, not a wrong answer; a `parseNamespaceFileKey` branch (or a namespace-state throw
before the suffix checks) closes it.

Two claims from the source finding do NOT hold. Raw pool keys ARE enumerable: `cas-fsck --detail`
prints `class\tkey\tsize` per object (`programs/disks/CommandFsck.cpp:118-138`), which is exactly
what the shipped runbook tells the operator to feed `cas-inspect`
(`docs/en/antalya/cas/operations/debugging.md:185`), and the fixed control-object keys are documented
in `docs/en/antalya/cas/architecture/storage-layout.md`. And a wedged ref lane IS nameable at the
moment it wedges: the writer's own error names the namespace and the txn id
(`Pool/CasRefLedger.cpp:2358-2362,2371-2375`) alongside `CASRefAppendWedged`. What is missing is a
QUERYABLE surface listing the currently-wedged namespaces —
`content_addressed_mounts.wedged_namespace_count` is an aggregate
(`src/Storages/System/StorageSystemContentAddressedMounts.cpp:55`, from `wedgedRefLaneCount`,
`Pool/CasRefLedger.cpp:1958-1975`, whose own map is keyed by namespace) — which is the
`per-part/ref system.* views` half of `{#b15-b99-b169-b159-system-views}`.

### [disks-exit-code-upstream] `clickhouse-disks --query` non-interactive exit code — carve-out obligation {#disks-exit-code-upstream}

`DisksApp::main` now returns a failing command's error code as the process exit code for
non-interactive `--query` runs, so CI and cron can gate on `clickhouse-disks` at all. It **rides in the
CAS pull request for now** — pre-release, and the gating it enables is needed there — but it is a
behavior change to a shared tool for every user of it, so it must later be carved out into its own
upstream PR together with the integration-test fix it forces.

The record lives with the carve inventory, not here: `docs/superpowers/cas/upstream.md`, §G list plus
the G-item section below it (site, rationale, the two reviewer-facing details, the latent
`test_replicated_table_structure_alter` defect it exposed with its mechanism, and the blast-radius
conclusion).

### `clickhouse-disks` exit code: 8-bit truncation and five undocumented subcommands (opus review M6) {#disks-exit-code-truncation}

The contract change itself is tracked as `{#disks-exit-code-upstream}` (above, a different
half — the carve-out obligation to move the "non-interactive runs exit nonzero at all" commit into its
own upstream PR — no duplication). Confirmed unchanged at HEAD, both branches: (1) `DisksApp::main`
(`programs/disks/DisksApp.cpp:622-623`) still `return`s the raw ClickHouse error code as a POSIX
status, so it is masked to 8 bits — codes ≥256 report a mangled value and 256/512/768 become `exit 0`,
i.e. a failure that scripts read as success. Map to a small set of stable exit codes instead. (2)
`docs/en/operations/utilities/clickhouse-disks.md` documents neither the exit-code contract nor the
five `cas-*` subcommands. P2.

### fsck substitutes the default `BlobRef{}` for an unparsable blob key, and that value is the identity of an empty blob under the default algo (2031-triage CAS-124) {#fsck-unparsable-blob-key-sentinel-collides-with-the-empty-blob}

`Tools/CasFsck.cpp:954` classifies an unreachable listed object with
`layout.parseBlobKey(bkey).value_or(BlobRef{})`, and `BlobRef{}` is `{CityHash128, all-zero digest}`
(`Primitives/CasBlobDigest.h:207-214`). An empty blob hashes to exactly that: `IHashingBuffer` starts
at `state(0, 0)` and `getHash` returns it unchanged when nothing was hashed
(`src/IO/HashingWriteBuffer.h:20-29`), so `blobHashHexOneShot(CityHash128, "")` is `0…0`. Zero-length
blobs are creatable — files matching `partFileMustStayBlob` take the blob path regardless of size
(`ContentAddressedTransaction.cpp:860-917`) and neither `stageBlobPartFile` nor `CasPartWriteTxn`
rejects `size == 0`. Confirmed unchanged at HEAD.

Consequence is one report label, not data: the `PendingGc` branch additionally requires a token match,
which a foreign key cannot satisfy, but `in_run_hashes.contains(hash)` does not, so a truly
unparsable/foreign key can be labeled `AwaitingGc`/`StaleEdge` instead of `Unaccounted` whenever the
pool also holds an empty `cityhash128` blob in the GC snapshot. `report.unreachable` is already
incremented before the classification, so nothing disappears from the report. The nearby comment
(added 2026-07-17 by `d8b401ff035`, a "comment denoise sweep") now actively asserts the false safety
claim this finding refutes — "cannot match a real `retired_by_hash`/`in_run_hashes` entry" — so the
comment needs correcting alongside the code. Fix is one line: keep the `std::optional<BlobRef>` and
classify a parse failure as `Unaccounted` directly instead of looking the sentinel up in
`retired_by_hash`/`in_run_hashes`.

### fsck large-pool reporting: two of three post-2026-07-26 residuals have moved (2031-triage) {#fsck-large-pool-fixed}

MINOR — `corrupted_runs` visibility/fatality and inverted timeout budgets are fixed from the original
2026-07-26 pass. Re-checked at HEAD:

(a) **Still open.** `checker.py`/`fsck.py` still print the old `M-F debris, B140` label for what the
product now classifies as `AwaitingGc` (`utils/ca-soak/soak/checker.py:108,292,545`,
`utils/ca-soak/soak/fsck.py:249`) — docstring cleanup only.

(b) **Largely fixed.** The remaining timeout path no longer substitutes silent fabricated
`{"dangling": 0, ...}` zeros: `run.py`'s `FSCK_SUMMARY_TIMEOUT_S = 600` path marks a timed-out gate's
result dict `_fabricated`, and `render_checkpoint_result` (`utils/ca-soak/soak/run.py`) checks that
flag to print an honest `GATE-SKIPPED ... reachable=not-measured ... dangling=not-measured` line
instead of zeros, plus an end-of-run `SKIPPED_FSCK_GATES` block that WARNs how many checkpoints skipped
their assertions. The residual: `wait_for_pool_consistent` and `settle_fsck_for_dump`
(`utils/ca-soak/soak/run.py`) still default `timeout_s=180.0` — those two call sites were not updated
to the honest-degrade shape.

(c) **Partially fixed.** The main GC-checkpoint entry-gate budget was raised 180s → 600s on
2026-07-29 (`run.py`, comment names the exact motivating failure: "a 90-minute phase-3 run failed here"
on tens of thousands of Outdated parts loading on a CA disk) — a 5.5 GiB pool now has headroom it did
not have. The two functions named in (b) above are the visible remaining 180s defaults; auditing every
`timeout_s` default in the harness against the current backlog size is the remaining work, not a
redesign.

### the fsck exit set and SQL row still have no test that can fail for them {#fsck-untestable-render-surfaces}

The hard-finding rule EXECUTES — `FsckReport::clean` is computed from `kFsckHardFindings`, and a
`static_assert` on that list's deduced size trips in all three surfaces' translation units
(confirmed unchanged: `src/Interpreters/InterpreterSystemQuery.cpp:27,2473-2474`,
`Tools/CasFsck.h:244-251`) — so the rule no longer depends on a reader remembering it. What remains: an
author who reads the assert as an arithmetic complaint, bumps the count, and does not visit the three
surfaces. The summary line has a real test; the nonzero-exit set and the SQL row do not, because
`contentAddressedFsckColumns` and `appendContentAddressedFsckRow` still have internal linkage inside
the anonymous `namespace {}` opened at `InterpreterSystemQuery.cpp:2353` (confirmed unchanged), and
`programs/disks` is not linked into `unit_tests_dbms`. Closing it means giving those functions external
linkage plus a header — a structural change to `src/Interpreters/InterpreterSystemQuery.cpp`, a shared
non-CAS file. **CONSULT ITEM, not a task**: per the standing rule that shared/upstream surfaces are not
edited without consultation, this needs a decision before anyone implements it; the cheap alternative
worth weighing first is whether the assert's message can be made harder to satisfy without visiting the
surfaces at all. Also recorded: `05020_content_addressed_fsck.reference` pins the row via
`TSVWithNames`, so it is a ONE-DIRECTIONAL fence — it fails on a column added without updating the
reference, not on a `clean()` term added without a column; only the `static_assert` covers that
direction.

### a loose mountpoint object under `_files/` is classified as a corrupt namespace file {#loose-mountpoint-object-as-corrupt-namespace-file}

`Layout::mountpointObjectKey` (`Formats/CasLayout.h:312`) still does not enforce the `_files`
reservation in code — its own doc comment says the reservation "still applies to its segments via the
path itself (these never appear in a real ClickHouse loose-file path)", i.e. it is an assumption, not a
check — so a loose object at `roots/<srid>/_files/x` satisfies `parseNamespaceFileKey`'s necessary
condition and gets treated as ours — `ca-decommission` refuses fail-close and `ca-fsck` posts a hard
`lifeless_keys` finding against a key that is not damage. Direction is safe (refuse/report, never
delete) so not urgent, but a hard finding against an undamaged key trains an operator to disbelieve
hard findings. **Open decision**: enforce the reservation in `mountpointObjectKey` (makes the existing
doc comment true) or narrow the classifier.

### Close the `cas_` config-prefix migration window {#cas-config-prefix-window}

Scheduled removal, not a defect. The CAS disk settings move to a `cas_` config-key prefix
(`docs/superpowers/specs/2026-08-25-cas-disk-settings-namespace-design.md`, deleted in `05b2a33ff32`; landed, see `917600b122b`), and the unprefixed
spelling is accepted for a bounded period so that configurations already living in external CI/CD
scripts keep working across a binary upgrade. Confirmed still open, both branches: the migration block
(legacy-name collection, aggregated `LOG_WARNING`, apply loop) is fully intact in
`ContentAddressedSettings::loadFromConfig`; `UNKNOWN_SETTING` is declared but not used on this path.

**Trigger:** the CAS configurations in the `clickhouse-regression` suite are on the `cas_` spelling.

**The work:** in `ContentAddressedSettings::loadFromConfig`, replace the migration block — the
aggregated `WARNING` and the loop that applies legacy values — with a throw that lists every
unprefixed CAS setting name found and its `cas_` spelling. Detection stays; only the response
changes. Then rewrite the two tests that pin the open-window behaviour into one that pins
`UNKNOWN_SETTING`, and drop the deprecation sentence from
`docs/en/antalya/cas/configuration.md` and `docs/en/operations/storing-data.md`.

**Why it must not be forgotten:** while the window is open, a stale configuration runs with a warning
nobody reads. Closing it is what turns a silently-stale config into a startup failure that names the
key.

### No CAS disk setting is applied by `SYSTEM RELOAD CONFIG`, and the no-op is silent (2031-triage CAS-107) {#cas-settings-not-reloadable-silently}

`ContentAddressedSettings::loadFromConfig` runs exactly once, from the `cas` metadata-storage factory
lambda (`src/Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.cpp:232-241`), i.e. only
when the disk object is CREATED. On a config reload `DiskSelector::updateFromConfig` calls
`disk->applyNewSettings(...)` for every disk that already exists
(`src/Disks/DiskSelector.cpp:180`), `DiskObjectStorage::applyNewSettings` forwards to
`metadata_storage->applyNewSettings(...)`
(`src/Disks/DiskObjectStorage/DiskObjectStorage.cpp:985`), and
`ContentAddressedMetadataStorage` does not override that virtual (confirmed unchanged at HEAD, both
branches — the base is an empty no-op at `src/Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h:365-368`;
only `MetadataStorageFromCacheObjectStorage` overrides it, to forward to the underlying storage). So an
operator who edits `gc_interval_sec`, `part_folder_cache_bytes`,
`manifest_decode_cache_bytes`, any `gc_round_*` budget, ... and reloads gets a
successful reload, no log line, and no behaviour change. The S3 half of the same disk block DOES
reload (`DiskObjectStorage.cpp:988-989`), which makes the split especially surprising.

Not every setting is reloadable in principle — `server_root_id`, `gc_shards`, `blob_hash`,
`scratch_path`, `staging_backend` are pool-/mount-creation identities and must stay creation-time
only. Owed shape: an `applyNewSettings` override that (a) re-parses the block, (b) applies the
genuinely dynamic subset (GC cadence and the per-round budgets, the remaining cache byte/entry budgets,
`gc_enabled` — the last already has runtime verbs, `SYSTEM CAS GC
STOP`/`START`), and (c) LOGS a warning naming any changed creation-time key as ignored-until-restart,
instead of today's silence. Fixing this also removes a second silent surface: the unknown-key gate in
`ContentAddressedSettings.cpp` (formerly tracked as the now-DONE `[cas-disk-s3-key-whitelist-gap]`,
fixed by `917600b122b`) is only ever evaluated at disk creation, so a typo introduced by an
edit-and-reload is not diagnosed until the next restart.

Second half, lower severity and mostly generic: removing a CAS disk from `storage_configuration` and
reloading only produces the upstream warning "disappeared from configuration, this change will be
applied after restart of ClickHouse" (`src/Disks/DiskSelector.cpp:215-218`) — no `shutdown()` on the
dropped disk. For a CAS disk this means the mount lease keeps being heartbeaten until the process
exits, so the `server_root_id` slot cannot be taken over by another server before a restart. This is
the same disk-registry-caches-forever class as the deferred disk-lifecycle leak
(`BACKLOG/mounts-and-lifecycle.md`{#disk-lifecycle-rev8-closure}) and is bounded by the restart, but
the warning does not say that a lease is still held; the honest short-term fix is a CAS-specific line
in that path (or in the mount log) naming the retained lease and the `FORGET` verb.

## Runbooks and docs {#runbooks-and-docs}

### `[B198]` backup/restore runbook {#b198-backup-restore-runbook}

GATE — no runbook exists yet for CAS pool backup/restore under `docs/en/antalya/cas/`; needed before
the feature can be called operationally supported. Progress since filing:
`umbrella-roadmap.md` §1 records the server-side copy fix ([PR #2415](https://github.com/Altinity/ClickHouse/pull/2415)),
tests ([PR #2437](https://github.com/Altinity/ClickHouse/pull/2437)), and `clickhouse-backup` embedded
mode verified — the runbook doc itself is still not written.

### the fsck exit-code rule is restated in prose the fence cannot reach {#fsck-rule-restated-in-unfenceable-prose}

The rule EXECUTES in code (`FsckReport::clean` computed from `kFsckHardFindings`, `static_assert`
tripping in three TUs — see `{#fsck-untestable-render-surfaces}`), but the same rule is also RESTATED
in prose that no build can check. The original citation of `docs/superpowers/cas/08-testing-and-soak.md`
is stale: that file no longer exists, deleted by `85c95839160` ("cas-docs: consolidation deletion (c)"),
an ancestor of both branches — the 2026-08 docs consolidation removed it. Of the "two harness files"
originally named, only one restatement survives at HEAD: `utils/ca-soak/soak/fsck.py:74` ("throws
(nonzero exit) on `dangling`, `chain_broken`, `corrupted_runs`") and `:194` ("cas-fsck exits nonzero
when `dangling > 0`") — no matching restatement was found in `checker.py` or the scenarios framework.
Three restatements were found wrong in one round before the doc was deleted, one of them inside an
operator-facing warning string; nothing mechanical will catch a fourth. The real fix is structural and
a documentation decision, not a code task: `fsck.py`'s comments should point at `kFsckHardFindings` and
`CommandFsck::executeImpl` rather than restate the exit set. Related: `{#fsck-untestable-render-surfaces}`
is the other half — together they bound what the fence does and does not reach.

### Incomplete multipart uploads and `_probe/` debris are invisible to every CAS accounting surface (2031-triage CAS-082) {#mpu-and-probe-debris-unaccounted}

Two cost-only (never correctness) residuals, both P3. Confirmed unchanged at HEAD, both branches: no
mention of `AbortIncompleteMultipartUpload` anywhere in `docs/en/antalya/cas/`, and no `probe_debris`
counter or `_probe/*` sweep anywhere in `Pool::open`/`CasProbe.cpp`/`CasFsck.cpp`.

**Incomplete multipart uploads.** CAS adds no multipart bookkeeping of its own — every remote body
goes out through `IObjectStorage::writeObject` → `WriteBufferFromS3`, which already aborts the upload
on cancel and in its destructor (`src/IO/WriteBufferFromS3.cpp:244`, `:313-316`,
`abortMultipartUpload` at `:469`), so an exception or a normal-shutdown teardown leaves nothing
behind. What survives is the process-kill case (SIGKILL/OOM/host loss) and a failed `AbortMultipartUpload`
call: the parts stay billed until the bucket's own lifecycle rule expires them. This is identical to
every other ClickHouse S3 disk and cannot be fixed inside the process, but CAS is the storage whose
docs promise a complete byte accounting for the pool (fsck `physical_bytes` sums HEAD sizes of visible
objects only — `Tools/CasFsck.cpp:744`, `:759`, `:1071`), so the gap is worth naming where the
operator reads. Owed: one line in the CAS operations docs recommending an
`AbortIncompleteMultipartUpload` lifecycle rule on the pool bucket, plus a note in the fsck docs that
`physical_bytes` counts committed objects and not in-flight multipart parts.

**Capability-probe debris.** `runCapabilityProbe` cleans up on every exit path (`Backend/CasProbe.cpp:252`,
`:258`; the lambda HEADs then `deleteExact`s both keys, `:26-41`), and a mis-provisioned bucket fails at
step 0/0b (`:47-53`) before any object is written — so the audit's "left behind on exactly the
mis-provisioned buckets the probe exists to detect" is wrong. The real residual is a hard kill mid-probe:
two tiny objects under `<pool>/_probe/<u128hex>/` (`Pool/CasPool.cpp:466-467`), bounded by the number of
crashed mounts, never per-write. Nothing ever sweeps them: the bootstrap residual scan skips the whole
`_probe/` subtree deliberately (`Backend/CasSentinelProbe.cpp:18-30`, `:54-55`) — correct there, since
that scan decides whether a fresh `_pool_meta` may be minted and probe scratch must not fail-close a
healthy open — and fsck's unaccounted pipeline classifies only keys under the blob plane, so `_probe/`
keys are neither reported nor reclaimed. Owed (cheap): have `Pool::open` best-effort delete stale
`_probe/*` entries older than a threshold, or have fsck count them under a `probe_debris` line so the
class is at least visible. Not urgent — the bytes are negligible and cannot mask real data.

## Issue #2233 adjudication residue: soak-harness observability + Poco shared-pool risk (2026-08-20) {#issue-2233-followups}

Adjudication of https://github.com/Altinity/ClickHouse/issues/2233 ("replica HTTP dies on green-path soak
after relink NETWORK_ERROR storm"): the refusal storm is the known, designed
`[relink-confirm-busy-lane]` behavior (all four remediations there still open — the per-ref rule-3
refinement is the availability fix); the claimed causality "storm -> HTTP death" is contradicted by our
own artifacts (a 90-minute phase-3 soak absorbed 112,598 refusals — peak 9,219/min — and ended
`PHASE3 OK` with both replicas alive; the reporter saw ~278 total), and no fd/socket/thread leak exists
on the abandoned-relink path (drain-then-throw + `SCOPE_EXIT` verified). Prime suspects for the
reporter's observation: VM-level OOM (28g `mem_limit` x2 on a 16 GiB Docker Desktop VM; their upstream-
compose symptom was "Connection refused after the peer exits") and the Poco shared-`server_pool`
silent-refusal upstream bug (below). Items:

- (1) **ca-soak compose: `ch2` has NO healthcheck** (`docker-compose.yml` — only `ch1` has the HTTP
  `/ping` probe, added for capability-probe serialization). "Container healthy while HTTP dead" on ch2
  is therefore vacuous. Add the same healthcheck to ch2. Trivial.
- (2) **soak driver: `TRANSPORT FAILURE` is an `else`-branch catch-all** (`soak/run.py:2020-2026`) that
  names a subsystem it never diagnosed — the same triage-misdirection failure mode #2219 complains
  about, one layer up. Phase 1 additionally does exactly one attempt (`transport_resilient=False`) and
  checkpoints do not gate on HTTP health (phase-2-only wait), so any transient `OSError` becomes the
  issue's exact headline. Split the label (name the errno/op) and consider a phase-1 HTTP-health gate
  at checkpoints. Small.
- (3) **Poco shared-`server_pool` silent connection refusal — assess exposure** (upstream bug, comment
  in `base/poco/Net/src/TCPServerDispatcher.cpp:154-180`): one `Poco::ThreadPool` capped at
  `max_connections` is shared by 8123/HTTPS/native/9009; `_currentThreads` is per-dispatcher, so
  saturation by long-lived interserver byte fetches can make the 8123 dispatcher drop accepted sockets
  with NO ClickHouse-level error (client sees RST; at most a Poco `Warning`). Relink-storm second-order
  effect: refusals suppress zero-byte relinks and FORCE long byte fetches, i.e. the storm converts
  cheap transfers into thread-holding ones. Candidate observability first: expose refused-connection
  counts / alarm on pool saturation before considering upstream surgery (upstream file = consult-first).
- (4) **Confirm-path observability gaps** (feeds `[relink-confirm-busy-lane]` items (b)/(c)): no
  ProfileEvents pair for proven/refused confirms; refusal reason (which rule) logged only at Debug on
  both sides; the receiver collapses refusal vs transport failure vs timeout into one message — the
  reporter's logs could not distinguish them even in principle. This adjudication would have taken
  minutes with (b)+(c) implemented.

## Later / design questions {#later-design-questions}

### `lazy_load_tables` / `StorageTableProxy` — feature-level decision needed (consult audit 2026-07-21) {#lazy-load-tables-decision-2026-07-21}

Third incident of the same class (unforwarded `IStorage` virtual / direct cast through the proxy):
SYSTEM verbs (fixed, 05017), action-lock parking (open), mutations (`checkMutationIsPossible`,
fixed + 05021). A commissioned audit
(`docs/superpowers/reports/2026-07-21-storageproxy-forwarding-audit.md`, deleted in `f5c01e88d01`) found **~60 unforwarded
virtuals, ~45 of class "must forward"**, including a critical one: `backupData`'s no-op default
means a BACKUP of a not-yet-materialized lazy table silently contributes NO data. Design findings:
no compile-time guard exists for "new virtual not forwarded"; swap-on-materialize does NOT fix the
class (escaped `StoragePtr`s in the UUID map/action locks + two lock domains); the clean long-term
shape is catalog-entry laziness (real refactor). Consultant recommendation: the feature as
implemented is net-negative — disable/quarantine rather than fix one virtual at a time. Still
undecided as of 2026-09-25 (`umbrella-roadmap.md:80` lists it as "decision of 2026-07-21 to revisit").

- [ ] **USER DECISION**: quarantine/disable `lazy_load_tables` vs fund the full remediation
  (complete forwarding sweep + Clang-AST CI guard + backup regression test) vs catalog-entry
  laziness refactor. Until decided: treat every new lazy-table symptom as this class first.
- [ ] THIRD bug of the class found while validating the mutation fix (2026-07-22): `MATERIALIZE
  TTL` through a lazy proxy fails with `INCORRECT_QUERY` "no TTL set" even after the
  `checkMutationIsPossible` forward — the proxy's cached in-memory metadata carries columns only
  (no TTL/ORDER BY), and `getInMemoryMetadataPtr` deliberately does not forward (confirmed unchanged,
  `899fadfbcb4`: "do not override `getInMemoryMetadataPtr` for storage proxy"). Candidate rule if the
  feature stays: forward metadata to nested ONCE MATERIALIZED (no laziness left to preserve at that
  point); needs its own consult.
- [ ] If the feature stays: forward `backupData`/`restoreDataFromBackup`/
  `supportsBackupPartition`/`finalizeRestoreFromBackup`, `onActionLockRemove` (the audit's
  most-urgent remaining items), then the rest of class B.
  (`supportsOptimizationToSubcolumns` was forwarded since filing — `StorageProxy.h:35` — and is
  removed from this list; see also `[storageproxy-subcolumn-forwarding-bug]` below, now DONE.)
- [ ] FOURTH bug of the class + a REVERT (2026-07-22, xhigh review): the `checkTableCanBeRenamed`
  forward added on `StorageTableProxy` (7ab1fc15f4c) was REVERTED — it materializes the lazy table
  (`getNested`) while `DatabaseAtomic` holds its non-recursive database mutex (DatabaseAtomic.cpp:321/346),
  and a schema-inferred lazy `Buffer` resolves its destination via `DatabaseCatalog::getTable` in its
  constructor (StorageBuffer.cpp:180), re-entering the same database and self-deadlocking (cross-database
  RENAME/EXCHANGE can hold two database mutexes across the same work). So the nested engine's rename
  restriction is once again bypassed for a lazy (never-accessed) table — the pre-existing gap is REOPENED,
  not newly introduced. Confirmed still reverted and open at HEAD (`8009d6e5f69`; `StorageTableProxy.h:57`
  states the deliberate non-forward). Correct fix (same shape as the other class-C bugs): materialize
  the proxy BEFORE any database mutex is taken, at the interpreter level, then re-fetch/verify
  identities under the lock and run the check on the materialized storage. NOTE for any upstream PR:
  the KEPT generic `checkMutationIsPossible` forward on `StorageProxy` also changes
  `StorageTableFunctionProxy` semantics (a table-function proxy now answers the mutation-possibility
  check from its nested storage rather than the `IStorage` default) — sound, but call it out
  explicitly (codex F5).
- [x] RESOLVED as misdiagnosis + REAL FIX LANDED (f1f11 soak 2026-07-21): the "post-kill CA table load
  takes minutes" finding was an artifact — the table sits in a lazy_load_tables=1 DB (706095958ea) and
  materializes in ~18 ms on first touch; nothing touched it post-kill, while SYSTEM SYNC REPLICA
  misreported the unmaterialized StorageTableProxy as "is not replicated". Fixed in 2ba28ac4b6f
  (`unwrapTableProxy` across single-table SYSTEM verbs + stateless test
  `05017_lazy_load_tables_sync_replica.sh`). OPEN EMPIRICAL TAIL: measure post-fault getNested cost
  under churn at the next soak's first chaos checkpoint — no such measurement found in
  `RUN_HISTORY.md` as of this grooming pass; if genuinely minutes, that is the real availability item.
- [ ] lazy_load_tables follow-ups (from T15 review, pre-existing; confirmed unchanged, `2ba28ac4b6f`'s
  own commit message excludes these by design): whole-db DROP REPLICA safety scan
  (InterpreterSystemQuery.cpp:~1687) and RESTART REPLICAS iteration skip unmaterialized proxies — a
  stale remote replica in ZK may stay uncleaned for lazy tables; STOP/START <action> on a single lazy
  table parks the ActionLock on the PROXY, invisible to the later-materialized nested storage.
  *(Formerly tracked separately as `[drop-replica-stop-proxy-forwarding-tails]`, 2026-08-04 orphan
  triage; folded in here as the same finding.)*
- [ ] `StorageProxy` doesn't forward `isMergeTree`/`supportsTTL` (2026-08-04 orphan triage
  `[storageproxy-mergetree-virtuals-not-forwarded]`, confirmed unchanged — neither symbol appears in
  `StorageProxy.h`/`StorageTableProxy.h` on either branch): a plausible functional bug, queries
  checking these virtuals on a not-yet-materialized lazy table get wrong answers. Affects any
  lazy-loaded MergeTree, not just CAS, so scope and file it generically if the feature stays.
- [ ] Clang AST CI check for `StorageProxy` interface growth (2026-08-04 orphan triage
  `[storageproxy-ast-interface-guard]`, confirmed still absent): compares virtual `IStorage`
  declarations against `StorageProxy` overrides, requires rationale for allowlisted omissions; concrete
  tooling proposal, no existing coverage. This is the compile-time guard the audit above says does not
  exist yet.
- [x] `[storageproxy-subcolumn-forwarding-bug]` — DONE. `StorageProxy` no longer over-reports
  `supportsOptimizationToSubcolumns` for opted-out nested engines: forwarded at `StorageProxy.h:35`,
  confirmed on both branches.

### Disk-error (ENOSPC / inode-exhaustion) audit follow-ups {#disk-error-audit-followups-2026-07-21}

Staging/target/GC disk-error audit verdict held (staging ENOSPC fail-loud, Native S3 corruption-free,
GC decision-durable-before-delete). Residual gaps, ordered by value:

- **✅ CLOSED: size guard at blob adoption** — `PartWriteTxn::ensureBlobPresent`
  (`Pool/CasPartWriteTxn.cpp:411-419`) now subtracts the envelope length from the mandatory `HEAD`
  size and compares the logical result with the source; loaded metadata size is checked too. A
  truncated object is refused as `CORRUPTED_DATA`, not adopted.
- **HARD: temp-file + rename in the local blob write path** — the guard above prevents silent
  adoption but did not, at filing time, make local publication atomic or remove a partial final key
  after failure. Confirmed since resolved: `formats-and-storage.md`'s `[disk-error-audit]` item is
  independently `✅ CLOSED 2026-08-23` — `ObjectStorageBackend::emuPublishBlobAtomically` now writes via
  a sibling `.publish-<uuid>.tmp` + rename under `emu_mutex`, with deterministic tests.
- **DESIRABLE: fsck physical-size check for blob bodies** — `runFsck` HEADs every blob but never
  compares physical size against the expected size, so a truncated blob passes as `Reachable`; the
  listing already carries the sizes, so this is free. Confirmed unchanged.
- **DESIRABLE: free-space guard + orphan sweeper for `scratch_path`** — no `statvfs` check before a
  local staging write, and orphaned `*.tmp` files from an unclean restart are never swept (the S3
  staging prefix has a sweeper; local scratch does not). Confirmed unchanged.
- **MINOR: wrap the GC post-CAS cleanup in try/catch** — the post-CAS manifest-body delete loop and
  hand-off prefix wholesale delete aren't wrapped, so a genuine backend error escapes the round after
  its `gc/state` CAS already committed (data-safe, but reddens the round unnecessarily). Confirmed
  unchanged (`Gc/CasGc.cpp:1034-1100+`).
- **DESIRABLE: GC scheduler backoff + a distinct storage-full signal** — the pacing loop retries a
  failing round forever with no backoff and no ProfileEvent distinguishing target-storage-full from
  generic instability. Confirmed unchanged: only the lease-held-by-another-mounter backoff exists.
- **MINOR: destructor-`abandon` live-epoch precommit debris** — if `abandon` fails during a failed
  transaction's destruction, the live-epoch precommit binding persists until remount; bounded, but
  worth a periodic re-`abandon` retry under a persistently broken backend. Confirmed unchanged.

*(The "late-landing mutable conditional PUT after fence loss" item formerly listed here was
SUPERSEDED-BY [`[MOUNT-CLAIM-EPOCH-REGRESSION]`](ref-protocol.md#ref-protocol-rev6), which already asks
for the same successor-side `writer_epoch` gating verification; the fold is confirmed in
`2031-triage.md:5660` and the duplicate bullet is removed.)*

### `[CA-s3 Disk session pressure]` `ConnectionGroup: Too many active sessions in group Disk` {#ca-s3-disk-session-pressure}

On the asan CA-s3 lane (run for `e2d04bfe37e`), `00149_quantiles_timing_distributed` flipped on a
leaked stderr warning: `ConnectionGroup: Too many active sessions in group Disk, count 10400,
warning limit 8000`. The test's stdout was correct and reruns passed — the failure is warning noise,
but 10k+ concurrently active Disk-group sessions under parallel load is a real pressure signal for
the CA-s3 request fan-out (compare the write-path request-class findings in the disk-error audit
follow-ups above and the insert-slowness item). Distinct from `umbrella-roadmap.md`'s "Connection
churn" bullet (F27, a connection-creation-RATE finding, one new TLS connection per ~98 requests) — this
is a concurrent-session-COUNT finding; no anchor currently covers it. Worth a look at whether CA holds
S3 sessions longer than needed (e.g. across retry backoffs) before raising any limit.

### `DiskEncrypted` over a CA disk hides `isContentAddressed` from every CA-aware branch {#encrypted-wrapper-hides-content-addressed}

Prerequisite detail for `[B17]` whenever encryption-at-rest × content-addressing is actually designed
(2031 triage, CAS-060). Two independent facts, confirmed unchanged at HEAD on both branches:

1. `DiskEncryptedTransaction::writeFile` mints a fresh random IV for every rewrite-mode write
   (`src/Disks/DiskEncryptedTransaction.cpp:106-112`), and CAS hashes the bytes handed to it
   (`ContentAddressedTransaction.cpp:1814`, `:1876`). So every write through an encrypted wrapper
   is a unique blob: dedup does not merely narrow to per-key scope, it disappears entirely — even
   for the same server rewriting byte-identical plaintext.
2. `DiskEncrypted` does not forward `isContentAddressed` (only `ReadOnlyDiskWrapper` does —
   `src/Disks/ReadOnlyDiskWrapper.h:96`; the base returns false at `src/Disks/IDisk.h:477`). Every
   CA-aware branch therefore sees a non-CA disk: the whole-part transaction choices
   (`DataPartStorageOnDiskBase.cpp:422`, `:542`, `:735`), the relink fetch path
   (`DataPartsExchange.cpp:161`), the projection parent-transaction rule
   (`IMergeTreeDataPart.cpp:1364`, `MergeTask.cpp:567`) and the BACKUP-restore whole-part transaction
   (`MergeTreeData.cpp:7544`) all take the plain-object-storage path over a pool that is in fact
   content-addressed.

Neither is silent corruption — (1) is a space/cost regression and (2) lands on CAS's own per-file
autocommit rejections, i.e. loud failures — but any encryption work must start by deciding the
dedup-scope/key-derivation question and by making the wrapper CA-transparent (or refusing the
combination at config validation, which nothing does today).

### `always_use_copy_instead_of_hardlinks=1` is accepted on a CA table and then breaks every mutation and same-disk partition clone (2031-triage CAS-085) {#always-copy-instead-of-hardlinks-no-gate}

Nothing rejects the MergeTree setting `always_use_copy_instead_of_hardlinks`
(`src/Storages/MergeTree/MergeTreeSettings.cpp:1902`, default `false`) on a content-addressed table
— neither at `CREATE`, nor at `ALTER ... MODIFY SETTING`. Confirmed unchanged at HEAD, both branches.
Once it is on, the copy variant of every clone/hardlink site is taken and lands on
`ContentAddressedTransaction::generateObjectKeyForPath`, which is a `notYet` throw
(`ContentAddressed/ContentAddressedTransaction.cpp:578-580`), because
`DiskObjectStorageTransaction::copyFileImpl` derives destination keys from it
(`src/Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:522-524`).

Reachable paths at HEAD:
- Mutations (`ALTER ... UPDATE/DELETE`, `MATERIALIZE INDEX`, lightweight delete materialization):
  `MutateTask.cpp:2493-2496` and `:2516-2519` call
  `DataPartStorageOnDiskFull::copyFileFrom` → `disk->copyFile`
  (`DataPartStorageOnDiskFull.cpp:372-388` → `DiskObjectStorage.cpp:291-321`).
- The unchanged-part mutation clone: `MutateTask.cpp:3312` sets `copy_instead_of_hardlink`, reaching
  `DataPartStorageOnDiskBase::freeze` → `Backup` → `BackupImpl`, whose copy branch calls
  `transaction->copyFile` (`src/Storages/MergeTree/Backup.cpp:61-65`).
- Same-disk `ATTACH/REPLACE PARTITION FROM` and `MOVE PARTITION TO TABLE` on `StorageMergeTree`
  (`StorageMergeTree.cpp:3215`) reach the same `freeze` path.

Fail-closed, loud `NOT_IMPLEMENTED`, no silent corruption — but a mutation entry then retries
forever until the setting is reverted, and the thrown message ("the disk is wrapped by a layer that
bypasses the content-addressed write path") misdescribes this trigger. Fix direction: reject the
setting for a CA storage policy at `CREATE`/`ALTER MODIFY SETTING` (the same shape as the
`SUPPORT_IS_DISABLED` gates in `MergeTreeData::checkAlterIsPossible`), or teach the CA transaction
to serve `copyFile` as a manifest-level carry-forward like `createHardLink` already does.

Two claims from the source finding do NOT hold: `ALTER TABLE ... FREEZE` does not consult this
setting (`MergeTreeData.cpp:9988-9991` builds `ClonePartParams` with `make_source_readonly` only),
and neither does BACKUP/RESTORE cloning; the zero-copy implicit `copy_instead_of_hardlink` term
(`StorageReplicatedMergeTree.cpp:3357`, `:9220`) is dead on CA because
`DiskObjectStorage::supportZeroCopyReplication` returns false for `MetadataStorageType::CAS`
(`src/Disks/DiskObjectStorage/DiskObjectStorage.h:53-58`).

### CAS has no experimental gate (opus review M11) {#no-experimental-gate}

There is no `allow_experimental_*` setting or equivalent for CAS — 82 such settings exist in
`Settings.cpp` and none is CAS's; the word "experimental" does not appear anywhere in the CAS source
tree. Confirmed unchanged at HEAD: `MetadataStorageFactory.cpp:219-241`'s `"cas"` registration lambda
prints no warning. The practical gate today is one config line (`<metadata_type>cas</metadata_type>`)
plus a "Status" section in `docs/en/antalya/cas/index.md` that predates this review and does not
resolve the either/or. Decide deliberately: either add a gate (setting or a loud registration warning
naming the experimental status), or state in the docs that the config line IS the gate. P2 — this is
the difference between "a user opted in" and "a user typed a metadata_type".

### `[dynamic-cas-disk-no-gate]` SQL-defined dynamic `CAS` disks have no operator opt-in gate {#dynamic-cas-disk-no-gate}

`disk(type=object_storage, metadata_type=cas, ...)` in SQL creates a `CAS` disk through
`DiskFromAST` with no gate at all: any user who can `CREATE TABLE` joins a process-wide shared pool,
may use server credentials, and starts background work — none of which the operator ever declared in
`storage_configuration`. `RegisterDiskObjectStorage.cpp` ignores its `custom_disk` argument for
`metadata_type=cas` on both `cas-gc-rebuild` and `altinity/antalya-26.6` (verified 2026-09-26).

A full design and TDD implementation plan already exist and are ready to execute:
`docs/superpowers/specs/2026-08-25-dynamic-cas-disk-gate-design.md` and
`docs/superpowers/plans/2026-08-25-dynamic-cas-disk-gate.md` — add a default-`false` server setting
`cas_allow_unsafe_dynamic_disks`, checked in the `object_storage` disk factory before any
disk-construction side effect; server-configured disks (`custom_disk=false`) are unaffected.

Related: [`no-experimental-gate`](#no-experimental-gate), which covers the *static*
`storage_configuration` path (the operator already had to write the config line). This item is the
dynamic/SQL path, and is the more severe of the two: it needs no operator action whatsoever, only
`CREATE TABLE` privilege.

Duplicate-source note: `[cas-custom-disk-privilege-bypass]` below reaches the same missing
`custom_disk` gate from an independent source document; kept as a separate entry per that item's own
note, since this one is the more actionable of the two (cites an existing design doc and TDD plan).

### `[cas-custom-disk-privilege-bypass]` Inline `disk(metadata_type='cas', …)` bypasses the `SYSTEM CAS` privilege model {#cas-custom-disk-privilege-bypass}

`MetadataStorageFactory::registerMetadataStorageType("cas", ...)` (`MetadataStorageFactory.cpp:219`)
has no `custom_disk` gate, so any user who can `CREATE TABLE ... SETTINGS disk = disk(metadata_type='cas', ...)`
mints a permanent pool member with a pool-wide view of other tenants' namespaces, without going
through any `SYSTEM CAS` grant. The ready-made pattern to copy is the existing `use_fake_transaction`
rejection one file over. Decide: reject `custom_disk` for pool-joining metadata types outright, or
require a dedicated grant before a pool member can be minted this way. Originally raised as fable
umbrella-review B1 2026-08-05 (P1); re-verified open on HEAD 2026-08-21/2026-09-26; was previously
untracked by any BACKLOG entry. Not the same gap as [`pool-trust-boundary-undocumented`](docs-and-cleanup.md#pool-trust-boundary-undocumented),
which only asks for docs explaining the trust model, not a code-level gate. This is the same missing
gate as `[dynamic-cas-disk-no-gate]` above, reached from an independent source document
(`final-checks-todo.md` item 9.B3 vs. an independent design+plan pair); kept separate per that item's
own note pending a future consolidation pass.

Source: `docs/superpowers/cas/final-checks-todo.md` item 9.B3 (file deleted by the u21 grooming pass).

### The mount-lease / request-budget / snapshot-pacing knobs have no `ContentAddressedSettings` entry (2031-triage CAS-105) {#pool-pacing-knobs-no-config-surface}

`ContentAddressedSettings` declares 31 disk keys today (was 29 at filing) — since then
`mount_lease_ttl_ms`, `mount_renew_period_ms`, `attempt_timeout_ms` and `lease_safety_margin_ms` were
added (`81b96aa6edf` on `cas-gc-rebuild` / `572c10a99bc` on `antalya-26.6`, both "add settings"), and
`openPoolView` wires exactly those into `Cas::PoolConfig`
(`ContentAddressedMetadataStorage.cpp:776-810`). Everything else below is still a struct default in
production, reachable only from gtests:

- the rest of the whole `CasRequestBudget` (`Backend/CasRequestControl.h:145-198`): `operation_deadline_ms`
  90 s, `max_attempts` 16, the two inter-attempt backoffs, and the three `recovery_retry_*` values
  (120 s / 1 s / 30 s).
- `snapshot_log_count_threshold` / `snapshot_log_bytes_threshold` and the publish- and
  precommit-sweep backoffs (`Pool/CasPool.h:234-253`) — these trade write-side full-snapshot `PUT`
  volume against read-side cold-fold `GET`s, i.e. exactly a per-workload dial.
- `gc_fold_threshold` (1) and `gc_fold_max_defer_rounds` (8) — `Pool/CasPool.h:159-165`; the
  skip-unchanged batching dial is fixed at "fold as soon as anything changed".
- `rebuild_edge_budget` (8 M edges ≈ 256 MB) — `Pool/CasPool.h:172`, an in-memory ceiling for
  `rebuildBaseline`.

Two members of the same struct are deliberately NOT part of this item: `gc_stuck_removal_rounds` is
self-documented as a test seam ("no user-facing setting is registered", `Pool/CasPool.h:166-168`), and
`gc_frontier_probe_budget` defaults to effectively unbounded with the explicit reasoning that a cap
there converts into a permanent GC stop (`Pool/CasPool.h:136-155`) — exposing that one needs the
argument for it, not just a `DECLARE`. `ref_table_cache_bytes` is the same class but already tracked
with its own two extra residuals under `{#ref-table-cache-budget-admission-only}` in `performance.md`.

Owed shape: `DECLARE` the remaining pacing/budget subset (the rest of the request budget, the
snapshot thresholds/backoffs, `rebuild_edge_budget`, the fold-batching pair), keep `validate` extended
so the mount-lease inequality is checked against the CONFIGURED values rather than only the defaults,
and land it together with the reload gap (`{#cas-settings-not-reloadable-silently}`) so a configured
value is not silently ignored on `SYSTEM RELOAD CONFIG`. Not a gate: the defaults are the ones every
soak ran on, and `Pool::open` already refuses an inconsistent budget out loud.

### `eraseView`/`publishStaging` no-throw-after-commit residual {#partb-review-findings}

MINOR — the 2026-07-25 publish-confirm protocol review's two blockers and two majors are fixed
(`8e6fe6ef0af`); the one residual window: `eraseView` still runs after the durable commit and can
throw, and `ContentAddressedTransaction::publishStaging`'s `out_slot` is assigned only after
`promoteBuild` returns — closing it means extending the no-throw-after-commit discipline one frame
outward. No fix found on either branch at HEAD (`eraseView` still called unconditionally after commit
from multiple `PartFolderAccess.cpp` sites; `out_slot` assignment still gated on `promoteBuild`'s
return in `ContentAddressedTransaction.cpp`).

### `[fsck-crossepoch-life-omission]` needs re-verification against the current ref-recheck code {#fsck-crossepoch-life-omission}

Review finding NEW-3 (2026-08-04 orphan triage): `CasFsck` was reported to omit `life` when calling
`crossEpochFromSeal`, risking a false unconsumed-closing-seal report. At HEAD, `CasFsck.cpp` no longer
calls `crossEpochFromSeal` at all (that function is still real and called from `Gc/CasGc.cpp` and
`Gc/CasOrphanManifestSweep.cpp`, `Pool/CasRefProtocol.cpp:857`); `CasFsck.cpp` now has its own "late ref
recheck" logic (`:122-152`, `readCkpt`/`record_unchecked` against the same physical life) that looks
like it replaced the old call site, but whether the SAME omission applies to the new code was not
confirmed within this grooming pass. Flagging for an implementer to re-verify against
`checkRefStream`/the "late ref recheck" helper before either closing or re-scoping this item — do not
mark DONE without that check.

### `[B180 / format-freeze]` rollout machinery (format freeze itself is DONE) {#b180-format-freeze}

GATE — the format-freeze half is DONE: `umbrella-roadmap.md` §1 states the on-S3 formats are "frozen
as of `26.6.4.20001.altinityantalya`, the first fixed format version", matching the grooming charter's
own note. What remains open: a durable roster + `max_content_addressable_pool_format`
setting/rollout machinery — zero hits for that setting name on either branch. Also flagged, not part
of this item's scope: `docs/en/antalya/cas/roadmap.md` still calls CAS "experimental... pre-release...
format can still change", which now contradicts the freeze — a prose fix owed elsewhere.

### `[B13]` migration path for existing tables {#b13-migration-path}

HARD — `ALTER TABLE … MOVE PARTITION` to a `content_addressed` disk re-packs; mixed-version rollout
rule (read-new-before-write-new; format self-check fails closed) + a rollout-safety spec. Confirmed
still open: no CAS-pool-format migration/rollout-safety spec exists in `docs/superpowers/specs/`;
`umbrella-roadmap.md` §7 still lists "Migration at scale" as future work.

### `[F1-prod]` read-only same-pool shadow disk (`ca_ro`) breaks table load on restart {#f1-prod-ca-ro-shadow-disk}

GATE (prod) — MergeTree part discovery finds every part twice → `UNKNOWN_DISK` on restart with CA
tables. Stand workaround shipped (standalone `clickhouse-disks -C` fsck-only config; propagated to the
default stand); PRODUCT fix (part discovery skips `readonly` same-pool disks, or a
`hidden`/`introspection_only` disk flag) still open, confirmed unchanged — `utils/ca-soak/configs/fsck_only_aws.xml`
and `fsck_only_ca.xml` still keep `ca_ro` out of `config.d/` as the only mitigation; `10replicas`/
`gc_shards2`/`awss3` server configs may still embed `ca_ro`.

### `[B165]` server OOM at hour-4 soak (~49 GiB RSS) {#b165-server-oom-hour4-soak}

VERIFY — not reproduced since the streaming `publishBlob` path landed; re-run a long soak to confirm
resolved. Still blocked, confirmed: the 4h continuous-chaos soak that would re-confirm this is itself
blocked on other soak-infra work per `testing-and-ci.md:22` (compacting object store, streaming fsck,
TTL-robust oracle). Emulated/local publication no longer materializes one complete body either
(`fe80d150eec7`, `emuPublishBlobAtomically` streams into the temporary file).

### `[B14]` expedited / GDPR right-to-erasure delete {#b14-gdpr-erasure}

DESIRABLE — under GC lock, confirm no live ref, then delete bypassing the two-phase graduation delay;
no layout change. No implementation on either branch.

### `[B17]` encryption-at-rest × content-addressing {#b17-encryption-at-rest}

DESIRABLE — dedup scope per-encryption-key; local to key/hash derivation. No implementation on either
branch. See `{#encrypted-wrapper-hides-content-addressed}` for the prerequisite `DiskEncrypted`
forwarding gap this work would need to close first.

### `[B131]` repo hygiene: M-W/D-W1 comment sweep, now down to two test files {#b131-repo-hygiene-mw-sweep}

GATE — was "30 dangling `M-W`/`D-W1`/`2026-06-12-ca-core-m-w` comment references across 13 src files".
Re-measured at HEAD, both branches identical: now 8 references across exactly 2 test files
(`src/Disks/tests/gtest_cas_layout.cpp:126`, `src/Disks/tests/gtest_ca_wiring.cpp:31,332,875,1029,1468,1520,1632`).
The production files originally named (`ContentAddressedMetadataStorage.{h,cpp}`, `CasGcScheduler.h`,
`DataPartsExchange.cpp:106`) are already clean. Remaining scope is much smaller than filed: sweep the
two test files' wording to be self-contained. Non-shippable files: `poc/cas_mergetree/` already deleted
(F1 landed); the untracked empty `poc/` husk remains.

### Internal development provenance ships in source, docs and SQL metadata (opus review M8) {#internal-provenance-ships}

Scale re-measured at this grooming pass: ≈244 lines carrying `B<number>` tags across 74 files (the
original ~239/74 count is essentially unchanged; the counting regex over-matches some hex literals, so
treat both numbers as approximate), 62 of the 74 files outside the `ContentAddressed/` directory —
confirmed genuine hits remain in shared code, e.g. `src/Common/ProfileEvents.cpp:789` (`B168 P0`),
`src/Common/ThreadStatus.h:91` (`B90`). No single "provenance sweep" commit exists; only piecemeal
comment-cleanup commits touch individual sites. Partly adjudicated already as prose findings
(`fable-review-triage.md` {#n1}, {#m12} 12a), but nothing tracks the sweep itself. Per
[[feedback_comment_policy_no_internal_refs]] the reason stays and the provenance goes; do it as ONE
pass, not a fix round per site. P2 because of the upstream-code share.

### `CasInMemoryBackend` ships in the production binary with zero callers (opus review M10) {#in-memory-backend-ships-unused}

Re-measured: the file is now ~490 lines (was 598 at filing), still landing in `dbms` through the
unconditional directory glob (`src/CMakeLists.txt:138`, unchanged both branches) with zero production
construction sites and no registry entry — only consumers are `src/Disks/tests/gtest_cas_backend.cpp`
and a README mention. Either move it under `src/Disks/tests/` (where its only users live) or exclude it
from the production target. P3: dead weight and an audit smell, no behaviour.

### Open product question: is `CAS_WRITE_UNATTRIBUTED` worth a distinct signal, or should the dead code be retired? (2026-09-03) {#cas-write-unattributed-product-question}

`CAS_WRITE_UNATTRIBUTED` is unreachable on the Native/S3 write path now — its only throw site was the
deleted legacy minter, and the request engine settles a 2xx whose value fits no grammar by a resolve
read (`GaveUp{Unresolved}` at the deadline). Decide whether a distinct unattributed-write signal is
wanted (an event/counter) or the error code is retired; the three tests that pinned the throw now pin
the give-up. Confirmed still the case on both `cas-gc-rebuild` and `altinity/antalya-26.6`: the only
throw site is `emuMintToken` (`CasObjectStorageBackend.cpp:573`), emulated backend only. Spec revision
13 records this as an open product question rather than deciding it — see [the rulings
doc](../2026-09-03-request-contract-rulings.md).
