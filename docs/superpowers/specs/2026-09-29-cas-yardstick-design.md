# CAS yardstick: CAS vs S3 on the same workload — design (rev.1) {#design}

Task: CAS-321.1 (stage 0 of the parity umbrella CAS-321). Origin: `tmp/cas223/rig` and the CAS-223 RCA
(`docs/superpowers/reports/2026-09-28-cas-223-rca/RCA.md`), which measured one card by hand: a create / 3 insert / select /
drop cycle costs 83 GET + 64 PUT on a CAS disk against 12 + 31 on an S3 disk, wall 3.1x (release, CI environment, RustFS
1.0.0-rc.3, 2026-09-28). Decisions taken during brainstorming on 2026-09-29: `object_storage_type=local` is out of scope;
three disks (`cas`, `s3`, `s3plain`), each with and without a filesystem cache; cold and hot runs reported separately; K
repeats with statistics over them.

## 0. Purpose {#purpose}

One checked-in tool that answers, for every stage of CAS-321, the same question the same way: on one server, one bucket
and one cache configuration, what does a fixed set of operations cost on a CAS disk compared with the two ordinary S3 disks
— wall time, S3 requests by kind, bytes — cold and hot. Its output is the number every parity pull request quotes before
and after, and the reference it is compared with is checked in next to it.

Not in scope: pass/fail thresholds in CI (CAS-77), the CI-lane log dumps (CAS-321.2), any change to the server.

## 1. Layout and launch {#layout}

`utils/cas-yardstick/`:

- `rustfs.sh` — starts a fresh RustFS in docker on a given port with an empty data directory and creates the bucket
  `test` (the recipe of `tmp/cas223/rig/rustfs_fresh.sh`: scanner and heal off, relaxed durability, `--network host`);
  `--endpoint URL` skips the start and uses an already running store. The RustFS image version is pinned in the script and
  reported in every run header.
- `server.sh` — generates the server configuration into the run directory and starts `clickhouse server` from the given
  binary (ports from a base port; embedded Keeper off; caches, tmp, user files, access under the run directory); prints
  the PID file and waits for `SELECT 1` with a bounded poll. Stopping is `SYSTEM SHUTDOWN` plus a bounded wait on the PID.
- `yardstick.py` — the runner: `run`, `report`, `compare` subcommands (see §4).
- `cards/` — one Python module per card (`cycle.py`, `wide_insert.py`, `attach_200.py`, `mutation.py`,
  `lwd_concurrent.py`, `select_subset.py`, `dup_content.py`), each exposing `setup`, `measured`, `teardown`, `unit`
  (name and count) and `scale` handling.
- `reference/` — accepted `summary.tsv` files named `<date>-<git-rev>.summary.tsv` and a one-line `CURRENT` file.
- `tests/test_report.py` — unit tests of the report arithmetic (§5).
- `README.md`.

Server configuration (one file generated per run, `config.d/yardstick.xml`): three disks over one bucket with distinct
prefixes — `cas` (`type=object_storage`, `object_storage_type=s3`, `metadata_type=cas`, `cas_server_root_id` fixed,
`cas_gc_enabled` per the `--gc` flag, `cas_gc_interval_sec=20`), `s3` (`type=s3`, local metadata), `s3plain`
(`type=s3_plain`); three cache disks `cas_cached`, `s3_cached`, `s3plain_cached` (`type=cache` over each, identical
`max_size`, one cache directory per disk under the run directory — a cache cannot be shared between disks); six
single-volume policies named after the disks. S3 client settings (keep-alive, retries, request concurrency) are the same
block for all three disks. Logging at `debug`, `trace_log` enabled with a 7.5 s flush.

Run order: for each policy → for each repeat k in 1..K → **cold**: start a fresh server process on a fresh run
directory (empty caches, empty local metadata for `s3`), run the card's `setup`, run `measured` once and measure it; →
**hot**: run `measured` again on the same process and measure it; → `teardown`; → stop the server. The bucket prefix of
a policy is reused across repeats; each card cleans up after itself (§2), so a repeat starts from an empty prefix except
for what RustFS keeps of deleted objects. Policies never share a server process: `system.events` counters are per
process, and this is what makes the attribution exact.

Run directory: `runs/<timestamp>/<policy>/<card>/k<N>/{cold,hot}/` with `events_before.tsv`, `events_after.tsv`,
`events_delta.tsv`, `card.tsv` (per-operation timings where the card records them), `gc_window.tsv` (§3) and the server
log of that process; `runs/<timestamp>/summary.tsv` and `header.tsv` at the top.

## 2. The cards {#cards}

Every card is `setup` (built once per repeat, not measured), `measured` (the only measured step, run twice: cold, then hot),
`teardown`. Tables are `MergeTree ORDER BY k` with `storage_policy` set to the policy under test; every query carries
`database_atomic_wait_for_drop_and_detach_synchronously = 1`. Where `measured` creates and drops its own tables, hot is the
same step on the warm process; where it reads data built by `setup` (3, 4, 6), the table stays until `teardown`. Merges are
stopped (`SYSTEM STOP MERGES`) in cards 2, 3 and 6 so the part count is deterministic; they run in 1, 4, 5 and 7 as in
production. `--scale f` multiplies rows and part counts; the reference is defined at `f = 1` only.

1. **cycle** — unit: cycle (200 at scale 1). 8 workers × 25 iterations; one iteration: `CREATE TABLE (k UInt64, s String)`,
   3 × `INSERT … SELECT number, toString(number) FROM numbers(j*1000, 1000)`, `SELECT count(), sum(k)`, `DROP TABLE … SYNC`.
   Per-operation timings recorded to `card.tsv`. Reference (RCA): 83 GET + 64 PUT per cycle on `cas`, 12 + 31 on `s3`,
   wall 3.1x.
2. **wide_insert** — unit: insert (500). Table of 30 columns (15 `UInt64`, 15 `String`), merges stopped; `measured` = 500
   sequential `INSERT … FROM numbers(i*20000, 20000)` with `max_insert_threads = 1` from one worker: 10M rows, 500 parts.
   `teardown` drops the table; hot re-creates and re-inserts.
3. **attach_200** — unit: part (200). `setup`: 200 inserts of 10k rows with merges stopped; `measured` = `DETACH TABLE`
   then `ATTACH TABLE` (the part-loading read path: how many requests per part open). Hot repeats the pair.
4. **mutation** — unit: part (100). `setup`: 100 parts × 10k rows of `(k, s)`; `measured` =
   `ALTER TABLE … UPDATE s = concat(s, 'x') WHERE 1 SETTINGS mutations_sync = 2`. Hot: a second identical mutation.
5. **lwd_concurrent** — unit: delete (80). `setup`: 50 parts × 100k rows with `enable_block_number_column = 1` and
   `enable_block_offset_column = 1`; `measured` = 8 workers × 10 `DELETE FROM t WHERE k % 8 = w AND intDiv(k, 8) % 10 = i`
   with `lightweight_deletes_sync = 2`. Hot: another 10 predicates per worker (`intDiv(k, 80) % 10 = i`).
6. **select_subset** — unit: query (1). `setup`: 200 compact parts (`min_bytes_for_wide_part = 1073741824`) × 50k rows × 10
   `UInt64` columns; `measured` = `SELECT sum(c3) FROM t` (one column of ten: the range-read class of `cas_selects`). Hot:
   the same query (cached policies are expected to make zero GETs).
7. **dup_content** — unit: table (20). `measured` = 20 tables each filled by one `INSERT … SELECT number, toString(number)
   FROM numbers(1000000)` (identical content); the card where CAS should beat both S3 disks in PUTs and bytes. Hot: 20 more
   tables with the same content; `teardown` drops all.

## 3. Measurement {#measurement}

**Counters.** The difference of `system.events` before and after `measured`: requests by kind — `S3GetObject`,
`S3PutObject`, `S3HeadObject`, `S3ListObjects`, `S3DeleteObjects`, `S3CopyObject`, `S3CreateMultipartUpload`,
`S3UploadPart`, `S3CompleteMultipartUpload` and their `DiskS3*` twins — aggregated into GET, PUT (including multipart parts
and completes), HEAD, LIST, DELETE; bytes — `ReadBufferFromS3Bytes` and `WriteBufferFromS3Bytes`; CAS events
(`CASRequestAttempt`, `CASRootGet`, `CASRootPut`, `CASBlobPut`, `CASBlobHead`, `CASManifestPut`, `CASManifestGet`,
`CASOtherGet`, `CASHotKeyQueueWaitMicroseconds`, `CASRefQueueWaitMicroseconds`) in the CAS column only; wall by the
runner's clock around `measured`. Every event whose delta is non-zero is written to `events_delta.tsv`; the table shows
the aggregates, the file keeps everything. The exact event names are resolved against `system.events` of the binary under
test at start; a name in the aggregate lists that the binary does not know is reported once in the header, not silently
skipped.

**Normalization.** Each card declares its unit (§2); the table shows totals and per-unit values; the RCA reference is per
cycle.

**GC.** CAS GC rounds run in the background and land in the same counters. They are not subtracted (they are real cost);
they are shown: `gc_window.tsv` holds, for the measured window, the rounds from `system.cas_gc_log` (count, outcomes,
`objects_deleted`, total duration), and the CAS column of the table carries "GC rounds in window". `--gc off` runs the
CAS policies with `cas_gc_enabled = 0` for the foreground-only cost.

**Repeats and statistics.** K repeats (default 3) × {cold, hot}. For each metric the report shows the median and the
min..max over repeats; ratios are computed on medians. Request counts are expected to be deterministic: a count that differs
across repeats is marked `~` and its spread is kept in `summary.tsv`. A wall time whose max/min exceeds 1.2 is marked
`noisy`. The run header records the binary's version and git revision, the RustFS image version, the host, the date and
the flags.

**Isolation.** A new server process and run directory per policy; a cold repeat is a fresh process with an emptied cache
directory and dropped server-side caches (`SYSTEM DROP FILESYSTEM CACHE`, `DROP MARK CACHE`, `DROP UNCOMPRESSED CACHE`
after start, for symmetry with hot); hot is the same process without any drop. Cards of one policy run sequentially;
concurrency exists only inside a card's workers.

**Errors.** Any error inside a card (a failed query, a timeout, a server that did not start) marks the card `FAILED`
without numbers, keeps its logs, and the run continues with the next card; the final exit code is non-zero. There are no
default substitutions.

## 4. Output and reference {#output}

**Printed table** (stdout, aligned text): rows = card × phase (cold, hot); two blocks — policies without cache
(`cas`, `s3`, `s3plain`) and with cache (`cas_cached`, `s3_cached`, `s3plain_cached`); columns per policy: wall, GET, PUT,
HEAD, LIST, DELETE, MiB read, MiB written, and for the CAS policies "GC rounds in window"; after the absolute columns, the
ratios `cas/s3` and `cas/s3plain` for wall, GET, PUT and bytes. Cell values are medians with the spread and the `~` /
`noisy` marks; under each block a per-unit line per card. A `FAILED` card prints `FAILED` in every cell.

**Machine output.** `runs/<ts>/summary.tsv`: one row per policy × card × phase × metric with `median`, `min`, `max`,
`repeats`, `per_unit`; `header.tsv` holds the run header. `yardstick.py report runs/<ts>` re-prints the table from these
files.

**Reference.** `reference/<date>-<git-rev>.summary.tsv` is an accepted `summary.tsv`; `reference/CURRENT` names the one in
force. `yardstick.py run --compare [file]` (default: `CURRENT`) prints a third block: current against reference per metric
with the delta in percent, marking `|Δ wall| > 10 %` and any change in a request count. No threshold fails the run (that is
CAS-77). The first checked-in reference is `reference/2026-09-28-rca.summary.tsv` with the RCA numbers for the `cycle` card
on `cas` and `s3` (cold; 83/64, 12/31, wall 3.1x); the other cards carry no reference until the first full run of the
tool on `antalya-26.6`, whose `summary.tsv` becomes `CURRENT`.

## 5. Tests and Definition of Done {#tests}

1. `tests/test_report.py` (`python3 -m unittest discover utils/cas-yardstick/tests`): the arithmetic without a server —
   event deltas to aggregates, median and min..max over repeats, ratios, the `~` and `noisy` marks, per-unit values,
   `summary.tsv` round trip to the printed table, comparison with a reference and its deltas; inputs are small TSV
   fixtures including the RCA reference.
2. `yardstick.py run --smoke`: all seven cards at `--scale 0.01`, `--repeats 1`, the three uncached policies, on a fresh
   RustFS: the end-to-end check of the pipeline in a few minutes; the README names it as the first thing to run after a
   change to the tool.
3. Reproduction of the reference: `cycle` at scale 1, K = 3, three uncached policies on the CI-like environment; the numbers
   must fall inside the RCA spread; that run's `summary.tsv` becomes `reference/CURRENT`. Done inside the task; the log is
   attached to the pull request.
4. `shellcheck` on the shell scripts, `python3 -m py_compile` on the Python files.

Definition of Done (from the task): no C++ is touched, so the `CAS*` gtest gate and the ASan lane are run once to confirm
nothing is affected; documentation is `utils/cas-yardstick/README.md` (launch, cards, output format, how to update the
reference) plus one link from `docs/en/antalya/cas/operations`; no fallback paths (§3 errors); the on-S3 format is not
touched.
