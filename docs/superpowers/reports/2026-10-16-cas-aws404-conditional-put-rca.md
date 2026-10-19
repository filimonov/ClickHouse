# Stray `<Error> AWSClient: Response status: 404` on CAS s3 stateless lanes: RCA

Code references are at `fe7609c4371` (PR #2555 head). `antalya-26.6-20261008` (`c147de3f55e`) and `antalya-26.6` (`20d68b0d4ae`) contain the same code at these sites.

## Verdict

- **Request:** a conditional `PUT` with `If-Match` on a blob freshness marker `cas_s3/blobs/ch128/<xx>/<hash>.meta`. The PUT comes from an INSERT's blob publication: `PartWriteTxn::ensureBlobPresent` → `reconcileMetaClean` → `CasOperation::readModifyWrite`.
- **Why 404:** GC's `deleteConfirmedMeta` deleted the `Condemned` marker (exact-token `DELETE If-Match`) between the writer's marker GET and its `PUT If-Match`. On S3 and RustFS, `If-Match` on an absent key returns `404 NoSuchKey`.
- **Is it a CAS bug?** No. Both actors run the designed protocol, and the end state is correct: a fresh body plus a `Clean` marker. The engine maps the 404 to `Conflict`, re-reads (scoped GET, 404 at Debug), then creates the marker on attempt 2. The query succeeds, and the test output matches the reference.
- **Defect:** `ObjectStorageBackend::nativeConditionalPut` sends the PUT without `Expect404ResponseScope`, so `PocoHTTPClient.cpp:737` logs the 404 at Error. The line carries the query id. `send_logs_level=warning` sends it to the client, and the test fails on stderr. This is the defect already filed as CAS-354; these three cases are new instances.
- **Fix:** add `Expect404ResponseScope` in `nativeConditionalPut`, only when the write carries `If-Match`. This is a CAS-only change.

## Evidence

### Q1. Same sequence in all 3 occurrences

PR #2555, query `a5d0de48`, `02346_text_index_bug106809`, query 20 (`INSERT INTO tab VALUES`). Same thread `[3281]` throughout:

```
02:45:14.891203 <Information> AWSClient: Response status: 412, Precondition Failed
02:45:14.891810 <Debug> WriteBufferFromS3: S3Exception name PreconditionFailed ... key cas_s3/blobs/ch128/5b/5b0c1b50afa730b48d3a8fe4e5de878e.meta, object size 81
02:45:14.936368 <Error> AWSClient: Response status: 404, Not Found
02:45:14.939881 <Debug> AWSClient: AWSErrorMarshaller: Encountered AWSError 'NoSuchKey'
02:45:14.947835 <Information> WriteBufferFromS3: Single part upload failed with NO_SUCH_KEY error ... key ...5b0c...878e.meta, size 81
02:45:14.977060 <Debug> ReadBufferFromS3: ... Key: ...5b0c...878e.meta ... This error is expected for S3 disk.   <- scoped resolve GET
02:45:15.076378 <Debug> AWSClient: Retrying S3 request to: .../5b0c...878e.meta, aws sdk attempt: 1, clickhouse attempt: 2, kind: Write
02:45:15.101473 [4243] <Debug> TCPHandler: Processed in 0.31888066 sec.
```

The other two occurrences show the same lines in the same order:

- PR #2566, query `498a32f1`, `03169_modify_column_data_loss` (`INSERT`), key `.../6c/6c2cb5c585c796d7da9b4a2954bf9593.meta`, 412 → 404 → scoped GET 404 → attempt-2 write → query OK.
- PR #2574, query `0bb54e1c`, `03671_pk_in_subquery_context_expired` (`INSERT`), key `.../06/065c5119002a9249f5402f38659aa602.meta`, same sequence → query OK.
- CAS-354 already recorded a fourth case of the same shape: PR #2513, `00652_mutations_alter_update`.

Where the steps come from in the code:

1. `CasPartWriteTxn.cpp:370`/`375`: the first marker write. Either `op.create` (`If-None-Match: *`) after an absent-body publication, or `op.replace` (`If-Match`) on a resurrect. It gets **412** because GC's `Condemned` marker is still there.
2. `CasPartWriteTxn.cpp:386`: `readModifyWrite` → `observe` GET. It finds the `Condemned` marker with etag E. A successful GET logs nothing.
3. `readModifyWrite` (`CasRequests.cpp:1141`) → `writeLoop` sends `PUT If-Match: E` and gets **404 NoSuchKey**. `detail::finalizeConditionalWrite` (`CasObjectStorageBackend.cpp:259-270`) maps the 404 to `PreconditionLost` → `Conflict`.
4. The resolve read (scoped GET) returns 404 → `ProvenAbsent`. `decide(nullopt)` → create `Clean` with `clickhouse attempt: 2`. It succeeds.

The only deleter of `.meta` keys: `deleteMetaExact` has one non-test caller, `deleteConfirmedMeta` (`Gc/CasGcMetaWriter.cpp:57-63`). That job is scheduled from `Gc::applyRedeleteOutcome` (`CasGc.cpp:493`) right after the exact-token body delete (`CasGc.cpp:445`). Each timestamp falls inside a GC round that deleted blobs:

| PR | Event | GC round | Round window | Blobs deleted |
|---|---|---|---|---|
| #2555 | 02:45:14.936 | 11 | 02:44:50 → 02:45:17 | 83 |
| #2566 | 21:47:20.201 | 35 | 21:46:32 → 21:47:23 | 660 |
| #2574 | 17:20:19.534 | 54 | 17:18:04 → 17:20:28 | 2187 |

### Q2. Not a missing-object bug

Why the 404 is benign:

- The 404 is on the marker key, in a write. It is not on a body read.
- The protocol allows GC to delete a marker concurrently. `CasGcMetaWriter.cpp:28-40`: "a delete job removes only `Condemned` from a round before the deleting one, `If-Match` the etag it read ... safe because a writer adopts only on `Clean`".
- The order of the race does not change the outcome:
  - Writer's `If-Match` wins: GC's delete gets 412 and leaves `Clean`.
  - GC's delete wins: the writer gets 404, re-reads, and creates `Clean`.
- The writer's fresh body has a fresh `incarnation_tag`, so its ETag is new. GC's exact-token body delete cannot remove it.

The run confirms this:

- All three queries finished without an exception.
- For #2555, the report's failure reason is `having stderror` only. Stdout matches `02346_text_index_bug106809.reference` line for line, and the randomized-settings diagnosis re-ran the test 15 times with 15 passes.
- For #2566, an earlier triage reported 16/16 reruns passing.
- Later in the #2555 run, GC tracks the same hash correctly: `delete_pending blob ch128:5b0c... recovered in-degree 1 -- a fresh dedup-adopt raced the condemn; spared` at 03:23:52 and 03:33:03.

### Q3. Frequency, and why a random test fails

`Response status: 404` in `clickhouse-server.err.log` of every CAS job, latest run of each PR:

| PR | asan s3 1/2 | asan s3 2/2 | binary s3 | binary cas (local) | stress asan | stress debug |
|---|---|---|---|---|---|---|
| #2513 | 0 | 0 | 0 | 0 | 0 | 0 |
| #2555 | **1** | 0 | 0 | 0 | 0 | 0 |
| #2566 | **1** | 0 | 0 | 0 | 0 | 0 |
| #2567 | 0 | 0 | 0 | 0 | 0 | 0 |
| #2570 | 0 | 0 | 0 | 0 | 0 | 0 |
| #2574 | **1** | 0 | 0 | 0 | 0 | 0 |

- Total: 3 lines in 36 job runs. `gh pr checks 2577` listed no CAS jobs.
- Base rate, from the #2555 asan 1/2 full log: 51 `PreconditionFailed` on `blobs/` keys, 8 of them on `.meta` from query threads. One escalated to the 404.

The race needs three things:

- **Content collision.** An INSERT republishes a blob whose old incarnation GC is deleting in that round. The blob store is content-addressed, so tiny parts from different tests share hashes. Tests drop tables constantly, so condemned hashes are common.
- **Narrow timing.** GC's `DELETE If-Match` must land between the writer's marker GET and its `PUT If-Match`. That window is one S3 round trip.
- **A query thread.** The PUT runs on the INSERT's blob-upload thread, which is attached to the query. The line therefore carries the query id and goes to that client. The failing test is whichever INSERT happened to hit the window, so the failures look random.

Why only asan 1/2 and never the binary lanes: plausibly the slower ASan build widens the GET → PUT window. I have not established this. With 3 hits in 6 runs of asan 1/2, against 0 in 6 for each other lane, this could also be shard composition or chance.

### Q4. Absence handling in the CAS backend

| Path | 404 handling |
|---|---|
| `readUnder` (`CasObjectStorageBackend.cpp:637`) | scoped |
| `stream` (`:691`) | scoped |
| `nativeHead` → `S3::getObjectInfoIfExists` (`getObjectInfo.cpp:157/182/212`) | scoped |
| `removeUnder` → `removeObjectIfTokenMatchesImpl` → `Client::DeleteObject` (`Client.cpp:655`) | scoped |
| `abortMultipartUpload` under a single-attempt client | scoped |
| **`nativeConditionalPut` (`:310-327`), used by `write` (`:837-856`) for every `create`/`replace`** | **not scoped** |
| `publish` verbatim `copyObject` (`:955`) | not scoped. A 404 there means the staged source is missing, which is a genuine error, so it should stay at Error. |

### Q5. Reproduction

Not run locally.

- `build/src/unit_tests_dbms` dates from 2026-09-07 and was built from another tree.
- Rebuilding means editing the shared worktree, and this investigation was read-only.

The three CI occurrences are natural reproductions with identical sequences. A failing-first test is specified below.

## Fix

Recommended: CAS-only, about 5 lines in `ObjectStorageBackend::nativeConditionalPut`:

```cpp
/// An `If-Match` PUT on a key deleted since it was read gets 404 `NoSuchKey`, an ordinary
/// answer that `finalizeConditionalWrite` maps to a lost precondition.
std::optional<Expect404ResponseScope> vanished_key_is_an_answer;
if (!ws.object_storage_write_if_match.empty())
    vanished_key_is_an_answer.emplace();
auto buf = object_storage->writeObject(...);
buf->write(...);
... finalizeConditionalWriteInstrumented(*buf) ...
```

Why this works:

- The scope is thread-local. `conditionalWriteSettings` forces `s3_allow_parallel_part_upload = false`, so the PUT runs on the calling thread. Test `CASBackend.ConditionalPutRunsOnTheCallingThread` covers that.
- The scope only affects logging. `PutObject` does not go through `Client::processRequestResult`, so `DiskS3NoSuchKeyErrors` is not counted either way.
- Limiting the scope to `If-Match` keeps one anomaly visible: a 404 on an `If-None-Match: *` create, the "MinIO answers NO_SUCH_KEY to PUT" case.

Risk, by direction of failure:

- **Scope too wide:** a 404 that signals a real problem goes unlogged. Here the engine resolves every `If-Match` 404 with an exact-key read, and callers that need the object present throw with context. The cost is low.
- **Scope too narrow (today):** random stateless reds at roughly 1 per 2 asan 1/2 runs in this sample.

Rejected alternatives:

- Demote 404 globally in `PocoHTTPClient`: an upstream change that hides real missing-object errors on every disk.
- Filter the line in the test harness: hides the line from every reader.
- Wrap the scope at the `ensureBlobPresent` level: the GC and manifest `If-Match` writers hit the same race (none seen in these logs), and a single choke point covers them all.

## Failing-first test

Add to `src/Disks/tests/gtest_cas_s3_single_attempt_client.cpp`, which already has `CountingS3` and `conditionalBackendForTest`.

Logger capture: copy `ScopedAWSClientLoggerRecording` from `src/IO/S3/tests/gtest_aws_logger.cpp`.

**`CASBackend.IfMatchPutOnVanishedKeyIsAConflictNotAnErrorLog`**

1. Seed `/test-bucket/key` in `CountingS3::ObjectStore`.
2. Read it with `op.read` to get its etag.
3. Install a fault that answers `PutObject` with `sendError(404, "NoSuchKey")`.
4. Call `op.replace("key", "v2", etag, Retry::once())`.
5. Expect `Conflict`.
6. Expect the captured `AWSClient` text not to contain `Response status: 404`.

This fails today on step 6 and passes with the fix.

**Control: `CASBackend.IfNoneMatchPutAnswered404StillLogs`**

1. Use the same fault.
2. Call `op.create` on an absent key.
3. Expect `Conflict`.
4. Expect the captured text to contain `Response status: 404`.

This proves the recorder sees the line and pins the `If-Match` narrowing.

## Limits

- **GC's delete was not observed directly.** That GC deleted these exact markers is inferred: GC is the only deleter of `.meta` keys, and a deleting GC round spans each timestamp. GC meta jobs are silent on success, and CI exports no `blob_storage_log`, so the specific `DELETE` was not seen.
- **Which first write got the 412 is unknown.** It was either `create` on the absent path or `replace` on the resurrect path. Both lead to the same `readModifyWrite` and the same conclusion.
- **Lane skew is unexplained.** All hits were on asan 1/2; see Q3.
- **Scope of the conclusion.** It covers these three occurrences and the #2513 one in CAS-354, all from the same mechanism. It does not cover other 404 sources: none appeared in the 36 jobs, but other lanes or older runs were not searched. The stress and integration harness question in CAS-402 is not addressed.

## Artifacts

- `tmp/aws404/2555-asan1/server.log`: full log of the #2555 asan 1/2 job.
- `tmp/aws404/jobs/<pr>/<job>/err.log.zst`: error logs of all 36 jobs.
- `tmp/aws404/jobs/{2566,2574}/stateless_tests_amd_asan_ubsan_cas_s3_storage_parallel_1_2/server.log.zst`: full logs of the other two hits.
- `tmp/aws404/jobs.tsv`: list of the jobs.
