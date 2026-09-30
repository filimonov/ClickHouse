---
description: 'Design for CAS-29.9 (r8): on a folding round the namespace janitor walks pages sequentially on the round thread, deletes each page''s dead-life `_log`/`_snap` keys in one storage-sized batch, and takes the next page only while a 10 s phase budget has time left, so a dropped table drains in one folding round instead of one page per folding round.'
---

# CAS GC: janitor batch deletes under a phase time budget {#cas-gc-janitor-batches-and-budget}

## Decision {#decision}

- **Sequential pages on the round thread.** A page is: authority read, LIST (one logical page), catalog read, one batch
  delete of its dead write-once keys, and the exact-token deletes of its dead `_ckpt`/`_files` keys. Every
  request uses `Retry::standard`, as today. No I/O pool.
- **Batches, storage-sized.** A batch holds `bulkDeleteChunkKeys()` = clamp(min(`gc_bulk_delete_chunk_keys`,
  the storage's batch-delete limit), 1, `kBulkDeleteMaxKeys`) keys. One CAS batch is one admitted request.
  It is one storage request, except that a credential-refresh reissue (`refreshAndClassifyReadFault` in
  `CasRequests.cpp`, and `S3ObjectStorage::refreshAndRetryOnExpiredCredentials` inside the storage call)
  sends it again without a new admission. With the default 1000 and a 1,000-key LIST page, a page is one batch.
- **Authority per cohort, requests per storage.** Authority is read once per cohort: once per page in the
  janitor, once per 1,000-key cohort in `cleanupRefObjects`, as today. A cohort goes out as consecutive
  storage-sized requests, each admitted. A store without batch delete pays one request per key, never
  one authority read per key.
- **Budget checked between pages.** Constant `kJanitorPhaseBudget` = 10 s. The phase takes the next page
  iff elapsed < budget and the last page was complete and had debris. The phase bound is budget + one
  page. Stage C's round deadline later absorbs the constant and bounds the tail of one page.
- **Capability read once per phase.** `isBatchDeleteSupported()` false → one-key batches. Unknown → the
  first batch is the probe. A rejection stops the phase; the next folding round cuts at 1.
- **No per-key recovery of a failed batch, anywhere.** `removeChunkWriteOnceOrOneByOne` loses its
  catch-and-loop. Both other write-once families contain the propagated `NOT_IMPLEMENTED` themselves.
- **Failed batch leaks and advances.** Once its retry policy is exhausted, an ordinary delete failure,
  transient or permanent, counts its keys as `leaked` and the cursor moves past the page, as the
  exact-token path does today. Only lost authority, `NOT_IMPLEMENTED` and a deterministic local failure
  hold the cursor. A key that fails every time is leaked on every pass that reaches it and shows in
  `batches_leaked`/`leaked`; the next full pass retries it.
- **S3 adapter keeps the canonical error name** at all three throw sites, the one-key path included, so
  a name-only refusal ends its CAS retry at once instead of spending the whole retry window. With a
  credential-refresh callback configured, a refusal the storage classifies as an expired token is
  reissued once by the refresh path before it leaks ([adapter](#adapter)).
- **Deferred rounds unchanged.** A `DEFER` round takes one suppressed page: no deletes, no cursor move.
- **Follow-up, not in this change.** Parallel batches across pages on the GC I/O pool, taken only if
  [M1](#m1-long-stream-drain) shows the phase LIST-bound or CPU-bound per page.
- **Proof before merge.** Fresh alternating A/B on `e2dd0f5d343`. Predicted: a 20k-key stream drains in
  one folding round after the DROP. Quiet pools keep one logical LIST page per folding round and no
  more physical LIST requests than base.

## Context {#context}

Figures from `docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/08-janitor-batches-on-long-streams-and-memo-reuse-distance.md`.
They are **hypotheses, not a baseline**: report 08's BEFORE was `61d24118251`, not `e2dd0f5d343`, and its
AFTER carried the manifest memo as well as batches. [Value proof](#value-proof) re-measures both sides.

| | report 08 before (`61d24118251`) | report 08 after (batches + memo) |
|---|---|---|
| janitor DELETE requests, 7k / 20k stream | 2,333 / 15,913 | 3 / 16 |
| janitor phase, 7k at 20 ms RTT | 60.8 s | 5.3 s |
| janitor time per page, no latency / 20 ms | 1.0–1.1 s / 24 s | 0.4–0.6 s / 1.9–2.3 s |
| drain wall time, 7k / 20k | 112 s / 1,307 s | 112 s / 1,307 s |

Drain time does not move with batches alone. `Gc::runNamespaceJanitorPage` runs one page per round, and
only folding rounds delete. `shouldDeferRound` defers up to `gc_fold_max_defer_rounds` (8) rounds in a row,
so after a DROP the janitor deletes on rounds 2 and 3 and then on every 9th round. Drain time is
`pages × 9 × gc_interval_sec`.

The one-page cap bounded round time when a page cost ~1,005 requests (one exact DELETE per key). With a
batch, a page costs ~8 requests on S3.

**Open: report 08's per-page time.** The request model below predicts ~160 ms per page at 20 ms RTT.
Report 08 measured 0.4–0.6 s without latency and 1.9–2.3 s at 20 ms, unexplained. Which one holds decides
whether a 20k drain fits one 10 s window or needs two. M1 measures it; nothing below depends on the model
being right except the predicted round count.

## Execution model {#execution}

### A page {#page}

`NamespaceJanitor::run` on the round thread, per page:

1. `refreshAuthority`. Authority lost: stop; the page is not taken.
2. LIST one page (1,000 keys) from the in-memory cursor.
3. Read the catalog cut after the LIST, as today, and classify every key.
4. Delete dead `_ckpt`/`_files` by exact token, as today.
5. Cut the page's dead canonical `_log`/`_snap` keys into batches of the phase's batch size and send them
   one after another.

Once per phase: the maintenance read before page 1, and one cursor publication after the last page.

| Request | S3, per page | GCS (no batch delete), per page |
|---|---|---|
| authority GET, catalog GET | 2 | 2 |
| LIST, physical `ListObjectsV2` | 3 for a full page with more keys after it; 2 for the page before the last; 1 for the last | same |
| dead `_log`/`_snap` deletes | 1 batch | one per key, serial |
| exact `_ckpt`/`_files` deletes | a few (listing carries tokens) | a few |
| **time at 20 ms RTT** | **~8 requests ≈ 160 ms** | **~1,000 requests ≈ 20 s** |

One logical page costs three `ListObjectsV2` (report `06-list-requests-per-page.md` in the same folder,
measured on RustFS): the response holding the page, the next response for the sentinel key that proves
more keys exist (`CasObjectStorageBackend.cpp:1095`), and the prefetch the async iterator schedules
when that response arrives (`ObjectStorageIteratorAsync.cpp:71`) and its destructor waits for. The round
thread blocks on all three, but they run on the iterator's threads. This change keeps the page contract
and the iterator as they are.

Keys per 10 s budget at 20 ms: S3 ≈ 60 pages ≈ 60k keys. GCS ≈ 500 key-requests, but the check is
between pages, so a GCS phase finishes the page it started: one 1,000-key page in ~20 s per folding
round. That is base's GCS cost; this change neither helps nor hurts it.

### Stop rules {#stop-rules}

The phase takes the next page iff elapsed < `kJanitorPhaseBudget` and the last page was complete and
had a dead-life candidate.

| Stop when | Reason |
|---|---|
| elapsed ≥ budget after a page | the round-duration bound; `budget_exhausted` = 1 |
| authority lost, at a refresh, at the admission check before a batch, or during a batch | no further request is licensed; nothing is published |
| a batch threw a deterministic local failure | the cursor must not pass a page the code did not understand |
| the storage rejected a batch as unsupported | [capability](#capability): the phase ends |
| page not decided (suppressed, ambiguous cut, admission lost) | the cursor did not move; the next page would re-list the same keys |
| page had no dead-life candidate | steady state stays at one page per folding round, today's LIST cost. otel.demo shows `janitor_deleted` 0 over 24 h |
| `next_cursor` empty | no wrap inside a round; the next round starts from the beginning |
| a read of page ≥ 2 threw | [cursor](#cursor): publish the complete prefix |

A leaked batch does not stop the phase: its keys are `leaked` and its page stays complete. A batch that
spent its whole retry window usually exhausts the budget, so the phase then ends after that page.
"Dead-life candidate" is a classification outcome, not a delete outcome, so a page of already-gone keys
still counts as debris.

### Why no per-request deadline {#no-request-deadline}

A per-request deadline was the r4 design (`Retry::within(budget)` frozen for the phase). It fails:

- Every request reserves its whole attempt envelope before admission (`attemptReservationMs`,
  `Backend/CasRequests.h:462`): 7 s with default timeouts, and a conditional write reserves two
  (`CasRequests.cpp:906`). A 10 s window admits requests only for its first ~3 s, and a store with a
  larger envelope admits nothing after page 1.
- A deadline that cuts a page mid-way needs a first-page exemption to avoid starving, and the exemption
  gives back the worst case it was meant to remove.

**Failure asymmetry.**

| Rule | Fails how |
|---|---|
| deadline binds every request | a healthy fast store gets ~3 s of useful work, or none past page 1; budget refusals surface as read exceptions mid-page |
| budget checked between pages | a phase overruns the budget by one page; that page's worst case is today's |

Under-binding costs one page of tail that base already pays on every folding round. Over-binding loses
the throughput the change exists for. The between-pages check is chosen.

### Bound on phase duration {#phase-bound}

`T_phase < kJanitorPhaseBudget + one page + maintenance read + cursor publication`

- **Healthy page:** ~160 ms on S3 at 20 ms RTT; ~20 s on GCS.
- **Page against a failing store:** each request is `Retry::standard`, up to 90 s (`Backend/CasRetry.h:40`)
  plus its envelope. A page is at most its request count × that ceiling. A batch that exhausts its
  window leaks and pushes elapsed past the budget, so a failing store costs at most one page's authority,
  LIST, catalog and batch requests, plus the exact `_ckpt`/`_files` deletes, which leak on failure as
  today.
- **Maintenance read and cursor publication:** one each, `Retry::standard`, as today.

This keeps today's worst case for one page and improves the healthy case only. Stage C's round deadline
is the mechanism that later bounds the tail: it passes `Retry::within(remaining())` to the requests and
replaces the budget with `min(phase_start + budget, round deadline)`. If a round reaches the janitor with
no time left, stage C decides whether it takes a page; this design does not.

**Why 10 s.** 1/6 of `gc_interval_sec` (60 s), 1/30 of the planned round deadline (300 s). A constant
with a one-line reason in `CasGc.cpp`; a setting now would be removed with stage C, and nothing shows a
need to tune it.

### Authority {#authority}

- `refreshAuthority` runs before each page, as before the single page today. Every request samples
  `authority_held` through the operation's liveness predicate, as today. The janitor adds no shared
  state.
- Observed loss: no further page, and the cursor publication is not admitted. Earlier pages keep their
  deletes. A batch that throws because liveness failed holds its page and is not counted as leaked.
- `op.admitted()` is checked before every batch and every exact delete, as base checks before each exact
  delete, and before the publication. A batch whose delete succeeded while liveness turned false stays
  counted: `readLoop` returns the store's success without re-checking liveness (`CasRequests.h:475`),
  and the keys are gone. The next check stops the phase.
- **No instantaneous fencing.** Another leader can replace the durable lease while a synchronous batch is
  in flight, or after the last refresh. `authority_held` does not change until this leader observes the
  replacement, so that batch and the cursor publication can still be sent. Sequential execution does not
  remove this schedule.
- **Safety rests on life identity, not fencing.** Every key in a batch belongs to a life absent from an
  unambiguous catalog cut taken after its LIST. Such a life is terminal: a table re-created with the same
  UUID writes under a fresh incarnation prefix (`mintFreshIncarnation`), so its keys are never in the
  batch, and `buildRefWalkPlan` discards parent lives absent from the catalog. A second leader deleting
  the same keys repeats an idempotent delete.
- The cursor publication is a conditional write on the maintenance etag read at phase start. If a
  competing leader published first and this leader has not yet observed the loss, the gate admits the
  write, the store refuses the precondition, and the resolve read finds the other leader's state:
  `Conflict` (`CasRequests.cpp:1059`). The competing cursor stands. The janitor records an anomaly only
  for `Refused` and `GaveUp` (`CasNamespaceJanitor.cpp:153`), so a `Conflict` is silent, as today. This
  change adds no conflict logging.
- There is no refresh after the publication in the same phase. This leader observes the loss at the next
  invocation's refresh, which stops it before its first page.

### Cursor {#cursor}

A page is **complete** when it was decided, its exact deletes ran (a failed exact delete is `leaked` and
does not block, as today), and each of its batches succeeded or [leaked](#batch-failure). The
published cursor is the `next_cursor` of the last page of the longest complete prefix.

- **Publication:** one conditional maintenance write after the last page, `Retry::standard`, counted in
  the [bound](#phase-bound). A `Refused`, `GaveUp` or thrown publication is an anomaly, not an
  exception, as today; a `Conflict` means another leader's cursor stands and is not recorded. Published
  only if the operation is still admitted.
- **Rejected persisted cursor:** kept as today (`CasNamespaceJanitor.cpp:40`). If page 1's LIST from the
  persisted cursor throws, the phase makes the one-shot maintenance reset under `Retry::once` and
  rethrows. No delete follows in that invocation. `BackendRejectedCursorResetsExactlyAndDeletesNothing`
  stays.
- **Read failure on page ≥ 2** (LIST or catalog GET from this phase's own cursor): the phase stops,
  records an anomaly and publishes the complete prefix. No reset: the failing cursor is not the persisted
  one.
- A crash publishes nothing. The next round re-lists; deleting an absent write-once key is a no-op.

## Batch size {#batch-size}

### Where the limit lives today {#limit-today}

- `S3ObjectStorage::removeObjectsIfExist` splits by `s3_objects_chunk_size_to_delete` (default 1000). The
  CAS path does not use it: `ObjectStorageBackend::removeManyWriteOnce` calls
  `removeObjectsIfExistUnderProfile`, and `removeObjectsIfExistImpl` sends all its keys as one
  `DeleteObjects`. A store configured at 500 still receives 1,000-key requests from GC today.
- `s3_objects_chunk_size_to_delete` is read with `config.getUInt` and no positive check
  (`S3Settings.cpp:54`); 0 is accepted.
- With the capability false, `removeObjectsIfExistImpl` throws `NOT_IMPLEMENTED` for more than one key
  without sending (`S3ObjectStorage.cpp:696`). With it unknown, it sends one `DeleteObjects`; a
  `NotImplemented`/`InvalidRequest`/`InvalidArgument` rejection sets it false and throws `NOT_IMPLEMENTED`
  (`S3ObjectStorage.cpp:742-749`). It never loops per key.
- `removeChunkWriteOnceOrOneByOne` (`CasGc.cpp:393`) catches that `NOT_IMPLEMENTED` and sends one
  admitted request per key.

### The rule {#chunk-rule}

**CAS passes a chunk no larger than the storage batch.** One admitted request is one storage request.

- New `IObjectStorage::batchDeleteKeyLimit() const`, **always ≥ 1**: the most keys one
  `removeObjectsIfExistUnderProfile` call sends as one request.
  - `S3ObjectStorage`: max(1, `s3_objects_chunk_size_to_delete`) while `isBatchDeleteSupported()` is not
    false; 1 once it is false.
  - default: 1.
- `Backend::bulkDeleteKeyLimit()` forwards it; the in-memory and emulated backends take a configurable
  value. `Backend` gives it a default of 1, so **`InstrumentedBackend` must forward it explicitly**:
  `Pool::open` wraps every production backend in it (`CasPool.cpp:391`, `:906`), and an inherited default
  would turn production S3 batches back into one-key requests. The same holds for any capability accessor
  the GC reads. The forwarding sits next to `attemptEnvelopeMs` and `refreshCredentials` in
  `CasInstrumentedBackend.h`.
- `Gc::bulkDeleteChunkKeys()` = `std::clamp(min(gc_bulk_delete_chunk_keys, bulkDeleteKeyLimit()), 1,
  kBulkDeleteMaxKeys)`. The CAS clamp stays although the getter never returns 0: an empty chunk loops
  without advancing (`cleanupRefObjects` reaches `chunk.front()`).
- All three write-once families cut their requests with it: the janitor, `cleanupRefObjects`
  (`CasGc.cpp:3836`) and the owner-removed manifest cleanup (`CasGc.cpp:1280`). The latter two change
  only on a store with `s3_objects_chunk_size_to_delete` below 1000 or without batch delete.
- It sizes **requests, not cohorts**. See [cohort and request](#cohort-and-request).

**Rejected: hand the whole page to the storage and let it split.** One admission would cover several
storage requests, the gap `ObjectStorageBackend::removeManyWriteOnce` exists to close, and CAS could not
tell which request failed.

`batchDeleteKeyLimit` is a fork patch on shared object-storage code: one virtual with a conservative
default of 1, plus the S3 override. Non-CAS deletion does not call it.

### Cohort and request {#cohort-and-request}

A **cohort** is the set of keys one authority validation licenses. A **request** is one admitted
`removeManyWriteOnce`. The storage limit sizes requests; it never shrinks a cohort.

| Family | Cohort | Authority read per cohort | Requests per cohort |
|---|---|---|---|
| janitor | one LIST page | `refreshAuthority` | the page's dead canonical keys, `bulkDeleteChunkKeys()` each |
| `cleanupRefObjects` | clamp(`gc_bulk_delete_chunk_keys`, 1, `kBulkDeleteMaxKeys`) keys, cut by the ref-cleanup budget, as today | `authorityHolds`: a catalog GET and a `gc/state` GET, as today | `bulkDeleteChunkKeys()` each |
| manifest cleanup | clamp(`gc_bulk_delete_chunk_keys`, 1, `kBulkDeleteMaxKeys`) bodies, as today | none, as today | `bulkDeleteChunkKeys()` each; each request's bodies are recorded when it returns |

- Admission differs by family. `CasOperation::gate` checks the mount fence, then the operation's
  `Liveness` predicate if one was given. The janitor supplies its `authority_held` predicate, so an
  observed GC lease loss stops its next request. `cleanupRefObjects` and the manifest cleanup use plain
  `admit()` (`CasGc.cpp:3715`, `:642`) with no predicate: each of their requests checks only the mount
  fence. They check GC leadership per cohort (`authorityHolds` in `cleanupRefObjects`, nothing in the
  manifest cleanup), as base does; this change adds no per-request authority read.
- `NOT_IMPLEMENTED` from any request stops the family for the round ([other families](#other-families)).
  No failed request is re-sent in smaller pieces.
- On a store without batch delete, `cleanupRefObjects` sends exactly base's GETs: base validates once per
  cohort, then its per-key fallback deletes the cohort key by key.

**Failure asymmetry.**

| Rule | Fails how |
|---|---|
| authority read per request | on a no-batch store, 2 GETs per key: ~10,000 extra GETs per 5,000-key cleanup, ~200 s at 20 ms, outside the janitor budget |
| authority read per cohort | a catalog or lease change inside a cohort is seen at the next cohort; that is base's window, and each request still passes the mount-fence admission |

Per cohort costs nothing base does not already accept. Per request is a GCS round-time regression.
Per cohort is chosen.

### Capability {#capability}

Read `isBatchDeleteSupported()` once, before the phase's first batch, and fix the phase's batch size:

| Capability | Batch size | Notes |
|---|---|---|
| false (config or learned) | 1 | one-key admitted requests, no probe |
| unknown | storage limit | the first batch is the probe; unknown is the default for AWS too |
| true | storage limit | |

**A rejection stops the phase.** The batch throws `NOT_IMPLEMENTED`: its page is incomplete, the cursor
stays before it, no further page or batch is taken, and `capability_learned` = 1. The next folding round
reads the capability as false and cuts at 1. **The stop is family-local.** A family that runs later in
the same round reads false at its own start and sends one-key requests that round; that is intended. Its one-key requests are ordinary requests of a known
capability, each licensed by that round's own classification.

**No same-phase per-key recovery can occur.** Batches are sent one at a time on the round thread, and
the phase ends on the rejection before cutting another. The r4 schedule (a worker learns false while the
round thread cuts a one-key batch of the same page) needs two concurrent senders; there is one. If
another user of the same storage flips the capability mid-phase, the janitor's next multi-key batch
throws `NOT_IMPLEMENTED` without a request and stops the phase the same way.

**Why no recovery at all.** Deleting keys one by one in response to a failed batch is a destructive
fallback. The rejection is also not reliably "no batch verb": `InvalidRequest` and `InvalidArgument` map
to the same `NOT_IMPLEMENTED`.

**Cost on GCS:** one request per key, serial. A store whose capability is still unknown pays one rejected
`DeleteObjects` and one held folding round, once per process.

GCS's JSON-API multipart batch is unreachable through the S3 client; out of scope.

### The other two families {#other-families}

`removeChunkWriteOnceOrOneByOne` becomes `removeCohortWriteOnce(op, cohort, request_keys, policy,
on_request_done)`: consecutive `op.removeManyWriteOnce` requests of at most `request_keys` keys, a callback
after each one returns, and any exception propagated. Today neither caller contains a propagated `NOT_IMPLEMENTED`: it escapes the round,
`isTransientGcRoundError` rejects it, and the scheduler clears `i_am_leader` (`CasGcScheduler.cpp:460`).
So each caller catches it.

| Caller | On `NOT_IMPLEMENTED` from a chunk | Reports | Recovery of the chunk's keys |
|---|---|---|---|
| `cleanupRefObjects` (`CasGc.cpp:3853`) | catch, WARNING, stop the family for this round; other exceptions propagate as today | `capability_learned` = 1 on its phase | the plan recomputes the same candidates from the durable checkpoint next round (`CasGc.cpp:3838`), cut at 1 |
| owner-removed manifest cleanup (`CasGc.cpp:1293`) | catch, WARNING, stop the family for this round; requests before it stay recorded (`CasGc.cpp:1276-1279`); other exceptions propagate as today | `capability_learned` = 1 and the count of unsent bodies on `manifest_deletes` | **not the next fold**: the intake cursor that found them is already committed. The orphan-manifest sweep reclaims them (`CasGc.cpp:1252-1259`) |

The round, the tenure and `i_am_leader` are unaffected. It happens at most once per storage per process:
after it the capability is false and no chunk above one key is cut. The manifest family's cost is one
round's unsent bodies handed to the bounded orphan sweep, once.

## Batch failure {#batch-failure}

`CasOperation::removeManyWriteOnce` reissues the whole batch inside its policy; an absent key counts as
success. `removeObjectsIfExistImpl` sends `DeleteObjects` in quiet mode, skips per-key `NoSuchKey`, and
throws `S3Exception` for the first remaining per-key error after the other keys are gone. A batch that
finally throws may have deleted any subset of its keys; the janitor does not parse which.

**Rule: an ordinary failure leaks and advances; three cases hold.** The same rule the exact-token path
has today (`CasNamespaceJanitor.cpp:134`). `op.admitted()` is checked first: a batch that fails after
authority is lost holds, whatever it threw.

| Class | Exceptions | Retry inside the policy | Action after the throw |
|---|---|---|---|
| refused | `isDefinitelyRefusedWrite(e)`: malformed request, entity too large, access denied, a credential error the one refresh did not fix | none: the read loop ends at once (`CasRequests.cpp:480`) | count the keys as `leaked`, `batches_leaked` += 1, one anomaly, page stays complete, continue |
| exhausted | transport, throttling (`SlowDown`, 503), 5xx, timeouts, a non-S3 `Poco::Exception` | until `Retry::standard` gives up | as refused |
| authority lost | the liveness predicate failed; `op.admitted()` is false after the throw | none | hold: `batches_held` += 1, keys not counted, stop, no publication |
| capability | `NOT_IMPLEMENTED` | none | [stop the phase](#capability), hold |
| local bug | `isDeterministicLocalFailure`: `LOGICAL_ERROR`, `BAD_ARGUMENTS`, `CORRUPTED_DATA`; a non-`Poco` exception | none (`CasRequests.cpp:458`) | hold: `batches_held` += 1, stop; never advance past a page the code did not understand |

**Failure asymmetry.**

| Rule for an exhausted transient failure | Fails how |
|---|---|
| hold the cursor | a key that returns `SlowDown` forever starves every page behind it, indefinitely. With one-key batches, each run starts at that key and deletes nothing |
| leak and advance | a key that could have been deleted a round later waits one full pass, counted in `leaked` |

Holding can wedge debris reclamation for good; advancing costs one pass and is never a wrong delete.
Advance is chosen.

- **Repeat failures are visible.** A key that fails every time is leaked on every pass that reaches it:
  on a one-page namespace, every folding round. It shows in `batches_leaked` and `leaked`, and the
  anomaly names the key range and the exception.
- **Termination.** Every batch ends in success, a leak or a hold. Success and leak advance past the
  page; a hold is one of the three stop cases, none of which a storage throttle can produce.
- **Partial deletes.** A leaked batch may have deleted any subset of its keys. `leaked` counts the whole
  batch: an upper bound. The next pass lists only the survivors.
- **Mixed per-key errors** classify by the first remaining one, the type the storage throws. The class
  decides only how long the batch retries; both leak.
- The next pass re-lists and re-classifies against a fresh catalog cut; nothing from a failed attempt is
  reused as a license.

### S3 adapter: keep the error name {#adapter}

`isDefinitelyRefusedWrite` matches `EntityTooLarge` and several malformed-request errors by name only
(`S3Common.cpp:62-80`). All three delete throw sites a CAS batch reaches build `S3Exception` without
the name:

- whole-response error, `S3ObjectStorage.cpp:751`;
- first remaining per-key error, `S3ObjectStorage.cpp:769`;
- **one-key batch:** `removeObjectsIfExistImpl` sends a single key as a plain `DeleteObject` through
  `deleteFileFromS3` (`S3ObjectStorage.cpp:681`), which throws at `deleteFileFromS3.cpp:77`. Every batch
  on a store without batch delete, and any chunk of one key, goes this way.

**Change:** use the existing `S3Exception(PreformattedMessage &&, S3Errors, String exception_name)`
constructor, built with `PreformattedMessage::create` from the current format strings. Pass
`err.GetExceptionName()` at `:751` and at `deleteFileFromS3.cpp:77`, and the first remaining per-key
`err.GetCode()` at `:769`. `deleteFileFromS3` is shared with non-CAS deletes; the change adds the name
and keeps the error code and message.

**Why it matters under leak-and-advance.** Without the name, a name-only refusal counts as exhausted: the
batch retries for the whole `Retry::standard` window (up to 90 s), overruns the budget, and ends the
phase after that page on every pass. With it, the CAS retry loop ends at once and the batch leaks. The
"one attempt" in the tests below means one CAS attempt with no refresh callback configured.
`AccessDenied` is unaffected: it also matches by enum.

**Refresh reissue.** `removeObjectsIfExistUnderProfile` runs inside
`S3ObjectStorage::refreshAndRetryOnExpiredCredentials`, which repeats the call once when
`isAccessTokenExpiredError` holds and a `credentials_refresh_callback` returns a client
(`S3ObjectStorage.cpp:280`); `S3Common.cpp:41` treats an `UNKNOWN` code as expired. CAS has its own
refresh-and-reissue for the same class (`refreshAndClassifyReadFault`). So one admission can produce one
extra storage request, without a new gate check, when a refresh callback is configured. The guarantee
is: **one admitted request = one storage request, except a credential-refresh reissue**. Whether the
preserved name removes the refresh on a name-only refusal is decided by the test below, not assumed.

**Tests** (`gtest_cas_s3_bulk_delete_fallback.cpp`, emulated S3): a whole-response `EntityTooLarge` and
`MalformedXML`, a per-key `EntityTooLarge`, and a one-key `EntityTooLarge` each throw an `S3Exception`
whose `getExceptionName()` is the canonical code and for which `isDefinitelyRefusedWrite` is true. Red on
base. With a refresh callback configured, the same refusals cost at most one extra storage request and
then leak; the test records the callback count and the request count. The janitor side is `OneKeyNameOnlyRefusalLeaksAfterOneAttempt` in [tests](#tests).

### Versioned buckets {#versioned}

**Precondition: the bucket is unversioned.** The mount probe rejects a store that answers a delete with a
delete marker (`CasProbe.cpp:24`). If versioning is enabled after mount, the quiet batch reports success
while noncurrent versions remain, as `cleanupRefObjects` and the manifest cleanup already do. An empty LIST
then proves the namespace drained, not that storage was reclaimed. Detection is out of scope.

## Deferred rounds {#deferred-rounds}

**No janitor deletes on a deferred round.** The deferred path keeps its single suppressed page: one
maintenance GET, one logical LIST page, one catalog GET, no delete, no cursor move. This is the existing conservative
policy, kept because nothing needs it changed.

A DROP changes shards, so the next rounds fold. The janitor starts on round 2, because in round 1 the
life still resolves. At 160 ms per page round 2 drains a 20k stream. At report 08's 0.5 s per page it
drains all but the last pages, and round 3, which also folds, takes the rest. No 9-round wait either way.

Why a deferred-round delete was not shown unsafe, and why folding does not protect against a vanished
catalog row: see the lifecycle facts in `mintFreshIncarnation`, `CasRefCatalog::createNamespace`,
`identityPreserving` (`CasRefCatalog.cpp:208`) and `buildRefWalkPlan` (`CasGc.cpp:215`).

| Wrong choice | Cost |
|---|---|
| keep suppression, and deferred deletes were safe | debris beyond one 10 s window waits up to 9 rounds per further window |
| allow deferred deletes, and an unexamined term of the fold gate mattered | a destructive family runs on a round the gate would have stopped |

**Reopen if** an A/B shows a realistic drain needing more than two folding rounds, the defer cadence
dominates its wall time, and stage C3 (`requestRoundSoon`) has not landed.

## Settings {#settings}

- None new. `kJanitorPhaseBudget` is a constant until stage C absorbs it.
- Test seam: `PoolConfig::gc_janitor_phase_budget_ms_for_test` (`std::optional<uint64_t>`). Not a setting.
- `gc_bulk_delete_chunk_keys`: its description gains "dead-life ref logs and snapshots of the namespace
  janitor" and "capped by the storage's batch-delete limit".
- `s3_objects_chunk_size_to_delete` now also caps GC batches on a CAS disk; 0 behaves as 1 there.
- CAS-29.9's acceptance criteria #2 and #3 name page settings and a `pages=10` run; this design replaces
  both. The backlog task needs that edit when the spec is accepted.

## Code shape {#code-shape}

- `NamespaceJanitor::runOnePage` → `NamespaceJanitor::run(bool suppress_deletes, JanitorRunContext,
  NamespaceJanitorResult & out)`. The context carries the phase budget, the clock, the batch-size
  function and a `refresh_authority` callback. `run` owns the loop, the maintenance read and the single
  cursor publication, and writes into `out` as it goes, so a throw leaves completed work counted.
- From `cas/gc-request-cuts` @`1edf28f2a9a`: the classification and `Layout::writeOnceStreamKey`
  (`CasLayout.h`), which batches a listed key only if it equals its canonical rendering; others go to the
  exact path. A write-once batch has no token, so it must name only keys the layout renders. Not taken:
  `flush_chunk`'s per-key retry through `remove_exactly`, and `batch_supported`.
- `removeChunkWriteOnceOrOneByOne` → `removeCohortWriteOnce`; both callers keep their cohort loop and
  per-cohort work (`authorityHolds`, the budget cut, recording) and catch `NOT_IMPLEMENTED`
  ([cohort and request](#cohort-and-request), [other families](#other-families)).
- `Gc::runNamespaceJanitorPage` → `Gc::runNamespaceJanitor`. Its existing `catch` emits the partial `out`.
  The deferred call site keeps `suppress_destructive = true`; the stop rule ends it after one page.
- `run` times each step with a stopwatch (`list_ms`, `catalog_ms`, `delete_ms`) and snapshots the global
  `DiskS3ListObjects` counter at phase start and end (`list_requests_process`).
- `IObjectStorage::batchDeleteKeyLimit`, `S3ObjectStorage` override, `Backend::bulkDeleteKeyLimit`,
  `InstrumentedBackend` forwarding (and of any capability accessor), `Gc::bulkDeleteChunkKeys`; the
  [adapter](#adapter) change.

## Observability {#observability}

The janitor issues its requests from the round thread, but the LIST requests run on the async
iterator's threads. `GcPhaseTimer` snapshots the round thread's `ProfileEvents`, so its delta misses every
`ListObjectsV2`: report 06 found `DiskS3ListObjects` 0 on phase rows. `delete_requests` relies on the
delete events landing on the calling thread; `WireRequestsIncludeRetriedAttempts` checks it. LIST is therefore measured two other ways:

- **Count:** `list_requests_process`, the process-wide `DiskS3ListObjects` delta over the phase. It
  includes other threads' LISTs, so it attributes to the janitor only on an otherwise idle server (M1).
- **Time:** `list_ms`, the round thread's wall time inside the LIST step. The round thread blocks on
  every response the page needs, and the iterator's destructor waits for the prefetch, so this covers
  all three requests.

Metrics are `std::map<String, UInt64>` (`CasGc.h:219`), so every value is numeric.

`namespace_cleanup` row in `system.cas_gc_log`, summed over the round's pages:

| Metric | Status | Meaning |
|---|---|---|
| `janitor_pages`, `janitor_keys`, `janitor_deleted`, `leaked`, `evidence_rows` | existing | now sums; `janitor_pages` counts logical pages; `janitor_deleted` counts keys of succeeded batches, including already-absent keys; `leaked` adds all keys of leaked batches |
| `batches` | new | batches sent |
| `batches_leaked` | new | batches refused or exhausted: keys leaked, cursor advanced |
| `batches_held` | new | batches that held the page: authority lost or a deterministic local failure |
| `list_requests_process` | new | process-wide `DiskS3ListObjects` delta over the phase |
| `list_ms`, `catalog_ms`, `delete_ms` | new | round-thread wall time in each step, summed over pages; with the phase's round-thread CPU they split per-page time into waits and CPU |
| `batch_keys` | new | the phase's batch size; shows the storage limit and capability in effect |
| `capability_learned` | new | 1 when a batch was rejected as unsupported |
| `delete_calls` | new | logical delete calls (`removeManyWriteOnce` and exact `remove`), failed ones included |
| `delete_requests` | new | wire deletes: the phase's delta of the S3 delete `ProfileEvents`, (`DiskS3DeleteObjects` is incremented per batch attempt, `S3ObjectStorage.cpp:720`). `WireRequestsIncludeRetriedAttempts` checks that it counts attempts. `system.blob_storage_log` is not a substitute: it has one row per key per attempt (`:728`) and no request id, so it counts objects, not requests. If the event does not land on the calling thread, `delete_requests` is reported as unavailable and the A/B uses the server-side `DiskS3DeleteObjects` total over the drain window |
| `budget_exhausted` | new | 1 when the budget stopped the phase |
| `cursor_start_fp`, `cursor_end_fp` | new | `sipHash64` of the cursor at phase start and the one published; 0 for the empty cursor |
| `cursor_advanced` | new | 1 when a new cursor was published |

The same `cursor_start_fp` with `cursor_advanced` 0 on every folding round is a stuck page; a moving
`cursor_start_fp` is failures on successive pages.

ProfileEvents: new `CASGCNamespaceCleanupObjectsDeleted` (twin of `CASRefCleanupObjectsDeleted`, same
"includes absent keys" caveat), new `CASGCNamespaceCleanupBatchFailures` (leaked and held batches), and
`CASGCNamespaceCleanupLeaks` also counts keys of leaked batches. One WARNING per failed batch. No new `CasEvent`.

Docs: phase 16 of `docs/en/antalya/cas/architecture/garbage-collection.md`, the `namespace_cleanup` row
of `docs/en/operations/system-tables/cas_gc_log.md`, the batch-size note in
`docs/en/antalya/cas/configuration.md`, and the unversioned-bucket consequence for batched keys.

## Value proof {#value-proof}

Owner rule: no merge without an A/B that moves the target metric. **Target: drain wall time from the end
of `DROP TABLE ... SYNC` until no `_log`/`_snap` key of the life remains.** Secondary: janitor wire DELETE
requests, janitor phase time, per-page time, round duration.

Base = `e2dd0f5d343`. After = this change on the same base. Both built from the same tree, run
alternately (B, A, B, A, ...) on the same stand. Report 08 supplies predictions only.

### M1: long-stream drain {#m1-long-stream-drain}

Driver `master/tmp/gc-cuts-stand/value/m1.py`, `ch1` + RustFS + Keeper, rounds forced every 10 s, ≥ 3 runs
per side per cell. **M1 carries the value proof.** ch1 runs nothing but the forced rounds, so
`list_requests_process` is the janitor's LIST count. Every after-run reports per-page time split into
`list_ms`, `catalog_ms`, `delete_ms` and round-thread CPU; that settles the [open question](#context).

| Cell | Metric | Predicted (after) | Pass |
|---|---|---|---|
| 7k, no latency | janitor phase | ≤ 1 s | |
| | janitor-active rounds | 1 | 1 |
| | janitor wire DELETE requests | 7 | ≤ 10 |
| | physical LIST requests | janitor: ~3 per page, 2 and 1 on the last two: ~18 | process-wide `DiskS3ListObjects` over the drain window (`metric_log`, report 01's method) ≤ base; base has no `list_requests_process` |
| 20k, no latency | janitor phase | ~3 s at 160 ms per page; ~10 s at 0.5 s | |
| | janitor-active rounds | 1 | ≤ 2 |
| | drain wall | round 1 (~23 s) + one interval + round 2 ≈ 45 s | ≤ 90 s, and below every base run |
| | janitor wire DELETE requests | 20 | ≤ 25 |
| 7k, 20 ms RTT | janitor phase | ~1 s at 160 ms per page | ≤ 5.3 s (report 08's batches-only figure) |
| 100k, no latency | janitor-active rounds | 2 at 160 ms per page (60 pages per window) | `budget_exhausted` 1 on all but the last |

If per-page time is report 08's 0.4–0.6 s rather than ~160 ms, the 20k cell needs two folding rounds; that
still passes, and the profile names the hot spot. Fixing it is a separate task unless trivial.

Base runs the 7k, 20k and 7k latency cells, measured fresh. Base is not run at 100k: ~95 pages × 9 × 10 s
≈ 2.4 h per run. If seeding 100k keys takes more than 1 h, stop at 50k and say so.

**Fail:** janitor-active rounds on the 9-round cadence with `budget_exhausted` 0: the loop did not
continue. Debug before re-measuring.

**Parallel follow-up** is justified only if M1 shows the phase hitting the budget on S3 at ≤ 20 ms RTT
with requests, not round-thread CPU, dominating the page.

### M2: S30 create/drop churn {#m2-s30}

`utils/ca-soak/gc-ab/run-stand.sh run <label> <binary> --scenario S30 ...`, read with `t2.py` and
`extract.py`, 3 runs per side. Prediction: `defer_decision.dead_life_debris` and `ref_keys_listed` grow
with drops on base and stay flat on after; leader round duration over the last third is after ≤ base;
`janitor_pages` 1 on folding rounds without debris, both sides. If base does not grow at S30's scale, say
so; M1 carries the proof.

### M3: steady state, no regression {#m3-steady-state}

S11 `--scale ci`, 3 runs per side (`compare.py`). On every folding round: `janitor_pages` = 1 (one
logical page), `batches` = 0, phase time unchanged within spread. Physical LIST requests: process-wide
`DiskS3ListObjects` per round from the `metric_log` window, after ≤ base; `list_requests_process` is
reported but not a criterion, since S11's other LIST callers run concurrently. Request mix per round
otherwise unchanged: authority GET, maintenance GET, catalog GET, maintenance PUT.

### M4: storage without batch delete {#m4-no-batch}

GCS stand (`reference_gcs_soak_stand_recipe`), after binary only except the
[ref-cleanup comparison](#m4-ref-cleanup), one 20k drain with the capability
configured false: `batch_keys` 1 from the first round, `delete_requests` ≈ keys, one page per folding
round, phase ≈ one page (~20 s at 20 ms), no `NOT_IMPLEMENTED`. Then once with it unknown, asserted
**per family** because the stop is family-local. Families run in order: manifest cleanup
(`CasGc.cpp:1252`), namespace janitor (`CasGc.cpp:1334`), ref cleanup.

- The rejecting family: exactly one rejected `DeleteObjects`, `capability_learned` 1 on its row, no
  one-key request for its failed cohort in that round.
- Families after it in the same round may read false and send one-key requests; allowed.
- If manifest cleanup rejected, its unsent bodies are reported and later deleted by the orphan-manifest
  sweep.
- The janitor has `batch_keys` 1 from the round after its own rejection, or from the same round if an
  earlier family learned false.

Which family learns first depends on whether the DROP's manifest bodies reach cleanup before the janitor
sees debris. The run records which family had `capability_learned` 1 and checks the bullets for that
family; either outcome passes.

#### M4 ref-cleanup comparison, base vs after {#m4-ref-cleanup}

Same GCS stand, capability configured
false, 2 runs per side, alternating. A live table takes single-row inserts until `ref_object_cleanup`
deletes ≥ 2,000 keys per round on both sides.

| Metric, per `ref_object_cleanup` phase | Source | Pass |
|---|---|---|
| authority GETs: reads of the catalog object and of `gc/state` | `system.blob_storage_log` inside the phase window | after = base, = ⌈keys / 1,000⌉ each |
| delete requests | `system.blob_storage_log` | after = base, ≈ keys |
| phase duration | `system.cas_gc_log` | after ≤ base + spread |

## Tests, failing first {#tests}

`src/Disks/tests/gtest_cas_namespace_janitor.cpp`, suites `CASNamespaceJanitor` (in-memory backend,
injected request clock) and `CASNamespaceJanitorIntegration` (`Gc` + `openPoolForTest`). "Red" fails
against `e2dd0f5d343`; "guard" is green on base and stays green. A red test fails on
an assertion of new behaviour, not only on a missing hook or metric name. Orderings use backend hooks, never
sleeps. Fault schedules first; the happy path once.

**Batching and size**

| Test | Asserts | Base |
|---|---|---|
| `DeadLifeStreamKeysDrainInBatches` | 2,500 dead `_log` over 3 pages, limit 1000: 3 `removeManyWriteOnce` calls, no exact remove on them, `delete_calls` = 3, all gone | red |
| `BatchSizeFollowsTheStorageLimit` | storage limit 250: a 1,000-key dead page is 4 batches of 250 | red |
| `ZeroStorageLimitCutsOneKeyBatches` | backend limit 0: batches of 1, the loop advances; same for `cleanupRefObjects` and the manifest cleanup | red |
| `PoolWrappedBackendForwardsStorageLimit` | a pool opened through `Pool::open` (so the backend is wrapped in `InstrumentedBackend`) over storage with limit 250: `bulkDeleteKeyLimit` is 250 and a 1,000-key dead page is 4 batches; a decorator that dropped the forwarding would give 1,000 batches | red |
| `S3BatchDeleteKeyLimitFollowsChunkSetting` | `objects_chunk_size_to_delete` 500 → 500; 0 → 1; after `setIsBatchDeleteSupported(false)` → 1 | red |
| `StateKeysNeverEnterTheBatchPath` | dead `_ckpt`/`_files` go by exact token | guard |
| `NonCanonicalStreamKeyUsesExactPath` | a parseable key whose canonical rendering differs goes to the exact path | guard |

**Capability and the other families**

| Test | Asserts | Base |
|---|---|---|
| `NoBatchCapabilitySendsOneKeyRequestsWithoutProbe` | capability false: every dead canonical stream key goes through a one-key `removeManyWriteOnce`, zero exact-token `remove` calls for them, `batch_keys` 1, no `NOT_IMPLEMENTED`, `batches_held` 0 | red |
| `UnknownCapabilityRejectionStopsThePhase` | unknown, 3 dead pages, the first batch throws `NOT_IMPLEMENTED`: 1 backend delete call, zero one-key calls, no page 2 LIST, cursor unchanged, `capability_learned` 1; the next run cuts at 1 and drains | red |
| `CapabilityFlippedByAnotherUserStopsThePhase` | capability flips false between page 1's and page 2's batch: page 2's batch throws without a backend call; phase stops; cursor after page 1 | red |
| `CohortHelperPropagatesNotImplemented` | `removeCohortWriteOnce`, cohort 1,000: request size 1,000, first request rejected → 1 call, rethrows, no per-key call; request size 250, second request rejected → 2 calls, callback once, rethrows. Replaces the per-key tests of `gtest_cas_gc_bulk_delete_fallback.cpp`; the teardown-between-keys test goes with the loop | red |
| `RefCleanupReadsAuthorityOncePerCohort` (integration) | capability false (backend limit 1), 2,500 deletable ref keys of a live namespace, `gc_bulk_delete_chunk_keys` 1000, budget 5,000. A counting decorator around the in-memory backend records reads by key: 3 catalog reads and 3 `gc/state` reads in the phase, 2,500 one-key `removeManyWriteOnce` calls, all keys gone | red on the call count (base sends 3 bulk calls); the authority count is the guard and equals base |
| `ManifestCleanupSendsCohortAsStorageRequests` (integration) | capability false, 2,500 owner-removed bodies: 2,500 one-key requests, `requests` 2,500, `accepted` 2,500, zero catalog and `gc/state` reads in the phase | red |
| `RefCleanupContainsCapabilityRejection` (integration) | `cleanupRefObjects` chunk rejected: the round completes, `i_am_leader` stays, no one-key call that round; next round deletes the candidates at size 1 | red |
| `ManifestCleanupContainsCapabilityRejection` (integration) | manifest chunk rejected: the round completes, earlier chunks recorded, unsent count reported; the orphan-manifest sweep later deletes the unsent bodies | red |

**Batch failure and adapter**

| Test | Asserts | Base |
|---|---|---|
| `ExhaustedTransientBatchLeaksAndAdvances` | every attempt answers `SlowDown`: attempts until `Retry::standard` gives up on the injected clock, then `leaked` = batch size, `batches_leaked` 1, cursor advances, no per-key delete | red |
| `PermanentRefusalLeaksAndAdvances` | armed `AccessDenied`, no refresh callback: one attempt, `leaked` = batch size, `batches_leaked` 1, cursor advances, next page taken | red |
| `UndeletableKeyDoesNotStarveLaterDebris` | 3 dead pages, one key in page 1 always refused: pages 2 and 3 gone after one run, cursor past page 3 | red |
| `SlowDownForeverDoesNotStarveLaterDebris` | capability false, key 500 of page 1 answers `SlowDown` on every attempt, forever; 2 dead pages behind it. Budget seam above the retry window: one run deletes every other key of the 3 pages, `leaked` 1, `batches_leaked` 1, cursor past page 3. Default budget: run 1 ends after page 1 with `budget_exhausted` 1 and the cursor past page 1; run 2 drains pages 2–3. After the pass wraps, the next run leaks key 500 again | red |
| `OneKeyNameOnlyRefusalLeaksAfterOneAttempt` | emulated S3, capability false, no refresh callback: one key's `DeleteObject` answers `EntityTooLarge` by name only; dead keys follow it on the same page and the next. Exactly one attempt for that key, `leaked` 1, every other key gone in the same phase, cursor past both pages, `budget_exhausted` 0 | red |
| `MixedPerKeyErrorsClassifyByFirst` | no refresh callback; `AccessDenied` then `SlowDown`: one attempt; reversed: attempts until the policy gives up. Both leak the batch and advance | red |
| `PerKeyErrorInOkResponseLeaksTheBatch` | all but one key deleted, the survivor answers `SlowDown` on every attempt: `janitor_deleted` excludes the batch, `leaked` = batch size, cursor advances, the next pass lists only the survivor | red |
| `DeterministicLocalFailureHolds` | batch throws `CORRUPTED_DATA`: cursor held, `batches_held` 1, `leaked` 0, phase stops | red |
| `AuthorityLostBeforeRetryHolds` | limit 500, one dead 1,000-key page, 2 dead pages behind it. `failNextBulkRemoveWith` arms a `SlowDown` on the first batch; liveness is true only while `bulkRemoveCalls()` is 0, so the reissue fails admission. The in-memory store is the record of the outcome: `bulkRemoveCalls()` 1, all 1,000 keys present, `batches_held` 1, `leaked` 0, `batches` 1, no second batch, no page 2 LIST, no cursor publication | red |
| `AuthorityLostDuringSuccessfulBatchCounts` | same page; `onBeforeBulkRemove` turns liveness false on the first batch, and the delete then applies. `bulkRemoveCalls()` 1, the first 500 keys gone, the second 500 present, `batches` 1, `janitor_deleted` 500, `batches_held` 0, `leaked` 0, no second batch, no page 2 LIST, no cursor publication | red |
| `S3BatchErrorKeepsCanonicalName` | [adapter](#adapter) test: whole-response `EntityTooLarge` and `MalformedXML`, per-key `EntityTooLarge`, one-key `EntityTooLarge` through `deleteFileFromS3` → name preserved, `isDefinitelyRefusedWrite` true | red |
| `NameOnlyRefusalLeaksAndAdvances` | multi-key batch, per-key `EntityTooLarge` through the emulated S3 backend, no refresh callback: one attempt, `leaked`, cursor advances | red |
| `NameOnlyRefusalWithRefreshCallbackReissuesAtMostOnce` | same refusal with a counting `credentials_refresh_callback` that returns a client: at most 2 storage requests for the batch, callback called at most once, then `leaked` and the cursor advances; `delete_requests` counts both requests, `delete_calls` 1 | red |

**Budget, cursor and authority**

| Test | Asserts | Base |
|---|---|---|
| `PhaseBudgetCheckedBetweenPages` | budget 10 s, clock advances 4 s per page: pages start at 0, 4, 8 s; page 3 completes although it ends at 12 s; no page 4; `budget_exhausted` 1; cursor after page 3 | red |
| `SlowFirstPageCompletesThenStops` | budget 1 s, page 1 takes 20 s (one-key batches), dead pages behind it: page 1 completes, cursor advances, no page 2, `budget_exhausted` 1. Base also completes one page and stops; only `budget_exhausted` makes it red | red |
| `RejectedPersistedCursorResetsExactlyAndDeletesNothing` | existing `BackendRejectedCursorResetsExactlyAndDeletesNothing`, kept | guard |
| `LaterPageReadFailurePublishesPrefix` | pages 1–2 delete, page 3's catalog GET throws: cursor after page 2 published, no reset, counts of pages 1–2 on the row | red |
| `ObservedAuthorityLossPublishesNothing` | refresh before page 3 fails: no page 3 LIST, no cursor publication, pages 1–2 deletes stand | red |
| `LeaseReplacedBeforeFinalBatch` | hook after the last page's classification, before its final batch: a second leader replaces the durable lease, the table's UUID is re-created under a fresh incarnation and writes keys, and the second leader publishes its maintenance state. This leader's batch is still sent and names only old-life keys; new-life keys survive; this leader's cursor publication returns `Conflict` (recorded by a test wrapper around `casGcMaintenanceState`), no publication anomaly is recorded, and the stored maintenance state is the second leader's. The next invocation's refresh stops it before its first LIST | guard: expected green on base's exact path under the same schedule; kept to pin it on the batch path |
| `RecreatedLifeAfterClassificationKeepsItsKeys` | a new life of the same table created after page classification, before its batch: the batch names only old-life keys, new-life keys survive | guard |
| `RecreatedLifeBetweenListAndDeleteKeepsItsKeys` | from the branch: a new life created between LIST and catalog read keeps its keys | guard |

**Stop rules and observability**

| Test | Asserts | Base |
|---|---|---|
| `SteadyStateTakesOnePage` | 5,000 live keys: `janitor_pages` 1, one maintenance PUT | guard |
| `DeferredRoundTakesOneSuppressedPage` | `DEFER` with debris: 1 page, 0 deletes, cursor unchanged, no PUT | guard |
| `EndOfPassStopsWithoutWrapping` | debris smaller than a page: 1 page, cursor reset to empty | guard |
| `CursorFingerprintShowsStuckVsMoving` | two runs holding at one page (`CORRUPTED_DATA`): equal `cursor_start_fp`, `cursor_advanced` 0; successive pages: different; empty cursor → 0 | red |
| `WireRequestsIncludeRetriedAttempts` | emulated S3 fails a batch's first attempt: `delete_requests` 2, `delete_calls` 1 | red |
| `FoldingRoundDrainsWithinBudget` (integration) | one folding round, 3,500 dead keys then live keys: `janitor_pages` 5, all dead keys gone, one maintenance CAS | red |

No new `LOGICAL_ERROR`. If implementation adds one, its test is an `EXPECT_DEATH` whose child exits through
`std::_Exit`, split from the throwing tests.

**Integration.** `tests/integration/test_cas_janitor_drain` on RustFS (`with_rustfs`), no `no-parallel`.
`gc_round_ref_cleanup_budget` = 1 leaves the stream to the janitor; a long `cas_gc_interval_sec` lets the
test drive rounds with `SYSTEM CAS GC RUN`. Setup: `SYSTEM STOP MERGES`, ~2,500 single-row inserts,
`DROP TABLE ... SYNC`. Assert: within 3 rounds no `_log`/`_snap` key of the life remains; exactly one round
has `janitor_deleted` > 0, with `janitor_pages` ≥ 3 and `budget_exhausted` 0. Base leaves keys behind. A
second case sets `s3_objects_chunk_size_to_delete` 100 and asserts `batch_keys` 100, `batches` ≥ 25.

## Out of scope {#out-of-scope}

- Parallel batches on the GC I/O pool ([M1](#m1-long-stream-drain) decides).
- A per-request phase deadline ([why not](#no-request-deadline)); stage C's round deadline owns the tail.
- GCS JSON-API batch deletes.
- The manifest-body memo (`CasGcManifestMemo` on the parked branch).
- Batching `_ckpt`/`_files`: mutable, needs a bulk conditional delete.
- Reclassifying the exact-token path's failures: it keeps leaking on every failure.
- Detecting bucket versioning enabled after mount.
- LIST cuts: skipping the deferred round's page, per-life discovery, overlapping authority and catalog
  GETs.
- Janitor deletes on deferred rounds ([reopen condition](#deferred-rounds)).
- Stage C (round deadline, `requestRoundSoon`).
- The targeted drain of retired lives (frozen).

## Review findings addressed {#findings}

| Finding | Resolution | Section |
|---|---|---|
| r3-1 MAJOR: unknown capability deletes per key after a failed batch | rejection stops the phase; `removeChunkWriteOnce`, no per-key loop anywhere | [Capability](#capability) |
| r3-2 MAJOR: bound ignores 90 s retry windows | bound stated as budget + one page, one page's worst case = today's; stage C bounds the tail | [Phase bound](#phase-bound) |
| r3-3 MAJOR: permanent error starves later debris | permanent refusal leaks and advances; starvation test | [Batch failure](#batch-failure) |
| r3-4 MAJOR: storage limit may be 0 | getter ≥ 1, CAS clamp; zero-limit tests | [Batch size](#chunk-rule) |
| r3-5 MAJOR: phase counters miss worker requests | no delete workers; `delete_calls` + `delete_requests`. LIST still runs off the round thread: see r5-3 | [Observability](#observability) |
| r3-6 MAJOR: report 08 is not a same-base baseline | fresh alternating A/B on `e2dd0f5d343`; report 08 gives predictions; per-page time open, measured in M1 | [Context](#context), [Value proof](#value-proof) |
| r3-7 MINOR: authority-loss test vs publication | observed loss publishes nothing. "Sequential, so no in-flight batch" was wrong: see r5-5 | [Authority](#authority) |
| r3-8 MINOR: deferred-round rationale | suppression kept as the conservative policy; reopen condition | [Deferred rounds](#deferred-rounds) |
| r3-9 MINOR: versioned buckets | unversioned precondition; drain ≠ reclamation | [Versioned](#versioned) |
| r3-10 MINOR: test plan | fault schedules; `CasLayout.h` canonical-key check a prerequisite | [Tests](#tests), [Code shape](#code-shape) |
| r4-1 MAJOR: frozen 10 s policy gives ~3 s of work under attempt reservations | no per-request deadline; budget checked between pages; cursor publication policy stated | [Why no per-request deadline](#no-request-deadline), [Cursor](#cursor) |
| r4-2 MAJOR: first-page exemption rests on a false wedge argument | exemption dropped; stage C interplay stated. The "partial page is progress" answer did not hold: see r5-2 | [Phase bound](#phase-bound), [Batch failure](#batch-failure) |
| r4-3 MAJOR: removing the fallback escapes both cleanup families; manifest retry claim false | per-caller containment of `NOT_IMPLEMENTED`; manifest bodies go to the orphan sweep; tests for both callers | [Other families](#other-families) |
| r4-4 MAJOR: parallel capability learning allows same-phase per-key deletes | sequential batches and a phase stop make the schedule impossible; external flip also stops | [Capability](#capability) |
| r4-5 MAJOR: S3 adapter drops the canonical error name | adapter passes the name at the batch throw sites; the one-key site added in r5-1 | [Adapter](#adapter) |
| r4-6 MINOR: "any exception publishes nothing" loses rejected-cursor recovery | reset kept for page 1's persisted cursor; page ≥ 2 read failure publishes the prefix | [Cursor](#cursor) |
| r4-7 MINOR: test expectations contradict the model | tests rewritten for sequential pages; recreate-after-classification schedule added | [Tests](#tests) |
| r4-8 MINOR: string fingerprints; inline double-count | numeric `sipHash64`, 0 for empty; round-thread delta only | [Observability](#observability) |
| r5-1 MAJOR: a one-key chunk goes through `deleteFileFromS3`, which drops the error name | name passed at `deleteFileFromS3.cpp:77` too; one-key adapter test and `OneKeyNameOnlyRefusalLeaksAfterOneAttempt` | [Adapter](#adapter), [Tests](#tests) |
| r5-2 MAJOR: holding on transient failure starves later debris; no strictly positive progress | exhausted ordinary failures leak and advance, as the exact-token path; holds only for lost authority, `NOT_IMPLEMENTED`, local failures; `SlowDownForeverDoesNotStarveLaterDebris` | [Batch failure](#batch-failure), [Stop rules](#stop-rules) |
| r5-3 MINOR: one logical page is three `ListObjectsV2` on iterator threads | request table per report 06; `list_requests_process`, `list_ms`; M1/M3 criteria on logical pages and process-wide LIST ≤ base | [Page](#page), [Observability](#observability), [M1](#m1-long-stream-drain), [M3](#m3-steady-state) |
| r5-4 MINOR: M4's round-wide expectation contradicts the family-local stop | M4 asserted per family; later families may send one-key requests the same round | [Capability](#capability), [M4](#m4-no-batch) |
| r5-5 MINOR: a lease replacement can overlap an in-flight batch | no-fencing statement; safety rests on terminal life identity; `LeaseReplacedBeforeFinalBatch` | [Authority](#authority), [Tests](#tests) |
| r5-6 MINOR: some "red" tests pass on base | one-key-path assertions on `NoBatchCapabilitySendsOneKeyRequestsWithoutProbe`; `budget_exhausted` on `SlowFirstPageCompletesThenStops`; the lease test marked guard | [Tests](#tests) |
| r6-1 MAJOR: storage limit 1 makes `cleanupRefObjects` run `authorityHolds` before every key, ~10k extra GETs per 5,000-key cleanup on GCS | the storage limit sizes requests, not cohorts; authority once per 1,000-key cohort as today; manifest cleanup gets the same shape; `RefCleanupReadsAuthorityOncePerCohort`; M4 base-vs-after authority GETs | [Cohort and request](#cohort-and-request), [M4](#m4-ref-cleanup), [Tests](#tests) |
| r6-2 MINOR: lease-replacement test expects `Refused` for the maintenance write | the stale-etag write resolves to `Conflict`, silent as today; the competing cursor stands; loss seen at the next invocation | [Authority](#authority), [Cursor](#cursor), [Tests](#tests) |
| r6-3 MINOR: authority-loss test conflates a rejected retry with a successful in-flight delete | split into `AuthorityLostBeforeRetryHolds` and `AuthorityLostDuringSuccessfulBatchCounts`; the in-memory store records each batch's fate | [Authority](#authority), [Tests](#tests) |
| r7-1 MINOR: per-request admission does not check GC leadership in all families | admission is mount fence plus an optional `Liveness`; only the janitor supplies `authority_held`; ref and manifest cleanup check leadership per cohort as base does | [Cohort and request](#cohort-and-request) |
| r7-2 MINOR: a credential refresh can reissue a batch without new admission | guarantee restated as one admitted request = one storage request except a refresh reissue; "one attempt" tests run without a refresh callback; refresh-callback test added | [Decision](#decision), [Adapter](#adapter), [Tests](#tests) |
| r7-3 MINOR: capability plumbing omits the production decorator | `InstrumentedBackend` forwards the limit and any capability accessor; `PoolWrappedBackendForwardsStorageLimit` | [Batch size](#chunk-rule), [Code shape](#code-shape), [Tests](#tests) |
| r7-4 MINOR: `blob_storage_log` counts objects, not requests | alternative dropped; `DiskS3DeleteObjects` stays the primary source, with a stated fallback to the server-side total | [Observability](#observability) |
