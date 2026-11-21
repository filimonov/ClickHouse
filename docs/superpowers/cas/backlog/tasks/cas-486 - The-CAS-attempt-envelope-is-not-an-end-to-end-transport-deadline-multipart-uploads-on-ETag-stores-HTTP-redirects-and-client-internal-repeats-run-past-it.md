---
id: CAS-486
title: >-
  The CAS attempt envelope is not an end-to-end transport deadline: multipart
  uploads on ETag stores, HTTP redirects and client-internal repeats run past it
status: To Do
assignee: []
created_date: '2026-11-13 22:02'
updated_date: '2026-11-21 08:26'
labels:
  - 'area:lease'
  - 'area:backend'
  - 'origin:review'
  - 'impact:data-loss'
  - 'needs:decision'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: high
ordinal: 591000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The mount fence admits a request when its attempt envelope (`attempt_timeout_ms` + two connect caps, 7 s by default) plus the safety margin fits before the lease deadline, and the lease handover argument assumes an admitted request ends inside that envelope. The transport does not enforce it. Found by the codex review of the self-fence spec (master/tmp/prbundle/wave2/review/sf-r1-codex.out, findings 1 and 2), code at 802077e5c1a: (1) `CA/Backend/CasObjectStorageBackend.cpp:814` forces single-part uploads only for generation-token stores; on ETag stores a mutable object above the multipart threshold (the ref catalog) is written by `WriteBufferFromS3` as create, parts and a conditional complete (`src/IO/WriteBufferFromS3.cpp:409`, `:203`, `:708`), none of which asks the fence again; (2) `src/IO/S3/PocoHTTPClient.cpp:557-744` follows redirects in a loop and `src/IO/S3/Client.cpp:749` repeats a request for credentials, signing region and endpoint changes outside the SDK retry strategy, so `SingleAttemptRetryStrategy` does not make one call one request; send and receive timeouts are per socket operation, not per transfer. Effect today: a write admitted shortly before the lease deadline can send its final conditional request after the successor's reclaim (the observation threshold gives about 11 s of slack past the deadline). The R4 case of tmp/prbundle/wave2/lease-inventory.md (catalog Live -> Removing) is the named damage. Not established: whether a real store produces redirects or a catalog above the multipart threshold in practice; the size of the catalog object against `s3_min_upload_part_size`. Directions: an absolute deadline per backend call passed to the transport; forced single-part for every conditional CAS write; or a fence check before the multipart completion.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-13: DUPLICATE in substance of CAS-343 (per-attempt wall-clock deadline; redirects, client-internal repeats, trickling responses, throttler, credential fetch: High, open since 2026-10-02) and CAS-188 (region redirect re-sign). The controller filed this without searching the backlog first. The only new item here is the multipart case: `conditionalWriteSettings` forces a single PUT only on generation-token stores (`CA/Backend/CasObjectStorageBackend.cpp:810-814`), so on an ETag store a conditional write above `max_single_part_upload_size` (default 32 MiB; CAS mutable objects are kilobytes, so this needs a misconfigured limit or a huge catalog) runs as create, parts, conditional complete without a fence check in between. Fix for that item alone: force single-part for every conditional CAS write (one line). Close this task when the multipart item is moved under CAS-343.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:26
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 Backend/CasObjectStorageBackend.cpp:~815-822 conditionalWriteSettings forces s3_force_single_part_upload only when native_token_type == Dialect::Generation, so ETag-store conditional writes can still go multipart past the fence; no per-call absolute deadline added. Changes: impact:data-loss; label needs:decision. Pointers stale: CasObjectStorageBackend.cpp:814 (now ~:817). Owner question: Which direction: absolute per-call deadline in the transport, forced single-part for every conditional CAS write, or a fence check before multipart completion?
---
<!-- COMMENTS:END -->
