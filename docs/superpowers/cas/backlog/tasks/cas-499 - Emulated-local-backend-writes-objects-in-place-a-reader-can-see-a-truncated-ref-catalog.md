---
id: CAS-499
title: >-
  Emulated local backend writes objects in place: a reader can see a truncated
  ref catalog
status: To Do
assignee: []
created_date: '2026-11-18 17:13'
updated_date: '2026-11-21 08:23'
labels:
  - cas
  - local-backend
  - 'area:backend'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:plausible'
  - 'impact:wrong-result'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: bug
ordinal: 604000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On a pool over local object storage, `ObjectStorageBackend::emuWrite` (`CasObjectStorageBackend.cpp:495`) writes every emulated object, the ref catalog included, through the plain branch of `LocalObjectStorage::writeObject` (`LocalObjectStorage.cpp:630`), which truncates the file on open. A reader can then see a partial object: through `stream` (holds `emu_mutex` only for the open), after a write that threw before `finalize`, or through a second backend instance on the same path. Seen once: stateless test `04284_cas_backup_pointer_holding` under -j4 logged `CAS cas_ref_catalog: truncated object (line without terminator)` (CORRUPTED_DATA) from a background `dropTableFinally`; the drop retried and succeeded; not reproduced in 6 reruns. Older than the blob-intents branch (unchanged since `0aa837e942e`); an S3 store cannot show it. Smallest fix: temporary file plus rename as `emuPublishBlobAtomically` does; blocker: `listUnder` lists the directory before taking the lock (`:1105`), so the temporary key would appear in emulated listings of refs, `blob-intents/` and `gc/gen/`: the fix must hide temporary names from listings. Related: the emulated backend's token table never shrinks; the `CasBackend.h` comment still calls the backend tests-and-development only while the docs describe a single-node setup on it.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 Backend/CasObjectStorageBackend.cpp:495-499 emuWrite still calls object_storage->writeObject(..., WriteMode::Rewrite) and writes in place; a temporary file plus rename exists only for blobs (emuPublishBlobAtomically, ~line 909). Changes: area set to backend; size unset -> small (S): temp file plus rename, and hide temporary names from listUnder; risk set to medium; confidence set to plausible; impact:wrong-result; type unset -> bug: a reader can see a truncated object. Size: S. Pointers stale: line numbers match (:495).
---
<!-- COMMENTS:END -->
