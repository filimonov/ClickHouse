# `CaRefCleanupLicenceCore` TLA+ gate results {#carefcleanuplicencecore-results}

Model: `CaRefCleanupLicenceCore.tla`. It covers the per-chunk licence of `Gc::cleanupRefObjects`, the GC
phase that deletes covered ref logs and snapshots. The model answers one question: which licence terms
protect data, which bound stale work, and what the whole-catalog etag term adds. Design:
`docs/superpowers/specs/2026-11-11-cas-ref-cleanup-per-life-licence-design.md`.

Runner: `./run_refcleanuplicence.sh`, TLC `2026.07.18.145032 (rev: 30cc360)`, one worker. The table records
the 2026-11-11 run. All 12 expectations passed in 6 min 32 s.

## Results {#carefcleanuplicencecore-results-table}

All configurations use `MaxInc = 2`, `MaxId = 3`.

| Configuration | Licence and sabotage | Expected result | Generated / distinct | Depth |
|---|---|---|---:|---:|
| `_sab_rederive` | row, life, lease; delete resolves the name again | `NamedLifeIntact` violated | 68818 / 22953 | 12 |
| `_sab_reuseincarnation` | row, life, lease; rebirth reuses the erased incarnation | `NamedLifeIntact` violated | 36808 / 10159 | 12 |
| `_old_reuseincarnation` | row, life, lease, whole catalog; rebirth reuses the erased incarnation | `NamedLifeIntact` violated | 35590 / 9362 | 12 |
| `_sab_nolease` | row, life | `DeposedLeaderDeletesAtMostOneChunk` violated | 59494 / 19743 | 12 |
| `_sab_norow` | life, lease | `ChangedRowDeletesAtMostOneChunk` violated | 60180 / 20066 | 12 |
| `_witness_churnprogress` | row, life, lease | `WITNESS_LICENSED_AFTER_UNRELATED_WRITE` violated | 4187 / 1602 | 8 |
| `_witness_rebirth` | row, life, lease | `WITNESS_PENDING_CHUNK_ACROSS_REBIRTH` violated | 34998 / 11729 | 11 |
| `_witness_twokeyplan` | row, life, lease | `WITNESS_TWO_KEY_PLAN` violated | 1355 / 605 | 7 |
| `_safe` | row, life, lease | green | 53789436 / 8364961 | 35 |
| `_datafloor` | no term | green (`TypeOK`, `NamedLifeIntact` only) | 63370866 / 9999773 | 36 |
| `_nolife` | row, lease | green | 53789436 / 8364961 | 35 |
| `_old_churnblocked` | row, life, lease, whole catalog | green, including `NoLicenceAfterCatalogWrite` | 31084401 / 4805533 | 32 |

Green runs exhaust their queue. Violation runs stop at the shortest counterexample.

## Properties and traces {#carefcleanuplicencecore-property}

- `NamedLifeIntact`: for the life the catalog names, the checkpoint's snapshot exists, and so does every
  log at or above the checkpoint or above the fold cursor.
- `DeposedLeaderDeletesAtMostOneChunk`: after a lease steal the old leader deletes at most the chunk it
  already licensed.
- `ChangedRowDeletesAtMostOneChunk`: after the fold-time row changes, the pass deletes at most the chunk
  it already licensed.
- `NoLicenceAfterCatalogWrite`: no chunk is licensed after any catalog write since the fold. Checked only
  with the whole-catalog term on.

What the runs show:

- **Data safety does not rest on the licence.** `_datafloor` drops every term and `NamedLifeIntact` still
  holds. It rests on the plan bounds (`log <= cursor`, `log < checkpoint`, `snapshot < checkpoint`), on the
  monotone checkpoint and cursor of one life, and on keys built from the fold-time incarnation of an
  incarnation that is never reused. `Recreate` allocates a new incarnation unless
  `SabotageReuseIncarnation` is on, so this is an assumption of the green runs. The code has one path that
  breaks it: `createNamespace` mints once and its admission retries insert the same incarnation (CAS-449).
- **Fold-time keys are load-bearing.** `_sab_rederive`: `Append` ×2 → `PublishSnap` → `CkptCas` → `Drop` →
  `Fold` → `Check` → `Erase` → `Recreate` → `Append` → `Delete`. The full licence passes at `Check`; the
  rebirth lands before `Delete`, and the re-resolved name points the old plan at the successor's log 1.
- **Incarnation freshness is load-bearing, with and without the whole-catalog term.**
  `_sab_reuseincarnation` and `_old_reuseincarnation` fail on the same trace as `_sab_rederive`. The
  whole-catalog term is evaluated at `Check` and does not cover the window to `Delete`. Dropping it adds
  no premise.
- **The lease term bounds a deposed leader.** `_sab_nolease`: `Append` ×3 → `PublishSnap` → `CkptCas` →
  `Fold` → `Check` → `Steal` → `Delete` → `Check` → `Delete`. The second `Check` passes without the term.
- **The row comparison bounds work on a changed row.** `_sab_norow`: `Append` ×3 → `PublishSnap` →
  `CkptCas` → `Fold` → `Drop` → `Check` → `Delete` → `Check` → `Delete`. `Live -> Removing` keeps the
  incarnation, so the life term alone keeps licensing.
- **The life term is implied by the row comparison here.** `_nolife` has the same state graph as `_safe`.
  The term guards an ambiguous catalog (two rows with one incarnation), which the model does not
  represent.
- **The whole-catalog term stops cleanup after any catalog write.** `_old_churnblocked` holds
  `NoLicenceAfterCatalogWrite`. `_witness_churnprogress` reaches a licensed chunk after an unrelated
  write: `Append` ×2 → `PublishSnap` → `CkptCas` → `Fold` → `Churn` → `Check`.

The other witnesses keep the green runs non-vacuous: `_witness_rebirth` reaches a licensed, undeleted
chunk while the name already points at a successor life with data; `_witness_twokeyplan` reaches a plan
with a log and a snapshot.

## Scope {#carefcleanuplicencecore-scope}

The model represents:

1. one logical name with up to `MaxInc` lives; row states absent, `Live`, `Removing`;
2. per life: log and snapshot ids restarting at 1, a checkpoint, a fold cursor;
3. a fold that takes the lease, records the row and plans deletions for the row's life;
4. `Check` and `Delete` as separate steps, one key per chunk, so the check-to-delete window exists;
5. catalog writes that do not touch the row (`Churn`), a lease steal, and a successor leader that folds
   and deletes on its own.

Not modelled:

- two rows naming one incarnation (an ambiguous catalog) and the pass stop it causes;
- the refusal scope (one namespace or the whole pass), since there is one name;
- `retained_log_proof` and the per-round work budget;
- the plan rule's interaction with writers beyond append, snapshot publish and checkpoint CAS, which
  `CaRefTableSnapshotLogCore` owns;
- dead-life debris cleanup, which `CaRefNsCleanupStaleLeaderCore` owns.
