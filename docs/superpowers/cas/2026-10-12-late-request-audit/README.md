# CAS late-request audit, 2026-10-12 {#late-request-audit}

State as of **2026-10-12**, code at base `d76b59f9295` (`altinity/antalya-26.6-20261008`; lease-series tip `3b4037ae265` where noted). Read-only static analysis: no scenario was reproduced, no TLC run. Claims are marked V (traced in code) or I (inferred). Reports are in Russian except the codex outputs.

Question: which store writes can a late request (paused process, partition, late apply by the store, SDK resend, same-srid twin, stale read, deposed GC worker) turn harmful, what protects each one today (the store or the clock), and can the guarantee move to the store cheaply.

## Verdict {#verdict}

- The worst defects are **not time-based**: a live writer adopts a blob body GC deleted between its `HEAD` and marker `GET` (CAS-360); the orphan sweep deletes a manifest staged before its namespace row (CAS-36); two same-uuid instances get one epoch (CAS-362); decommission lets an epoch be re-minted (CAS-363); SDK resends restore a condemned blob ETag (CAS-361). Neither the lease nor a transport deadline closes them.
- Time-protected residue: `_files/` (phantom mutation, CAS-370), `beginRemoving` (wedge, CAS-367), ref-log append into a reclaimed seal slot (CAS-366), the holder-death certificate (22–27 s slack).
- Store-protected today: snapshots and `_ckpt`, ref-log keys, manifests against ordinary lateness, the GC control plane, `_pool_meta`/`_probe`, the mount slot as an object.
- TLA+ models hid two data-loss defects by fusing code steps into one action; a codex atomicity audit of all 35 models found ~23 such issues (CAS-375).
- Transport: the attempt envelope `E` bounds a socket operation, not an attempt; a deadline in `src/IO` (~170 lines) closes the transport part (CAS-343).

Action packages P1–P6 with sizes: [objects/SUMMARY.md](objects/SUMMARY.md#options). Every finding is in the backlog with label `origin:noclock-audit` (CAS-360…383, notes on CAS-10, 36, 158, 178, 179, 284.5, 329, 334, 343, 351, 29.3).

## Layout {#layout}

| Path | What |
|---|---|
| [inventory.md](inventory.md) | 101-row nomenclature: object × operation, request type, protection, late-write risk, TLA+ coverage |
| [inventory-codex.md](inventory-codex.md) | codex completeness check of the inventory |
| [topics.md](topics.md) | unsafe topics T1–T14 after the check |
| [objects/](objects/) | 15 per-object reports (blob, meta, manifest, reflog, snapckpt, files, roots, catalog, gcctl, mount, epoch-owner, staging, poolprobe, protocol, transport) and [SUMMARY.md](objects/SUMMARY.md) |
| [atomicity/](atomicity/) | codex model-vs-code atomicity audit, waves 1–2, with `SUMMARY-w1.md`, `SUMMARY-w2.md` |
| [prior/](prior/) | the earlier no-clock-fencing study (against `3b4037ae265`) and its codex refutation |
| [prompts/](prompts/) | briefs used for the agents, to rerun the audit on a later base |
