# Custom Variables Implementation Status

## Current snapshot

The project is **through Phase 3** (local_persistent). `getVariableOrDefault` is implemented.
Phase 4 (cluster) is not started.

## Key design decisions already settled

- Variable names are `scope.name`, with **case-sensitive** names.
- Scope is an enum: `local`, `local_persistent`, `session`, `cluster`.
- Variable definitions **must not reference** `getVariable()`/`getVariableOrDefault()`.
- `load_time` replaced the earlier misleading `create_time`.
- `getVariable()` is **deterministic in scope of query**.
- Refresh scheduling shares the core helper `RefreshScheduler.h` with refreshable MVs.

## Done

### Phase 1: static local variables
- Parser/AST/interpreter for `CREATE VARIABLE` / `DROP VARIABLE`.
- Disk definitions storage (`custom_variables/variable_<scope>_<name>.sql`).
- Global runtime manager (`CustomVariablesManager`) with atomic per-entry value pointer.
- Startup loading in both `clickhouse-server` and `clickhouse-local`.
- `getVariable()` function, `system.custom_variables` table.
- Access types `CREATE_VARIABLE`, `DROP_VARIABLE`, `GET_VARIABLE`, `SHOW_CUSTOM_VARIABLES`, `SYSTEM_CUSTOM_VARIABLES`.
- `ON CLUSTER` for non-session non-refresh DDL.

### Phase 1a: session variables
- Isolated per session, no DDL storage, no `ON CLUSTER`.

### Phase 2: refreshable local variables
- `REFRESH` (via `ASTRefreshStrategy`, DEPENDS ON rejected).
- Retry/backoff via shared `RefreshScheduler`.
- `SYSTEM REFRESH VARIABLE[S]`.
- Failures keep last-good value.

### Phase 3: local_persistent
- Disk values storage (`CustomVariablesValuesDiskStorage`, `custom_variables_values/<name>.bin`).
- Persist on create/refresh, load on startup, immediate refresh if stale.
- Constant expressions forbidden for `local_persistent`.
- `DROP VARIABLE` now removes both the definition `.sql` and the value `.bin`.
- Stored DDL now round-trips through parser even when the expression is a subquery
  (CAST-wrapped SELECTs are parenthesized via `ASTSubquery`).
- `03553_custom_variables_local_persistent` is green.

### API
- `getVariable(name)` — throws `UNKNOWN_IDENTIFIER` / `INCORRECT_QUERY` on missing / no-value.
- `getVariableOrDefault(name, default)` — returns `default` if the variable is missing or has no value.

### Access
- `SHOW_CUSTOM_VARIABLES` is now grouped under `SHOW` (was under `SHOW_ACCESS`).
- `system.custom_variables` is also readable by users who have `GET_VARIABLE`
  (so `getVariable` callers can inspect metadata without admin grants).

## Tests

Passing:
- `03550_custom_variables`
- `03551_custom_variables_session`
- `03552_custom_variables_refresh`
- `03553_custom_variables_local_persistent` (reload + DROP cleanup)
- `03554_custom_variables_or_default`

### Phase 4 — cluster scope (CREATE CLUSTER VARIABLE)
- New DDL surface: `CREATE [OR REPLACE] CLUSTER VARIABLE [IF NOT EXISTS] <name> [REFRESH ...] AS <expr>`
  and `DROP CLUSTER VARIABLE [IF EXISTS] <name>`. Single-identifier names; no `ON CLUSTER`.
- Definitions + values live in ZooKeeper under `<custom_variables_zookeeper_path>/{definitions,values}/<name>`;
  refresh uses an ephemeral lock at `values/<name>/lock`.
- Every node runs a single `CustomVariablesClusterCoordinator` thread that watches the root path,
  picks up new/renamed/removed entries, and swaps in fresh values on ZK data events.
- Refreshable cluster variables schedule ticks on every node; only the node holding the ephemeral
  lock actually evaluates and writes to ZK; others skip the tick and pick up the winner's write via watch.
- Integration test suite `tests/integration/test_custom_variables_cluster/` covers cross-node
  discovery, DROP propagation, restart reload, OR REPLACE propagation, system.custom_variables,
  REFRESH cadence, single-leader-per-tick, SYSTEM REFRESH VARIABLE, and failover on node kill.

## Not done / follow-ups

### Distributed query semantics
- Design doc §219-224 describes resolve-on-initiator + literal injection for `cluster` and `session` variables.
- Not implemented yet.

### Backups
- Variable definitions are NOT yet hooked into `BACKUP`.
- Plan: integrate with `UserDefinedSQLObjectsBackup`-style flow; do not back up values.

### Documentation
- `docs/en/sql-reference/statements/create/variable.md` not yet written.
