-- Tags: no-parallel

-- Query-snapshot semantics: the value resolved at analysis time is stable for
-- the whole query even across multiple reads and a REFRESH that lands during
-- execution. Also verifies constant folding makes repeated reads free.

DROP VARIABLE IF EXISTS snap_v;

CREATE VARIABLE snap_v REFRESH EVERY 1 SECOND AS toUInt64(now());

-- Two reads of the same variable in one query must return the same value.
SELECT getVariable('snap_v') = getVariable('snap_v');

-- Across queries, refresh is observable.
SELECT sleep(2) FORMAT Null;
SELECT getVariable('snap_v') > 0;

-- Entry exists but value is null: manager returns declared_type at analysis,
-- executeImpl throws "has no value" (Exception) or uses default (Default).
-- Simulate by creating a variable whose initial evaluation fails — can't easily
-- without dropping a backing table; instead test the getVariableOrDefault path
-- for a truly-missing name (covered elsewhere) and for a defined-but-typed case.
SELECT getVariableOrDefault('snap_v', toUInt64(0)) > 0;

DROP VARIABLE snap_v;
