-- Tags: no-parallel

-- After the local / local_persistent collapse, `local_persistent` is no longer
-- a recognised scope. The parser / interpreter must reject it outright so
-- users don't silently land on some unintended scope.
CREATE VARIABLE local_persistent.x AS 1; -- {serverError INCORRECT_QUERY}
DROP VARIABLE local_persistent.x; -- {serverError INCORRECT_QUERY}
SELECT getVariable('local_persistent.x'); -- {serverError INCORRECT_QUERY}
SYSTEM REFRESH VARIABLE local_persistent.x; -- {serverError INCORRECT_QUERY}
