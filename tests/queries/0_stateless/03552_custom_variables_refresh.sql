-- Tags: no-parallel

DROP VARIABLE IF EXISTS local.cv_refresh;
DROP VARIABLE IF EXISTS local.cv_refresh_fail;
DROP TABLE IF EXISTS default.cv_refresh_src;

CREATE VARIABLE local.cv_refresh REFRESH EVERY 1 SECOND AS now();
SYSTEM REFRESH VARIABLE local.cv_refresh;

SELECT scope, name, has_value, is_valid
FROM system.custom_variables
WHERE scope = 'local' AND name = 'cv_refresh'
ORDER BY name;

SYSTEM REFRESH VARIABLES;

CREATE TABLE default.cv_refresh_src (x UInt8) ENGINE = Memory;
INSERT INTO default.cv_refresh_src VALUES (1);

CREATE VARIABLE local.cv_refresh_fail REFRESH EVERY 1 SECOND AS (SELECT count() FROM default.cv_refresh_src);
SYSTEM REFRESH VARIABLE local.cv_refresh_fail;
SELECT sleep(1) FORMAT Null;

DROP TABLE default.cv_refresh_src;
SYSTEM REFRESH VARIABLE local.cv_refresh_fail;
SELECT sleep(1) FORMAT Null;

SELECT scope, name, has_value, is_valid, coalesce(last_error, '') != '' AS has_error
FROM system.custom_variables
WHERE scope = 'local' AND name = 'cv_refresh_fail'
ORDER BY name;

CREATE VARIABLE local.cv_refresh_const REFRESH EVERY 1 SECOND AS 1; -- {serverError BAD_ARGUMENTS}

-- CREATE evaluates the expression synchronously on the initiator, so a missing
-- backing relation fails the CREATE itself (the entry is not registered).
DROP TABLE IF EXISTS default.cv_missing_src;
CREATE VARIABLE local.cv_broken REFRESH EVERY 1 YEAR AS (SELECT count() FROM default.cv_missing_src); -- {serverError UNKNOWN_TABLE}

-- Flapping: create backing table, refresh succeeds, then drop the table and
-- force another refresh. The last-good value is retained (has_value stays 1),
-- is_valid flips to 0, last_error is populated, and the earlier
-- last_successful_update_time is preserved.
DROP VARIABLE IF EXISTS local.cv_flap;
DROP TABLE IF EXISTS default.cv_flap_src;
CREATE TABLE default.cv_flap_src (x UInt8) ENGINE = Memory;
INSERT INTO default.cv_flap_src VALUES (7);

CREATE VARIABLE local.cv_flap REFRESH EVERY 1 YEAR AS (SELECT count() FROM default.cv_flap_src);
SELECT getVariable('local.cv_flap');

DROP TABLE default.cv_flap_src;
SYSTEM REFRESH VARIABLE local.cv_flap;
SELECT sleep(1) FORMAT Null;

SELECT has_value, is_valid, coalesce(last_error, '') != '' AS has_error,
       last_successful_update > toDateTime('1970-01-01 00:00:00', 'UTC') AS kept_prior_success
FROM system.custom_variables
WHERE scope = 'local' AND name = 'cv_flap';

-- During the outage the last-good value is still served to getVariable.
SELECT getVariable('local.cv_flap');

-- Recovery: re-create the backing table, refresh succeeds, is_valid returns
-- to 1 and last_error is cleared.
CREATE TABLE default.cv_flap_src (x UInt8) ENGINE = Memory;
INSERT INTO default.cv_flap_src VALUES (1), (2);
SYSTEM REFRESH VARIABLE local.cv_flap;
SELECT sleep(1) FORMAT Null;

SELECT has_value, is_valid, coalesce(last_error, '') = '' AS cleared_error
FROM system.custom_variables
WHERE scope = 'local' AND name = 'cv_flap';
SELECT getVariable('local.cv_flap');

DROP VARIABLE local.cv_refresh;
DROP VARIABLE local.cv_refresh_fail;
DROP VARIABLE local.cv_flap;
DROP TABLE IF EXISTS default.cv_refresh_src;
DROP TABLE IF EXISTS default.cv_flap_src;
