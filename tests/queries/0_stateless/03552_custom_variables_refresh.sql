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

DROP VARIABLE local.cv_refresh;
DROP VARIABLE local.cv_refresh_fail;
DROP TABLE IF EXISTS default.cv_refresh_src;
