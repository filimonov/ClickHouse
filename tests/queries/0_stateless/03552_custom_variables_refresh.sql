-- Tags: no-parallel

DROP VARIABLE IF EXISTS local.cv_refresh;

CREATE VARIABLE local.cv_refresh REFRESH EVERY 1 SECOND AS now();
SYSTEM REFRESH VARIABLE local.cv_refresh;

SELECT scope, name, has_value, is_valid
FROM system.custom_variables
WHERE scope = 'local' AND name = 'cv_refresh'
ORDER BY name;

SYSTEM REFRESH VARIABLES;

CREATE VARIABLE local.cv_refresh_const REFRESH EVERY 1 SECOND AS 1; -- {serverError BAD_ARGUMENTS}

DROP VARIABLE local.cv_refresh;
