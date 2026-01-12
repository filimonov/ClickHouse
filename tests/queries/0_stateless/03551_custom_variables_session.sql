-- Tags: no-parallel

DROP VARIABLE IF EXISTS session.cv_sess;

CREATE VARIABLE session.cv_sess AS 10;
SELECT getVariable('session.cv_sess');

SELECT scope, name, value, has_value, is_valid
FROM system.custom_variables
WHERE scope = 'session' AND name = 'cv_sess'
ORDER BY name;

DROP VARIABLE session.cv_sess;
SELECT getVariable('session.cv_sess'); -- {serverError BAD_ARGUMENTS}
