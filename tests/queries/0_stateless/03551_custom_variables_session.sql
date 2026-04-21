-- Tags: no-parallel

DROP VARIABLE IF EXISTS session.cv_sess;

CREATE VARIABLE session.cv_sess AS toUInt32(10);
SELECT getVariable('session.cv_sess');

SELECT scope, name, value, has_value, is_valid
FROM system.custom_variables
WHERE scope = 'session' AND name = 'cv_sess'
ORDER BY name;

-- IF NOT EXISTS on an existing variable is a silent no-op, keeps original value.
CREATE VARIABLE IF NOT EXISTS session.cv_sess AS toUInt32(99);
SELECT getVariable('session.cv_sess');

-- Duplicate CREATE without OR REPLACE / IF NOT EXISTS is rejected.
CREATE VARIABLE session.cv_sess AS toUInt32(11); -- {serverError FILE_ALREADY_EXISTS}

-- OR REPLACE must preserve declared type.
CREATE OR REPLACE VARIABLE session.cv_sess AS 'x'; -- {serverError BAD_ARGUMENTS}

-- REFRESH is not supported for session-scoped variables.
CREATE VARIABLE session.cv_refresh REFRESH EVERY 1 SECOND AS now(); -- {serverError INCORRECT_QUERY}

DROP VARIABLE session.cv_sess;
DROP VARIABLE session.cv_sess; -- {serverError FILE_DOESNT_EXIST}
DROP VARIABLE IF EXISTS session.cv_sess;

SELECT getVariable('session.cv_sess'); -- {serverError UNKNOWN_IDENTIFIER}
