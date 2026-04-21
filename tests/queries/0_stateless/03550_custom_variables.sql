-- Tags: no-parallel

DROP VARIABLE IF EXISTS local.cv_test;

CREATE VARIABLE local.cv_test AS toUInt32(1);
SELECT getVariable('local.cv_test');

SELECT name, scope, value, type, has_value, is_valid
FROM system.custom_variables
WHERE name = 'cv_test'
ORDER BY name;

CREATE OR REPLACE VARIABLE local.cv_test AS toUInt32(2);
SELECT getVariable('local.cv_test');

CREATE OR REPLACE VARIABLE local.cv_test AS 'x'; -- {serverError BAD_ARGUMENTS}
CREATE VARIABLE local.cv_ref AS getVariable('local.cv_test'); -- {serverError BAD_ARGUMENTS}
CREATE VARIABLE local.cv_ref AS getVariableOrDefault('local.cv_test', toUInt32(0)); -- {serverError BAD_ARGUMENTS}
SELECT getVariable('local.missing'); -- {serverError UNKNOWN_IDENTIFIER}

CREATE VARIABLE local.cv_test AS toUInt32(3); -- {serverError FILE_ALREADY_EXISTS}
CREATE VARIABLE IF NOT EXISTS local.cv_test AS toUInt32(4);
SELECT getVariable('local.cv_test');

DROP VARIABLE local.cv_test;
DROP VARIABLE local.cv_test; -- {serverError FILE_DOESNT_EXIST}
DROP VARIABLE IF EXISTS local.cv_test;

CREATE VARIABLE local.cv_big AS repeat('x', 2048); -- {serverError TOO_LARGE_STRING_SIZE}
