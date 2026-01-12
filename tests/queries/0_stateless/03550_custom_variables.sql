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
SELECT getVariable('local.missing'); -- {serverError UNKNOWN_IDENTIFIER}

DROP VARIABLE local.cv_test;
