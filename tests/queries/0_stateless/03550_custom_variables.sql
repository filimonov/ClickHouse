-- Tags: no-parallel

DROP VARIABLE IF EXISTS cv_test;
DROP VARIABLE IF EXISTS cv_ref;
DROP VARIABLE IF EXISTS cv_big;
DROP VARIABLE IF EXISTS cv_fixed;
DROP VARIABLE IF EXISTS cv_const;

CREATE VARIABLE cv_test AS toUInt32(1);
SELECT getVariable('cv_test');

SELECT name, kind, value, type, has_value, is_valid
FROM system.custom_variables
WHERE name = 'cv_test'
ORDER BY name;

CREATE OR REPLACE VARIABLE cv_test AS toUInt32(2);
SELECT getVariable('cv_test');

CREATE OR REPLACE VARIABLE cv_test AS 'x'; -- {serverError BAD_ARGUMENTS}
CREATE VARIABLE cv_ref AS getVariable('cv_test'); -- {serverError BAD_ARGUMENTS}
CREATE VARIABLE cv_ref AS getVariableOrDefault('cv_test', toUInt32(0)); -- {serverError BAD_ARGUMENTS}
SELECT getVariable('missing'); -- {serverError UNKNOWN_IDENTIFIER}

CREATE VARIABLE cv_test AS toUInt32(3); -- {serverError FILE_ALREADY_EXISTS}
CREATE VARIABLE IF NOT EXISTS cv_test AS toUInt32(4);
SELECT getVariable('cv_test');

DROP VARIABLE cv_test;
DROP VARIABLE cv_test; -- {serverError FILE_DOESNT_EXIST}
DROP VARIABLE IF EXISTS cv_test;

CREATE VARIABLE cv_big AS repeat('x', 2048); -- {serverError TOO_LARGE_STRING_SIZE}

-- Existence check must fire before the expression is evaluated so that both
-- `IF NOT EXISTS` and duplicate CREATE are no-ops / clean errors even when
-- the new expression is invalid.
CREATE VARIABLE cv_fixed AS toUInt32(1);
CREATE VARIABLE IF NOT EXISTS cv_fixed AS (SELECT * FROM nonexistent_table);
SELECT getVariable('cv_fixed');

CREATE VARIABLE cv_fixed AS (SELECT * FROM nonexistent_table); -- {serverError FILE_ALREADY_EXISTS}
SELECT getVariable('cv_fixed');
DROP VARIABLE cv_fixed;

-- Server variables always persist their value; constant expressions are allowed.
CREATE VARIABLE cv_const AS 42;
SELECT getVariable('cv_const');
DROP VARIABLE cv_const;
