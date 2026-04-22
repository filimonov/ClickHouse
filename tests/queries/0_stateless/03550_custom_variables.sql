-- Tags: no-parallel

DROP VARIABLE IF EXISTS local.cv_test;
DROP VARIABLE IF EXISTS local.cv_ref;
DROP VARIABLE IF EXISTS local.cv_big;
DROP VARIABLE IF EXISTS local.cv_fixed;
DROP VARIABLE IF EXISTS local.cv_const;

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

-- Existence check must fire before the expression is evaluated so that both
-- `IF NOT EXISTS` and duplicate CREATE are no-ops / clean errors even when
-- the new expression is invalid.
CREATE VARIABLE local.cv_fixed AS toUInt32(1);
CREATE VARIABLE IF NOT EXISTS local.cv_fixed AS (SELECT * FROM nonexistent_table);
SELECT getVariable('local.cv_fixed');

CREATE VARIABLE local.cv_fixed AS (SELECT * FROM nonexistent_table); -- {serverError FILE_ALREADY_EXISTS}
SELECT getVariable('local.cv_fixed');
DROP VARIABLE local.cv_fixed;

-- After the scope collapse, `local` is always persistent and no longer bans
-- constant expressions (the old `local_persistent` rule is gone).
CREATE VARIABLE local.cv_const AS 42;
SELECT getVariable('local.cv_const');
DROP VARIABLE local.cv_const;
