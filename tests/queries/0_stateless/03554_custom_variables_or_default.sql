-- Tags: no-parallel

DROP VARIABLE IF EXISTS local.cv_defined;

CREATE VARIABLE local.cv_defined AS toUInt32(7);

SELECT getVariableOrDefault('local.cv_defined', toUInt32(0));
SELECT getVariableOrDefault('local.cv_missing', toUInt32(42));
SELECT getVariableOrDefault('local.cv_missing', 'fallback');
SELECT getVariableOrDefault('local.cv_missing', NULL) IS NULL;

DROP VARIABLE local.cv_defined;
