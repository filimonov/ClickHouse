-- Tags: no-parallel

DROP VARIABLE IF EXISTS cv_defined;
DROP TEMPORARY VARIABLE IF EXISTS tmp_defined;

-- Server kind (no modifier) + getVariableOrDefault.
CREATE VARIABLE cv_defined AS toUInt32(7);

SELECT getVariableOrDefault('cv_defined', toUInt32(0));
SELECT getVariableOrDefault('missing', toUInt32(42));
SELECT getVariableOrDefault('missing', 'fallback');
SELECT getVariableOrDefault('missing', NULL) IS NULL;

-- Temporary kind has its own OrDefault function.
CREATE TEMPORARY VARIABLE tmp_defined AS toUInt32(3);
SELECT getTemporaryVariableOrDefault('tmp_defined', toUInt32(0));
SELECT getTemporaryVariableOrDefault('missing', toUInt32(77));

-- Replicated OrDefault without Keeper configured cannot find any replicated
-- variable, so it always falls through to the default; confirm that path.
SELECT getReplicatedVariableOrDefault('missing', toUInt32(11));

DROP TEMPORARY VARIABLE tmp_defined;
DROP VARIABLE cv_defined;
