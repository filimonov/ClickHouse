-- Tags: no-parallel

DROP TEMPORARY VARIABLE IF EXISTS cv_sess;

CREATE TEMPORARY VARIABLE cv_sess AS toUInt32(10);
SELECT getTemporaryVariable('cv_sess');

SELECT kind, name, value, has_value, is_valid
FROM system.custom_variables
WHERE kind = 'temporary' AND name = 'cv_sess'
ORDER BY name;

-- IF NOT EXISTS on an existing variable is a silent no-op, keeps original value.
CREATE TEMPORARY VARIABLE IF NOT EXISTS cv_sess AS toUInt32(99);
SELECT getTemporaryVariable('cv_sess');

-- Duplicate CREATE without OR REPLACE / IF NOT EXISTS is rejected.
CREATE TEMPORARY VARIABLE cv_sess AS toUInt32(11); -- {serverError FILE_ALREADY_EXISTS}

-- OR REPLACE must preserve declared type.
CREATE OR REPLACE TEMPORARY VARIABLE cv_sess AS 'x'; -- {serverError BAD_ARGUMENTS}

-- REFRESH and ON CLUSTER are grammar-level forbidden for TEMPORARY.
CREATE TEMPORARY VARIABLE cv_refresh REFRESH EVERY 1 SECOND AS now(); -- {clientError SYNTAX_ERROR}
CREATE TEMPORARY VARIABLE cv_oncluster ON CLUSTER x AS 1; -- {clientError SYNTAX_ERROR}

-- Server and Temporary with the same bare name are independent: each read
-- function targets its own domain, no shadowing.
DROP VARIABLE IF EXISTS cv_both;
DROP TEMPORARY VARIABLE IF EXISTS cv_both;
CREATE VARIABLE cv_both AS toUInt32(1);
CREATE TEMPORARY VARIABLE cv_both AS toUInt32(2);
SELECT getVariable('cv_both'), getTemporaryVariable('cv_both');
DROP TEMPORARY VARIABLE cv_both;
DROP VARIABLE cv_both;

-- Reading the wrong domain names the domain in the error.
CREATE TEMPORARY VARIABLE tmp_only AS 1;
SELECT getVariable('tmp_only'); -- {serverError UNKNOWN_IDENTIFIER}
DROP TEMPORARY VARIABLE tmp_only;

DROP TEMPORARY VARIABLE cv_sess;
DROP TEMPORARY VARIABLE cv_sess; -- {serverError FILE_DOESNT_EXIST}
DROP TEMPORARY VARIABLE IF EXISTS cv_sess;

SELECT getTemporaryVariable('cv_sess'); -- {serverError UNKNOWN_IDENTIFIER}
