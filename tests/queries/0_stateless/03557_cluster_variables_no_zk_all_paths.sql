-- Tags: no-parallel

-- All REPLICATED DDL paths produce a clear configuration error when
-- <custom_variables_zookeeper_path> is not set.
CREATE REPLICATED VARIABLE z AS toUInt64(1); -- {serverError BAD_ARGUMENTS}
CREATE OR REPLACE REPLICATED VARIABLE z AS toUInt64(1); -- {serverError BAD_ARGUMENTS}
CREATE REPLICATED VARIABLE IF NOT EXISTS z AS toUInt64(1); -- {serverError BAD_ARGUMENTS}
DROP REPLICATED VARIABLE z; -- {serverError BAD_ARGUMENTS}
DROP REPLICATED VARIABLE IF EXISTS z; -- {serverError BAD_ARGUMENTS}

-- SYSTEM REFRESH for a never-created REPLICATED variable is an in-memory
-- "not found" error, so no Keeper configuration is required to recognise it.
SYSTEM REFRESH REPLICATED VARIABLE never_existed; -- {serverError UNKNOWN_IDENTIFIER}

-- SYSTEM REFRESH TEMPORARY VARIABLE is grammar-level forbidden (no such grammar).
SYSTEM REFRESH TEMPORARY VARIABLE anything; -- {clientError SYNTAX_ERROR}
