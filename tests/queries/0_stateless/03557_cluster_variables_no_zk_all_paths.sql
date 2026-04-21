-- Tags: no-parallel

-- Without <custom_variables_zookeeper_path>, every cluster DDL path returns a
-- clear configuration error instead of any generic / confusing diagnostic.
CREATE CLUSTER VARIABLE z AS toUInt64(1); -- {serverError BAD_ARGUMENTS}
CREATE OR REPLACE CLUSTER VARIABLE z AS toUInt64(1); -- {serverError BAD_ARGUMENTS}
CREATE CLUSTER VARIABLE IF NOT EXISTS z AS toUInt64(1); -- {serverError BAD_ARGUMENTS}
DROP CLUSTER VARIABLE z; -- {serverError BAD_ARGUMENTS}
DROP CLUSTER VARIABLE IF EXISTS z; -- {serverError BAD_ARGUMENTS}

-- SYSTEM REFRESH for a never-created cluster variable is a "not found" error,
-- not a configuration error — the path does not require ZK to recognise the
-- variable is missing from the in-memory manager.
SYSTEM REFRESH VARIABLE cluster.never_existed; -- {serverError UNKNOWN_IDENTIFIER}
