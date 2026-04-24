-- Tags: no-parallel

-- REPLICATED variables require <custom_variables_zookeeper_path>. Without it,
-- every REPLICATED DDL path returns a clear configuration error rather than a
-- generic / confusing diagnostic.
CREATE REPLICATED VARIABLE foo AS 1; -- {serverError BAD_ARGUMENTS}
CREATE OR REPLACE REPLICATED VARIABLE foo AS toUInt64(5); -- {serverError BAD_ARGUMENTS}
CREATE REPLICATED VARIABLE IF NOT EXISTS foo REFRESH EVERY 1 MINUTE AS now(); -- {serverError BAD_ARGUMENTS}
DROP REPLICATED VARIABLE foo; -- {serverError BAD_ARGUMENTS}
DROP REPLICATED VARIABLE IF EXISTS foo; -- {serverError BAD_ARGUMENTS}

-- Grammar rejects ON CLUSTER for REPLICATED (Keeper already distributes).
CREATE REPLICATED VARIABLE foo ON CLUSTER x AS 1; -- {clientError SYNTAX_ERROR}
DROP REPLICATED VARIABLE foo ON CLUSTER x; -- {clientError SYNTAX_ERROR}

-- Grammar rejects ON CLUSTER and REFRESH for TEMPORARY.
CREATE TEMPORARY VARIABLE t ON CLUSTER x AS 1; -- {clientError SYNTAX_ERROR}
CREATE TEMPORARY VARIABLE t REFRESH EVERY 1 SECOND AS now(); -- {clientError SYNTAX_ERROR}

-- Bare identifier is required for variable names: no compound (dotted) names.
CREATE VARIABLE foo.bar AS 1; -- {clientError SYNTAX_ERROR}
DROP VARIABLE foo.bar; -- {clientError SYNTAX_ERROR}
