-- Tags: no-parallel

-- Without <custom_variables_zookeeper_path> configured, CLUSTER VARIABLE DDL errors clearly.
CREATE CLUSTER VARIABLE foo AS 1; -- {serverError BAD_ARGUMENTS}
CREATE OR REPLACE CLUSTER VARIABLE foo AS toUInt64(5); -- {serverError BAD_ARGUMENTS}
CREATE CLUSTER VARIABLE IF NOT EXISTS foo REFRESH EVERY 1 MINUTE AS now(); -- {serverError BAD_ARGUMENTS}
DROP CLUSTER VARIABLE foo; -- {serverError BAD_ARGUMENTS}
DROP CLUSTER VARIABLE IF EXISTS foo; -- {serverError BAD_ARGUMENTS}

-- Name must be a single identifier (no scope prefix).
CREATE CLUSTER VARIABLE cluster.foo AS 1; -- {serverError BAD_ARGUMENTS}
CREATE CLUSTER VARIABLE local.foo AS 1; -- {serverError BAD_ARGUMENTS}

-- ON CLUSTER is not allowed with CLUSTER VARIABLE.
CREATE CLUSTER VARIABLE foo ON CLUSTER x AS 1; -- {clientError SYNTAX_ERROR}
DROP CLUSTER VARIABLE foo ON CLUSTER x; -- {clientError SYNTAX_ERROR}

-- Cluster scope prefix is not allowed with plain CREATE VARIABLE.
CREATE VARIABLE cluster.foo AS 1; -- {serverError BAD_ARGUMENTS}
DROP VARIABLE cluster.foo; -- {serverError BAD_ARGUMENTS}
