Custom Variables Design Doc (Phase 0)

Goals
- Add CREATE/DROP VARIABLE with local/session/local_persistent/cluster scopes.
- Store variable definitions similarly to CREATE FUNCTION (SQL DDL on disk; ZooKeeper optional and configurable, disk-only by default).
- Store variable values in RAM (local/session) and optionally on disk or ZooKeeper (local_persistent/cluster).
- Provide getVariable/getVariableOrDefault functions with cheap access (no heavy locks).
- Provide refresh support with retries/backoff and observability via system.custom_variables.

Non-goals
- Full multi-tenant or database-qualified variable namespaces (only scope prefix + name).
- Arbitrary UPDATE/SET syntax beyond CREATE OR REPLACE.
- Cross-node transactional guarantees beyond best-effort refresh and ZooKeeper CAS.

Prior art / references in ClickHouse
- CREATE/DROP FUNCTION parsing and ASTs: src/Parsers/ParserCreateFunctionQuery.cpp, src/Parsers/ASTCreateFunctionQuery.*,
  src/Parsers/ParserDropFunctionQuery.cpp, src/Parsers/ASTDropFunctionQuery.*
- CREATE/DROP FUNCTION interpreters: src/Interpreters/InterpreterCreateFunctionQuery.cpp,
  src/Interpreters/InterpreterDropFunctionQuery.cpp
- User-defined SQL object storage (disk + ZooKeeper):
  src/Functions/UserDefined/UserDefinedSQLObjectsDiskStorage.cpp,
  src/Functions/UserDefined/UserDefinedSQLObjectsZooKeeperStorage.cpp,
  src/Functions/UserDefined/UserDefinedSQLObjectsStorageBase.*
- UDF factory and normalization: src/Functions/UserDefined/UserDefinedSQLFunctionFactory.cpp
- dictGet access pattern + constant arg validation: src/Functions/FunctionsExternalDictionaries.h (FunctionDictHelper)
- getSetting/getSettingOrDefault pattern for const string arg: src/Functions/getSetting.cpp
- Refresh scheduling patterns: src/Storages/MaterializedView/RefreshTask.*,
  src/Storages/MaterializedView/RefreshSchedule.*,
  src/Storages/MaterializedView/RefreshSettings.*
- SYSTEM REFRESH VIEW plumbing: src/Parsers/ASTSystemQuery.*,
  src/Parsers/ParserSystemQuery.cpp,
  src/Interpreters/InterpreterSystemQuery.cpp
- system.functions and system.dictionaries patterns: src/Storages/System/StorageSystemFunctions.cpp,
  src/Storages/System/StorageSystemDictionaries.cpp
- Field serialization helpers: src/Common/FieldBinaryEncoding.* (encodeField/decodeField)

High-level architecture
1) Definition storage: stores CREATE VARIABLE statements (DDL) to reload on startup.
2) Runtime store: holds current value + metadata in RAM (cheap reads).
3) Refresh engine: schedules value recomputation, retry/backoff, and writes to disk/ZK where needed.
4) SQL functions + system tables: query access and observability.

Data model
- CustomVariableScope enum: local, local_persistent, session, cluster.
- CustomVariableKey { scope, name } (name is identifier after scope prefix).
- CustomVariableDefinition
  - scope
  - name
  - ASTPtr expression (original AST for CREATE VARIABLE ... AS ...)
  - optional refresh strategy (ASTRefreshStrategy)
  - declared_type (DataTypePtr, inferred at CREATE)
  - load_time
- CustomVariableValue
  - DataTypePtr runtime_type (must match declared_type)
  - Field value (or ColumnPtr with single row)
  - last_update_time
  - last_successful_update_time
  - last_update_hostname
  - last_error, last_error_type
  - has_value (bool, last-good exists)
  - is_valid (bool, last refresh attempt succeeded)
- CustomVariableEntry
  - definition
  - atomic/shared_ptr to CustomVariableValue (fast reads)
  - refresh scheduler handle (optional)
- MAX_CUSTOM_VARIABLE_SIZE (1024 bytes) enforced on serialized value size.
  Suggested check: encodeField(value, WriteBufferFromOwnString) and validate size.

SQL syntax
- CREATE [OR REPLACE] VARIABLE [IF NOT EXISTS] <scope>.<name> [REFRESH <ASTRefreshStrategy>] AS <expr_or_select>
- DROP VARIABLE [IF EXISTS] <scope>.<name>
- CREATE VARIABLE ... ON CLUSTER only for scopes allowed by phase rules (see below).
- For expressions: allow either a scalar SELECT or a scalar expression; if not a SELECT, wrap into SELECT.
- Variable definition MUST NOT reference getVariable() or other variable access functions.
Examples (refresh grammar)
- REFRESH EVERY 1 MINUTE
- REFRESH AFTER 5 MINUTE
- REFRESH with DEPENDS ON is not supported

Parser + AST
New files (modeled after CREATE/DROP FUNCTION):
- src/Parsers/ParserCreateVariableQuery.{h,cpp}
- src/Parsers/ASTCreateVariableQuery.{h,cpp}
- src/Parsers/ParserDropVariableQuery.{h,cpp}
- src/Parsers/ASTDropVariableQuery.{h,cpp}
- Update src/Parsers/ParserQuery.cpp to register parsers.

ASTCreateVariableQuery fields (proposed)
- ASTPtr name (identifier token, contains scope prefix)
- ASTPtr expression or select_query
- bool or_replace, if_not_exists
- String cluster (ON CLUSTER)
- Optional refresh strategy (ASTRefreshStrategy; reuse MV REFRESH grammar, but no DEPENDS ON)

Interpreter + factory
New files:
- src/Interpreters/InterpreterCreateVariableQuery.{h,cpp}
- src/Interpreters/InterpreterDropVariableQuery.{h,cpp}
- Update src/Interpreters/registerInterpreters.cpp and src/Interpreters/InterpreterFactory.cpp

Interpreter logic (modeled after functions)
- Normalize query (similar to FunctionNameNormalizer for functions; for variables we can normalize identifiers in expression).
- Enforce scope rules and ON CLUSTER support per phase.
- Access checks: CREATE_VARIABLE / DROP_VARIABLE (+ optional VARIABLE usage check in getVariable).
- Store definition via CustomVariablesDefinitionStorage (or local-only for session scope).
- Evaluate expression and set runtime value (or schedule refresh if REFRESH used).
- Type stability: infer declared_type on CREATE. CREATE OR REPLACE must keep declared_type or allow safe cast to it (canBeSafelyCast on the evaluated result).
- Persist declared_type by normalizing stored DDL to `AS CAST(<expr> AS <declared_type>)` to keep reload deterministic.

Definition storage (DDL)
Proposed interface (similar to IUserDefinedSQLObjectsStorage):
- ICustomVariableDefinitionsStorage (store/load DDL for non-session variables)
  - loadObjects(), reloadObjects(), reloadObject(scope, name)
  - storeObject(scope, name, ASTPtr create_query, throw_if_exists, replace_if_exists)
  - removeObject(scope, name, throw_if_not_exists)
  - factory: createCustomVariableDefinitionsStorage(Context) with config lookup

Disk storage implementation (modeled after UDF disk storage)
- CustomVariablesDefinitionsDiskStorage
  - dir: <path>/custom_variables/ (config key custom_variables_path)
  - files: variable_<scope>_<escaped_name>.sql
  - same atomic rename + fsync patterns as UserDefinedSQLObjectsDiskStorage.cpp

ZooKeeper storage (optional, configurable; disk-only by default)
- CustomVariablesDefinitionsZooKeeperStorage
  - root: <custom_variables_definitions_zookeeper_path> (config key; disk-only by default)
  - nodes: variable_<scope>_<escaped_name>.sql
  - watch queue, refresh logic copied from UserDefinedSQLObjectsZooKeeperStorage.cpp

Notes
- The phrase "definition stored same way as functions" maps to: same SQL format, same parsing, and same file naming scheme,
  not necessarily the same storage instance. This keeps ON CLUSTER behavior under our control.

Runtime store
New manager (global + session instances):
- CustomVariablesManager
  - owned by Context (global) and Context (session) for session scope
  - map<CustomVariableKey, shared_ptr<CustomVariableEntry>>
  - shared_mutex for map; per-entry atomic shared_ptr for value (lock-free read of value)

Context integration
- Add shared global manager in ContextSharedPart (lazy init, similar to user_defined_sql_objects_storage).
- Add session manager pointer in ContextData for session contexts only.
- New accessors: Context::getCustomVariablesManager() and Context::getSessionCustomVariablesManager().

Read path (getVariable)
- parse scope + name from const string argument
- for session scope: read from session manager
- for local/local_persistent/cluster: read from global manager
- use atomic load of stored value; avoid heavy locks

Write/update path
- update entry value with new shared_ptr<CustomVariableValue> (atomic_store)
- record metadata and errors

Expression evaluation
- For constant expressions: evaluate via evaluateConstantExpression (fast, no SELECT pipeline)
- For ASTSelectWithUnionQuery: execute with InterpreterSelectWithUnionQuery, ensure exactly 1 row / 1 column
- For non-SELECT non-constant: wrap into SELECT <expr> and execute
- Use DataType from sample block; store result as Field + DataType
- Enforce MAX_CUSTOM_VARIABLE_SIZE on serialized value
- For REFRESH not allowed on constant expression:
  - detect with ExpressionAnalyzer/ActionsDAG or evaluateConstantExpression and check deterministic/constant
  - if constant, reject REFRESH

Functions API
- getVariable(name)
- getVariableOrDefault(name, default)
Implementation (modeled after getSetting.cpp)
- require const string for name
- getReturnTypeImpl:
  - getVariable: look up definition; throw if missing; return stored DataType
  - getVariableOrDefault: if defined return stored DataType; else derive type from default constant
- executeImpl: return ColumnConst of value or default
- behavior on errors:
  - if has_value: return last-good value even if last refresh failed
  - if no value: getVariable throws, getVariableOrDefault returns default
- mark non-deterministic; require const arguments
- Access control: new AccessType::GET_VARIABLE or AccessType::VARIABLE (similar to dictGet)

System table: system.custom_variables
New storage class:
- src/Storages/System/StorageSystemCustomVariables.{h,cpp}
- attach in src/Storages/System/attachSystemTables.cpp
Columns (per spec)
- name (String) -> <scope>.<name>
- value (String or Nullable(String)) -> format value via FieldVisitorToString (truncate if needed)
- has_value (UInt8)
- is_valid (UInt8)
- load_time (DateTime)
- last_update (DateTime)
- refresh_next_time (DateTime)
- last_update_hostname (String)
- last_successful_update (DateTime)
- refresh_interval (Interval or UInt64 seconds)
- expression (String) -> DDL expression formatted
- scope (Enum or String)
- type (String) -> DataType name
- last_error (String)
- last_error_type (String)
Access control
- New AccessType::SHOW_CUSTOM_VARIABLES or reuse SYSTEM/SHOW; recommend adding SHOW_VARIABLES style check in fillData.
Visibility
- include session variables only for current session; no cross-session visibility
Semantics
- has_value: whether a last-good value exists
- is_valid: whether the last refresh attempt succeeded
- value: NULL if has_value = 0; otherwise a formatted last-good value
- initial state: is_valid = 1 only after successful initial evaluation; 0 if initial evaluation fails
- refresh_interval/refresh_next_time: NULL when no REFRESH is configured
- for non-refresh variables, is_valid remains 1 after successful initial evaluation

SYSTEM REFRESH VARIABLE(S)
- Extend ASTSystemQuery Type with REFRESH_VARIABLE or REFRESH_VARIABLES
- ParserSystemQuery: parse SYSTEM REFRESH VARIABLE <name> (and maybe SYSTEM REFRESH VARIABLES)
- InterpreterSystemQuery: locate variable manager, schedule refresh now
- Add AccessType::SYSTEM_CUSTOM_VARIABLES or reuse SYSTEM_VIEWS pattern

Distributed query semantics
- cluster variables: resolve on initiator, inject literal into remote shards (single snapshot per query).
- local/local_persistent variables: resolve per-node on each shard (node-local semantics).
- session variables: resolve on initiator and inject literal into remote shards.
  - session variables are not visible outside the initiating session context
  - if a session variable is referenced without an active session context, throw a clear error (no silent fallback)

Refresh scheduling
- Reuse RefreshTask directly (no custom scheduler).
- Add a thin adapter (CustomVariableRefreshTask) to map variable state into RefreshTask/RefreshSet,
  without forking or duplicating the scheduling logic.
- If RefreshTask is too MV-specific, extract the scheduling/backoff core into a reusable helper and wrap it from both MV and variables.

Phase-specific behavior
Phase 1: static local variables
- CREATE/DROP VARIABLE local.<name>
- DDL stored on disk (definitions storage)
- value computed at create/load, stored in RAM
- ON CLUSTER supported (executeDDLQueryOnCluster)

Phase 1a: session variables
- CREATE/DROP VARIABLE session.<name>
- no DDL storage, stored only in session manager
- no ON CLUSTER
- removed on session end

Phase 2: refreshable local variables
- REFRESH clause allowed for local only
- schedule refresh tasks using RefreshTask (thin adapter; no custom scheduler)
- on error: keep previous value, set last_error, exponential backoff
- SYSTEM REFRESH VARIABLE
- ON CLUSTER not supported

Phase 3: local_persistent
- same as local, but persist value to disk
- storage file layout:
  - definitions: variable_local_persistent_<name>.sql
  - values: <path>/custom_variables_values/<name>.bin (binary Field + DataType string + metadata)
- on load:
  - if persisted value is too old, load but schedule immediate refresh
  - always keep RAM copy as source for getVariable
- ON CLUSTER supported (DDL propagation)
- forbid constant expression (must use local scope instead)
- REFRESH supported (same scheduler as phase 2)

Phase 4: cluster variables
- values stored in ZooKeeper at custom_variables_zookeeper_path
- only one node updates at a time (lock znode or ephemeral node)
- local cache in RAM; watchers update cache on changes
- serialization/deserialization only when value reloads
- ON CLUSTER supported for definitions (store DDL on each node); values in ZooKeeper
- REFRESH supported (scheduler + ZooKeeper lock)

ZooKeeper layout (cluster values)
Goal: minimize transactions and keep metadata atomic.
Proposed layout:
- <custom_variables_zookeeper_path>/<name>
  - data: binary blob with {value, type, timestamps, last_error, last_error_type, last_update_hostname, refresh_interval, next_refresh_time, attempt_number}
  - children:
    - lock (ephemeral) for leader refresh
    - [optional] version/metadata nodes only if we decide to split value from metadata

Update flow
- refresher acquires lock (create ephemeral node)
- compute new value
- set znode data with version (single transaction)
- release lock
- watchers on data node notify other replicas to reload into RAM

Error handling
- if refresh fails: keep old value, increment attempt_number, store error string/type, schedule backoff
- if no prior value (first load fails): value is null; getVariable throws; getVariableOrDefault returns default

Backups
- Definitions: include in backup (similar to UserDefinedSQLObjectsBackup.cpp)
- Values: not backed up (recomputed via refresh or initial evaluation)

Tests
- Parser + formatter tests for CREATE/DROP VARIABLE
- Access rights tests
- getVariable/getVariableOrDefault type behavior
- Refresh scheduling (success + failure + backoff)
- local_persistent serialization roundtrip
- cluster ZK coordination and watcher updates (integration tests)

Step-by-step implementation plan
Phase 1: static local variables
1) Add AST + parser for CREATE/DROP VARIABLE (parse REFRESH but reject until Phase 2) and wire into ParserQuery.
2) Implement CustomVariablesDefinitionsDiskStorage + factory (disk-only default).
3) Implement CustomVariablesManager (global) and Context integration.
4) Load definitions at startup and reload on demand (loadObjects/reloadObjects wiring).
5) Add AccessType entries (CREATE_VARIABLE, DROP_VARIABLE, GET_VARIABLE, SHOW_CUSTOM_VARIABLES, SYSTEM_CUSTOM_VARIABLES).
6) Add interpreters for CREATE/DROP VARIABLE and register them.
7) Implement expression evaluation + MAX_CUSTOM_VARIABLE_SIZE checks (persist DDL with CAST to declared_type).
8) Implement getVariable/getVariableOrDefault functions.
9) Add system.custom_variables with access checks.
10) Add tests for parser, access, functions, and system table.

Phase 1a: session variables
1) Add session manager wiring and lifetime cleanup.
2) Allow CREATE/DROP VARIABLE session.* (no DDL storage, no ON CLUSTER).
3) Ensure getVariable uses session manager and errors without session context.
4) Add tests for session isolation and cleanup.

Phase 2: refreshable local variables
1) Enable/validate previously parsed ASTRefreshStrategy (reuse MV grammar; disallow DEPENDS ON).
2) Add CustomVariableRefreshTask adapter that uses RefreshTask/RefreshSet directly.
3) If RefreshTask is too MV-specific, extract the scheduling/backoff core into a reusable helper and wrap it from both MV and variables.
4) Implement retry/backoff and error tracking (keep last-good value).
5) Add SYSTEM REFRESH VARIABLE(S).
6) Add tests for refresh success/failure/backoff and SYSTEM REFRESH.

Phase 3: local_persistent
1) Add value serialization/deserialization (FieldBinaryEncoding + metadata).
2) Persist values on update; load values on startup; schedule refresh if stale.
3) Enforce non-constant expression rule.
4) Add ON CLUSTER support for definitions.
5) Add tests for persistence roundtrip and stale reload behavior.

Phase 4: cluster variables
1) Define ZooKeeper state blob format and update logic (single znode + lock).
2) Add ZooKeeper value storage and watcher updates.
3) Use RefreshTask adapter with ZooKeeper lock coordination.
4) Add ON CLUSTER for definitions (definitions on disk on all nodes).
5) Add tests for ZooKeeper coordination and watcher-driven updates.
