#!/usr/bin/env bash
# Hashing a GROUP BY key, ORDER BY key, filter atom or aggregate argument that is a column of a
# subquery walks the whole subquery unless the hash skips column sources. The subquery below has
# 32 columns of 60 nested calls each (about 7700 nodes), so a query hashes well over 50000 nodes
# if any container it uses hashes column sources, and a few thousand otherwise.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

columns=32
depth=60

wide="SELECT "
for ((i = 0; i < columns; i++)); do
    expr="x"
    for ((d = 0; d < depth; d++)); do
        expr="($expr + $((i * depth + d + 1)))"
    done
    wide+="$expr AS c$i, "
done
wide+="y, s FROM t"

# column lists: plain, prefixed, wrapped in a function
list() { local sep="" out="" i; for ((i = 0; i < columns; i++)); do out+="$sep${2:+$2(}$1c$i${2:+)}"; sep=", "; done; echo "$out"; }
all=$(list)
# 8 grouping sets of 16 columns, shifted by 4 so that every column is in some set
sets="GROUPING SETS ("
for ((s = 0; s < 8; s++)); do
    [ $s -gt 0 ] && sets+=", "
    sets+="("
    for ((k = 0; k < 16; k++)); do [ $k -gt 0 ] && sets+=", "; sets+="c$(( (s * 4 + k) % columns ))"; done
    sets+=")"
done
sets+=")"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x UInt64, y UInt64, s String) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO t VALUES (1, 2, 'a')"

# run <name> <query> [client settings...]
run() { $CLICKHOUSE_CLIENT --enable_analyzer 1 --log_comment "hash_work_$1" --format Null "${@:3}" -q "$2"; }

run grouping_function "SELECT grouping(c0), grouping(c1, c2), sum(y) FROM ($wide) GROUP BY $sets"
run aggregate_of_key "SELECT $(list '' min), sum(y) FROM ($wide) GROUP BY $sets"
run truncate_order_by "SELECT $all, sum(y) FROM ($wide) GROUP BY $all ORDER BY $all, sum(y)"
order_by=""; for ((i = 0; i < columns; i++)); do order_by+="${order_by:+, }c$i, toString(c$i)"; done
run redundant_order_by "SELECT $all FROM ($wide) ORDER BY $order_by"
run duplicate_order_by "SELECT $all FROM ($wide) ORDER BY $all, c0 LIMIT 1 BY $all, c0"
keys=""; for ((i = 0; i < columns; i += 2)); do keys+="${keys:+, }c$i + c$((i + 1)), c$i, c$((i + 1))"; done
run group_by_function_keys "SELECT sum(y) FROM ($wide) GROUP BY $keys"
run limit_by_injective "SELECT $all, sum(y) FROM ($wide) GROUP BY $all LIMIT 1 BY $(list '' toString)"
where=""; for ((i = 0; i < columns; i++)); do where+="${where:+ AND }(c$i = 1 OR c$i = 2 OR c$i = 3)"; done
run or_chain_to_in "SELECT sum(y) FROM ($wide) WHERE $where"
where=""; for ((i = 0; i < columns; i++)); do where+="${where:+ OR }toString(c$i) LIKE '%a%' OR toString(c$i) LIKE '%b%' OR toString(c$i) LIKE '%c%'"; done
run like_chain "SELECT sum(y) FROM ($wide) WHERE $where" --optimize_or_like_chain_min_patterns 2 --optimize_or_like_chain_min_substrings 2
aggregates=""; for ((i = 0; i < columns; i++)); do aggregates+="${aggregates:+, }sum(c$i), count(c$i), avg(c$i)"; done
run fuse_functions "SELECT $aggregates FROM ($wide)" --optimize_syntax_fuse_functions 1
run nullable_group_by_keys "SELECT $all, sum(y) FROM ($wide) GROUP BY $sets" --group_by_use_nulls 1
run join_use_nulls "SELECT $(list 'r.') FROM (SELECT x AS k FROM t) AS l LEFT JOIN ($wide) AS r ON l.k = r.y" --join_use_nulls 1
run interpolate "SELECT $all, y FROM ($wide) ORDER BY y WITH FILL INTERPOLATE (c0 AS c0 + 1)"
outer=""; for ((i = 0; i < columns; i++)); do outer+="${outer:+ + }o.c$i"; done
run correlated_columns "SELECT c0 FROM ($wide) AS o WHERE EXISTS (SELECT 1 FROM t WHERE t.x = $outer)" --allow_correlated_subqueries 1

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT replaceOne(log_comment, 'hash_work_', '') AS test,
        max(ProfileEvents['QueryTreeHashNodes']) > 0 AS hashed,
        max(ProfileEvents['QueryTreeHashNodes']) < 50000 AS bounded
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'hash_work_%'
    GROUP BY log_comment ORDER BY log_comment"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
