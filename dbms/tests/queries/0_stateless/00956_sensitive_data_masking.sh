#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

#CLICKHOUSE_CLIENT='../../../../cmake-build-debug/dbms/programs/clickhouse client'


# normal execution
$CLICKHOUSE_CLIENT \
  --query="SET send_logs_level='trace'; SELECT 'TOPSECRET=TOPSECRET' FROM numbers(1) FORMAT Null" \
  --log_queries=1 --ignore-error --multiquery 2>&1 | grep TOPSECRET

# failure at parsing stage
echo "SELECT 'TOPSECRET=TOPSECRET' FRRRROM numbers" | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL} -d @- | grep TOPSECRET


# failure at before query start
$CLICKHOUSE_CLIENT \
  --query="SET send_logs_level='trace';SELECT 'TOPSECRET=TOPSECRET' FROM non_existing_table FORMAT Null" \
  --log_queries=1 --ignore-error --multiquery   2>&1 | grep TOPSECRET

# failure at the end of query
$CLICKHOUSE_CLIENT \
  --query="SET send_logs_level='trace'; SELECT 'TOPSECRET=TOPSECRET', intDiv( 100, number - 10) FROM numbers(11) FORMAT Null" \
  --log_queries=1 --ignore-error --max_block_size=2 --multiquery 2>&1 | grep TOPSECRET

# run in background
bash -c "$CLICKHOUSE_CLIENT \
  --query=\"SET send_logs_level='trace'; select sleepEachRow(0.5) from numbers(4) where ignore('TOPSECRET=TOPSECRET')=0 and ignore('fwerkh_that_magic_string_make_me_unique') = 0 FORMAT Null\" \
  --log_queries=1 --ignore-error --multiquery   2>&1 | grep TOPSECRET" &

sleep 0.1

# $CLICKHOUSE_CLIENT --query='SHOW PROCESSLIST'

# check that executing query doesn't expose secrets in processlist
$CLICKHOUSE_CLIENT \
  --query="SHOW PROCESSLIST" \
  --log_queries=0 2>&1 | grep fwerkh_that_magic_string_make_me_unique | grep TOPSECRET

wait

$CLICKHOUSE_CLIENT \
  --query="system flush logs"

# check events count properly increments
$CLICKHOUSE_CLIENT \
  --query="select * from (select sum(value) as matches from system.events where event='QueryMaskingRulesMatch') where matches < 5"

# and finally querylog
$CLICKHOUSE_CLIENT \
  --query="select * from system.query_log where event_time>now() - 10 and query like '%TOPSECRET%';"

