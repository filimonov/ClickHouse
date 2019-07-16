#!/usr/bin/env bash

# Get all server logs
# export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="trace"
#export CLICKHOUSE_BINARY='../../../../cmake-build-debug/dbms/programs/clickhouse'
#export CLICKHOUSE_CLIENT_BINARY=='../../../../cmake-build-debug/dbms/programs/clickhouse client'

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

cur_name=$(basename "${BASH_SOURCE[0]}")
tmp_file=${CLICKHOUSE_TMP}/$cur_name"_server.logs"

# normal execution
$CLICKHOUSE_CLIENT \
  --query="SELECT 'find_me_TOPSECRET=TOPSECRET' FROM numbers(1) FORMAT Null" \
  --send_logs_level=trace --log_queries=1 --ignore-error --multiquery >$tmp_file 2>&1

grep 'find_me_[hidden]' $tmp_file >/dev/null || echo 'fail 1a'
grep 'TOPSECRET=TOPSECRET' $tmp_file && echo 'fail 1b'

# failure at parsing stage
echo "SELECT 'find_me_TOPSECRET=TOPSECRET' FRRRROM numbers" | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL} -d @- >$tmp_file 2>&1

grep 'find_me_[hidden]' $tmp_file >/dev/null || echo 'fail 2a'
grep 'TOPSECRET=TOPSECRET' $tmp_file && echo 'fail 2b'

# failure at before query start
$CLICKHOUSE_CLIENT \
  --query="SELECT 'find_me_TOPSECRET=TOPSECRET' FROM non_existing_table FORMAT Null" \
  --log_queries=1 --send_logs_level=trace --ignore-error --multiquery >$tmp_file 2>&1

grep 'find_me_[hidden]' $tmp_file >/dev/null || echo 'fail 3a'
grep 'TOPSECRET=TOPSECRET' $tmp_file && echo 'fail 3b'

# failure at the end of query
$CLICKHOUSE_CLIENT \
  --query="SELECT 'find_me_TOPSECRET=TOPSECRET', intDiv( 100, number - 10) FROM numbers(11) FORMAT Null" \
  --log_queries=1 --ignore-error --send_logs_level=trace --max_block_size=2 --multiquery >$tmp_file 2>&1

grep 'find_me_[hidden]' $tmp_file >/dev/null || echo 'fail 4a'
grep 'TOPSECRET=TOPSECRET' $tmp_file && echo 'fail 4b'

# run in background
bash -c "$CLICKHOUSE_CLIENT \
  --query=\"select sleepEachRow(0.5) from numbers(4) where ignore('find_me_TOPSECRET=TOPSECRET')=0 and ignore('fwerkh_that_magic_string_make_me_unique') = 0 FORMAT Null\" \
  --log_queries=1 --ignore-error --send_logs_level=trace --multiquery 2>&1 | grep TOPSECRET" &

sleep 0.1

# $CLICKHOUSE_CLIENT --query='SHOW PROCESSLIST'

# check that executing query doesn't expose secrets in processlist
$CLICKHOUSE_CLIENT \
  --query="SHOW PROCESSLIST" \
  --log_queries=0 >$tmp_file 2>&1

grep 'fwerkh_that_magic_string_make_me_unique' $tmp_file >/dev/null || echo 'fail 5a'
grep 'fwerkh_that_magic_string_make_me_unique' | grep 'find_me_[hidden]' $tmp_file && echo 'fail 5b'
grep 'TOPSECRET' $tmp_file && echo 'fail 5c'

wait

$CLICKHOUSE_CLIENT \
  --query="system flush logs"

# check events count properly increments
$CLICKHOUSE_CLIENT \
  --query="select * from (select sum(value) as matches from system.events where event='QueryMaskingRulesMatch') where matches < 5"

# and finally querylog
$CLICKHOUSE_CLIENT \
  --send_logs_level=trace \
  --query="select * from system.query_log where event_time>now() - 10 and query like '%TOPSECRET%';"

$CLICKHOUSE_CLIENT \
   --query="drop table if exists sensetive; create table sensitive ( id UInt64, date Date, value1 String, value2 UInt64) Engine=MergeTree ORDER BY id PARTITION BY date;
insert into sensitive select number as id, toDate('2019-01-01') as date, 'abcd' as value1, rand() as valuer from numbers(10000);
insert into sensitive select number as id, toDate('2019-01-01') as date, 'find_me_TOPSECRET=TOPSECRET' as value1, rand() as valuer from numbers(10);
insert into sensitive select number as id, toDate('2019-01-01') as date, 'abcd' as value1, rand() as valuer from numbers(10000);
select * from sensitive WHERE value1 = 'find_me_TOPSECRET=TOPSECRET' FORMAT Null;
drop table sensetive;" --log_queries=1 --ignore-error --send_logs_level=trace --multiquery >$tmp_file 2>&1

grep 'find_me_[hidden]' $tmp_file >/dev/null || echo 'fail 4a'
grep 'TOPSECRET' $tmp_file && echo 'fail 4b'

