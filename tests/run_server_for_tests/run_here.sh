#!/usr/bin/env bash

CLICKHOUSE_DATADIR=${CLICKHOUSE_DATADIR:=$(pwd)/var/lib/clickhouse}
CLICKHOUSE_LOGDIR=${CLICKHOUSE_LOGDIR:=$(pwd)/var/log/clickhouse-server}
CLICKHOUSE_ORIGINAL_CONFDIR=${CLICKHOUSE_ORIGINAL_CONFDIR:=$(pwd)/etc/clickhouse-server}
CLICKHOUSE_CONFDIR=${CLICKHOUSE_CONFDIR:=$(pwd)/etc/clickhouse-server-run-here}
CLICKHOUSE_BINDIR=${CLICKHOUSE_BINDIR:=$(pwd)/usr/bin}

echo That script will copy clickhouse-server configs to $CLICKHOUSE_CONFDIR
echo $CLICKHOUSE_DATADIR will be used to store ClickHouse datacd
echo $CLICKHOUSE_LOGDIR will be used to store ClickHouse logs

rm -rf $CLICKHOUSE_CONFDIR # clean prev configs copy
echo cp -r $CLICKHOUSE_ORIGINAL_CONFDIR $CLICKHOUSE_CONFDIR
cp -r $CLICKHOUSE_ORIGINAL_CONFDIR $CLICKHOUSE_CONFDIR

mkdir -p $CLICKHOUSE_CONFDIR/config.d

echo "<?xml version=\"1.0\"?>
<yandex>
    <logger>
        <log replace=\"replace\">${CLICKHOUSE_LOGDIR}/clickhouse-server.log</log>
        <errorlog replace=\"replace\">${CLICKHOUSE_LOGDIR}/clickhouse-server.err.log</errorlog>
    </logger>
    <path replace=\"replace\">${CLICKHOUSE_DATADIR}/</path>
    <tmp_path replace=\"replace\">${CLICKHOUSE_DATADIR}/tmp/</tmp_path>
    <user_files_path replace=\"replace\">${CLICKHOUSE_DATADIR}/user_files/</user_files_path>
    <format_schema_path replace=\"replace\">${CLICKHOUSE_DATADIR}/format_schemas/</format_schema_path>
    <access_control_path replace=\"replace\">${CLICKHOUSE_DATADIR}/access/</access_control_path>
    <openSSL>
        <server>
            <certificateFile replace=\"replace\">${CLICKHOUSE_CONFDIR}/server.crt</certificateFile>
            <privateKeyFile replace=\"replace\">${CLICKHOUSE_CONFDIR}/server.key</privateKeyFile>
            <dhParamsFile replace=\"replace\">${CLICKHOUSE_CONFDIR}/dhparam.pem</dhParamsFile>
        </server>
    </openSSL>

</yandex>
" > $CLICKHOUSE_CONFDIR/config.d/paths.xml

mkdir -p $CLICKHOUSE_DATADIR
mkdir -p $CLICKHOUSE_LOGDIR

if [ "$1" == "background" ]; then
    ${CLICKHOUSE_BINDIR}/clickhouse server --config=${CLICKHOUSE_CONFDIR}/config.xml >>${CLICKHOUSE_LOGDIR}/stdout 2>>${CLICKHOUSE_LOGDIR}/stderr &
    disown %-
else
    ${CLICKHOUSE_BINDIR}/clickhouse server --config=${CLICKHOUSE_CONFDIR}/config.xml
fi