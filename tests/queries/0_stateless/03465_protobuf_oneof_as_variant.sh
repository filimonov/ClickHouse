#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE file('nonexist', 'Protobuf') SETTINGS format_schema='${SCHEMADIR}/03465_protobuf_oneof:MyMessage', input_format_protobuf_oneof_as_variant=1, allow_experimental_variant_type=1"
