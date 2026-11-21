#!/usr/bin/env bash
# RCA instrumentation for the S08 timeout finding: the scenario's own insert-creation loop is
# sequential, single-threaded Python with NO per-iteration logging, so silence in its own log
# proves nothing about whether it's hung or just slow. Poll actual server-side state (part count
# + any currently-executing query) every ~15s so we get direct, timestamped evidence of real
# progress (or its absence) during the run.
set -u
OUT="${1:-utils/ca-soak/logs/s08_sidecar.log}"
mkdir -p "$(dirname "$OUT")"
: > "$OUT"
echo "sidecar started $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUT"
while true; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  for c in ca-soak-ch1-1 ca-soak-ch2-1; do
    echo "=== $ts $c ===" >> "$OUT"
    docker exec "$c" clickhouse-client -q \
      "SELECT count() AS parts, sum(rows) AS rows FROM system.parts WHERE table='s08_manyparts' AND active FORMAT TSVWithNames" \
      >> "$OUT" 2>>"$OUT"
    docker exec "$c" clickhouse-client -q \
      "SELECT query_id, elapsed, left(query,120) FROM system.processes WHERE query LIKE '%s08_manyparts%' ORDER BY elapsed DESC LIMIT 3 FORMAT TSVWithNames" \
      >> "$OUT" 2>>"$OUT"
  done
  sleep 15
done
