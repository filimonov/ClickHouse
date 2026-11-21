#!/usr/bin/env bash
# Diagnostic sidecar for the S13 attempt-3 quiesce-wedge investigation.
# Polls system.processes + system.replication_queue on both replicas every ~20s
# so a wedge inside "quiescing cluster" leaves direct evidence instead of silence.
# Not part of the scenario framework itself -- an external overlay, safe to run
# against the stable ca-soak-ch1-1/ch2-1 container names regardless of compose variant.
set -u
OUT="${1:-utils/ca-soak/logs/s13_sidecar.log}"
mkdir -p "$(dirname "$OUT")"
: > "$OUT"
echo "sidecar started $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUT"
while true; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  for c in ca-soak-ch1-1 ca-soak-ch2-1; do
    echo "=== $ts $c processes ===" >> "$OUT"
    docker exec "$c" clickhouse-client -q \
      "SELECT query_id, elapsed, query_kind, is_cancelled, left(query,150) FROM system.processes ORDER BY elapsed DESC FORMAT TSVWithNames" \
      >> "$OUT" 2>>"$OUT"
    echo "=== $ts $c replication_queue ===" >> "$OUT"
    docker exec "$c" clickhouse-client -q \
      "SELECT database, table, type, num_tries, left(last_exception,150), is_currently_executing FROM system.replication_queue ORDER BY num_tries DESC FORMAT TSVWithNames" \
      >> "$OUT" 2>>"$OUT"
  done
  sleep 20
done
