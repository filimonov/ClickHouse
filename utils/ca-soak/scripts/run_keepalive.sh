#!/usr/bin/env bash
# Instrumented reproduction runner: same as run_24h.sh but DOES NOT tear the cluster down on exit,
# so a checkpoint failure (e.g. dangling>0) leaves the live cluster up for forensic investigation.
# Pair with the CAGCDEL audit log (INFO) to attribute missing blobs to GC-delete vs object-store loss.
set -uo pipefail
cd "$(dirname "$0")/.."

SEED="${SEED:-20260617}"
DURATION="${DURATION:-3h}"
WORKERS="${WORKERS:-2}"
METRICS="${METRICS:-soak.db}"
MAX_POOL_GB="${MAX_POOL_GB:-40}"

LOGDIR="$(pwd)/logs"
mkdir -p "$LOGDIR"
RUN_TS="$(date +%Y%m%dT%H%M%S)"
COMPOSE_LOG="$LOGDIR/keepalive_${RUN_TS}_server.log"

rm -rf "$LOGDIR/ch1" "$LOGDIR/ch2"
mkdir -p "$LOGDIR/ch1" "$LOGDIR/ch2"
chmod 777 "$LOGDIR/ch1" "$LOGDIR/ch2"

docker compose down -v >/dev/null 2>&1; docker compose up -d
# KEEP-ALIVE: preserve docker logs on exit but DO NOT `docker compose down -v` — leave the cluster up.
trap 'docker compose logs --no-color > "$COMPOSE_LOG" 2>&1 || true; echo "preserved docker logs -> $COMPOSE_LOG (cluster LEFT UP for investigation)"' EXIT

for url in http://localhost:8123 http://localhost:8124; do
  for i in $(seq 1 90); do curl -sf "$url/ping" >/dev/null 2>&1 && break; sleep 1; done
done

PYTHONPATH="$(pwd)" python3 -m soak.run \
  --seed "$SEED" --phase 3 --duration "$DURATION" --workers "$WORKERS" \
  --metrics "$METRICS" --max-pool-gb "$MAX_POOL_GB" ${NO_CHAOS:+--no-chaos}
rc=$?
echo "RUNPY_EXIT=$rc (cluster LEFT UP)"
exit "$rc"
