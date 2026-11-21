#!/usr/bin/env bash
# T9 re-validation poller: capture S3-counter time-series + system.parts responsiveness on both
# replicas every 60s, plus docker-stats CPU, until the containers go away (soak teardown). Robust to
# the run_24h.sh auto-teardown at the duration boundary — it just stops when curl starts failing.
set -uo pipefail
cd "$(dirname "$0")/.."
OUT="logs/revalidate_poll_$(date +%Y%m%dT%H%M%S).tsv"
EVENTS="S3HeadObject,S3ReadRequestsErrors,S3GetObject,S3PutObject,S3WriteRequestsThrottling,S3ReadRequestsThrottling"

echo -e "ts\tnode\tparts_active\tparts_query_s\tS3HeadObject\tS3ReadRequestsErrors\tS3GetObject\tS3PutObject\tS3WriteThrottle\tS3ReadThrottle\tcpu_ch1\tcpu_ch2\tcpu_rustfs" > "$OUT"
echo "polling -> $OUT"

# Wait up to 120s for at least one node to answer.
for i in $(seq 1 120); do
  curl -sf "http://localhost:8123/ping" >/dev/null 2>&1 && break
  sleep 1
done

miss=0
while : ; do
  ts="$(date +%H:%M:%S)"
  # docker stats once per iteration (no-stream)
  stats="$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' 2>/dev/null)"
  cpu_ch1="$(echo "$stats" | awk '/ch1/{print $2}' | head -1)"
  cpu_ch2="$(echo "$stats" | awk '/ch2/{print $2}' | head -1)"
  cpu_rustfs="$(echo "$stats" | awk '/rustfs/{print $2}' | head -1)"

  any=0
  for hp in 8123 8124; do
    node="ch_${hp}"
    # time a system.parts count query (responsiveness probe)
    t0="$(date +%s.%N)"
    parts="$(curl -s --max-time 60 "http://localhost:${hp}/?query=SELECT%20count()%20FROM%20system.parts%20WHERE%20active%20FORMAT%20TabSeparated" 2>/dev/null)"
    t1="$(date +%s.%N)"
    pq="$(awk "BEGIN{printf \"%.2f\", $t1-$t0}")"
    if [ -n "$parts" ] && [[ "$parts" =~ ^[0-9]+$ ]]; then any=1; else parts="NA"; fi
    # S3 counters
    ev="$(curl -s --max-time 30 "http://localhost:${hp}/?query=SELECT%20event,value%20FROM%20system.events%20WHERE%20event%20IN%20('S3HeadObject','S3ReadRequestsErrors','S3GetObject','S3PutObject','S3WriteRequestsThrottling','S3ReadRequestsThrottling')%20FORMAT%20TabSeparated" 2>/dev/null)"
    g(){ echo "$ev" | awk -v k="$1" '$1==k{print $2}' | head -1; }
    h="$(g S3HeadObject)"; re="$(g S3ReadRequestsErrors)"; ge="$(g S3GetObject)"; pu="$(g S3PutObject)"; wt="$(g S3WriteRequestsThrottling)"; rt="$(g S3ReadRequestsThrottling)"
    echo -e "${ts}\t${node}\t${parts}\t${pq}\t${h:-NA}\t${re:-NA}\t${ge:-NA}\t${pu:-NA}\t${wt:-NA}\t${rt:-NA}\t${cpu_ch1:-NA}\t${cpu_ch2:-NA}\t${cpu_rustfs:-NA}" >> "$OUT"
  done

  if [ "$any" -eq 0 ]; then
    miss=$((miss+1))
    # 3 consecutive all-node misses => containers gone (teardown) => stop.
    if [ "$miss" -ge 3 ]; then echo "nodes unreachable x3 — stopping poller"; break; fi
  else
    miss=0
  fi
  sleep 60
done
echo "poller done -> $OUT"
