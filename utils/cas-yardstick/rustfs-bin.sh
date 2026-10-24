#!/bin/bash
# RustFS binary (no container) for the cost rig.
# ensure exit codes: 0 = started by this call (writes .started-by-rig), 3 = already up (not ours), other = failure.
# usage: rustfs-bin.sh start [--data DIR] [--bin PATH] | stop [--data DIR] [--force] | status [--data DIR] | ensure [--data DIR] [--bin PATH]
set -euo pipefail

HERE="$(realpath "$(dirname "$0")")"
REPO="$(realpath "$HERE/../..")"
BIN="$REPO/ci/tmp/rustfs"
DATA="$REPO/tmp/cas-yardstick/rustfs-bin-data"
PORT=19121
BUCKET=yardstick-cost
MARKER=.cas-yardstick-rustfs
OWNED=.started-by-rig
PIDFILE=.rustfs.pid
READY_TIMEOUT=60

up()
{
    # An unauthenticated request is answered with 403 once the S3 endpoint is up.
    [ "$(curl -s --max-time 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/" || true)" = 403 ]
}

pid_alive()
{
    local pid
    pid=$(cat "$DATA/$PIDFILE" 2>/dev/null) || return 1
    # The comm check guards against a recycled pid.
    [ "$(cat "/proc/$pid/comm" 2>/dev/null || true)" = rustfs ]
}

parse_args()
{
    FORCE=0
    while [ $# -gt 0 ]; do
        case "$1" in
            --data) DATA=$(realpath -m "$2"); shift 2 ;;
            --bin) BIN=$2; shift 2 ;;
            --force) FORCE=1; shift ;;
            *) echo "unknown argument: $1" >&2; exit 2 ;;
        esac
    done
}

do_start()
{
    if up || pid_alive; then
        echo "rustfs is already up on 127.0.0.1:$PORT" >&2
        exit 1
    fi
    [ -x "$BIN" ] || { echo "rustfs binary not found or not executable: $BIN" >&2; exit 1; }
    # Refuse to wipe a directory this script did not create.
    if [ -e "$DATA" ] && [ -n "$(ls -A "$DATA")" ] && [ ! -f "$DATA/$MARKER" ]; then
        echo "refusing to wipe non-empty $DATA without $MARKER" >&2
        exit 1
    fi
    rm -rf "$DATA"
    mkdir -p "$DATA/volume"
    touch "$DATA/$MARKER"
    RUSTFS_ADDRESS="127.0.0.1:$PORT" RUSTFS_ACCESS_KEY=clickhouse RUSTFS_SECRET_KEY=clickhouse \
        RUSTFS_SCANNER_ENABLED=false RUSTFS_HEAL_ENABLED=false RUSTFS_DURABILITY_MODE=relaxed \
        RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=64 RUSTFS_RUNTIME_THREAD_KEEP_ALIVE=5 \
        RUSTFS_ALLOCATOR_RECLAIM_ENABLED=true RUSTFS_ALLOCATOR_RECLAIM_INTERVAL_SECS=30 \
        RUSTFS_OBS_USE_STDOUT=true \
        setsid nohup "$BIN" server "$DATA/volume" > "$DATA/rustfs.log" 2>&1 < /dev/null &
    echo $! > "$DATA/$PIDFILE"
    local ready=0
    for _ in $(seq 1 "$READY_TIMEOUT"); do
        if ! pid_alive; then
            break
        fi
        if up && (cd "$HERE" && python3 -m yardstick.cost.s3client ensure-bucket "$BUCKET" --endpoint "http://127.0.0.1:$PORT") 2> /dev/null; then
            ready=1
            break
        fi
        sleep 1
    done
    if [ "$ready" != 1 ]; then
        echo "rustfs not ready after $READY_TIMEOUT s; log follows" >&2
        tail -20 "$DATA/rustfs.log" >&2
        kill "$(cat "$DATA/$PIDFILE")" 2> /dev/null || true
        exit 1
    fi
    echo "endpoint=http://127.0.0.1:$PORT bin=$BIN data=$DATA"
}

cmd=${1:-}
shift || true

case "$cmd" in
    start)
        parse_args "$@"
        do_start
        ;;
    ensure)
        parse_args "$@"
        if up; then
            (cd "$HERE" && python3 -m yardstick.cost.s3client ensure-bucket "$BUCKET" --endpoint "http://127.0.0.1:$PORT")
            echo "endpoint=http://127.0.0.1:$PORT (already up)"
            exit 3
        else
            do_start
            touch "$DATA/$OWNED"
        fi
        ;;
    stop)
        parse_args "$@"
        if ! pid_alive; then
            echo "rustfs is not running (data $DATA)" >&2
            exit 0
        fi
        if [ ! -f "$DATA/$OWNED" ] && [ "$FORCE" != 1 ]; then
            echo "not stopping rustfs: $DATA/$OWNED is missing (use --force)" >&2
            exit 0
        fi
        pid=$(cat "$DATA/$PIDFILE")
        kill "$pid"
        for _ in $(seq 1 30); do
            pid_alive || break
            sleep 1
        done
        if pid_alive; then
            echo "rustfs pid $pid ignored SIGTERM" >&2
            exit 1
        fi
        rm -f "$DATA/$PIDFILE" "$DATA/$OWNED"
        ;;
    status)
        parse_args "$@"
        if ! up; then
            echo "rustfs is not answering on 127.0.0.1:$PORT" >&2
            exit 1
        fi
        if pid_alive; then
            echo "up pid=$(cat "$DATA/$PIDFILE") data=$DATA"
        else
            echo "up, not started from $DATA"
        fi
        ;;
    *)
        echo "usage: $0 start [--data DIR] [--bin PATH] | stop [--data DIR] [--force] | status [--data DIR] | ensure [--data DIR] [--bin PATH]" >&2
        exit 2
        ;;
esac
