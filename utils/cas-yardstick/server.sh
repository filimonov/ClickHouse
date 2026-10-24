#!/bin/bash
# One clickhouse-server process per run directory.
# usage: server.sh start <binary> <run_dir> <port_base> | stop <run_dir>
set -euo pipefail

START_TIMEOUT=${SERVER_START_TIMEOUT:-120}
STOP_TIMEOUT=${SERVER_STOP_TIMEOUT:-60}
TERM_GRACE=${SERVER_TERM_GRACE:-30}

cmd=${1:-}
shift || true

# Signal the server's whole process group (setsid made it its own): under TRACE_PREFIX `server.pid` is the
# tracer, and the server is its child, so a signal to the pid alone would leave the server running.
group_of() {
    ps -o pgid= -p "$1" 2>/dev/null | tr -d ' ' || true
}

# `server.comm` holds the comm of `server.pid` once the server answered; a different comm means the pid was reused.
recorded_comm() {
    cat "$1/server.comm" 2>/dev/null || true
}

live_comm() {
    cat "/proc/$1/comm" 2>/dev/null || true
}

# A group other than the caller's own; empty when the pid is gone or shares the caller's group.
own_group() {
    local pgid
    pgid=$(group_of "$1")
    if [ -n "$pgid" ] && [ "$pgid" != "$(group_of $$)" ]; then
        echo "$pgid"
    fi
}

kill_group() {
    local sig=$1 pid=$2 pgid
    pgid=$(own_group "$pid")
    if [ -n "$pgid" ]; then
        kill "-$sig" -- "-$pgid" 2>/dev/null || true
    else
        kill "-$sig" "$pid" 2>/dev/null || true
    fi
}

# The tracer exits on TERM while a server that ignores TERM lives on, so liveness is the group's, not the pid's.
# A killed orphan stays a zombie until its new parent reaps it; a zombie is not alive.
group_alive() {
    local pgid=$1 pid=$2
    if [ -n "$pgid" ]; then
        ps -e -o pgid=,stat= | awk -v g="$pgid" '$1 == g && $2 !~ /^Z/ { found = 1 } END { exit !found }'
    else
        ps -o stat= -p "$pid" 2>/dev/null | grep -qv '^Z'
    fi
}

case "$cmd" in
    start)
        [ $# -eq 3 ] || { echo "usage: $0 start <binary> <run_dir> <port_base>" >&2; exit 2; }
        binary=$1; dir=$(realpath "$2"); port=$3
        [ -f "$dir/etc/config.xml" ] || { echo "missing $dir/etc/config.xml; run write_config first" >&2; exit 1; }
        cd "$dir"
        # Otherwise a leftover server on the port would answer the readiness check for this one.
        if [ "$(curl -s --max-time 5 "http://127.0.0.1:$((port + 1))/?query=SELECT+1" || true)" = 1 ]; then
            echo "port $((port + 1)) already answers; stop the server that holds it" >&2
            exit 1
        fi
        # Cache disk paths resolve against custom_cached_disks_base_directory.
        # setsid: SYSTEM SHUTDOWN signals the server's whole process group, which must not contain the caller.
        # No watchdog: server.pid is then the server itself, and SYSTEM SHUTDOWN delivers one SIGTERM, not two.
        # TRACE_PREFIX (e.g. "strace -f -o 'a file'") is a command prefix in shell quoting, so a quoted path may hold
        # spaces. It is already a command the caller runs; eval parses its quotes and gives it no further power.
        eval "trace=(${TRACE_PREFIX:-})"
        CLICKHOUSE_WATCHDOG_ENABLE=0 setsid nohup "${trace[@]}" "$binary" server --config-file="$dir/etc/config.xml" -- \
            --path="$dir/data/" --tmp_path="$dir/tmp/" --user_files_path="$dir/uf/" \
            --logger.log="$dir/server.log" --logger.errorlog="$dir/server.err.log" \
            --tcp_port="$port" --http_port=$((port + 1)) --interserver_http_port=$((port + 2)) \
            --mysql_port=0 --postgresql_port=0 --prometheus.port=0 \
            --filesystem_caches_path="$dir/caches/" --custom_cached_disks_base_directory="$dir/caches/" \
            > "$dir/stdout.log" 2>&1 &
        echo $! > "$dir/server.pid"
        echo "$port" > "$dir/server.port"
        for i in $(seq 1 "$START_TIMEOUT"); do
            if ! kill -0 "$(cat "$dir/server.pid")" 2>/dev/null; then
                echo "server exited during start; see $dir/server.err.log" >&2
                exit 1
            fi
            if [ "$(curl -s --max-time 5 "http://127.0.0.1:$((port + 1))/?query=SELECT+1" || true)" = 1 ]; then
                live_comm "$(cat "$dir/server.pid")" > "$dir/server.comm"
                echo "up after $i s"
                exit 0
            fi
            sleep 1
        done
        echo "server not answering SELECT 1 after $START_TIMEOUT s; killing it; see $dir/server.err.log" >&2
        pid=$(cat "$dir/server.pid")
        pgid=$(own_group "$pid")
        kill_group TERM "$pid"
        for _ in $(seq 1 "$TERM_GRACE"); do
            group_alive "$pgid" "$pid" || exit 1
            sleep 1
        done
        if [ -n "$pgid" ]; then
            kill -KILL -- "-$pgid" 2>/dev/null || true
        else
            kill -KILL "$pid" 2>/dev/null || true
        fi
        for _ in $(seq 1 5); do
            group_alive "$pgid" "$pid" || exit 1
            sleep 1
        done
        echo "server group ${pgid:-$pid} survived SIGKILL" >&2
        exit 1
        ;;
    stop)
        [ $# -eq 1 ] || { echo "usage: $0 stop <run_dir>" >&2; exit 2; }
        dir=$(realpath "$1")
        [ -f "$dir/server.pid" ] || { echo "no $dir/server.pid" >&2; exit 1; }
        [ -f "$dir/server.port" ] || { echo "no $dir/server.port" >&2; exit 1; }
        port=$(cat "$dir/server.port")
        pid=$(cat "$dir/server.pid")
        comm=$(recorded_comm "$dir")
        if kill -0 "$pid" 2>/dev/null && [ -n "$comm" ] && [ "$(live_comm "$pid")" != "$comm" ]; then
            echo "pid $pid is now '$(live_comm "$pid")', not the server ('$comm'); the server is gone, not signalling it"
            exit 0
        fi
        if kill -0 "$pid" 2>/dev/null; then
            curl -s --max-time 10 "http://127.0.0.1:$((port + 1))/" --data-binary "SYSTEM SHUTDOWN" > /dev/null || true
            for _ in $(seq 1 "$STOP_TIMEOUT"); do
                kill -0 "$pid" 2>/dev/null || { echo "stopped"; exit 0; }
                sleep 1
            done
            if [ -z "$comm" ] || [ "$(live_comm "$pid")" != "$comm" ]; then
                echo "server $pid still alive after $STOP_TIMEOUT s, but its comm '$(live_comm "$pid")' does not match" \
                     "the recorded '${comm:-<no server.comm>}'; not sending SIGKILL" >&2
                exit 1
            fi
            echo "server $pid still alive after $STOP_TIMEOUT s; sending SIGKILL to its process group" >&2
            kill_group KILL "$pid"
            for _ in $(seq 1 5); do
                kill -0 "$pid" 2>/dev/null || exit 1
                sleep 1
            done
            echo "server $pid survived SIGKILL" >&2
            exit 1
        fi
        echo "already stopped"
        ;;
    *)
        echo "usage: $0 start <binary> <run_dir> <port_base> | stop <run_dir>" >&2
        exit 2
        ;;
esac
