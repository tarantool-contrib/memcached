#!/usr/bin/env bash
# Profile a Tarantool memcached server under one workload.
#
#   OUT=/tmp/prof CMOD_SRC=... bench/profile.sh
#
# Steps, each over a DURATION-second run of the workload WL (default w1) on
# a prefilled and warmed-up server:
#   1. no profiler: misc.getmetrics() and CPU before/after (GC counters);
#   2. misc.sysprof in the default mode: samples per VM state (GC, C
#      functions, compiled traces, interpreter, ...), 1 ms interval. The
#      callchain ('C') and leaf ('L') modes crash Tarantool 3.9.0 in
#      stream_guest() under this load, so they are not used;
#   3. jit.p (LuaJIT's profiler) in the modes of JITP_MODES: v = VM state,
#      F = function, l = line, vF = function per VM state; 1 ms interval.
#      jitp_categories.py groups the F profile into layers.
# SERVER=cmod runs steps 1 and 2 only.
set -euo pipefail

BENCH=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=bench/lib.sh
. "$BENCH/lib.sh"

OUT=${OUT:-/tmp/mcc-bench/profile/$(date +%Y%m%d-%H%M%S)}
SERVER=${SERVER:-mcc}
WL=${WL:-w1}
WAL=${WAL:-none}
DURATION=${DURATION:-20}
WARMUP=${WARMUP:-5}
JITP_MODES=${JITP_MODES:-vi1m0 Fi1m0 li1m0 vFi1m0}

main() {
    mkdir -p "$OUT/prof"
    trap stop_server EXIT
    log "profile: server=$SERVER workload=$WL wal=$WAL into $OUT"
    start_server "$SERVER" "$WAL"
    prefill "$(workload_prefill_size "$WL")"
    workload "" "$WL" "$WARMUP" >/dev/null

    log "plain run"
    local cpu0 cpu1
    ctrl metrics >"$OUT/plain.before.json"
    cpu0=$(server_cpu)
    workload "$OUT/plain.json" "$WL" "$DURATION" >"$OUT/plain.log"
    cpu1=$(server_cpu)
    ctrl metrics >"$OUT/plain.after.json"
    echo "$cpu0 $cpu1" >"$OUT/plain.cpu"

    log "sysprof"
    ctrl sysprof_start D >"$OUT/sysprof.start.json"
    workload "$OUT/sysprof.json" "$WL" "$DURATION" >"$OUT/sysprof.log"
    ctrl sysprof_stop >"$OUT/sysprof.report.json"

    if [ "$SERVER" != cmod ]; then
        local mode
        for mode in $JITP_MODES; do
            log "jit.p $mode"
            ctrl jitp_start "$mode" "/out/prof/jitp-$mode.txt" >/dev/null
            workload "$OUT/jitp-$mode.json" "$WL" "$DURATION" \
                >"$OUT/jitp-$mode.log"
            ctrl jitp_stop >/dev/null
        done
    fi
    docker logs "$SRV" >"$OUT/server.log" 2>&1 || true
    stop_server

    python3 -I "$BENCH/jitp_categories.py" "$OUT" "$MCC_SRC" | tee "$OUT/categories.md"
}

main "$@"
