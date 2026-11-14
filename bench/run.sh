#!/usr/bin/env bash
# memtier_benchmark runs against tarantool-contrib/memcached, tarantool/memcached (the
# C module) and real memcached. See bench/README.md.
#
# Every server runs in its own container pinned to SERVER_CPUS, the client
# in another one pinned to CLIENT_CPUS, both on the host network. Each
# (wal mode, server, workload, repetition) gets a fresh server: start,
# prefill, warm up, measure.
set -euo pipefail

BENCH=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=bench/lib.sh
. "$BENCH/lib.sh"

OUT=${OUT:-/tmp/mcc-bench/results/$(date +%Y%m%d-%H%M%S)}
SERVERS=${SERVERS:-mcc cmod memcached1 memcached4}
WORKLOADS=${WORKLOADS:-w1 w2 w3 w4 w5 w6}
WAL_MODES=${WAL_MODES:-none}
REPS=${REPS:-3}
# The first repetition number: REP_FIRST=2 adds repetitions 2..REPS to an
# existing OUT.
REP_FIRST=${REP_FIRST:-1}
DURATION=${DURATION:-20}
WARMUP=${WARMUP:-5}

run_one() {
    local wal=$1 server=$2 wl=$3 rep=$4
    local dir=$OUT/$wal/$server/$wl
    mkdir -p "$dir"
    log "wal=$wal server=$server workload=$wl rep=$rep"
    start_server "$server" "$wal"
    prefill "$(workload_prefill_size "$wl")"
    workload "" "$wl" "$WARMUP" >/dev/null
    local tarantool=false
    case $server in mcc*|cmod) tarantool=true ;; esac
    $tarantool && ctrl metrics >"$dir/rep$rep.before.json"
    local cpu0 cpu1 io0 io1
    cpu0=$(server_cpu)
    io0=$(server_io)
    cat /proc/loadavg >"$dir/rep$rep.loadavg"
    workload "$dir/rep$rep.json" "$wl" "$DURATION" >"$dir/rep$rep.log"
    cpu1=$(server_cpu)
    io1=$(server_io)
    cat /proc/loadavg >>"$dir/rep$rep.loadavg"
    $tarantool && ctrl metrics >"$dir/rep$rep.after.json"
    echo "$cpu0 $cpu1" >"$dir/rep$rep.cpu"
    echo "$io0 $io1" >"$dir/rep$rep.io"
    docker logs "$SRV" >"$dir/rep$rep.server.log" 2>&1 || true
    stop_server
}

main() {
    mkdir -p "$OUT"
    [ "$REP_FIRST" = 1 ] || OUT_ENV_SUFFIX=.rep$REP_FIRST
    {
        echo "date: $(date -Is)"
        echo "host: $(nproc) CPUs, $(grep -m1 'model name' /proc/cpuinfo |
                                       cut -d: -f2)"
        echo "kernel: $(uname -r)"
        echo "mcc_src: $MCC_SRC"
        echo "cmod_src: $CMOD_SRC"
        echo "servers: $SERVERS"
        echo "workloads: $WORKLOADS"
        echo "wal_modes: $WAL_MODES"
        echo "reps: $REPS duration: $DURATION warmup: $WARMUP"
        echo "server_cpus: $SERVER_CPUS server_cpus_mt: $SERVER_CPUS_MT"
        echo "client_cpus: $CLIENT_CPUS threads: $THREADS clients: $CLIENTS"
        echo "keys: $KEYS"
        echo "gc_pause: ${BENCH_GC_PAUSE:-default}"
        echo "mcc_opts: ${BENCH_MCC_OPTS:-}"
    } >"$OUT/env.txt${OUT_ENV_SUFFIX:-}"
    trap stop_server EXIT
    local rep wal server wl
    # Repetitions are the outer loop, so that a noisy period of the host
    # does not hit all the repetitions of one server.
    for rep in $(seq "$REP_FIRST" "$REPS"); do
        for wal in $WAL_MODES; do
            for server in $SERVERS; do
                case $server in
                memcached*) [ "$wal" = none ] || continue ;;
                esac
                for wl in $WORKLOADS; do
                    run_one "$wal" "$server" "$wl" "$rep"
                done
            done
        done
    done
    python3 -I "$BENCH/summarize.py" "$OUT" | tee "$OUT/summary.md"
}

main "$@"
