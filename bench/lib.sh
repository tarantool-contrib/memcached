# Helpers shared by bench/run.sh and bench/profile.sh. Source it.
# shellcheck shell=bash

BENCH=${BENCH:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

MCC_SRC=${MCC_SRC:-$(cd "$BENCH/.." && pwd)}
# A directory with memcached/init.lua and memcached/internal.so (an in-source
# build of tarantool/memcached).
CMOD_SRC=${CMOD_SRC:-}
SERVER_CPUS=${SERVER_CPUS:-1}
# memcached -t 4 gets more than one core, see README.md.
SERVER_CPUS_MT=${SERVER_CPUS_MT:-0-1}
CLIENT_CPUS=${CLIENT_CPUS:-2-4}
PORT=${PORT:-21211}
CTRL_PORT=${CTRL_PORT:-21299}
THREADS=${THREADS:-4}
CLIENTS=${CLIENTS:-25}
KEYS=${KEYS:-10000}

MCC_IMAGE=${MCC_IMAGE:-mcc-dev:3.9}
CMOD_IMAGE=${CMOD_IMAGE:-mc-dev:3.9}
MEMTIER_IMAGE=${MEMTIER_IMAGE:-mcc-bench-memtier:1.3.0}
MEMCACHED_IMAGE=${MEMCACHED_IMAGE:-mcc-bench-memcached:apt}

SRV=${SRV:-mccb-server}
CLI=${CLI:-mccb-client}
CLK_TCK=$(getconf CLK_TCK)

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# Workload definitions: memtier arguments, prefill value size.
workload_args() {
    case $1 in
    w1) echo "--ratio=1:10 --data-size=100 --pipeline=1" ;;
    w2) echo "--ratio=1:10 --data-size=100 --pipeline=16" ;;
    w3) echo "--ratio=1:10 --data-size=4096 --pipeline=1" ;;
    w4) echo "--ratio=0:1 --data-size=100 --pipeline=1 --key-prefix=miss-" ;;
    w5) echo "--ratio=1:10 --data-size=100 --pipeline=1 --multi-key-get=10" ;;
    w6) echo "--ratio=1:0 --data-size=100 --pipeline=16" ;;
    *) echo "unknown workload $1" >&2; exit 1 ;;
    esac
}

workload_prefill_size() {
    case $1 in
    w3) echo 4096 ;;
    *) echo 100 ;;
    esac
}

stop_server() {
    docker rm -f "$SRV" >/dev/null 2>&1 || true
}

# start_server <server> <wal_mode>; OUT must be set (mounted at /out).
start_server() {
    local server=$1 wal=$2
    stop_server
    local common=(-d --name "$SRV" --network host --user "$(id -u):$(id -g)"
                  -e HOME=/tmp)
    case $server in
    mcc|mcc-submit|mcc-raw|mcc-fake|cmod)
        local src image
        if [ "$server" != cmod ]; then
            src=$MCC_SRC image=$MCC_IMAGE
        else
            [ -n "$CMOD_SRC" ] || { echo "CMOD_SRC is not set" >&2; exit 1; }
            src=$CMOD_SRC image=$CMOD_IMAGE
        fi
        docker run "${common[@]}" --cpuset-cpus "$SERVER_CPUS" \
            --mount type=tmpfs,destination=/work,tmpfs-mode=1777 \
            -v "$src":/src:ro -v "$BENCH":/bench:ro -v "$OUT":/out \
            -e BENCH_IMPL="$server" -e BENCH_PORT="$PORT" \
            -e BENCH_CTRL_PORT="$CTRL_PORT" -e BENCH_WAL_MODE="$wal" \
            -e BENCH_GC_PAUSE="${BENCH_GC_PAUSE:-}" \
            -e BENCH_GC_STEPMUL="${BENCH_GC_STEPMUL:-}" \
            -e BENCH_JITV="${BENCH_JITV:-}" \
            -e BENCH_MCC_OPTS="${BENCH_MCC_OPTS:-}" \
            -e BENCH_APP_THREADS="${BENCH_APP_THREADS:-}" \
            -e MEMCACHED_DATAPATH="${MEMCACHED_DATAPATH:-}" \
            --entrypoint "" "$image" \
            sh -c 'unset TT_APP_NAME TT_INSTANCE_NAME;
                   exec tarantool /bench/servers/server.lua' >/dev/null
        ;;
    memcached1)
        docker run "${common[@]}" --cpuset-cpus "$SERVER_CPUS" \
            --entrypoint "" "$MEMCACHED_IMAGE" \
            memcached -p "$PORT" -U 0 -t 1 -m 1024 -c 4096 >/dev/null
        ;;
    memcached4)
        docker run "${common[@]}" --cpuset-cpus "$SERVER_CPUS_MT" \
            --entrypoint "" "$MEMCACHED_IMAGE" \
            memcached -p "$PORT" -U 0 -t 4 -m 1024 -c 4096 >/dev/null
        ;;
    *) echo "unknown server $server" >&2; exit 1 ;;
    esac
    local i
    for i in $(seq 100); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$PORT" &&
            printf 'version\r\n' >&3 && head -c 7 <&3 | grep -q VERSION) \
            2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    docker logs "$SRV" >&2 || true
    echo "server $server did not start" >&2
    exit 1
}

# ctrl <command...>: a control port request to a Tarantool server.
ctrl() {
    (exec 3<>"/dev/tcp/127.0.0.1/$CTRL_PORT" && echo "$*" >&3 && cat <&3)
}

# CPU seconds (user + system, all threads) of the server process.
server_cpu() {
    local pid
    pid=$(docker inspect -f '{{.State.Pid}}' "$SRV")
    awk -v tck="$CLK_TCK" '{
        sub(/^.*\) /, ""); print ($12 + $13) / tck }' "/proc/$pid/stat"
}

# Read and write syscalls so far of the server process: "syscr syscw" from
# /proc/<pid>/io (the servers run with our uid, so it is readable).
server_io() {
    local pid
    pid=$(docker inspect -f '{{.State.Pid}}' "$SRV")
    awk '/^syscr/ {r = $2} /^syscw/ {w = $2} END {print r, w}' \
        "/proc/$pid/io"
}

# memtier <json out or ""> <args...>; the last line of the output is bash
# `times` of the client (user and system CPU of memtier).
memtier() {
    local json=$1; shift
    local jarg=()
    [ -n "$json" ] && jarg=(--json-out-file="/out/${json#"$OUT"/}")
    docker run --rm --name "$CLI" --network host --cpuset-cpus "$CLIENT_CPUS" \
        --user "$(id -u):$(id -g)" -v "$OUT":/out --entrypoint bash \
        "$MEMTIER_IMAGE" -c 'memtier_benchmark "$@" >/tmp/log 2>&1;
                              rc=$?; cat /tmp/log; times; exit $rc' _ \
        -s 127.0.0.1 -p "$PORT" -P memcache_text \
        --key-minimum=1 --key-maximum="$KEYS" --hide-histogram \
        "${jarg[@]}" "$@"
}

# prefill <value size>: store every key once.
prefill() {
    memtier "" -t "$THREADS" -c "$CLIENTS" --pipeline=16 --ratio=1:0 \
        --data-size="$1" --key-pattern=P:P --requests=allkeys >/dev/null
}

# workload <json out or ""> <workload> <seconds>
workload() {
    local json=$1 wl=$2 secs=$3 args
    args=$(workload_args "$wl")
    # shellcheck disable=SC2086
    memtier "$json" -t "$THREADS" -c "$CLIENTS" --key-pattern=R:R \
        --distinct-client-seed --test-time="$secs" $args
}
