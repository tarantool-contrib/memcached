# Benchmarks

memtier_benchmark runs of tarantool-contrib/memcached against tarantool/memcached
(the C module) and real memcached, plus a profile of tarantool-contrib/memcached and
microbenchmarks of its layers. They show where a C rewrite would pay off.

## Contents

| file | what |
|---|---|
| `run.sh` | the benchmark matrix: servers x workloads x wal modes x repetitions |
| `profile.sh` | GC counters, `misc.sysprof` VM states and `jit.p` profiles of one server under one workload |
| `micro.lua` | per-layer microbenchmarks without the network: parser, encoder, buffer copies, engine, bare box calls, the connection step; and the connection loops (C loop, Lua loop, Lua engine) over socket pairs with the clients in the same thread |
| `lib.sh` | helpers shared by `run.sh` and `profile.sh`: containers, pinning, memtier |
| `servers/server.lua` | the Tarantool server under test (`BENCH_IMPL=mcc` or `cmod`) with a control port |
| `summarize.py` | Markdown tables from a `run.sh` results directory |
| `jitp_categories.py` | groups a `jit.p` function profile into layers (parser, engine, I/O, ...) |
| `variants/make.py` | builds patched copies of a tree, one per Lua-level fix, for measuring them; never touches the source tree |
| `variants/run.sh` | measures every fix alone, all together and GC tuning against the unpatched tree |
| `variants/compare.py` | the table of `variants/run.sh`: ops/s and the change against the unpatched tree |
| `docker/memtier.Dockerfile` | memtier_benchmark built from source |
| `docker/memcached.Dockerfile` | memcached from the distribution packages |

## Images

- `mcc-dev:3.9` runs tarantool-contrib/memcached: Tarantool 3.9.0, see
  `docker/Dockerfile` at the repository root.
- `mc-dev:3.9` runs the C module: Tarantool 3.9.0 with libsasl2, which
  `memcached/internal.so` links to. It is the dev image of
  tarantool/memcached. Both images are built on `tarantool/tarantool:3.9.0`
  and contain the same `tarantool` binary.
- `mcc-bench-memtier:1.3.0` is memtier_benchmark, built on top of
  `mc-dev:3.9` (it has autotools, libevent and pcre):

  ```sh
  cp -a <tarantool/memcached>/third_party/memtier /tmp/memtier-src
  docker build -t mcc-bench-memtier:1.3.0 \
      -f bench/docker/memtier.Dockerfile /tmp/memtier-src
  ```

- `mcc-bench-memcached:apt` is memcached from Ubuntu 24.04 (1.6.24):

  ```sh
  docker build -t mcc-bench-memcached:apt \
      -f bench/docker/memcached.Dockerfile "$(mktemp -d)"
  ```

## Running

```sh
# The C module: a directory with memcached/init.lua and
# memcached/internal.so, i.e. an in-source build of tarantool/memcached.
export CMOD_SRC=/path/to/tarantool-memcached
# The tree to benchmark; default: this repository. Other people may be
# editing the working tree: benchmark a copy.
# It must be built: the C data path and version.lua come from tt.
export MCC_SRC=/path/to/snapshot
git clone --local . "$MCC_SRC"
"$MCC_SRC"/docker/run.sh 'tt package build'

bench/run.sh                                  # wal_mode = none, ~50 min
WAL_MODES="none write" bench/run.sh           # + wal_mode = write
SERVERS="mcc cmod" WORKLOADS=w1 REPS=1 DURATION=10 bench/run.sh   # quick
# commit_wait: complete (mcc) against submit (mcc-submit) on writes
SERVERS="mcc mcc-submit" WORKLOADS="w6 w2" WAL_MODES=write bench/run.sh
```

The results go to `OUT` (default `/tmp/mcc-bench/results/<date>`):
`<wal mode>/<server>/<workload>/rep<N>.*` (memtier JSON and log, server CPU,
load average, GC metrics before and after), and `summary.md` from
`summarize.py`.

Profiling and microbenchmarks:

```sh
bench/profile.sh                              # tarantool-contrib/memcached, w1
SERVER=cmod bench/profile.sh                  # the C module: no jit.p
WL=w2 bench/profile.sh

docker run --rm --cpuset-cpus 1 --entrypoint "" \
    -e 'LUA_CPATH=/src/.rocks/lib/tarantool/?.so;;' \
    -v "$MCC_SRC":/src:ro -v "$PWD/bench":/bench:ro mcc-dev:3.9 \
    sh -c 'unset TT_APP_NAME TT_INSTANCE_NAME; tarantool /bench/micro.lua'
# -e MICRO_EXPIRE=index: with the exptime index (expire.mode = 'index'; the
# default is 'scan'). For the servers: BENCH_MCC_OPTS='{"expire": {"mode": "index"}}'.

# Front-end vs box vs engine: tarantool-contrib/memcached's server.lua with a backend
# doing one bare box call per request (mcc-raw) or no box at all (mcc-fake).
SERVERS="mcc mcc-raw mcc-fake" WORKLOADS="w1 w2 w4" bench/run.sh

# JIT trace aborts (NYI etc.) during a warm-up:
(export OUT=/tmp/jitv BENCH_JITV=/out/jitv.txt; mkdir -p $OUT
 . bench/lib.sh; start_server mcc none; prefill 100
 workload "" w1 8 >/dev/null; stop_server)
grep -E 'abort|NYI|blacklist' /tmp/jitv/jitv.txt | sort | uniq -c | sort -rn

# Lua-level fixes: each alone, all of them, GC tuning (~20 min).
bench/variants/run.sh
python3 -I bench/variants/make.py      # lists the fixes
```

Settings (environment variables of `run.sh` and `profile.sh`, see `lib.sh`):

| variable | default | |
|---|---|---|
| `SERVERS` | `mcc cmod memcached1 memcached4` | memcached1/4 = memcached `-t 1` / `-t 4`; also `mcc-submit` (`mcc` with `commit_wait = 'submit'`), `mcc-raw`, `mcc-fake` (diagnostic, see `servers/server.lua`) |
| `WORKLOADS` | `w1 w2 w3 w4 w5 w6` | see below |
| `WAL_MODES` | `none` | `none write`; memcached runs only with `none` |
| `REPS`, `DURATION`, `WARMUP` | 3, 20, 5 | repetitions, seconds |
| `REP_FIRST` | 1 | first repetition: `REP_FIRST=2` adds repetitions to an existing `OUT` |
| `SERVER_CPUS` | `1` | the core of a server |
| `SERVER_CPUS_MT` | `0-1` | the cores of memcached `-t 4` |
| `CLIENT_CPUS` | `2-4` | the cores of memtier |
| `THREADS`, `CLIENTS`, `KEYS` | 4, 25, 10000 | memtier threads, connections per thread, key range |
| `PORT`, `CTRL_PORT` | 21211, 21299 | memcached and control ports on the host network |
| `BENCH_GC_PAUSE`, `BENCH_GC_STEPMUL` | unset | `collectgarbage('setpause'/'setstepmul')` in the Tarantool servers |
| `BENCH_JITV` | unset | a path under `/out`: `jit.v` log of the Tarantool server from its start |
| `BENCH_MCC_OPTS` | unset | more tarantool-contrib/memcached instance options, a JSON object: `'{"track_access": false}'` |
| `BENCH_APP_THREADS` | unset | `box.cfg.app_threads` of the Tarantool servers: front-ends in threads with `BENCH_MCC_OPTS='{"threads": "app"}'` and `SERVER_CPUS=0-1` |
| `MEMCACHED_DATAPATH` | unset | `lua`: the server runs with the C data path off (`memcached/datapath.c`). The tree in `MCC_SRC` needs `tt package build` for it to be on |

## Method

- Server and client run in separate containers on the host network. The
  server is pinned to one core (`--cpuset-cpus`), all of its threads (TX,
  WAL, iproto) included, so the numbers are per core. The client gets three
  other cores; its CPU use is recorded, so a client-bound run shows up.
- memcached `-t 4` gets two cores (`SERVER_CPUS_MT`): with five cores in the
  host and three for the client there is no more. It is a reference only.
- The Tarantool servers use memtx and `wal_mode = 'none'` (or `'write'`,
  on a tmpfs), the same `box.cfg` and the same Tarantool binary.
- Every run gets a fresh server, a prefill of all the keys (pipeline 16), a
  warm-up of `WARMUP` seconds, then the measured `DURATION` seconds.
  Repetitions are the outer loop, so noise on the host spreads over all the
  servers.
- memtier: `memcache_text` protocol, 4 threads x 25 connections, random
  keys (`R:R`) out of 10000, `--distinct-client-seed`.

| workload | memtier arguments |
|---|---|
| w1 | `--ratio=1:10 --data-size=100 --pipeline=1` |
| w2 | `--ratio=1:10 --data-size=100 --pipeline=16` |
| w3 | `--ratio=1:10 --data-size=4096 --pipeline=1` |
| w4 | `--ratio=0:1 --key-prefix=miss-` (every get misses) |
| w5 | `--ratio=1:10 --multi-key-get=10`: one set per one 10-key get |
| w6 | `--ratio=1:0 --data-size=100 --pipeline=16`: sets only, for the WAL (`WAL_MODES=write`) |

The tables report total ops/s (for w5 also keys/s), p50 and p99 latency of
all the requests, server and client CPU in cores, server CPU time per
request, read and write syscalls per request (`syscr` + `syscw` of
`/proc/<pid>/io`: `read`/`write`/`writev` count, `recvmsg`/`sendmsg` don't,
so memcached shows only its reads), and for the Tarantool servers the Lua
heap allocation per request (`misc.getmetrics().gc_allocated` delta), new
strings per request (`strhash_miss`) and full GC cycles (`gc_steps_atomic`)
during the run.

## Caveats

- Loopback TCP: the kernel work of a send is charged to the sender's core,
  so the server core also pays for part of the client's receive path. This
  is the same for all the servers.
- `misc.sysprof` callchain (`C`) and leaf (`L`) modes crash Tarantool 3.9.0
  under this load (`stream_guest()` in the signal handler), so only the
  default mode (samples per VM state) is used.
- `jit.p` attributes time spent in C (box calls, syscalls, the event loop)
  to the Lua function that runs next, i.e. the caller of the C function or
  the function that resumes after a yield. An FFI call made from compiled
  code (`ffi.C.write` in `socket.lua:syswrite`) is counted as compiled code,
  kernel time included. The two-level `vF` mode disagrees with the `v` mode
  on the VM state shares; trust `v` and sysprof, which agree.
- `variants/make.py` patches are prototypes for measuring: `nola` drops the
  meta `h`/`l` bookkeeping and `nobatch` ignores `flush_all`, so they fail
  `engine.test_get_item` and `engine.test_flush`; the other patches pass the
  unit and integration tests.
- Other processes on the host are not controlled; `rep<N>.loadavg` records
  the load average before and after each run.
